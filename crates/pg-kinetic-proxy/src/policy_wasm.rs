use std::{
    convert::TryFrom,
    fs,
    path::Path,
    time::{Duration, Instant},
};

use pg_kinetic_core::policy::{
    PolicyAction, PolicyDecision, PolicyHookPoint, PolicyId, PolicyMode, PolicyOutcome,
    PolicyPluginAbiVersion, PolicyPluginAction, PolicyPluginError, PolicyPluginInput,
    PolicyPluginOutput, PolicyVersion,
};
use wasmi::{Config, Engine, ExternType, Instance, Linker, Memory, Module, Store, TrapCode};

use crate::policy::{PolicyContextBuilder, PolicyEvalInput, PolicyPluginHostLimits};

const ABI_EXPORT: &str = "pg_kinetic_policy_abi_version";
const EVALUATE_EXPORT: &str = "pg_kinetic_policy_evaluate";
const MEMORY_EXPORT: &str = "memory";

#[derive(Clone, Debug)]
pub struct WasmPolicyEvaluator {
    engine: Engine,
    module: Module,
    host_limits: PolicyPluginHostLimits,
}

impl WasmPolicyEvaluator {
    pub fn validate_module_path(module_path: impl AsRef<Path>) -> Result<(), PolicyPluginError> {
        let module_path = module_path.as_ref();
        let module_bytes = fs::read(module_path).map_err(|error| {
            PolicyPluginError::output_validation_failed(format!(
                "failed to read wasm policy module {}: {error}",
                module_path.display()
            ))
        })?;
        Self::validate_module_bytes(&module_bytes)
    }

    pub fn validate_module_bytes(module_bytes: &[u8]) -> Result<(), PolicyPluginError> {
        let engine = configured_engine();
        let module = Module::new(&engine, module_bytes).map_err(map_wasm_module_error)?;
        validate_module_shape(&module)?;
        validate_module_abi(&engine, &module)?;
        Ok(())
    }

    pub fn load(
        module_path: impl AsRef<Path>,
        host_limits: PolicyPluginHostLimits,
    ) -> Result<Self, PolicyPluginError> {
        let module_path = module_path.as_ref();
        let module_bytes = fs::read(module_path).map_err(|error| {
            PolicyPluginError::output_validation_failed(format!(
                "failed to read wasm policy module {}: {error}",
                module_path.display()
            ))
        })?;
        Self::from_module_bytes(module_bytes, host_limits)
    }

    pub fn from_module_bytes(
        module_bytes: impl AsRef<[u8]>,
        host_limits: PolicyPluginHostLimits,
    ) -> Result<Self, PolicyPluginError> {
        let engine = configured_engine();
        let module = Module::new(&engine, module_bytes.as_ref()).map_err(map_wasm_module_error)?;
        validate_module_shape(&module)?;
        validate_module_abi(&engine, &module)?;

        Ok(Self {
            engine,
            module,
            host_limits,
        })
    }

    pub fn build_plugin_input(
        &self,
        policy_id: PolicyId,
        policy_version: PolicyVersion,
        hook_point: PolicyHookPoint,
        input: &PolicyEvalInput,
    ) -> Result<PolicyPluginInput, PolicyPluginError> {
        let context = PolicyContextBuilder::new(self.host_limits.max_input_bytes()).build(input);
        PolicyPluginInput::new(
            PolicyPluginAbiVersion::current().as_u16(),
            policy_id,
            policy_version,
            hook_point,
            context.context,
            false,
            false,
            false,
        )
    }

    pub fn evaluate(
        &self,
        policy_id: PolicyId,
        policy_version: PolicyVersion,
        hook_point: PolicyHookPoint,
        input: &PolicyEvalInput,
        policy_mode: PolicyMode,
    ) -> Result<PolicyDecision, PolicyPluginError> {
        let plugin_input = self.build_plugin_input(
            policy_id.clone(),
            policy_version,
            hook_point,
            input,
        )?;
        self.host_limits.validate_input(&plugin_input)?;

        let rendered_context = plugin_input.context.to_string();
        let started_at = Instant::now();
        let mut store = Store::new(&self.engine, ());
        store.set_fuel(fuel_budget_for_duration(self.host_limits.max_evaluation_duration()))
            .map_err(|error| map_wasm_error(error, self.host_limits.max_evaluation_duration()))?;

        let linker = Linker::new(&self.engine);
        let instance = linker
            .instantiate_and_start(&mut store, &self.module)
            .map_err(|error| map_wasm_error(error, self.host_limits.max_evaluation_duration()))?;

        let memory = exported_memory(&instance, &store)?;
        memory
            .write(&mut store, 0, rendered_context.as_bytes())
            .map_err(|error| PolicyPluginError::output_validation_failed(error.to_string()))?;

        let abi_version = call_abi_version(&instance, &mut store)?;
        let action_code = call_policy_action_code(
            &instance,
            &mut store,
            rendered_context.len(),
            self.host_limits.max_evaluation_duration(),
        )?;
        let action = policy_action_from_code(action_code)?;
        let outcome = policy_outcome_from_mode(policy_mode);
        let output = PolicyPluginOutput::new(
            abi_version.as_u16(),
            policy_id.clone(),
            policy_version,
            hook_point,
            policy_action_to_plugin_action(&action),
            outcome,
        )?;
        self.host_limits.validate_output(&output)?;

        let elapsed = started_at.elapsed();
        self.host_limits.validate_evaluation_duration(elapsed)?;

        Ok(PolicyDecision::new(
            policy_id,
            policy_version,
            action,
            outcome,
            hook_point,
            elapsed,
        ))
    }
}

fn configured_engine() -> Engine {
    let mut config = Config::default();
    config.consume_fuel(true);
    Engine::new(&config)
}

fn validate_module_shape(module: &Module) -> Result<(), PolicyPluginError> {
    if module.imports().next().is_some() {
        return Err(PolicyPluginError::output_validation_failed(
            "wasm policy modules may not import host functions",
        ));
    }

    match module.get_export(ABI_EXPORT) {
        Some(ExternType::Func(_)) => {}
        _ => {
            return Err(PolicyPluginError::output_validation_failed(format!(
                "wasm policy module must export function {ABI_EXPORT}"
            )));
        }
    }

    match module.get_export(EVALUATE_EXPORT) {
        Some(ExternType::Func(_)) => {}
        _ => {
            return Err(PolicyPluginError::output_validation_failed(format!(
                "wasm policy module must export function {EVALUATE_EXPORT}"
            )));
        }
    }

    match module.get_export(MEMORY_EXPORT) {
        Some(ExternType::Memory(_)) => {}
        _ => {
            return Err(PolicyPluginError::output_validation_failed(
                "wasm policy module must export linear memory",
            ));
        }
    }

    Ok(())
}

fn validate_module_abi(engine: &Engine, module: &Module) -> Result<(), PolicyPluginError> {
    let mut store = Store::new(engine, ());
    store
        .set_fuel(fuel_budget_for_duration(Duration::from_millis(1)))
        .map_err(|error| map_wasm_error(error, Duration::from_millis(1)))?;

    let linker = Linker::new(engine);
    let instance = linker
        .instantiate_and_start(&mut store, module)
        .map_err(|error| map_wasm_error(error, Duration::from_millis(1)))?;
    let abi_version = call_abi_version(&instance, &mut store)?;
    if abi_version != PolicyPluginAbiVersion::current() {
        return Err(PolicyPluginError::output_validation_failed(format!(
            "wasm policy module ABI version {abi_version} does not match expected {}",
            PolicyPluginAbiVersion::current()
        )));
    }

    Ok(())
}

fn call_abi_version(
    instance: &Instance,
    store: &mut Store<()>,
) -> Result<PolicyPluginAbiVersion, PolicyPluginError> {
    let func = instance
        .get_typed_func::<(), i32>(&mut *store, ABI_EXPORT)
        .map_err(|error| map_wasm_error(error, Duration::from_millis(1)))?;
    let raw_version = func
        .call(&mut *store, ())
        .map_err(|error| map_wasm_error(error, Duration::from_millis(1)))?;
    let abi_version = u16::try_from(raw_version).map_err(|_| {
        PolicyPluginError::output_validation_failed(format!(
            "wasm policy module returned invalid ABI version {raw_version}"
        ))
    })?;
    PolicyPluginAbiVersion::try_from(abi_version)
}

fn call_policy_action_code(
    instance: &Instance,
    store: &mut Store<()>,
    context_len: usize,
    max_duration: Duration,
) -> Result<i32, PolicyPluginError> {
    let func = instance
        .get_typed_func::<(i32, i32), i32>(&mut *store, EVALUATE_EXPORT)
        .map_err(|error| map_wasm_error(error, max_duration))?;
    let context_len = i32::try_from(context_len).map_err(|_| {
        PolicyPluginError::output_validation_failed(
            "wasm policy context is too large for the module ABI",
        )
    })?;
    func.call(&mut *store, (0, context_len)).map_err(|error| {
        if error.as_trap_code() == Some(TrapCode::OutOfFuel) {
            PolicyPluginError::evaluation_timeout(max_duration, max_duration)
        } else {
            map_wasm_error(error, max_duration)
        }
    })
}

fn exported_memory(instance: &Instance, store: &Store<()>) -> Result<Memory, PolicyPluginError> {
    match instance.get_export(store, MEMORY_EXPORT) {
        Some(export) => export
            .into_memory()
            .ok_or_else(|| {
                PolicyPluginError::output_validation_failed(
                    "wasm policy memory export must be linear memory",
                )
            }),
        None => Err(PolicyPluginError::output_validation_failed(
            "wasm policy module must export linear memory",
        )),
    }
}

fn policy_action_from_code(code: i32) -> Result<PolicyAction, PolicyPluginError> {
    match code {
        0 => Ok(PolicyAction::allow()),
        1 => Ok(PolicyAction::deny()),
        2 => Ok(PolicyAction::require_primary()),
        3 => Ok(PolicyAction::require_replica()),
        other => Err(PolicyPluginError::output_validation_failed(format!(
            "invalid wasm policy action code {other}"
        ))),
    }
}

fn policy_action_to_plugin_action(action: &PolicyAction) -> PolicyPluginAction {
    match action {
        PolicyAction::Allow => PolicyPluginAction::allow(),
        PolicyAction::Deny { .. } => PolicyPluginAction::deny("policy denied"),
        PolicyAction::RequirePrimary => PolicyPluginAction::require_primary(),
        PolicyAction::RequireReplica => PolicyPluginAction::require_replica(),
        PolicyAction::RouteOverride { target_id } => {
            PolicyPluginAction::route_override(target_id.clone())
        }
        PolicyAction::ShardOverride { target_id } => {
            PolicyPluginAction::shard_override(target_id.clone())
        }
    }
}

fn policy_outcome_from_mode(policy_mode: PolicyMode) -> PolicyOutcome {
    match policy_mode {
        PolicyMode::Disabled => PolicyOutcome::Skipped,
        PolicyMode::Enforce => PolicyOutcome::Applied,
        PolicyMode::DryRun => PolicyOutcome::DryRun,
    }
}

fn fuel_budget_for_duration(duration: Duration) -> u64 {
    let millis = duration.as_millis().max(1);
    let budget = millis.saturating_mul(1_000);
    budget.min(u64::MAX as u128) as u64
}

fn map_wasm_module_error(error: wasmi::Error) -> PolicyPluginError {
    PolicyPluginError::output_validation_failed(error.to_string())
}

fn map_wasm_error(error: wasmi::Error, max_duration: Duration) -> PolicyPluginError {
    if error.as_trap_code() == Some(TrapCode::OutOfFuel) {
        PolicyPluginError::evaluation_timeout(max_duration, max_duration)
    } else {
        PolicyPluginError::output_validation_failed(error.to_string())
    }
}
