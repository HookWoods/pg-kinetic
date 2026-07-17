use std::{
    fmt,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::Context;
use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Notify, Semaphore},
    time::timeout,
};

use crate::{
    backend::Backend,
    config::{MirrorConfig, MirrorSafetyConfig, SocketConfig, TlsConfig},
    routing::{RoutingReason, RoutingTarget},
};
use pg_kinetic_core::{
    mirror::{
        MirrorDecision, MirrorMode, MirrorOutcome, MirrorReason, MirrorSafetyGate, MirrorSample,
        MirrorTarget,
    },
    route::{QueryClass, RouteKey},
    sql::SqlCommand,
    virtual_session::PinReason,
};
use pg_kinetic_wire::{
    backend::BackendFrame,
    frame::FrontendFrame,
    protocol::{BackendTag, FrontendTag},
    rewrite::encode_frontend_frame,
};

const DEFAULT_MIRROR_SEED: u64 = 0x7e57_5eed_7e57_5eed;
const DEFAULT_MIRROR_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug)]
pub struct MirrorDispatchConfig {
    pub production_target: SocketAddr,
    pub target: Option<MirrorTarget>,
    pub mode: MirrorMode,
    pub sample_rate: f64,
    pub safety: MirrorSafetyConfig,
    pub timeout: Duration,
    pub max_in_flight: usize,
    pub tls: TlsConfig,
    pub socket: SocketConfig,
}

impl MirrorDispatchConfig {
    #[must_use]
    pub fn from_mirror_config(
        production_target: SocketAddr,
        mirror: &MirrorConfig,
        tls: TlsConfig,
        socket: SocketConfig,
    ) -> Self {
        Self {
            production_target,
            target: mirror
                .target
                .address
                .map(|address| MirrorTarget::new(address, mirror.target.isolated)),
            mode: mirror.mirror_mode,
            sample_rate: mirror.sampling.sample_rate(),
            safety: mirror.safety.clone(),
            timeout: Duration::from_millis(mirror.mirror_timeout_ms),
            max_in_flight: mirror.mirror_max_in_flight,
            tls,
            socket,
        }
    }

    #[must_use]
    pub fn disabled(production_target: SocketAddr, tls: TlsConfig, socket: SocketConfig) -> Self {
        Self {
            production_target,
            target: None,
            mode: MirrorMode::Off,
            sample_rate: 0.0,
            safety: MirrorSafetyConfig::default(),
            timeout: Duration::from_millis(100),
            max_in_flight: 0,
            tls,
            socket,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MirrorTaskStatus {
    Completed,
    TimedOut,
    Error,
    Dropped { reason: MirrorReason },
    Skipped { reason: MirrorReason },
    Rejected { reason: MirrorReason },
}

impl MirrorTaskStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::TimedOut => "timed_out",
            Self::Error => "error",
            Self::Dropped { .. } => "dropped",
            Self::Skipped { .. } => "skipped",
            Self::Rejected { .. } => "rejected",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MirrorTelemetry {
    pub session_id: u64,
    pub query_id: u64,
    pub route_label: String,
    pub command_label: &'static str,
    pub frame_count: usize,
    pub replay_count: usize,
    pub mode: MirrorMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MirrorObservation {
    pub telemetry: MirrorTelemetry,
    pub decision: MirrorDecision,
    pub status: MirrorTaskStatus,
    pub duration: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct MirrorOutcomeRecorder {
    inner: Arc<MirrorOutcomeRecorderInner>,
}

#[derive(Debug, Default)]
struct MirrorOutcomeRecorderInner {
    observations: Mutex<Vec<MirrorObservation>>,
    notify: Notify,
}

impl MirrorOutcomeRecorder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, observation: MirrorObservation) {
        self.inner
            .observations
            .lock()
            .expect("mirror observation lock")
            .push(observation);
        self.inner.notify.notify_waiters();
    }

    #[must_use]
    pub fn snapshot(&self) -> Vec<MirrorObservation> {
        self.inner
            .observations
            .lock()
            .expect("mirror observation lock")
            .clone()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .observations
            .lock()
            .expect("mirror observation lock")
            .len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner
            .observations
            .lock()
            .expect("mirror observation lock")
            .is_empty()
    }

    pub async fn wait_for_count(&self, expected: usize) {
        loop {
            if self.len() >= expected {
                return;
            }
            self.inner.notify.notified().await;
        }
    }
}

#[derive(Clone, Debug)]
pub struct MirrorTask {
    session_id: u64,
    query_id: u64,
    route_key: RouteKey,
    route_target: RoutingTarget,
    command: SqlCommand,
    startup_packet: Bytes,
    replay_frames: Vec<FrontendFrame>,
    frames: Vec<FrontendFrame>,
    session_pin_reason: Option<PinReason>,
}

impl MirrorTask {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session_id: u64,
        query_id: u64,
        route_key: RouteKey,
        route_target: RoutingTarget,
        command: SqlCommand,
        startup_packet: Bytes,
        replay_frames: Vec<FrontendFrame>,
        frames: Vec<FrontendFrame>,
        session_pin_reason: Option<PinReason>,
    ) -> Self {
        Self {
            session_id,
            query_id,
            route_key,
            route_target,
            command,
            startup_packet,
            replay_frames,
            frames,
            session_pin_reason,
        }
    }

    #[must_use]
    pub const fn session_id(&self) -> u64 {
        self.session_id
    }

    #[must_use]
    pub const fn query_id(&self) -> u64 {
        self.query_id
    }

    #[must_use]
    pub fn route_key(&self) -> &RouteKey {
        &self.route_key
    }

    #[must_use]
    pub fn route_target(&self) -> &RoutingTarget {
        &self.route_target
    }

    #[must_use]
    pub const fn command(&self) -> &SqlCommand {
        &self.command
    }

    #[must_use]
    pub fn startup_packet(&self) -> &Bytes {
        &self.startup_packet
    }

    #[must_use]
    pub fn replay_frames(&self) -> &[FrontendFrame] {
        &self.replay_frames
    }

    #[must_use]
    pub fn frames(&self) -> &[FrontendFrame] {
        &self.frames
    }

    #[must_use]
    pub const fn session_pin_reason(&self) -> Option<PinReason> {
        self.session_pin_reason
    }

    #[must_use]
    pub fn telemetry(&self, mode: MirrorMode) -> MirrorTelemetry {
        MirrorTelemetry {
            session_id: self.session_id,
            query_id: self.query_id,
            route_label: self.route_key.metric_label(),
            command_label: sql_command_label(&self.command),
            frame_count: self.frames.len(),
            replay_count: self.replay_frames.len(),
            mode,
        }
    }

    #[must_use]
    pub fn has_supported_protocol_state(&self) -> bool {
        self.frames.iter().all(|frame| {
            matches!(
                frame.tag,
                tag if tag == u8::from(FrontendTag::Query)
                    || tag == u8::from(FrontendTag::Parse)
                    || tag == u8::from(FrontendTag::Bind)
                    || tag == u8::from(FrontendTag::Describe)
                    || tag == u8::from(FrontendTag::Execute)
                    || tag == u8::from(FrontendTag::Close)
                    || tag == u8::from(FrontendTag::Sync)
            )
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MirrorSampler {
    sample: MirrorSample,
    seed: u64,
}

impl MirrorSampler {
    #[must_use]
    pub fn new(rate: f64) -> Self {
        Self::with_seed(rate, DEFAULT_MIRROR_SEED)
    }

    #[must_use]
    pub fn with_seed(rate: f64, seed: u64) -> Self {
        Self {
            sample: MirrorSample::new(rate),
            seed,
        }
    }

    #[must_use]
    pub const fn rate(self) -> f64 {
        self.sample.rate()
    }

    #[must_use]
    pub fn sample_value(self, session_id: u64, query_id: u64) -> f64 {
        let mixed = splitmix64(self.seed ^ session_id.rotate_left(17) ^ query_id.rotate_left(41));
        let mantissa = mixed >> 11;
        mantissa as f64 / ((1_u64 << 53) - 1) as f64
    }

    #[must_use]
    pub fn should_mirror(self, session_id: u64, query_id: u64) -> bool {
        self.sample
            .should_sample(self.sample_value(session_id, query_id))
    }
}

#[derive(Clone, Debug)]
pub struct MirrorSafetyClassifier {
    production_target: SocketAddr,
    target: Option<MirrorTarget>,
    mode: MirrorMode,
    safety: MirrorSafetyConfig,
}

impl MirrorSafetyClassifier {
    #[must_use]
    pub fn new(
        production_target: SocketAddr,
        target: Option<MirrorTarget>,
        mode: MirrorMode,
        safety: MirrorSafetyConfig,
    ) -> Self {
        Self {
            production_target,
            target,
            mode,
            safety,
        }
    }

    #[must_use]
    pub const fn mode(&self) -> MirrorMode {
        self.mode
    }

    #[must_use]
    pub fn target(&self) -> Option<&MirrorTarget> {
        self.target.as_ref()
    }

    #[must_use]
    pub fn classify(&self, task: &MirrorTask) -> MirrorDecision {
        if !self.mode.is_enabled() {
            return MirrorDecision::skipped(
                self.mode,
                MirrorSafetyGate::Disabled,
                MirrorReason::Disabled,
            );
        }

        let Some(target) = self.target.as_ref() else {
            return MirrorDecision::rejected(
                self.mode,
                MirrorSafetyGate::TargetConfigured,
                MirrorReason::TargetMissing,
            );
        };

        if self.safety.mirror_require_isolated_target
            && !target.is_isolated()
            && target.address() == self.production_target
        {
            return MirrorDecision::rejected(
                self.mode,
                MirrorSafetyGate::TargetIsolated,
                MirrorReason::TargetSharedWithProduction,
            );
        }

        if matches!(
            task.route_target(),
            RoutingTarget::Reject {
                reason: RoutingReason::PolicyDenied,
            }
        ) {
            return MirrorDecision::rejected(
                self.mode,
                MirrorSafetyGate::Disabled,
                MirrorReason::Disabled,
            );
        }

        if matches!(
            task.route_target(),
            RoutingTarget::Wait { .. } | RoutingTarget::Reject { .. }
        ) {
            return MirrorDecision::skipped(
                self.mode,
                MirrorSafetyGate::Disabled,
                MirrorReason::UnsupportedMode,
            );
        }

        if !task.has_supported_protocol_state() {
            return MirrorDecision::skipped(
                self.mode,
                MirrorSafetyGate::Disabled,
                MirrorReason::UnsupportedMode,
            );
        }

        if matches!(
            task.route_key().query_class(),
            QueryClass::Write | QueryClass::Maintenance
        ) && (self.mode == MirrorMode::ReadOnly || !self.safety.mirror_writes_enabled)
        {
            return MirrorDecision::skipped(
                self.mode,
                MirrorSafetyGate::Writes,
                MirrorReason::WritesDisabled,
            );
        }

        match task.command() {
            SqlCommand::Query => {}
            SqlCommand::Begin { .. }
            | SqlCommand::Commit
            | SqlCommand::Rollback
            | SqlCommand::SetTransaction { .. } => {
                if self.mode == MirrorMode::ReadOnly || !self.safety.mirror_transactions_enabled {
                    return MirrorDecision::skipped(
                        self.mode,
                        MirrorSafetyGate::Transactions,
                        MirrorReason::TransactionsDisabled,
                    );
                }
            }
            SqlCommand::Set { .. }
            | SqlCommand::Reset { .. }
            | SqlCommand::DiscardAll
            | SqlCommand::DiscardTemp
            | SqlCommand::DiscardPlans
            | SqlCommand::CreateTemp
            | SqlCommand::AdvisoryLock
            | SqlCommand::AdvisoryUnlock => {
                if self.mode == MirrorMode::ReadOnly || !self.safety.mirror_session_mutation_enabled
                {
                    return MirrorDecision::skipped(
                        self.mode,
                        MirrorSafetyGate::SessionMutation,
                        MirrorReason::SessionMutationDisabled,
                    );
                }
            }
            SqlCommand::Copy => {
                if self.mode == MirrorMode::ReadOnly || !self.safety.mirror_copy_enabled {
                    return MirrorDecision::skipped(
                        self.mode,
                        MirrorSafetyGate::Copy,
                        MirrorReason::CopyDisabled,
                    );
                }
            }
            SqlCommand::Listen | SqlCommand::Unlisten => {
                if self.mode == MirrorMode::ReadOnly || !self.safety.mirror_listen_notify_enabled {
                    return MirrorDecision::skipped(
                        self.mode,
                        MirrorSafetyGate::ListenNotify,
                        MirrorReason::ListenNotifyDisabled,
                    );
                }
            }
        }

        if let Some(reason) = task.session_pin_reason() {
            let (gate, mirror_reason) = match reason {
                PinReason::OpenTransaction | PinReason::FailedTransaction => (
                    MirrorSafetyGate::Transactions,
                    MirrorReason::TransactionsDisabled,
                ),
                PinReason::Copy => (MirrorSafetyGate::Copy, MirrorReason::CopyDisabled),
                PinReason::ListenNotify => (
                    MirrorSafetyGate::ListenNotify,
                    MirrorReason::ListenNotifyDisabled,
                ),
                PinReason::TempTable => {
                    (MirrorSafetyGate::TempTable, MirrorReason::TempTableDisabled)
                }
                PinReason::SessionState | PinReason::AdvisoryLock => (
                    MirrorSafetyGate::SessionMutation,
                    MirrorReason::SessionMutationDisabled,
                ),
                PinReason::UnknownProtocolState => {
                    (MirrorSafetyGate::Disabled, MirrorReason::UnsupportedMode)
                }
            };

            let enabled = match gate {
                MirrorSafetyGate::Writes => self.safety.mirror_writes_enabled,
                MirrorSafetyGate::Transactions => self.safety.mirror_transactions_enabled,
                MirrorSafetyGate::Copy => self.safety.mirror_copy_enabled,
                MirrorSafetyGate::ListenNotify => self.safety.mirror_listen_notify_enabled,
                MirrorSafetyGate::TempTable => self.safety.mirror_temp_table_enabled,
                MirrorSafetyGate::SessionMutation => self.safety.mirror_session_mutation_enabled,
                MirrorSafetyGate::Disabled
                | MirrorSafetyGate::TargetConfigured
                | MirrorSafetyGate::TargetIsolated
                | MirrorSafetyGate::Sampling => false,
            };

            if self.mode == MirrorMode::ReadOnly || !enabled {
                return MirrorDecision::skipped(self.mode, gate, mirror_reason);
            }
        }

        MirrorDecision::mirrored(self.mode, MirrorSafetyGate::Sampling)
    }
}

#[derive(Clone)]
enum MirrorRunnerKind {
    Default,
    Custom(Arc<MirrorRunner>),
}

impl fmt::Debug for MirrorRunnerKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => formatter.write_str("Default"),
            Self::Custom(_) => formatter.write_str("Custom(..)"),
        }
    }
}

type MirrorRunner =
    dyn Fn(MirrorTask) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> + Send + Sync;

#[derive(Clone)]
pub struct MirrorDispatcher {
    config: MirrorDispatchConfig,
    classifier: MirrorSafetyClassifier,
    sampler: MirrorSampler,
    recorder: MirrorOutcomeRecorder,
    in_flight: Arc<Semaphore>,
    runner: MirrorRunnerKind,
}

impl fmt::Debug for MirrorDispatcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MirrorDispatcher")
            .field("config", &self.config)
            .field("classifier", &self.classifier)
            .field("sampler", &self.sampler)
            .field("recorder", &self.recorder)
            .field("in_flight", &self.in_flight())
            .field("runner", &self.runner)
            .finish()
    }
}

impl MirrorDispatcher {
    #[must_use]
    pub fn new(config: MirrorDispatchConfig, recorder: MirrorOutcomeRecorder) -> Self {
        Self {
            classifier: MirrorSafetyClassifier::new(
                config.production_target,
                config.target.clone(),
                config.mode,
                config.safety.clone(),
            ),
            sampler: MirrorSampler::with_seed(config.sample_rate, DEFAULT_MIRROR_SEED),
            in_flight: Arc::new(Semaphore::new(config.max_in_flight)),
            runner: MirrorRunnerKind::Default,
            config,
            recorder,
        }
    }

    #[must_use]
    pub fn from_mirror_config(
        production_target: SocketAddr,
        mirror: &MirrorConfig,
        tls: TlsConfig,
        socket: SocketConfig,
        recorder: MirrorOutcomeRecorder,
    ) -> Self {
        Self::new(
            MirrorDispatchConfig::from_mirror_config(production_target, mirror, tls, socket),
            recorder,
        )
    }

    #[must_use]
    pub fn disabled(
        production_target: SocketAddr,
        tls: TlsConfig,
        socket: SocketConfig,
        recorder: MirrorOutcomeRecorder,
    ) -> Self {
        Self::new(
            MirrorDispatchConfig::disabled(production_target, tls, socket),
            recorder,
        )
    }

    #[must_use]
    pub fn with_runner<F, Fut>(
        config: MirrorDispatchConfig,
        recorder: MirrorOutcomeRecorder,
        runner: F,
    ) -> Self
    where
        F: Fn(MirrorTask) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let runner: Arc<MirrorRunner> = Arc::new(move |task| Box::pin(runner(task)));
        Self {
            classifier: MirrorSafetyClassifier::new(
                config.production_target,
                config.target.clone(),
                config.mode,
                config.safety.clone(),
            ),
            sampler: MirrorSampler::with_seed(config.sample_rate, DEFAULT_MIRROR_SEED),
            in_flight: Arc::new(Semaphore::new(config.max_in_flight)),
            runner: MirrorRunnerKind::Custom(runner),
            config,
            recorder,
        }
    }

    #[must_use]
    pub fn classifier(&self) -> &MirrorSafetyClassifier {
        &self.classifier
    }

    #[must_use]
    pub fn sampler(&self) -> MirrorSampler {
        self.sampler
    }

    #[must_use]
    pub fn max_in_flight(&self) -> usize {
        self.config.max_in_flight
    }

    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.config
            .max_in_flight
            .saturating_sub(self.in_flight.available_permits())
    }

    #[must_use]
    pub fn recorder(&self) -> MirrorOutcomeRecorder {
        self.recorder.clone()
    }

    #[must_use]
    pub fn dispatch(&self, task: MirrorTask) -> MirrorDecision {
        let decision = self.classifier.classify(&task);
        let telemetry = task.telemetry(self.classifier.mode());

        if !matches!(decision.outcome(), MirrorOutcome::Mirrored) {
            self.recorder.record(MirrorObservation {
                telemetry,
                decision,
                status: match decision.outcome() {
                    MirrorOutcome::Mirrored => MirrorTaskStatus::Completed,
                    MirrorOutcome::Skipped => MirrorTaskStatus::Skipped {
                        reason: decision.reason(),
                    },
                    MirrorOutcome::Rejected => MirrorTaskStatus::Rejected {
                        reason: decision.reason(),
                    },
                },
                duration: Duration::ZERO,
            });
            return decision;
        }

        if !self
            .sampler
            .should_mirror(task.session_id(), task.query_id())
        {
            let dropped_decision = MirrorDecision::skipped(
                self.classifier.mode(),
                MirrorSafetyGate::Sampling,
                MirrorReason::SampledOut,
            );
            self.recorder.record(MirrorObservation {
                telemetry,
                decision: dropped_decision,
                status: MirrorTaskStatus::Dropped {
                    reason: MirrorReason::SampledOut,
                },
                duration: Duration::ZERO,
            });
            return dropped_decision;
        }

        let Ok(permit) = Arc::clone(&self.in_flight).try_acquire_owned() else {
            let dropped_decision = MirrorDecision::skipped(
                self.classifier.mode(),
                MirrorSafetyGate::Sampling,
                MirrorReason::SampledOut,
            );
            self.recorder.record(MirrorObservation {
                telemetry,
                decision: dropped_decision,
                status: MirrorTaskStatus::Dropped {
                    reason: MirrorReason::SampledOut,
                },
                duration: Duration::ZERO,
            });
            return dropped_decision;
        };

        let recorder = self.recorder.clone();
        let runner = self.runner.clone();
        let config = self.config.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let status = match timeout(
                config.timeout,
                run_mirror_task(&runner, config.clone(), task),
            )
            .await
            {
                Ok(Ok(())) => MirrorTaskStatus::Completed,
                Ok(Err(error)) => {
                    tracing::debug!(error = %error, "mirror task failed");
                    MirrorTaskStatus::Error
                }
                Err(_) => {
                    tracing::debug!("mirror task timed out");
                    MirrorTaskStatus::TimedOut
                }
            };

            recorder.record(MirrorObservation {
                telemetry,
                decision,
                status,
                duration: started.elapsed(),
            });
            drop(permit);
        });

        decision
    }
}

async fn run_mirror_task(
    runner: &MirrorRunnerKind,
    config: MirrorDispatchConfig,
    task: MirrorTask,
) -> anyhow::Result<()> {
    match runner {
        MirrorRunnerKind::Default => run_default_mirror_task(config, task).await,
        MirrorRunnerKind::Custom(runner) => (runner)(task).await,
    }
}

async fn run_default_mirror_task(
    config: MirrorDispatchConfig,
    task: MirrorTask,
) -> anyhow::Result<()> {
    let Some(target) = config.target else {
        anyhow::bail!("mirror target is unavailable");
    };

    let mut backend = Backend::connect_with_socket(target.address(), &config.tls, &config.socket)
        .await
        .context("connect mirror target")?;

    backend
        .stream_mut()
        .write_all(task.startup_packet())
        .await
        .context("write mirror startup")?;
    drain_backend_until_ready(&mut backend, "mirror startup").await?;

    for frame in task.replay_frames() {
        backend
            .stream_mut()
            .write_all(&encode_frontend_frame(frame))
            .await
            .context("write mirror replay frame")?;
    }
    for frame in task.frames() {
        backend
            .stream_mut()
            .write_all(&encode_frontend_frame(frame))
            .await
            .context("write mirror query frame")?;
    }

    drain_backend_until_ready(&mut backend, "mirror query").await
}

async fn drain_backend_until_ready(
    backend: &mut Backend,
    context: &'static str,
) -> anyhow::Result<()> {
    let mut buffer = BytesMut::with_capacity(16 * 1024);
    loop {
        if buffer.len() >= DEFAULT_MIRROR_BUFFER_BYTES {
            anyhow::bail!("{context} buffer limit exceeded");
        }

        let read = backend
            .stream_mut()
            .read_buf(&mut buffer)
            .await
            .with_context(|| format!("{context} read"))?;
        if read == 0 {
            anyhow::bail!("{context} backend disconnected");
        }

        if buffer.len() > DEFAULT_MIRROR_BUFFER_BYTES {
            anyhow::bail!("{context} buffer limit exceeded");
        }

        while let Some(frame) = parse_backend_frame(&mut buffer)? {
            match frame.tag {
                tag if tag == u8::from(BackendTag::Authentication) => {
                    let code = auth_request_code(&frame.payload)?;
                    if code != 0 && auth_request_expects_client_response(&frame.payload)? {
                        anyhow::bail!("{context} backend requires client authentication");
                    }
                }
                tag if tag == u8::from(BackendTag::ErrorResponse) => {
                    anyhow::bail!("{context} backend returned error");
                }
                tag if tag == u8::from(BackendTag::ReadyForQuery) => {
                    return Ok(());
                }
                _ => {}
            }
        }
    }
}

fn auth_request_code(payload: &[u8]) -> anyhow::Result<i32> {
    anyhow::ensure!(payload.len() >= 4, "authentication request missing code");
    Ok(i32::from_be_bytes([
        payload[0], payload[1], payload[2], payload[3],
    ]))
}

fn auth_request_expects_client_response(payload: &[u8]) -> anyhow::Result<bool> {
    let code = auth_request_code(payload)?;
    Ok(matches!(code, 3 | 5 | 6 | 7 | 8 | 9 | 10 | 11))
}

fn sql_command_label(command: &SqlCommand) -> &'static str {
    match command {
        SqlCommand::Begin { .. } => "begin",
        SqlCommand::Commit => "commit",
        SqlCommand::Rollback => "rollback",
        SqlCommand::SetTransaction { .. } => "set_transaction",
        SqlCommand::Set { .. } => "set",
        SqlCommand::Reset { .. } => "reset",
        SqlCommand::DiscardAll => "discard_all",
        SqlCommand::DiscardTemp => "discard_temp",
        SqlCommand::DiscardPlans => "discard_plans",
        SqlCommand::CreateTemp => "create_temp",
        SqlCommand::AdvisoryLock => "advisory_lock",
        SqlCommand::AdvisoryUnlock => "advisory_unlock",
        SqlCommand::Copy => "copy",
        SqlCommand::Listen => "listen",
        SqlCommand::Unlisten => "unlisten",
        SqlCommand::Query => "query",
    }
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut result = value;
    result = (result ^ (result >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    result = (result ^ (result >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    result ^ (result >> 31)
}

fn parse_backend_frame(buffer: &mut BytesMut) -> anyhow::Result<Option<BackendFrame>> {
    pg_kinetic_wire::backend::parse_backend_frame(buffer).map_err(Into::into)
}
