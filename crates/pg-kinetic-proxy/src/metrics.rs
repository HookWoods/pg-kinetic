use std::{net::SocketAddr, sync::OnceLock, time::Duration};

use crate::routing::{RoutingReason as ProxyRoutingReason, RoutingTarget};
use crate::sharding::RouteMapReloadErrorCode;
use crate::snapshot::{
    AdaptiveOutcomeSnapshot, AdaptiveRecommendationSnapshot, BenchmarkRunSnapshot,
    MirrorSummarySnapshot, PerformanceSnapshot, PoolSnapshot, ReplicaHealthSnapshot,
    RouteCheckoutSnapshot, RouteMapReloadSnapshot, RuntimeSnapshot, ServerSnapshot,
    ShardLifecycleSnapshot, ShardMigrationSafetySnapshot, SnapshotStore,
};
use crate::socket::SocketOptionOutcome;
use metrics_exporter_prometheus::PrometheusBuilder;
use pg_kinetic_core::{
    cleanup::CleanupAction,
    constants::{MetricName, PreparedEvent},
    ha::{EndpointHealth, ReplicaLagState},
    lsn::FreshnessStatus,
    observability::{
        metric_catalog, MetricDescriptor, MetricKind, MetricName as ObservabilityMetricName,
        MetricOutcome, ProtocolPhase,
    },
    performance::{PerformanceBudgetOutcome, ProcessMetricKind},
    policy::{
        PolicyAction, PolicyAuditEvent, PolicyAuditKind, PolicyDecisionReason, PolicyHookPoint,
        PolicyMode, PolicyOutcome,
    },
    route::{QueryClass as RouteQueryClass, RouteKey},
    routing::{BackendRole, FallbackPolicy},
    runtime::{ReadinessState, RuntimeLifecycleState, ShutdownReason},
    security::{AuthMode, BackendTlsMode, ClientTlsMode, DrainState, HealthStatus},
    sharding::{MultiShardPolicy, ShardLifecycleState, ShardRouteReason, ShardStrategy},
};
use pg_kinetic_core::{
    recovery::{RecoveryAction, RecoveryTrigger},
    virtual_session::PinReason,
};
use pg_kinetic_wire::sqlstate::SqlState;

#[derive(Clone, Debug)]
pub struct MetricsConfig {
    pub listen_addr: Option<SocketAddr>,
}

const PROTOCOL_PHASE_COUNT: usize = 12;
const METRIC_OUTCOME_COUNT: usize = 6;

static PROTOCOL_PHASE_HISTOGRAMS: OnceLock<
    [[OnceLock<metrics_crate::Histogram>; METRIC_OUTCOME_COUNT]; PROTOCOL_PHASE_COUNT],
> = OnceLock::new();

pub fn install(config: MetricsConfig) -> anyhow::Result<()> {
    if let Some(addr) = config.listen_addr {
        PrometheusBuilder::new()
            .with_http_listener(addr)
            .install()
            .map_err(|error| anyhow::anyhow!("install prometheus exporter: {error}"))?;
        tracing::info!(%addr, "metrics listener enabled");
    }

    describe_metrics();
    Ok(())
}

pub fn record_pool_checkout(wait_ms: f64, stage: &'static str, outcome: &'static str) {
    metrics_crate::histogram!(
        MetricName::PoolCheckoutWaitMs.as_str(),
        "stage" => stage,
        "outcome" => outcome
    )
    .record(wait_ms);

    if stage == "route_gate_registry" {
        metrics_crate::histogram!(
            ObservabilityMetricName::PoolCheckoutLockWaitMs.as_str(),
            "outcome" => outcome,
        )
        .record(wait_ms);
    }
}

pub fn record_runtime_lifecycle_state(state: RuntimeLifecycleState) {
    for candidate in [
        RuntimeLifecycleState::Starting,
        RuntimeLifecycleState::Ready,
        RuntimeLifecycleState::Draining,
        RuntimeLifecycleState::Stopping,
        RuntimeLifecycleState::Stopped,
    ] {
        metrics_crate::gauge!(
            ObservabilityMetricName::RuntimeLifecycleState.as_str(),
            "state" => candidate.as_str()
        )
        .set(if candidate == state { 1.0 } else { 0.0 });
    }
}

pub fn record_runtime_readiness_state(state: ReadinessState) {
    for candidate in [
        ReadinessState::Ready,
        ReadinessState::NotReady,
        ReadinessState::Draining,
    ] {
        metrics_crate::gauge!(
            ObservabilityMetricName::RuntimeReadinessState.as_str(),
            "state" => candidate.as_str()
        )
        .set(if candidate == state { 1.0 } else { 0.0 });
    }
}

pub fn record_runtime_shutdown(reason: ShutdownReason) {
    metrics_crate::counter!(
        ObservabilityMetricName::RuntimeShutdownTotal.as_str(),
        "reason" => reason.as_str()
    )
    .increment(1);
}

pub fn record_runtime_snapshot(snapshot: &RuntimeSnapshot) {
    record_runtime_lifecycle_state(snapshot.lifecycle_state);
    record_runtime_readiness_state(snapshot.readiness_state);
    if let Some(reason) = snapshot.shutdown_reason {
        record_runtime_shutdown(reason);
    }
}

pub fn record_node_heartbeat_age(node: &str, heartbeat_age: Duration) {
    metrics_crate::gauge!(
        ObservabilityMetricName::NodeHeartbeatAgeMs.as_str(),
        "node" => node.to_string()
    )
    .set(heartbeat_age.as_secs_f64() * 1_000.0);
}

pub fn record_mirror_snapshot(snapshot: &MirrorSummarySnapshot) {
    let target = "mirror";
    metrics_crate::gauge!(
        ObservabilityMetricName::MirrorInFlight.as_str(),
        "mode" => snapshot.mode.as_str(),
        "target" => target
    )
    .set(snapshot.in_flight as f64);

    if snapshot.dropped_total > 0 {
        metrics_crate::counter!(
            ObservabilityMetricName::MirrorDroppedTotal.as_str(),
            "mode" => snapshot.mode.as_str(),
            "reason" => "sampled_out"
        )
        .increment(snapshot.dropped_total);
    }

    if snapshot.timed_out_total > 0 {
        metrics_crate::counter!(
            ObservabilityMetricName::MirrorDecisionsTotal.as_str(),
            "mode" => snapshot.mode.as_str(),
            "target" => target,
            "outcome" => "timeout"
        )
        .increment(snapshot.timed_out_total);
    }

    if snapshot.mirrored_total > 0 {
        metrics_crate::counter!(
            ObservabilityMetricName::MirrorDecisionsTotal.as_str(),
            "mode" => snapshot.mode.as_str(),
            "target" => target,
            "outcome" => "mirrored"
        )
        .increment(snapshot.mirrored_total);
    }

    if snapshot.skipped_total > 0 {
        metrics_crate::counter!(
            ObservabilityMetricName::MirrorDecisionsTotal.as_str(),
            "mode" => snapshot.mode.as_str(),
            "target" => target,
            "outcome" => "skipped"
        )
        .increment(snapshot.skipped_total);
    }

    if snapshot.rejected_total > 0 {
        metrics_crate::counter!(
            ObservabilityMetricName::MirrorDecisionsTotal.as_str(),
            "mode" => snapshot.mode.as_str(),
            "target" => target,
            "outcome" => "rejected"
        )
        .increment(snapshot.rejected_total);
    }

    if let Some(duration) = snapshot.last_duration {
        let outcome = if snapshot.mirrored_total > 0 {
            "mirrored"
        } else if snapshot.skipped_total > 0 {
            "skipped"
        } else if snapshot.rejected_total > 0 {
            "rejected"
        } else {
            "timeout"
        };
        metrics_crate::histogram!(
            ObservabilityMetricName::MirrorDurationMs.as_str(),
            "mode" => snapshot.mode.as_str(),
            "target" => target,
            "outcome" => outcome
        )
        .record(duration.as_secs_f64() * 1_000.0);
    }
}

pub fn record_adaptive_recommendation(snapshot: &AdaptiveRecommendationSnapshot) {
    metrics_crate::counter!(
        ObservabilityMetricName::AdaptiveRecommendationsTotal.as_str(),
        "mode" => "recommend",
        "target" => snapshot.knob.as_str(),
        "outcome" => snapshot.action.as_str()
    )
    .increment(1);
}

pub fn record_adaptive_outcome(snapshot: &AdaptiveOutcomeSnapshot) {
    metrics_crate::counter!(
        ObservabilityMetricName::AdaptiveApplyTotal.as_str(),
        "mode" => "apply",
        "target" => snapshot.knob.as_str(),
        "outcome" => snapshot.outcome.as_str()
    )
    .increment(1);
}

pub fn record_benchmark_run(snapshot: &BenchmarkRunSnapshot) {
    for result in &snapshot.results {
        let scenario = snapshot.scenario.metric_label();
        let target = result.target().metric_label();
        let workload = snapshot.scenario.workload().as_str();
        let driver = result.driver().as_str();
        metrics_crate::counter!(
            ObservabilityMetricName::BenchmarkRunsTotal.as_str(),
            "engine" => driver,
            "target" => target,
            "outcome" => "ok"
        )
        .increment(1);

        for (metric, value) in [
            ("p50", result.metrics().p50_ms()),
            ("p95", result.metrics().p95_ms()),
            ("p99", result.metrics().p99_ms()),
        ] {
            metrics_crate::histogram!(
                ObservabilityMetricName::BenchmarkLatencyMs.as_str(),
                "scenario" => scenario,
                "target" => target,
                "workload" => workload,
                "driver" => driver,
                "metric" => metric,
            )
            .record(value);
        }

        metrics_crate::histogram!(
            ObservabilityMetricName::BenchmarkThroughputQps.as_str(),
            "scenario" => scenario,
            "target" => target,
            "workload" => workload,
            "driver" => driver,
        )
        .record(result.metrics().throughput_qps());

        if result.metrics().error_rate() > 0.0 {
            metrics_crate::counter!(
                ObservabilityMetricName::BenchmarkErrorsTotal.as_str(),
                "scenario" => scenario,
                "target" => target,
                "workload" => workload,
                "driver" => driver,
                "outcome" => "nonzero_error_rate",
            )
            .increment(1);
        }
    }
}

pub fn record_performance_snapshot(snapshot: &PerformanceSnapshot) {
    for regression in &snapshot.regressions {
        for outcome in [
            PerformanceBudgetOutcome::Passed,
            PerformanceBudgetOutcome::Warning,
            PerformanceBudgetOutcome::Failed,
        ] {
            metrics_crate::gauge!(
                ObservabilityMetricName::PerformanceBudgetStatus.as_str(),
                "metric" => regression.metric().as_str(),
                "outcome" => outcome.as_str(),
            )
            .set(if outcome == regression.outcome() {
                1.0
            } else {
                0.0
            });
        }
    }

    if let Some(sample) = &snapshot.process_sample {
        if let Some(value) = sample.metric(ProcessMetricKind::CpuTime).as_f64() {
            metrics_crate::gauge!(ObservabilityMetricName::ProcessCpuSeconds.as_str()).set(value);
        }
        if let Some(value) = sample.metric(ProcessMetricKind::ResidentMemory).as_f64() {
            metrics_crate::gauge!(ObservabilityMetricName::ProcessResidentMemoryBytes.as_str())
                .set(value);
        }
    }

    if let Some(value) = snapshot.cpu_per_query {
        metrics_crate::gauge!(ObservabilityMetricName::CpuPerQuery.as_str()).set(value);
    }
    if let Some(value) = snapshot.memory_per_client_bytes {
        metrics_crate::gauge!(ObservabilityMetricName::MemoryPerClientBytes.as_str()).set(value);
    }
    metrics_crate::gauge!(
        ObservabilityMetricName::ProtocolBufferCopiesTotal.as_str(),
        "feature" => "protocol",
    )
    .set(snapshot.protocol_buffer_copies as f64);
    record_prepared_cache_totals(snapshot.prepared_cache_hits, snapshot.prepared_cache_misses);
    metrics_crate::gauge!(
        ObservabilityMetricName::ObservabilityHotPathAllocationsTotal.as_str(),
        "feature" => "metrics",
    )
    .set(snapshot.observability_hot_path_allocations as f64);
    metrics_crate::gauge!(ObservabilityMetricName::IdleClients.as_str())
        .set(snapshot.idle_clients as f64);
}

pub fn record_pool_checkout_lock_wait(wait_ms: f64) {
    metrics_crate::histogram!(
        ObservabilityMetricName::PoolCheckoutLockWaitMs.as_str(),
        "outcome" => "ok",
    )
    .record(wait_ms);
}

pub fn record_prepared_cache_totals(hits: u64, misses: u64) {
    metrics_crate::gauge!(ObservabilityMetricName::PreparedCacheHitsTotal.as_str())
        .set(hits as f64);
    metrics_crate::gauge!(ObservabilityMetricName::PreparedCacheMissesTotal.as_str())
        .set(misses as f64);
}

pub fn record_preflight_finding(check: &str, severity: &str) {
    metrics_crate::counter!(
        ObservabilityMetricName::PreflightFindingsTotal.as_str(),
        "check" => check.to_string(),
        "severity" => severity.to_string()
    )
    .increment(1);
}

pub fn record_protocol_phase_duration(
    phase: ProtocolPhase,
    outcome: MetricOutcome,
    duration: Duration,
) {
    protocol_phase_histogram(phase, outcome).record(duration.as_secs_f64() * 1_000.0);
}

pub fn record_protocol_phase_duration_sampled(
    phase: ProtocolPhase,
    outcome: MetricOutcome,
    duration: Duration,
    sampler: crate::telemetry::DebugSampler,
    session_id: u64,
) {
    if sampler.should_sample(session_id) {
        record_protocol_phase_duration(phase, outcome, duration);
    }
}

fn protocol_phase_histogram(
    phase: ProtocolPhase,
    outcome: MetricOutcome,
) -> &'static metrics_crate::Histogram {
    let histogram =
        &protocol_phase_histograms()[protocol_phase_index(phase)][metric_outcome_index(outcome)];
    histogram.get_or_init(|| {
        metrics_crate::histogram!(
            ObservabilityMetricName::ProtocolPhaseDuration.as_str(),
            "phase" => phase.as_str(),
            "outcome" => outcome.as_str()
        )
    })
}

fn protocol_phase_histograms(
) -> &'static [[OnceLock<metrics_crate::Histogram>; METRIC_OUTCOME_COUNT]; PROTOCOL_PHASE_COUNT] {
    PROTOCOL_PHASE_HISTOGRAMS
        .get_or_init(|| std::array::from_fn(|_| std::array::from_fn(|_| OnceLock::new())))
}

const fn protocol_phase_index(phase: ProtocolPhase) -> usize {
    match phase {
        ProtocolPhase::Startup => 0,
        ProtocolPhase::Auth => 1,
        ProtocolPhase::TlsHandshake => 2,
        ProtocolPhase::BackendCheckout => 3,
        ProtocolPhase::Parse => 4,
        ProtocolPhase::Bind => 5,
        ProtocolPhase::Execute => 6,
        ProtocolPhase::Rows => 7,
        ProtocolPhase::Drain => 8,
        ProtocolPhase::Reset => 9,
        ProtocolPhase::Cancel => 10,
        ProtocolPhase::Close => 11,
    }
}

const fn metric_outcome_index(outcome: MetricOutcome) -> usize {
    match outcome {
        MetricOutcome::Ok => 0,
        MetricOutcome::Error => 1,
        MetricOutcome::Timeout => 2,
        MetricOutcome::Rejected => 3,
        MetricOutcome::Canceled => 4,
        MetricOutcome::Discarded => 5,
    }
}

pub fn record_pool_snapshot(snapshot_store: &SnapshotStore, snapshot: PoolSnapshot) {
    snapshot_store.set_pool_snapshot(snapshot);
}

pub fn record_pool_connections(active: usize, idle: usize) {
    for (state, value) in [("active", active), ("idle", idle)] {
        metrics_crate::gauge!("pg_kinetic_pool_connections", "state" => state).set(value as f64);
    }
}

pub fn record_pool_eviction(reason: &'static str) {
    metrics_crate::counter!("pg_kinetic_pool_evictions_total", "reason" => reason).increment(1);
}

pub fn record_server_snapshot(snapshot_store: &SnapshotStore, snapshot: ServerSnapshot) {
    snapshot_store.set_server_snapshot(snapshot);
}

pub fn record_route_checkout_snapshot(snapshot: &RouteCheckoutSnapshot) {
    let decision = &snapshot.decision;
    let target_role = decision.target_role();
    let reason = decision.reason();
    metrics_crate::counter!(
        ObservabilityMetricName::RouteDecisionsTotal.as_str(),
        "route" => snapshot.route_key.metric_label_shared(),
        "target_role" => target_role_label(target_role),
        "query_class" => route_query_class_label(snapshot.route_key.query_class())
    )
    .increment(1);

    if let Some(fallback_policy) = fallback_policy_from_reason(reason) {
        metrics_crate::counter!(
            ObservabilityMetricName::RouteFallbacksTotal.as_str(),
            "route" => snapshot.route_key.metric_label_shared(),
            "reason" => reason.as_str(),
            "fallback_policy" => fallback_policy.as_str()
        )
        .increment(1);
    }

    if matches!(decision, RoutingTarget::Reject { .. }) {
        if let Some(outcome) = snapshot.freshness_outcome {
            increment_read_after_write_rejection(&snapshot.route_key, outcome);
        }
    }
}

pub fn record_replica_health_snapshot(snapshot: &ReplicaHealthSnapshot) {
    let endpoint = endpoint_label(snapshot.endpoint_id);

    for candidate in [
        EndpointHealth::Healthy,
        EndpointHealth::Degraded,
        EndpointHealth::Unhealthy,
        EndpointHealth::Unavailable,
    ] {
        metrics_crate::gauge!(
            ObservabilityMetricName::ReplicaHealth.as_str(),
            "endpoint" => endpoint.clone(),
            "health" => candidate.as_str()
        )
        .set(if candidate == snapshot.health.state {
            1.0
        } else {
            0.0
        });
    }

    let lag_ms = snapshot
        .lag_duration
        .map(|duration| duration.as_secs_f64() * 1_000.0)
        .unwrap_or(0.0);
    for candidate in [
        ReplicaLagState::Unknown,
        ReplicaLagState::Fresh,
        ReplicaLagState::Lagging,
    ] {
        metrics_crate::gauge!(
            ObservabilityMetricName::ReplicaLagMs.as_str(),
            "endpoint" => endpoint.clone(),
            "lag_state" => candidate.as_str()
        )
        .set(if candidate == snapshot.lag_state {
            lag_ms
        } else {
            0.0
        });
    }

    metrics_crate::gauge!(
        ObservabilityMetricName::ReplicaReplayLsn.as_str(),
        "endpoint" => endpoint,
        "target_role" => snapshot.expected_role.as_str()
    )
    .set(snapshot.replay_lsn.map_or(0.0, |lsn| lsn.as_u64() as f64));
}

pub fn record_split_brain_warning(endpoint_id: u64, expected_role: BackendRole) {
    metrics_crate::counter!(
        ObservabilityMetricName::SplitBrainWarningsTotal.as_str(),
        "endpoint" => endpoint_label(endpoint_id),
        "target_role" => expected_role.as_str(),
        "reason" => "role_mismatch"
    )
    .increment(1);
}

pub fn record_read_after_write_wait(route: &RouteKey, wait_ms: f64, outcome: FreshnessStatus) {
    metrics_crate::histogram!(
        ObservabilityMetricName::ReadAfterWriteWaitMs.as_str(),
        "route" => route.metric_label_shared(),
        "outcome" => freshness_outcome_label(outcome)
    )
    .record(wait_ms);
}

pub fn increment_read_after_write_rejection(route: &RouteKey, outcome: FreshnessStatus) {
    metrics_crate::counter!(
        ObservabilityMetricName::ReadAfterWriteRejectionsTotal.as_str(),
        "route" => route.metric_label_shared(),
        "outcome" => freshness_outcome_label(outcome)
    )
    .increment(1);
}

pub fn record_shard_route_decision(
    route: &RouteKey,
    shard: Option<&str>,
    strategy: ShardStrategy,
    reason: ShardRouteReason,
    outcome: &'static str,
) {
    metrics_crate::counter!(
        ObservabilityMetricName::ShardRouteDecisionsTotal.as_str(),
        "route" => route.metric_label_shared(),
        "shard" => shard_bucket_label(shard),
        "strategy" => strategy.as_str(),
        "reason" => reason.as_str(),
        "outcome" => outcome,
    )
    .increment(1);
}

pub fn record_shard_multi_shard_rejection(
    route: &RouteKey,
    shard: Option<&str>,
    policy: MultiShardPolicy,
    reason: ShardRouteReason,
    outcome: &'static str,
) {
    metrics_crate::counter!(
        ObservabilityMetricName::ShardMultiShardRejectionsTotal.as_str(),
        "route" => route.metric_label_shared(),
        "shard" => shard_bucket_label(shard),
        "policy" => policy.as_str(),
        "reason" => reason.as_str(),
        "outcome" => outcome,
    )
    .increment(1);
}

pub fn record_shard_primary_fallback(
    route: &RouteKey,
    shard: Option<&str>,
    policy: MultiShardPolicy,
    outcome: &'static str,
) {
    metrics_crate::counter!(
        ObservabilityMetricName::ShardPrimaryFallbacksTotal.as_str(),
        "route" => route.metric_label_shared(),
        "shard" => shard_bucket_label(shard),
        "policy" => policy.as_str(),
        "outcome" => outcome,
    )
    .increment(1);
}

pub fn record_route_map_reload_snapshot(snapshot: &RouteMapReloadSnapshot) {
    metrics_crate::counter!(
        ObservabilityMetricName::RouteMapReloadTotal.as_str(),
        "outcome" => if snapshot.success { "success" } else { "failure" },
        "error_code" => route_map_reload_error_code_label(snapshot.error_code),
    )
    .increment(1);

    metrics_crate::gauge!(ObservabilityMetricName::RouteMapGeneration.as_str())
        .set(snapshot.route_map_generation_id as f64);
}

pub fn record_shard_lifecycle_snapshot(snapshot: &ShardLifecycleSnapshot) {
    let shard = shard_bucket_label(Some(snapshot.shard_id.as_str()));
    for candidate in [
        ShardLifecycleState::Active,
        ShardLifecycleState::Draining,
        ShardLifecycleState::Readonly,
        ShardLifecycleState::Disabled,
    ] {
        metrics_crate::gauge!(
            ObservabilityMetricName::ShardLifecycleState.as_str(),
            "shard" => shard.clone(),
            "lifecycle_state" => candidate.as_str(),
        )
        .set(if candidate == snapshot.lifecycle_state {
            1.0
        } else {
            0.0
        });
    }
}

pub fn record_shard_migration_safety_snapshot(snapshot: &ShardMigrationSafetySnapshot) {
    let Some(report) = snapshot.rebalance_plan.safety_report() else {
        return;
    };

    let shard_values = snapshot
        .rebalance_plan
        .source_shard_ids()
        .iter()
        .map(|shard_id| shard_bucket_label(Some(shard_id.as_str())))
        .collect::<Vec<_>>();
    let prepared_statement_count = report.prepared_statements().len() as f64;
    let active_transaction_count = report.open_transaction_ids().len() as f64;

    for shard in shard_values {
        metrics_crate::gauge!(
            ObservabilityMetricName::ShardActiveTransactions.as_str(),
            "shard" => shard.clone(),
        )
        .set(active_transaction_count);
        metrics_crate::gauge!(
            ObservabilityMetricName::ShardPreparedStatements.as_str(),
            "shard" => shard,
        )
        .set(prepared_statement_count);
    }
}

pub fn remove_server_snapshot(snapshot_store: &SnapshotStore, backend_id: u64) {
    let _ = snapshot_store.remove_server_snapshot(backend_id);
}

pub fn record_backpressure_snapshot(
    snapshot_store: &SnapshotStore,
    route: RouteKey,
    waiting: usize,
    in_flight: usize,
) {
    snapshot_store
        .backpressure_handle()
        .set_route(route, waiting, in_flight);
}

pub fn increment_client_connections() {
    metrics_crate::counter!(MetricName::ClientConnectionsTotal.as_str()).increment(1);
}

pub fn increment_prepared_event(event: PreparedEvent) {
    metrics_crate::counter!(
        MetricName::PreparedEventsTotal.as_str(),
        "event" => event.as_str()
    )
    .increment(1);
}

pub fn increment_pin(reason: PinReason) {
    metrics_crate::counter!(
        MetricName::BackendPinTotal.as_str(),
        "reason" => reason.metric_label()
    )
    .increment(1);
}

pub fn increment_cleanup(action: CleanupAction) {
    metrics_crate::counter!(
        MetricName::BackendCleanupTotal.as_str(),
        "action" => action.metric_label()
    )
    .increment(1);
}

pub fn increment_recovery(trigger: RecoveryTrigger, action: RecoveryAction, outcome: &'static str) {
    metrics_crate::counter!(
        MetricName::BackendRecoveryTotal.as_str(),
        "trigger" => trigger.metric_label(),
        "action" => action.metric_label(),
        "outcome" => outcome
    )
    .increment(1);
}

pub fn increment_sqlstate(sqlstate: SqlState) {
    metrics_crate::counter!(
        MetricName::BackendSqlstateTotal.as_str(),
        "sqlstate" => sqlstate.as_str().to_string()
    )
    .increment(1);
}

pub fn increment_backpressure_event(route: &RouteKey, outcome: &'static str) {
    metrics_crate::counter!(
        MetricName::BackpressureEvents.as_str(),
        "route" => route.metric_label_shared(),
        "outcome" => outcome
    )
    .increment(1);
}

pub fn record_route_wait(route: &RouteKey, wait_ms: f64, outcome: &'static str) {
    metrics_crate::histogram!(
        MetricName::RouteCheckoutWaitMs.as_str(),
        "route" => route.metric_label_shared(),
        "outcome" => outcome
    )
    .record(wait_ms);
}

pub fn record_route_in_flight(route: &RouteKey, in_flight: usize) {
    metrics_crate::gauge!(
        MetricName::RouteInFlight.as_str(),
        "route" => route.metric_label_shared(),
        "scope" => QueueScope::Route.as_str()
    )
    .set(in_flight as f64);
}

pub fn record_route_waiting(route: &RouteKey, waiting: usize) {
    metrics_crate::gauge!(
        MetricName::RouteWaiting.as_str(),
        "route" => route.metric_label_shared(),
        "scope" => QueueScope::Route.as_str()
    )
    .set(waiting as f64);
}

pub fn increment_timeout(kind: &'static str) {
    metrics_crate::counter!(
        MetricName::TimeoutTotal.as_str(),
        "kind" => kind
    )
    .increment(1);
}

pub fn increment_buffer_limit(kind: &'static str) {
    metrics_crate::counter!(
        MetricName::BufferLimitTotal.as_str(),
        "kind" => kind
    )
    .increment(1);
}

pub fn record_read_after_write(outcome: FreshnessStatus) {
    metrics_crate::counter!(
        MetricName::ReadAfterWriteTotal.as_str(),
        "outcome" => freshness_outcome_label(outcome)
    )
    .increment(1);
}

pub fn record_tls_handshake<M: MetricLabelValue>(scope: TlsScope, mode: M) {
    metrics_crate::counter!(
        OperationalMetricName::TlsHandshakesTotal.as_str(),
        "scope" => scope.metric_label(),
        "mode" => mode.metric_label()
    )
    .increment(1);
}

pub fn record_tls_failure<M: MetricLabelValue>(scope: TlsScope, mode: M, reason: TlsFailureReason) {
    metrics_crate::counter!(
        OperationalMetricName::TlsFailuresTotal.as_str(),
        "scope" => scope.metric_label(),
        "mode" => mode.metric_label(),
        "reason" => reason.metric_label()
    )
    .increment(1);
}

pub fn record_auth_attempt(mode: AuthMode) {
    metrics_crate::counter!(
        OperationalMetricName::AuthAttemptsTotal.as_str(),
        "mode" => mode.metric_label()
    )
    .increment(1);
}

pub fn record_auth_failure(mode: AuthMode, reason: AuthFailureReason) {
    metrics_crate::counter!(
        OperationalMetricName::AuthFailuresTotal.as_str(),
        "mode" => mode.metric_label(),
        "reason" => reason.metric_label()
    )
    .increment(1);
}

pub fn record_config_reload(outcome: ReloadOutcome) {
    metrics_crate::counter!(
        OperationalMetricName::ConfigReloadTotal.as_str(),
        "outcome" => outcome.metric_label()
    )
    .increment(1);
}

pub fn record_policy_audit_event(mode: PolicyMode, event: &PolicyAuditEvent) {
    metrics_crate::counter!(
        ObservabilityMetricName::PolicyAuditEventsTotal.as_str(),
        "policy" => event.policy_id.as_str().to_string(),
        "mode" => mode.metric_label(),
        "hook" => event.hook_point.metric_label(),
        "action" => event.action.as_str(),
        "outcome" => event.outcome.metric_label(),
        "reason" => event
            .reason
            .as_deref()
            .unwrap_or("none")
            .to_string(),
    )
    .increment(1);

    if matches!(event.kind, PolicyAuditKind::Decision) {
        record_policy_decision(mode, event);
    }
}

pub fn record_policy_decision(mode: PolicyMode, event: &PolicyAuditEvent) {
    metrics_crate::counter!(
        ObservabilityMetricName::PolicyDecisionsTotal.as_str(),
        "policy" => event.policy_id.as_str().to_string(),
        "mode" => mode.metric_label(),
        "hook" => event.hook_point.metric_label(),
        "action" => event.action.as_str(),
        "outcome" => event.outcome.metric_label(),
    )
    .increment(1);

    metrics_crate::histogram!(
        ObservabilityMetricName::PolicyEvalDurationMs.as_str(),
        "policy" => event.policy_id.as_str().to_string(),
        "mode" => mode.metric_label(),
        "hook" => event.hook_point.metric_label(),
        "outcome" => event.outcome.metric_label(),
    )
    .record(event.decision.latency.as_secs_f64() * 1_000.0);

    if matches!(event.outcome, PolicyOutcome::DryRun) {
        metrics_crate::counter!(
            ObservabilityMetricName::PolicyDryRunTotal.as_str(),
            "policy" => event.policy_id.as_str().to_string(),
            "mode" => mode.metric_label(),
            "hook" => event.hook_point.metric_label(),
            "action" => event.action.as_str(),
        )
        .increment(1);
    }

    if let PolicyAction::Deny { reason, .. } = &event.action {
        metrics_crate::counter!(
            ObservabilityMetricName::PolicyDeniesTotal.as_str(),
            "policy" => event.policy_id.as_str().to_string(),
            "reason" => reason.metric_label(),
        )
        .increment(1);
    }
}

pub fn record_policy_reload(
    source: &'static str,
    mode: PolicyMode,
    success: bool,
    error_code: Option<&'static str>,
) {
    metrics_crate::counter!(
        ObservabilityMetricName::PolicyReloadTotal.as_str(),
        "source" => source,
        "mode" => mode.metric_label(),
        "outcome" => if success { "success" } else { "failure" },
        "error_code" => error_code.unwrap_or("none"),
    )
    .increment(1);

    if success {
        record_policy_active(source, mode);
    }
}

pub fn record_policy_active(source: &'static str, mode: PolicyMode) {
    metrics_crate::gauge!(
        ObservabilityMetricName::PolicyActive.as_str(),
        "source" => source,
        "mode" => mode.metric_label(),
    )
    .set(1.0);
}

pub fn record_policy_wasm_eval(
    source: &'static str,
    mode: PolicyMode,
    hook: PolicyHookPoint,
    outcome: PolicyOutcome,
    error_code: Option<&'static str>,
    duration: Duration,
) {
    metrics_crate::counter!(
        ObservabilityMetricName::PolicyWasmEvalTotal.as_str(),
        "source" => source,
        "mode" => mode.metric_label(),
        "hook" => hook.metric_label(),
        "outcome" => outcome.metric_label(),
        "error_code" => error_code.unwrap_or("none"),
    )
    .increment(1);

    metrics_crate::histogram!(
        ObservabilityMetricName::PolicyWasmEvalDurationMs.as_str(),
        "source" => source,
        "mode" => mode.metric_label(),
        "hook" => hook.metric_label(),
        "outcome" => outcome.metric_label(),
    )
    .record(duration.as_secs_f64() * 1_000.0);
}

pub fn record_drain_state(state: DrainState) {
    for candidate in [
        DrainState::Accepting,
        DrainState::Draining,
        DrainState::Drained,
    ] {
        metrics_crate::gauge!(
            OperationalMetricName::DrainState.as_str(),
            "state" => candidate.metric_label()
        )
        .set(if candidate == state { 1.0 } else { 0.0 });
    }
}

pub fn record_health_status(kind: HealthKind, status: HealthStatus) {
    for candidate in [
        HealthStatus::Ready,
        HealthStatus::NotReady,
        HealthStatus::Live,
        HealthStatus::Degraded,
    ] {
        metrics_crate::gauge!(
            OperationalMetricName::HealthStatus.as_str(),
            "kind" => kind.metric_label(),
            "status" => candidate.metric_label()
        )
        .set(if candidate == status { 1.0 } else { 0.0 });
    }
}

pub fn record_socket_option<S: MetricLabelValue, O: MetricLabelValue>(
    socket_kind: S,
    option: O,
    outcome: SocketOptionOutcome,
) {
    metrics_crate::counter!(
        OperationalMetricName::SocketOptionTotal.as_str(),
        "socket" => socket_kind.metric_label(),
        "option" => option.metric_label(),
        "outcome" => outcome.metric_label()
    )
    .increment(1);
}

fn describe_metrics() {
    for descriptor in metric_catalog() {
        describe_metric(descriptor);
    }
    metrics_crate::describe_counter!(
        "pg_kinetic_pool_evictions_total",
        "Pooled backend evictions by lifecycle reason."
    );
    metrics_crate::describe_gauge!(
        "pg_kinetic_pool_connections",
        "Current pooled backend connections by state."
    );
}

fn describe_metric(descriptor: &MetricDescriptor) {
    match descriptor.kind {
        MetricKind::Counter => {
            metrics_crate::describe_counter!(descriptor.name, descriptor.description);
        }
        MetricKind::Gauge => {
            metrics_crate::describe_gauge!(descriptor.name, descriptor.description);
        }
        MetricKind::Histogram => {
            metrics_crate::describe_histogram!(descriptor.name, descriptor.description);
        }
    }
}

fn shard_bucket_label(shard: Option<&str>) -> String {
    match shard {
        Some(shard) => format!("bucket_{}", shard_bucket(shard)),
        None => String::from("unassigned"),
    }
}

fn shard_bucket(value: &str) -> u8 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    (hash % 8) as u8
}

fn route_map_reload_error_code_label(error_code: Option<RouteMapReloadErrorCode>) -> &'static str {
    match error_code {
        Some(error_code) => error_code.as_str(),
        None => "none",
    }
}

fn freshness_outcome_label(outcome: FreshnessStatus) -> &'static str {
    match outcome {
        FreshnessStatus::Satisfied => "satisfied",
        FreshnessStatus::Waiting => "waiting",
        FreshnessStatus::Stale => "stale",
        FreshnessStatus::Unknown => "unknown",
        FreshnessStatus::Unavailable => "unavailable",
    }
}

fn endpoint_label(endpoint_id: u64) -> String {
    endpoint_id.to_string()
}

fn route_query_class_label(query_class: RouteQueryClass) -> &'static str {
    match query_class {
        RouteQueryClass::Default => "default",
        RouteQueryClass::Read => "read",
        RouteQueryClass::Write => "write",
        RouteQueryClass::Maintenance => "maintenance",
    }
}

fn target_role_label(target_role: Option<BackendRole>) -> &'static str {
    match target_role {
        Some(role) => role.as_str(),
        None => "unknown",
    }
}

fn fallback_policy_from_reason(reason: ProxyRoutingReason) -> Option<FallbackPolicy> {
    match reason {
        ProxyRoutingReason::FallbackPrimary => Some(FallbackPolicy::Primary),
        ProxyRoutingReason::FallbackReject => Some(FallbackPolicy::Reject),
        ProxyRoutingReason::FallbackWait => Some(FallbackPolicy::Wait),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueueScope {
    Route,
}

impl QueueScope {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Route => "route",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationalMetricName {
    TlsHandshakesTotal,
    TlsFailuresTotal,
    AuthAttemptsTotal,
    AuthFailuresTotal,
    ConfigReloadTotal,
    DrainState,
    HealthStatus,
    SocketOptionTotal,
}

impl OperationalMetricName {
    const fn as_str(self) -> &'static str {
        match self {
            Self::TlsHandshakesTotal => "pg_kinetic_tls_handshakes_total",
            Self::TlsFailuresTotal => "pg_kinetic_tls_failures_total",
            Self::AuthAttemptsTotal => "pg_kinetic_auth_attempts_total",
            Self::AuthFailuresTotal => "pg_kinetic_auth_failures_total",
            Self::ConfigReloadTotal => "pg_kinetic_config_reload_total",
            Self::DrainState => "pg_kinetic_drain_state",
            Self::HealthStatus => "pg_kinetic_health_status",
            Self::SocketOptionTotal => "pg_kinetic_socket_option_total",
        }
    }
}

pub trait MetricLabelValue {
    fn metric_label(self) -> &'static str;
}

impl MetricLabelValue for TlsScope {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Backend => "backend",
        }
    }
}

impl MetricLabelValue for ClientTlsMode {
    fn metric_label(self) -> &'static str {
        self.as_str()
    }
}

impl MetricLabelValue for BackendTlsMode {
    fn metric_label(self) -> &'static str {
        self.as_str()
    }
}

impl MetricLabelValue for AuthMode {
    fn metric_label(self) -> &'static str {
        self.as_str()
    }
}

impl MetricLabelValue for PolicyMode {
    fn metric_label(self) -> &'static str {
        self.as_str()
    }
}

impl MetricLabelValue for PolicyHookPoint {
    fn metric_label(self) -> &'static str {
        self.as_str()
    }
}

impl MetricLabelValue for PolicyAction {
    fn metric_label(self) -> &'static str {
        self.as_str()
    }
}

impl MetricLabelValue for PolicyOutcome {
    fn metric_label(self) -> &'static str {
        self.as_str()
    }
}

impl MetricLabelValue for PolicyDecisionReason {
    fn metric_label(self) -> &'static str {
        self.as_str()
    }
}

impl MetricLabelValue for DrainState {
    fn metric_label(self) -> &'static str {
        self.as_str()
    }
}

impl MetricLabelValue for HealthStatus {
    fn metric_label(self) -> &'static str {
        self.as_str()
    }
}

impl MetricLabelValue for SocketOptionOutcome {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Unsupported => "unsupported",
            Self::Failed => "failed",
        }
    }
}

impl MetricLabelValue for TlsFailureReason {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Denied => "denied",
            Self::HandshakeError => "handshake_error",
            Self::VerificationFailed => "verification_failed",
            Self::IoError => "io_error",
        }
    }
}

impl MetricLabelValue for AuthFailureReason {
    fn metric_label(self) -> &'static str {
        match self {
            Self::UnknownUser => "unknown_user",
            Self::PasswordRequired => "password_required",
            Self::InvalidPassword => "invalid_password",
            Self::ProtocolError => "protocol_error",
            Self::IoError => "io_error",
        }
    }
}

impl MetricLabelValue for ReloadOutcome {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Rejected => "rejected",
            Self::Unchanged => "unchanged",
            Self::Error => "error",
        }
    }
}

impl MetricLabelValue for HealthKind {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Process => "process",
            Self::Ready => "ready",
            Self::Backend => "backend",
        }
    }
}

impl MetricLabelValue for SocketKind {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Backend => "backend",
        }
    }
}

impl MetricLabelValue for SocketOption {
    fn metric_label(self) -> &'static str {
        match self {
            Self::TcpNodelay => "tcp_nodelay",
            Self::TcpKeepalive => "tcp_keepalive",
            Self::TcpUserTimeout => "tcp_user_timeout",
            Self::TcpSendBufferBytes => "tcp_send_buffer_bytes",
            Self::TcpRecvBufferBytes => "tcp_recv_buffer_bytes",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlsScope {
    Client,
    Backend,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlsFailureReason {
    Denied,
    HandshakeError,
    VerificationFailed,
    IoError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthFailureReason {
    UnknownUser,
    PasswordRequired,
    InvalidPassword,
    ProtocolError,
    IoError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReloadOutcome {
    Applied,
    Rejected,
    Unchanged,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HealthKind {
    Process,
    Ready,
    Backend,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketKind {
    Client,
    Backend,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketOption {
    TcpNodelay,
    TcpKeepalive,
    TcpUserTimeout,
    TcpSendBufferBytes,
    TcpRecvBufferBytes,
}
