use std::{mem::size_of, path::PathBuf};

use pg_kinetic::core::{
    performance::{
        DerivedPerformanceMetric, ProcessMetricKind, ProcessMetricSample, ProcessMetricValue,
    },
    session::SessionState,
};
use pg_kinetic_proxy::{
    benchmark::validate_benchmark_scenario,
    buffers::{BufferReusePolicy, OversizedBufferPolicy, ProxyBufferPool},
};

fn idle_scenario_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace crates directory")
        .parent()
        .expect("workspace root")
        .join("bench")
        .join("scenarios")
        .join("benchmark-idle-clients.toml")
}

fn test_buffer_pool() -> ProxyBufferPool {
    ProxyBufferPool::new(
        BufferReusePolicy {
            initial_capacity: 64,
            max_cached_sessions: 2_048,
        },
        OversizedBufferPolicy {
            max_retained_capacity: 128,
        },
    )
}

#[test]
fn idle_session_state_is_compact_and_unpinned() {
    assert!(size_of::<SessionState>() <= 64);
    assert_eq!(SessionState::default().pin_reason(), None);
}

#[test]
fn idle_client_leases_defer_buffer_allocation() {
    let pool = test_buffer_pool();
    let leases: Vec<_> = (0..1_024).map(|_| pool.acquire()).collect();

    assert_eq!(pool.stats().sessions_created, 1_024);
    assert_eq!(pool.stats().allocations, 0);
    assert_eq!(pool.stats().allocation_bytes, 0);

    drop(leases);
}

#[test]
fn idle_client_leases_reuse_empty_buffer_sets() {
    let pool = test_buffer_pool();
    let first: Vec<_> = (0..64).map(|_| pool.acquire()).collect();
    drop(first);

    let second: Vec<_> = (0..64).map(|_| pool.acquire()).collect();
    let stats = pool.stats();

    assert_eq!(stats.sessions_created, 64);
    assert_eq!(stats.sessions_reused, 64);

    drop(second);
}

#[test]
fn idle_cleanup_releases_oversized_session_buffers() {
    let pool = test_buffer_pool();
    let mut lease = pool.acquire();
    lease
        .buffers_mut()
        .append_frontend_frame(b'Q', &[b'x'; 1_024]);
    lease.buffers_mut().clear_backend_write();
    drop(lease);

    let mut reused = pool.acquire();
    assert!(reused.buffers_mut().capacities()[2] <= 128);
    assert_eq!(pool.stats().oversized_buffers_released, 1);
}

#[test]
fn idle_client_memory_estimate_is_derived_per_client() {
    let before = ProcessMetricSample::new(
        0,
        [(
            ProcessMetricKind::ResidentMemory,
            ProcessMetricValue::Integer(1_000),
        )],
    );
    let after = ProcessMetricSample::new(
        1,
        [(
            ProcessMetricKind::ResidentMemory,
            ProcessMetricValue::Integer(9_192),
        )],
    );

    assert_eq!(
        DerivedPerformanceMetric::memory_per_client(&before, &after, 1_024).value(),
        Some(8.0)
    );
}

#[test]
fn bounded_idle_client_scenario_validates_without_a_large_local_run() {
    let scenario = validate_benchmark_scenario(&idle_scenario_path()).expect("scenario parses");

    assert_eq!(scenario.connections().connection_count(), 128);
    assert!(scenario.expected_metrics().memory());
}
