use std::{net::SocketAddr, time::Duration};

use pg_kinetic::{
    config::Config,
    core::runtime::{RuntimeEngine, ShutdownReason},
    proxy::Proxy,
    proxy_runtime::snapshot::SnapshotStore,
};
use tokio::{net::TcpListener, time};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stable_thread_per_core_publishes_shards_and_drains() {
    let mut config = Config::default();
    config.connection.listen_addr = free_port().await;
    config.drain.drain_timeout_ms = 100;
    config.runtime.lifecycle.shutdown_grace_ms = 100;
    config.runtime.engine.runtime_engine = RuntimeEngine::ThreadPerCore;
    config.runtime.engine.runtime_shards = Some(2);

    let proxy = Proxy::new(config);
    let lifecycle = proxy.lifecycle_controller();
    let snapshot_store = proxy.snapshot_store();
    let runtime_thread = std::thread::spawn(move || proxy.run_thread_per_core());

    let rows = wait_for_runtime_shards(&snapshot_store, 2).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].shard_id, 0);
    assert_eq!(rows[1].shard_id, 1);
    assert!(rows.iter().all(|row| {
        row.lifecycle_state == pg_kinetic::core::runtime::RuntimeLifecycleState::Ready
    }));

    assert!(lifecycle.begin_drain(ShutdownReason::AdminRequest));
    let runtime_result = time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || runtime_thread.join().expect("runtime thread")),
    )
    .await
    .expect("runtime thread drains")
    .expect("join runtime thread");
    runtime_result.expect("thread-per-core runtime exits cleanly");

    let stopped_rows = snapshot_store.runtime_shard_snapshots();
    assert_eq!(stopped_rows.len(), 2);
    assert!(stopped_rows.iter().all(|row| {
        row.lifecycle_state == pg_kinetic::core::runtime::RuntimeLifecycleState::Stopped
    }));
}

async fn wait_for_runtime_shards(
    snapshot_store: &SnapshotStore,
    expected_rows: usize,
) -> Vec<pg_kinetic::proxy_runtime::snapshot::RuntimeShardSnapshot> {
    let deadline = time::Instant::now() + Duration::from_secs(5);

    while time::Instant::now() < deadline {
        let rows = snapshot_store.runtime_shard_snapshots();
        if rows.len() == expected_rows {
            return rows;
        }
        time::sleep(Duration::from_millis(25)).await;
    }

    panic!(
        "runtime shards did not become visible: {} rows",
        snapshot_store.runtime_shard_snapshots().len()
    );
}

async fn free_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind free port");
    let addr = listener.local_addr().expect("free addr");
    drop(listener);
    addr
}
