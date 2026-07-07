use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use bytes::{BufMut, BytesMut};
use pg_kinetic::{
    config::{SocketConfig, TlsConfig},
    core::{
        ha::{EndpointHealth, EndpointRoleState},
        routing::BackendRole,
    },
    proxy_runtime::health::EndpointHealthProbe,
    wire::{frame::parse_frontend_frame, message::parse_simple_query},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time,
};

#[tokio::test]
async fn healthy_primary_reports_pg_is_in_recovery_false() {
    let backend_addr = spawn_backend(vec![ProbePlan::Healthy {
        observed_role: BackendRole::Primary,
    }])
    .await;
    let probe = probe(backend_addr, BackendRole::Primary);

    let snapshot = probe.probe_once().await;
    assert_eq!(snapshot.health.state, EndpointHealth::Healthy);
    assert_eq!(snapshot.role.state, EndpointRoleState::Primary);
    assert!(snapshot.role.warning.is_none());
}

#[tokio::test]
async fn healthy_replica_reports_pg_is_in_recovery_true() {
    let backend_addr = spawn_backend(vec![ProbePlan::Healthy {
        observed_role: BackendRole::Replica,
    }])
    .await;
    let probe = probe(backend_addr, BackendRole::Replica);

    let snapshot = probe.probe_once().await;
    assert_eq!(snapshot.health.state, EndpointHealth::Healthy);
    assert_eq!(snapshot.role.state, EndpointRoleState::Replica);
    assert!(snapshot.role.warning.is_none());
}

#[tokio::test]
async fn replica_marked_unhealthy_after_failed_probe() {
    let backend_addr = spawn_backend(vec![ProbePlan::Failure]).await;
    let probe = probe(backend_addr, BackendRole::Replica);

    let snapshot = probe.probe_once().await;
    assert_eq!(snapshot.health.state, EndpointHealth::Unhealthy);
    assert_eq!(snapshot.health.consecutive_failures, 1);
    assert_eq!(snapshot.role.state, EndpointRoleState::Unknown);
}

#[tokio::test]
async fn primary_marked_warning_if_it_reports_recovery_mode() {
    let backend_addr = spawn_backend(vec![ProbePlan::Healthy {
        observed_role: BackendRole::Replica,
    }])
    .await;
    let probe = probe(backend_addr, BackendRole::Primary);

    let snapshot = probe.probe_once().await;
    assert_eq!(snapshot.health.state, EndpointHealth::Healthy);
    assert_eq!(snapshot.role.state, EndpointRoleState::Warning);
    let warning = snapshot.role.warning.expect("warning");
    assert_eq!(warning.expected_role, BackendRole::Primary);
    assert_eq!(warning.observed_role, BackendRole::Replica);
}

#[tokio::test]
async fn replica_marked_warning_if_it_reports_primary_role() {
    let backend_addr = spawn_backend(vec![ProbePlan::Healthy {
        observed_role: BackendRole::Primary,
    }])
    .await;
    let probe = probe(backend_addr, BackendRole::Replica);

    let snapshot = probe.probe_once().await;
    assert_eq!(snapshot.health.state, EndpointHealth::Healthy);
    assert_eq!(snapshot.role.state, EndpointRoleState::Warning);
    let warning = snapshot.role.warning.expect("warning");
    assert_eq!(warning.expected_role, BackendRole::Replica);
    assert_eq!(warning.observed_role, BackendRole::Primary);
}

#[tokio::test]
async fn health_timeout_marks_endpoint_degraded() {
    let backend_addr = spawn_backend(vec![ProbePlan::Timeout]).await;
    let probe = probe_with_timeout(
        backend_addr,
        BackendRole::Replica,
        Duration::from_millis(40),
    );

    let snapshot = probe.probe_once().await;
    assert_eq!(snapshot.health.state, EndpointHealth::Degraded);
    assert_eq!(snapshot.health.consecutive_failures, 1);
    assert_eq!(snapshot.role.state, EndpointRoleState::Unknown);
}

#[tokio::test]
async fn repeated_failures_mark_endpoint_unavailable() {
    let backend_addr = spawn_backend(vec![
        ProbePlan::Failure,
        ProbePlan::Failure,
        ProbePlan::Failure,
    ])
    .await;
    let probe = probe(backend_addr, BackendRole::Replica);

    let first = probe.probe_once().await;
    assert_eq!(first.health.state, EndpointHealth::Unhealthy);

    let second = probe.probe_once().await;
    assert_eq!(second.health.state, EndpointHealth::Unhealthy);

    let third = probe.probe_once().await;
    assert_eq!(third.health.state, EndpointHealth::Unavailable);
    assert_eq!(third.health.consecutive_failures, 3);
}

#[tokio::test]
async fn recovery_after_successful_probes_is_reported() {
    let backend_addr = spawn_backend(vec![
        ProbePlan::Failure,
        ProbePlan::Healthy {
            observed_role: BackendRole::Primary,
        },
    ])
    .await;
    let probe = probe(backend_addr, BackendRole::Primary);

    let failed = probe.probe_once().await;
    assert_eq!(failed.health.state, EndpointHealth::Unhealthy);
    assert!(!failed.health.recovered);

    let recovered = probe.probe_once().await;
    assert_eq!(recovered.health.state, EndpointHealth::Healthy);
    assert!(recovered.health.recovered);
    assert_eq!(recovered.health.consecutive_failures, 0);
    assert!(recovered.last_error.is_none());
}

#[derive(Clone, Debug)]
enum ProbePlan {
    Healthy { observed_role: BackendRole },
    Failure,
    Timeout,
}

fn probe(addr: SocketAddr, expected_role: BackendRole) -> Arc<EndpointHealthProbe> {
    probe_with_timeout(addr, expected_role, Duration::from_millis(75))
}

fn probe_with_timeout(
    addr: SocketAddr,
    expected_role: BackendRole,
    probe_timeout: Duration,
) -> Arc<EndpointHealthProbe> {
    EndpointHealthProbe::new(
        1,
        addr,
        expected_role,
        "postgres",
        "postgres",
        TlsConfig::default(),
        SocketConfig::default(),
        Duration::from_millis(10),
        probe_timeout,
    )
}

async fn spawn_backend(plans: Vec<ProbePlan>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let addr = listener.local_addr().expect("backend addr");
    let plans = Arc::new(StdMutex::new(VecDeque::from(plans)));

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept backend");
            let plan = plans
                .lock()
                .expect("backend plans poisoned")
                .pop_front()
                .unwrap_or(ProbePlan::Healthy {
                    observed_role: BackendRole::Primary,
                });

            tokio::spawn(async move {
                handle_probe_connection(stream, plan).await;
            });
        }
    });

    addr
}

async fn handle_probe_connection(mut stream: TcpStream, plan: ProbePlan) {
    if read_startup_packet(&mut stream).await.is_err() {
        return;
    }

    match plan {
        ProbePlan::Failure => {
            let _ = stream.shutdown().await;
        }
        ProbePlan::Timeout => {
            time::sleep(Duration::from_millis(200)).await;
        }
        ProbePlan::Healthy { observed_role } => {
            let response = if observed_role == BackendRole::Replica {
                "t"
            } else {
                "f"
            };

            let mut buffer = BytesMut::with_capacity(4_096);
            loop {
                let read = match stream.read_buf(&mut buffer).await {
                    Ok(read) => read,
                    Err(_) => return,
                };
                if read == 0 {
                    return;
                }

                while let Some(frame) =
                    parse_frontend_frame(&mut buffer).expect("parse frontend frame")
                {
                    if let Some(query) = parse_simple_query(&frame).expect("parse simple query") {
                        let normalized = query.trim().to_ascii_lowercase();
                        match normalized.as_str() {
                            "select 1" => {
                                if stream.write_all(&ready_for_query()).await.is_err() {
                                    return;
                                }
                            }
                            "select pg_is_in_recovery()" => {
                                if stream.write_all(&data_row(response)).await.is_err() {
                                    return;
                                }
                                if stream.write_all(&ready_for_query()).await.is_err() {
                                    return;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
}

async fn read_startup_packet(stream: &mut TcpStream) -> std::io::Result<()> {
    let mut len_bytes = [0_u8; 4];
    stream.read_exact(&mut len_bytes).await?;
    let len = i32::from_be_bytes(len_bytes);
    if len < 8 {
        return Ok(());
    }

    let mut remaining = vec![0_u8; len as usize - 4];
    stream.read_exact(&mut remaining).await?;
    Ok(())
}

fn data_row(value: &str) -> BytesMut {
    let mut payload = BytesMut::new();
    payload.put_i16(1);
    payload.put_i32(value.len() as i32);
    payload.extend_from_slice(value.as_bytes());
    encode_backend_message(b'D', payload)
}

fn ready_for_query() -> BytesMut {
    let mut payload = BytesMut::new();
    payload.put_u8(b'I');
    encode_backend_message(b'Z', payload)
}

fn encode_backend_message(tag: u8, payload: BytesMut) -> BytesMut {
    let mut frame = BytesMut::new();
    frame.put_u8(tag);
    frame.put_i32((payload.len() + 4) as i32);
    frame.extend_from_slice(&payload);
    frame
}
