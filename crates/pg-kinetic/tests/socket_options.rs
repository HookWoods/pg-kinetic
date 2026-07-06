use pg_kinetic::{
    config::SocketConfig,
    proxy_runtime::socket::{apply_socket_options, SocketOptionOutcome, SocketOptions},
};
use tokio::net::{TcpListener, TcpStream};

async fn connected_stream() -> TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let listen_addr = listener.local_addr().expect("listener addr");

    tokio::spawn(async move {
        let _ = listener.accept().await.expect("accept client");
    });

    TcpStream::connect(listen_addr)
        .await
        .expect("connect client")
}

fn basic_socket_config() -> SocketConfig {
    SocketConfig {
        tcp_nodelay: true,
        tcp_keepalive: true,
        tcp_keepalive_idle_ms: Some(1_000),
        tcp_keepalive_interval_ms: Some(2_000),
        tcp_keepalive_retries: Some(3),
        tcp_user_timeout_ms: None,
        tcp_send_buffer_bytes: Some(4_096),
        tcp_recv_buffer_bytes: Some(8_192),
        strict_socket_option_mode: false,
    }
}

#[tokio::test]
async fn applies_supported_socket_options() {
    let stream = connected_stream().await;
    let options = SocketOptions::from(&basic_socket_config());

    let report = apply_socket_options(&stream, &options, "test").expect("socket options apply");

    assert_eq!(report.tcp_nodelay, SocketOptionOutcome::Applied);
    assert_eq!(report.tcp_keepalive, SocketOptionOutcome::Applied);
    assert_eq!(report.tcp_send_buffer_bytes, SocketOptionOutcome::Applied);
    assert_eq!(report.tcp_recv_buffer_bytes, SocketOptionOutcome::Applied);
    assert!(stream.nodelay().expect("read TCP_NODELAY"));
}

#[cfg(windows)]
#[tokio::test]
async fn unsupported_socket_options_do_not_crash_without_strict_mode() {
    let stream = connected_stream().await;
    let mut config = basic_socket_config();
    config.tcp_user_timeout_ms = Some(1_500);

    let report = apply_socket_options(&stream, &SocketOptions::from(&config), "test")
        .expect("socket options apply");

    assert_eq!(report.tcp_user_timeout, SocketOptionOutcome::Unsupported);
    assert_eq!(report.tcp_nodelay, SocketOptionOutcome::Applied);
}

#[cfg(windows)]
#[tokio::test]
async fn strict_mode_fails_on_unsupported_socket_options() {
    let stream = connected_stream().await;
    let mut config = basic_socket_config();
    config.tcp_user_timeout_ms = Some(1_500);
    config.strict_socket_option_mode = true;

    let error = apply_socket_options(&stream, &SocketOptions::from(&config), "test")
        .expect_err("strict mode should fail");

    let error = error.to_string();
    assert!(
        error.contains("tcp_user_timeout") || error.contains("unsupported"),
        "unexpected error: {error}"
    );
}
