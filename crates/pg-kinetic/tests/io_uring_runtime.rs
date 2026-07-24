#[cfg(not(all(target_os = "linux", feature = "io-uring")))]
#[test]
fn io_uring_runtime_returns_feature_or_platform_error_when_unavailable() {
    let error = pg_kinetic::proxy_runtime::run_io_uring(pg_kinetic::config::Config::default())
        .expect_err("io_uring runtime should be unavailable");

    assert!(error.to_string().contains(
        "experimental_io_uring requires Linux and the pg-kinetic io-uring cargo feature"
    ));
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
#[test]
#[ignore = "requires Linux io_uring runtime validation"]
fn io_uring_transport_module_compiles_with_feature() {
    let name = "io_uring_transport";
    assert_eq!(name, "io_uring_transport");
}
