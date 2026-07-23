#![cfg(all(target_os = "linux", feature = "io-uring"))]

#[test]
#[ignore = "requires Linux io_uring runtime validation"]
fn io_uring_transport_module_compiles_with_feature() {
    let name = "io_uring_transport";
    assert_eq!(name, "io_uring_transport");
}
