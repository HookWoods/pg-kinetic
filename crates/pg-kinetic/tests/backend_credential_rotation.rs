use std::sync::Mutex;

use pg_kinetic::proxy_runtime::auth::{BackendCredentialProvider, EnvironmentCredentialProvider};

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn new_backend_connections_use_reloaded_service_credentials() {
    let _guard = ENV_LOCK.lock().expect("credential environment lock");
    let variable = "PG_KINETIC_TEST_BACKEND_PASSWORD";
    let provider = EnvironmentCredentialProvider::new("pool_user", variable);

    std::env::set_var(variable, "initial-secret");
    assert_eq!(
        provider
            .credentials()
            .expect("initial credentials")
            .password(),
        "initial-secret"
    );

    std::env::set_var(variable, "next-secret");
    assert_eq!(
        provider
            .credentials()
            .expect("rotated credentials")
            .password(),
        "next-secret"
    );

    std::env::remove_var(variable);
}
