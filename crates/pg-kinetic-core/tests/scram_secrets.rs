use pg_kinetic_core::secrets::{ScramVerifier, SecretError, UserSecret, UserStore};

#[test]
fn parses_postgres_style_scram_verifier_strings() {
    let verifier = ScramVerifier::parse(
        "SCRAM-SHA-256$4096:c2FsdHlzYWx0$RdRL9M4hIQ6KSGRy8YdcY/rWTt9c53a35goFQzcrGXw=:lNY6toUrz5jlkvLtdJbAj5bXIomZuncUbgsZq5rYF5M=",
    )
    .expect("valid verifier");

    assert_eq!(verifier.iterations, 4096);
    assert_eq!(verifier.salt, b"saltysalt");
    assert!(verifier.verify_password(b"pencil"));
    assert!(!verifier.verify_password(b"wrong-password"));
    assert_eq!(
        verifier.to_postgres_verifier(),
        "SCRAM-SHA-256$4096:c2FsdHlzYWx0$RdRL9M4hIQ6KSGRy8YdcY/rWTt9c53a35goFQzcrGXw=:lNY6toUrz5jlkvLtdJbAj5bXIomZuncUbgsZq5rYF5M="
    );
}

#[test]
fn rejects_malformed_verifier_strings() {
    for verifier in [
        "",
        "SCRAM-SHA-256",
        "SCRAM-SHA-256$0:c2FsdHlzYWx0$bad:bad",
        "SCRAM-SHA-256$4096:not-base64$bad:bad",
        "SCRAM-SHA-1$4096:c2FsdHlzYWx0$bad:bad",
        "SCRAM-SHA-256$4096:c2FsdHlzYWx0$bad",
    ] {
        assert!(ScramVerifier::parse(verifier).is_err(), "{verifier}");
    }
}

#[test]
fn user_lookup_is_case_sensitive_by_default() {
    let mut store = UserStore::new();
    store.insert("alice", UserSecret::Trust);

    assert!(matches!(store.get("alice"), Some(UserSecret::Trust)));
    assert!(store.get("Alice").is_none());
    assert!(store.is_case_sensitive());
}

#[test]
fn user_lookup_can_be_case_insensitive_when_configured() {
    let mut store = UserStore::case_insensitive();
    store.insert("alice", UserSecret::Trust);

    assert!(matches!(store.get("Alice"), Some(UserSecret::Trust)));
    assert!(!store.is_case_sensitive());
}

#[test]
fn generates_nonce_using_url_safe_text() {
    let nonce = pg_kinetic_core::secrets::generate_nonce().expect("nonce");
    assert!(!nonce.is_empty());
    assert!(!nonce.contains('='));
}

#[test]
fn rejects_invalid_key_lengths() {
    let error = ScramVerifier::parse(
        "SCRAM-SHA-256$4096:c2FsdHlzYWx0$YWJjZA==:YWJjZA==",
    )
    .expect_err("invalid key lengths");

    assert!(matches!(
        error,
        SecretError::InvalidKeyLength {
            field: "stored_key",
            ..
        }
    ));
}
