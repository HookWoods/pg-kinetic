use pg_kinetic::core::{
    recovery::{RecoveryAction, RecoveryTrigger},
    route::{QueryClass, RouteKey},
    security::AuthMode,
};
use pg_kinetic::{
    config::Config,
    proxy_runtime::telemetry::{
        auth_span, build_otel_tracer_provider, checkout_span, close_span, query_span,
        recovery_span, rows_span, startup_span,
    },
};
use std::net::SocketAddr;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Registry;

#[test]
fn otel_is_disabled_by_default_and_builds_a_noop_provider() {
    let config = Config::try_parse_from_args(["pg-kinetic"]).expect("defaults parse");

    assert!(!config.observability.otel_enabled);

    let provider = build_otel_tracer_provider(&config.observability).expect("build provider");
    drop(provider);
}

#[test]
fn otel_endpoint_and_service_name_are_parsed() {
    let config = Config::try_parse_from_args([
        "pg-kinetic",
        "--otel-enabled",
        "--otel-endpoint",
        "http://otel.example.com:4318",
        "--otel-service-name",
        "pg-kinetic-proxy",
    ])
    .expect("flags parse");

    assert!(config.observability.otel_enabled);
    assert_eq!(
        config.observability.otel_endpoint,
        Some(String::from("http://otel.example.com:4318"))
    );
    assert_eq!(config.observability.otel_service_name, "pg-kinetic-proxy");

    let provider = build_otel_tracer_provider(&config.observability).expect("build provider");
    drop(provider);
}

#[test]
fn trace_sampling_ratio_is_clamped_between_zero_and_one() {
    let high = Config::try_parse_from_args(["pg-kinetic", "--debug-trace-sampling-rate", "2.5"])
        .expect("high rate parse");
    assert_eq!(high.observability.trace_sampling_ratio(), 1.0);

    let low = Config::try_parse_from_args(["pg-kinetic", "--debug-trace-sampling-rate=-0.25"])
        .expect("low rate parse");
    assert_eq!(low.observability.trace_sampling_ratio(), 0.0);
}

#[test]
fn span_helpers_exclude_sql_text_and_secret_labels() {
    let forbidden_labels = [
        "sql",
        "sql_text",
        "query",
        "query_text",
        "statement",
        "password",
        "secret",
    ];

    let _ = Registry::default().try_init();

    {
        let route = RouteKey::new(
            "postgres",
            "pgkinetic",
            Some("api"),
            Some(
                "127.0.0.1:5432"
                    .parse::<SocketAddr>()
                    .expect("socket address"),
            ),
            QueryClass::Write,
        );

        let spans = [
            startup_span(),
            auth_span(AuthMode::ScramSha256),
            checkout_span(&route),
            query_span(&route, QueryClass::Write),
            rows_span(12),
            recovery_span(
                RecoveryTrigger::AbandonedTransaction,
                RecoveryAction::Rollback,
            ),
            close_span(),
        ];

        for span in spans {
            let metadata = span.metadata().expect("span metadata");
            let field_names = metadata
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>();

            for forbidden in forbidden_labels {
                assert!(
                    !field_names.iter().any(|name| name == &forbidden),
                    "span {} exposed forbidden label {forbidden}",
                    metadata.name()
                );
            }
        }
    }
}
