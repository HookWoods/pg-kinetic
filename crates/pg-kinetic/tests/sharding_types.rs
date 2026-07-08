use bytes::Bytes;
use pg_kinetic_core::{
    route::{QueryClass, RouteKey},
    routing::BackendRole,
    sharding::{
        MultiShardPolicy, ShardId, ShardKey, ShardKeyType, ShardRouteReason, ShardStrategy,
        ShardTarget, ShardValidationError,
    },
};
use std::net::SocketAddr;

#[test]
fn shard_id_rejects_empty_ids() {
    assert_eq!(ShardId::new(""), Err(ShardValidationError::EmptyShardId));
}

#[test]
fn shard_id_preserves_stable_string_labels() {
    let shard_id = ShardId::new("tenant-east-01").expect("valid shard id");

    assert_eq!(shard_id.as_str(), "tenant-east-01");
    assert_eq!(shard_id.to_string(), "tenant-east-01");
}

#[test]
fn shard_key_supports_text_integer_and_bytes_values() {
    let text_key = ShardKey::text("tenant-a");
    let integer_key = ShardKey::integer(42);
    let bytes_key = ShardKey::bytes(Bytes::from_static(b"\x01\x02"));

    assert_eq!(text_key.key_type(), ShardKeyType::Text);
    assert_eq!(text_key.as_text(), Some("tenant-a"));

    assert_eq!(integer_key.key_type(), ShardKeyType::Integer);
    assert_eq!(integer_key.as_integer(), Some(42));

    assert_eq!(bytes_key.key_type(), ShardKeyType::Bytes);
    assert_eq!(bytes_key.as_bytes(), Some(&b"\x01\x02"[..]));
}

#[test]
fn shard_strategy_labels_are_stable() {
    assert_eq!(ShardStrategy::Hash.as_str(), "hash");
    assert_eq!(ShardStrategy::Range.as_str(), "range");
    assert_eq!(ShardStrategy::List.as_str(), "list");
}

#[test]
fn shard_target_records_route_key_backend_role_and_shard_id() {
    let client_addr = "127.0.0.1:5000".parse::<SocketAddr>().ok();
    let route_key = RouteKey::new(
        "pgkinetic",
        "postgres",
        Some("api"),
        client_addr,
        QueryClass::Default,
    );
    let shard_id = ShardId::new("shard-01").expect("valid shard id");
    let target = ShardTarget::new(route_key.clone(), BackendRole::Replica, shard_id.clone());

    assert_eq!(target.route_key(), &route_key);
    assert_eq!(target.backend_role(), BackendRole::Replica);
    assert_eq!(target.shard_id(), &shard_id);
}

#[test]
fn multi_shard_policy_defaults_to_reject() {
    assert_eq!(MultiShardPolicy::default(), MultiShardPolicy::Reject);
}

#[test]
fn shard_route_reason_exposes_stable_admin_and_metric_labels() {
    assert_eq!(ShardRouteReason::AdminOverride.admin_label(), "admin_override");
    assert_eq!(ShardRouteReason::AdminOverride.metric_label(), "admin_override");
    assert_eq!(ShardRouteReason::MultiShardRejected.admin_label(), "multi_shard_rejected");
    assert_eq!(ShardRouteReason::MultiShardRejected.metric_label(), "multi_shard_rejected");
}
