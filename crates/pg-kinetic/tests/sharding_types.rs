use bytes::Bytes;
use pg_kinetic_core::{
    route::{QueryClass, RouteKey},
    routing::BackendRole,
    sharding::{
        deterministic_shard_hash, evaluate_shard_key, HashShardRule, ListShardRule,
        MultiShardPolicy, RangeShardRule, ShardDrainPolicy, ShardId, ShardKey, ShardKeyType,
        ShardLifecycleState, ShardMatch, ShardMigrationSafetyReport, ShardMigrationState,
        ShardRebalancePlan, ShardRouteReason, ShardStrategy, ShardStrategyEvaluator, ShardTarget,
        ShardValidationError,
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
fn hash_strategy_is_deterministic_across_evaluators() {
    let key = ShardKey::text("tenant-alpha");
    let rule = HashShardRule::new(ShardKeyType::Text, 97).expect("valid hash rule");
    let strategy_a = ShardStrategyEvaluator::hash(rule.clone());
    let strategy_b = ShardStrategyEvaluator::hash(rule);

    let match_a = evaluate_shard_key(&strategy_a, &key);
    let match_b = evaluate_shard_key(&strategy_b, &key);

    assert_eq!(match_a, match_b);
    assert_eq!(deterministic_shard_hash(&key), expected_shard_hash(&key));

    match match_a {
        ShardMatch::HashBucket { bucket } => {
            assert_eq!(bucket, (expected_shard_hash(&key) as usize) % 97);
        }
        other => panic!("unexpected hash match: {other:?}"),
    }
}

#[test]
fn range_strategy_respects_inclusive_and_exclusive_boundaries() {
    let first_range =
        RangeShardRule::with_bounds(ShardKey::integer(0), true, ShardKey::integer(10), true)
            .expect("valid inclusive range");
    let second_range =
        RangeShardRule::with_bounds(ShardKey::integer(10), false, ShardKey::integer(20), false)
            .expect("valid exclusive range");
    let strategy = ShardStrategyEvaluator::range(vec![first_range, second_range])
        .expect("valid range strategy");

    assert_eq!(
        evaluate_shard_key(&strategy, &ShardKey::integer(0)),
        ShardMatch::RangeRule { rule_index: 0 }
    );
    assert_eq!(
        evaluate_shard_key(&strategy, &ShardKey::integer(10)),
        ShardMatch::RangeRule { rule_index: 0 }
    );
    assert_eq!(
        evaluate_shard_key(&strategy, &ShardKey::integer(11)),
        ShardMatch::RangeRule { rule_index: 1 }
    );
    assert_eq!(
        evaluate_shard_key(&strategy, &ShardKey::integer(20)),
        ShardMatch::NoMatch
    );
}

#[test]
fn list_strategy_maps_explicit_values() {
    let first_rule = ListShardRule::new(vec![ShardKey::text("alpha"), ShardKey::text("beta")])
        .expect("valid list rule");
    let second_rule = ListShardRule::new(vec![ShardKey::text("gamma")]).expect("valid list rule");
    let strategy =
        ShardStrategyEvaluator::list(vec![first_rule, second_rule]).expect("valid list strategy");

    assert_eq!(
        evaluate_shard_key(&strategy, &ShardKey::text("alpha")),
        ShardMatch::ListRule { rule_index: 0 }
    );
    assert_eq!(
        evaluate_shard_key(&strategy, &ShardKey::text("beta")),
        ShardMatch::ListRule { rule_index: 0 }
    );
    assert_eq!(
        evaluate_shard_key(&strategy, &ShardKey::text("gamma")),
        ShardMatch::ListRule { rule_index: 1 }
    );
    assert_eq!(
        evaluate_shard_key(&strategy, &ShardKey::text("delta")),
        ShardMatch::NoMatch
    );
}

#[test]
fn overlapping_range_rules_are_rejected() {
    let left_range =
        RangeShardRule::with_bounds(ShardKey::integer(0), true, ShardKey::integer(10), true)
            .expect("valid range");
    let right_range =
        RangeShardRule::with_bounds(ShardKey::integer(10), true, ShardKey::integer(20), true)
            .expect("valid range");

    assert_eq!(
        ShardStrategyEvaluator::range(vec![left_range, right_range]),
        Err(ShardValidationError::OverlappingShardRules)
    );
}

#[test]
fn overlapping_list_rules_are_rejected() {
    let first_rule = ListShardRule::new(vec![ShardKey::text("alpha")]).expect("valid list rule");
    let second_rule = ListShardRule::new(vec![ShardKey::text("alpha"), ShardKey::text("beta")])
        .expect("valid list rule");

    assert_eq!(
        ShardStrategyEvaluator::list(vec![first_rule, second_rule]),
        Err(ShardValidationError::OverlappingShardRules)
    );
}

#[test]
fn empty_range_and_list_sets_are_rejected() {
    assert_eq!(
        ShardStrategyEvaluator::range(Vec::<RangeShardRule>::new()),
        Err(ShardValidationError::EmptyShardSet)
    );
    assert_eq!(
        ShardStrategyEvaluator::list(Vec::<ListShardRule>::new()),
        Err(ShardValidationError::EmptyShardSet)
    );
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
    assert_eq!(
        ShardRouteReason::AdminOverride.admin_label(),
        "admin_override"
    );
    assert_eq!(
        ShardRouteReason::AdminOverride.metric_label(),
        "admin_override"
    );
    assert_eq!(
        ShardRouteReason::MultiShardRejected.admin_label(),
        "multi_shard_rejected"
    );
    assert_eq!(
        ShardRouteReason::MultiShardRejected.metric_label(),
        "multi_shard_rejected"
    );
}

#[test]
fn shard_lifecycle_states_follow_drain_policy() {
    let read_only_drain = ShardDrainPolicy::default();

    assert_eq!(ShardLifecycleState::Active.as_str(), "active");
    assert_eq!(ShardLifecycleState::Draining.as_str(), "draining");
    assert_eq!(ShardLifecycleState::Readonly.as_str(), "readonly");
    assert_eq!(ShardLifecycleState::Disabled.as_str(), "disabled");

    assert!(ShardLifecycleState::Active.allows_reads(None));
    assert!(ShardLifecycleState::Active.allows_writes(None));

    assert!(ShardLifecycleState::Draining.allows_reads(Some(&read_only_drain)));
    assert!(!ShardLifecycleState::Draining.allows_writes(Some(&read_only_drain)));

    assert!(ShardLifecycleState::Readonly.allows_reads(None));
    assert!(!ShardLifecycleState::Readonly.allows_writes(None));

    assert!(!ShardLifecycleState::Disabled.allows_reads(None));
    assert!(!ShardLifecycleState::Disabled.allows_writes(None));
}

#[test]
fn migration_reports_and_rebalance_plans_keep_control_plane_state_only() {
    let report = ShardMigrationSafetyReport::new(
        vec![11, 3, 11],
        vec![
            String::from("stmt_z"),
            String::from("stmt_a"),
            String::from("stmt_a"),
        ],
        vec![88, 12, 88],
        Some(pg_kinetic_core::lsn::PgLsn::new(99)),
    );
    let plan = ShardRebalancePlan::new(
        vec![ShardId::new("tenant-a").expect("source shard")],
        vec![ShardId::new("tenant-b").expect("target shard")],
    )
    .with_drain_policy(ShardDrainPolicy::default())
    .with_migration_override_explicit(true)
    .with_migration_state(ShardMigrationState::Assessing)
    .with_safety_report(report.clone());

    assert_eq!(report.active_client_ids(), &[3, 11]);
    assert_eq!(
        report.prepared_statements(),
        &[String::from("stmt_a"), String::from("stmt_z")]
    );
    assert_eq!(report.open_transaction_ids(), &[12, 88]);
    assert_eq!(
        report.last_required_lsn(),
        Some(pg_kinetic_core::lsn::PgLsn::new(99))
    );

    assert_eq!(plan.source_shard_ids()[0].as_str(), "tenant-a");
    assert_eq!(plan.target_shard_ids()[0].as_str(), "tenant-b");
    assert_eq!(plan.drain_policy(), ShardDrainPolicy::default());
    assert!(plan.migration_override_explicit());
    assert_eq!(plan.migration_state(), ShardMigrationState::Assessing);
    assert_eq!(plan.safety_report().expect("migration report"), &report);
}

fn expected_shard_hash(key: &ShardKey) -> u64 {
    const SEED: u64 = 0xcbf29ce484222325;
    const NAMESPACE: &[u8] = b"pg-kinetic-shard-hash-v1";

    fn update(mut hash: u64, bytes: &[u8]) -> u64 {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }

        hash
    }

    let mut hash = SEED;
    hash = update(hash, NAMESPACE);
    hash = update(hash, &[0]);
    hash = update(hash, key.key_type().as_str().as_bytes());
    hash = update(hash, &[0]);

    match key {
        ShardKey::Text(value) => update(hash, value.as_bytes()),
        ShardKey::Integer(value) => update(hash, &value.to_le_bytes()),
        ShardKey::Bytes(value) => update(hash, value.as_ref()),
    }
}
