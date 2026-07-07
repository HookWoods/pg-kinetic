use std::str::FromStr;

use pg_kinetic_core::lsn::{FreshnessRequirement, FreshnessStatus, PgLsn, ReplicaReplayState};

#[test]
fn parses_valid_postgres_lsn_strings() {
    let lsn = PgLsn::from_str("0/16B6C50").expect("valid LSN");

    assert_eq!(lsn, PgLsn::from_parts(0, 0x16B6C50));
    assert_eq!(lsn.to_string(), "0/16B6C50");
}

#[test]
fn rejects_malformed_lsn_strings() {
    for input in [
        "",
        "0",
        "0/",
        "/16B6C50",
        "0/16B6C50/1",
        "0/16B6C5G",
        "G/16B6C50",
        "000000000/1",
    ] {
        assert!(PgLsn::from_str(input).is_err(), "{input:?} should fail");
    }
}

#[test]
fn compares_across_segment_boundaries() {
    let tail_of_first_segment = PgLsn::from_parts(0, u32::MAX);
    let head_of_next_segment = PgLsn::from_parts(1, 0);

    assert!(head_of_next_segment > tail_of_first_segment);
    assert!(PgLsn::from_parts(1, 1) > head_of_next_segment);
}

#[test]
fn formats_back_to_postgres_strings() {
    assert_eq!(PgLsn::from_parts(0, 0x16B6C50).to_string(), "0/16B6C50");
    assert_eq!(
        PgLsn::from_parts(0x1234_5678, 0x9ABC_DEF0).to_string(),
        "12345678/9ABCDEF0"
    );
}

#[test]
fn checks_replica_replay_against_session_lsn() {
    let requirement = FreshnessRequirement::session_write_lsn(PgLsn::from_parts(3, 4));

    assert!(requirement.is_satisfied_by(PgLsn::from_parts(3, 4)));
    assert!(requirement.is_satisfied_by(PgLsn::from_parts(3, 5)));
    assert!(!requirement.is_satisfied_by(PgLsn::from_parts(3, 3)));
}

#[test]
fn represents_freshness_states() {
    let requirement = FreshnessRequirement::session_write_lsn(PgLsn::from_parts(1, 0));
    let satisfied = requirement.status(ReplicaReplayState::ReplayLsn(PgLsn::from_parts(1, 0)));
    let waiting = requirement.status(ReplicaReplayState::ReplayLsn(PgLsn::from_parts(
        0,
        u32::MAX,
    )));
    let stale = requirement.status(ReplicaReplayState::Stale);
    let unknown = requirement.status(ReplicaReplayState::Unknown);
    let unavailable = requirement.status(ReplicaReplayState::Unavailable);

    assert_eq!(satisfied, FreshnessStatus::Satisfied);
    assert_eq!(waiting, FreshnessStatus::Waiting);
    assert_eq!(stale, FreshnessStatus::Stale);
    assert_eq!(unknown, FreshnessStatus::Unknown);
    assert_eq!(unavailable, FreshnessStatus::Unavailable);
}

#[test]
fn replica_replay_state_exposes_only_available_lsn_values() {
    let replay_lsn = PgLsn::from_parts(7, 11);

    assert_eq!(
        ReplicaReplayState::ReplayLsn(replay_lsn).replay_lsn(),
        Some(replay_lsn)
    );
    assert_eq!(ReplicaReplayState::Stale.replay_lsn(), None);
    assert_eq!(ReplicaReplayState::Unknown.replay_lsn(), None);
    assert_eq!(ReplicaReplayState::Unavailable.replay_lsn(), None);
}
