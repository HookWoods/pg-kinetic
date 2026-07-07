use pg_kinetic_core::{
    routing::{BackendRole, RoutingReason},
    session::{ClientEvent, ReadRoutingTransactionState, SessionState, TransactionAccessMode},
    sql::classify,
    virtual_session::{PinReason, VirtualSession},
};

fn assert_read_routing_state(
    state: ReadRoutingTransactionState,
    access_mode: TransactionAccessMode,
    target_role: BackendRole,
    route_reason: RoutingReason,
    primary_forced: bool,
) {
    assert_eq!(state.access_mode(), access_mode);
    assert_eq!(state.target_role(), target_role);
    assert_eq!(state.route_reason(), route_reason);
    assert_eq!(state.primary_forced(), primary_forced);
}

#[test]
fn begin_and_start_transaction_read_only_mark_transaction_replica_eligible() {
    for sql in ["begin read only", "start transaction read only"] {
        let mut state = SessionState::default();

        state.apply(ClientEvent::SimpleQuery(sql.to_string()));

        assert_eq!(
            state.transaction_access_mode(),
            Some(TransactionAccessMode::ReadOnly)
        );
        assert_eq!(
            state.current_transaction_target_role(),
            Some(BackendRole::Replica)
        );
        assert_eq!(
            state.current_transaction_route_reason(),
            Some(RoutingReason::ReadOnlyQuery)
        );
        assert_eq!(
            state.pin_reason(),
            Some(pg_kinetic_core::session::PinReason::OpenTransaction)
        );
    }
}

#[test]
fn set_transaction_read_only_marks_current_transaction_replica_eligible() {
    let mut state = SessionState::default();

    state.apply(ClientEvent::SimpleQuery("begin read write".into()));
    state.apply(ClientEvent::SimpleQuery("set transaction read only".into()));

    assert_eq!(
        state.transaction_access_mode(),
        Some(TransactionAccessMode::ReadOnly)
    );
    assert_eq!(
        state.current_transaction_target_role(),
        Some(BackendRole::Replica)
    );
    assert_eq!(
        state.current_transaction_route_reason(),
        Some(RoutingReason::ReadOnlyQuery)
    );
}

#[test]
fn begin_read_write_pins_to_primary() {
    let mut state = SessionState::default();

    state.apply(ClientEvent::SimpleQuery("begin read write".into()));

    assert_eq!(
        state.transaction_access_mode(),
        Some(TransactionAccessMode::ReadWrite)
    );
    assert_eq!(
        state.current_transaction_target_role(),
        Some(BackendRole::Primary)
    );
    assert_eq!(
        state.current_transaction_route_reason(),
        Some(RoutingReason::TransactionControl)
    );
}

#[test]
fn set_transaction_read_write_pins_to_primary() {
    let mut state = SessionState::default();

    state.apply(ClientEvent::SimpleQuery("begin read only".into()));
    state.apply(ClientEvent::SimpleQuery(
        "set transaction read write".into(),
    ));

    assert_eq!(
        state.transaction_access_mode(),
        Some(TransactionAccessMode::ReadWrite)
    );
    assert_eq!(
        state.current_transaction_target_role(),
        Some(BackendRole::Primary)
    );
    assert_eq!(
        state.current_transaction_route_reason(),
        Some(RoutingReason::TransactionControl)
    );
}

#[test]
fn transaction_end_clears_read_only_state() {
    let mut state = SessionState::default();

    state.apply(ClientEvent::SimpleQuery("begin read only".into()));
    state.apply(ClientEvent::SimpleQuery("commit".into()));

    assert_eq!(state.transaction_access_mode(), None);
    assert_eq!(state.read_routing_transaction_state(), None);
    assert_eq!(state.current_transaction_target_role(), None);
    assert_eq!(state.current_transaction_route_reason(), None);
}

#[test]
fn writes_inside_transaction_force_primary_and_prevent_replica_reuse() {
    let mut session = VirtualSession::default();

    session.apply_sql(classify("begin read only"));
    session.apply_transaction_sql("insert into accounts values (1)");

    assert_read_routing_state(
        session
            .read_routing_transaction_state()
            .expect("transaction state"),
        TransactionAccessMode::ReadOnly,
        BackendRole::Primary,
        RoutingReason::WriteQuery,
        true,
    );

    session.apply_sql(classify("commit"));

    assert_eq!(session.read_routing_transaction_state(), None);
    assert_eq!(session.current_transaction_target_role(), None);
    assert_eq!(session.current_transaction_route_reason(), None);
}

#[test]
fn temp_tables_advisory_locks_listen_copy_from_and_session_mutations_preserve_existing_pinning_behavior(
) {
    let mut temp_table_session = VirtualSession::default();
    temp_table_session.apply_sql(classify("create temporary table t(id int)"));
    assert_eq!(temp_table_session.pin_reason(), Some(PinReason::TempTable));

    let mut advisory_lock_session = VirtualSession::default();
    advisory_lock_session.apply_sql(classify("select pg_advisory_lock(1)"));
    assert_eq!(
        advisory_lock_session.pin_reason(),
        Some(PinReason::AdvisoryLock)
    );

    let mut listen_session = VirtualSession::default();
    listen_session.apply_sql(classify("listen account_events"));
    assert_eq!(listen_session.pin_reason(), Some(PinReason::ListenNotify));

    let mut copy_session = VirtualSession::default();
    copy_session.apply_sql(classify("copy accounts from stdin"));
    assert_eq!(copy_session.pin_reason(), Some(PinReason::Copy));

    let mut mutation_session = VirtualSession::default();
    mutation_session.apply_sql(classify("set role app_user"));
    assert_eq!(mutation_session.pin_reason(), Some(PinReason::SessionState));
}
