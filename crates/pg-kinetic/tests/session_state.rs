use pg_kinetic::session::{ClientEvent, PinReason, SessionState, TransactionState};

#[test]
fn simple_query_begin_pins_backend() {
    let mut state = SessionState::default();

    state.apply(ClientEvent::SimpleQuery("begin".into()));

    assert_eq!(state.transaction, TransactionState::InTransaction);
    assert_eq!(state.pin_reason(), Some(PinReason::OpenTransaction));
}

#[test]
fn commit_releases_transaction_pin() {
    let mut state = SessionState::default();

    state.apply(ClientEvent::SimpleQuery("begin".into()));
    state.apply(ClientEvent::SimpleQuery("commit".into()));

    assert_eq!(state.transaction, TransactionState::Idle);
    assert_eq!(state.pin_reason(), None);
}

#[test]
fn unknown_set_pins_session() {
    let mut state = SessionState::default();

    state.apply(ClientEvent::SimpleQuery("set search_path to public".into()));

    assert_eq!(state.pin_reason(), Some(PinReason::SessionState));
}

#[test]
fn copy_pins_session() {
    let mut state = SessionState::default();

    state.apply(ClientEvent::SimpleQuery("copy accounts to stdout".into()));

    assert_eq!(state.pin_reason(), Some(PinReason::Copy));
}
