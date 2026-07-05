use pg_kinetic::backpressure::{BackpressureError, BackpressureGate};
use std::time::Duration;

#[tokio::test]
async fn grants_capacity_when_slot_available() {
    let gate = BackpressureGate::new(1, 1);

    let permit = gate
        .checkout(Duration::from_millis(10))
        .await
        .expect("permit granted");

    assert_eq!(gate.in_flight(), 1);
    drop(permit);
    assert_eq!(gate.in_flight(), 0);
}

#[tokio::test]
async fn rejects_when_waiter_limit_is_reached() {
    let gate = BackpressureGate::new(1, 0);
    let _held = gate
        .checkout(Duration::from_millis(10))
        .await
        .expect("first permit granted");

    let error = gate
        .checkout(Duration::from_millis(10))
        .await
        .expect_err("second checkout rejected");

    assert_eq!(error, BackpressureError::QueueFull);
}

#[tokio::test]
async fn times_out_waiting_for_capacity() {
    let gate = BackpressureGate::new(1, 1);
    let _held = gate
        .checkout(Duration::from_millis(10))
        .await
        .expect("first permit granted");

    let error = gate
        .checkout(Duration::from_millis(1))
        .await
        .expect_err("second checkout times out");

    assert_eq!(error, BackpressureError::Timeout);
}
