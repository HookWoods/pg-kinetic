use pg_kinetic::backpressure::{
    BackpressureCoordinator, BackpressureError, BackpressureGate, RouteBackpressureSnapshot,
};
use pg_kinetic::route::{QueryClass, RouteKey};
use std::time::Duration;

fn route_key(application_name: &str) -> RouteKey {
    RouteKey::new(
        "postgres",
        "pgkinetic",
        Some(application_name),
        None,
        QueryClass::Default,
    )
}

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
async fn two_route_keys_have_independent_in_flight_limits() {
    let coordinator = BackpressureCoordinator::new(1, 1);
    let route_a = route_key("api-a");
    let route_b = route_key("api-b");

    let permit_a = coordinator
        .checkout(route_a.clone(), Duration::from_millis(10))
        .await
        .expect("route a permit granted");

    let permit_b = coordinator
        .checkout(route_b.clone(), Duration::from_millis(10))
        .await
        .expect("route b permit granted");

    assert_eq!(coordinator.route_snapshot(&route_a).in_flight, 1);
    assert_eq!(coordinator.route_snapshot(&route_b).in_flight, 1);
    assert_eq!(coordinator.global_snapshot().in_flight, 2);

    drop(permit_b);
    drop(permit_a);
}

#[tokio::test]
async fn one_saturated_route_key_does_not_block_an_idle_route_key() {
    let coordinator = BackpressureCoordinator::new(1, 1);
    let saturated = route_key("api-a");
    let idle = route_key("api-b");

    let _held = coordinator
        .checkout(saturated.clone(), Duration::from_millis(10))
        .await
        .expect("first route permit granted");

    let permit = coordinator
        .checkout(idle.clone(), Duration::from_millis(10))
        .await
        .expect("idle route still grants capacity");

    assert_eq!(coordinator.route_snapshot(&saturated).in_flight, 1);
    assert_eq!(coordinator.route_snapshot(&idle).in_flight, 1);
    assert_eq!(coordinator.global_snapshot().in_flight, 2);

    drop(permit);
}

#[tokio::test]
async fn rejects_when_waiter_limit_is_reached() {
    let coordinator = BackpressureCoordinator::new(1, 0);
    let route = route_key("api-a");
    let _held = coordinator
        .checkout(route.clone(), Duration::from_millis(10))
        .await
        .expect("first permit granted");

    let error = coordinator
        .checkout(route.clone(), Duration::from_millis(10))
        .await
        .expect_err("second checkout rejected");

    assert_eq!(error, BackpressureError::QueueFull);
}

#[tokio::test]
async fn times_out_waiting_for_capacity() {
    let coordinator = BackpressureCoordinator::new(1, 1);
    let route = route_key("api-a");
    let _held = coordinator
        .checkout(route.clone(), Duration::from_millis(10))
        .await
        .expect("first permit granted");

    let error = coordinator
        .checkout(route.clone(), Duration::from_millis(1))
        .await
        .expect_err("second checkout times out");

    assert_eq!(error, BackpressureError::Timeout);
}

#[tokio::test]
async fn dropped_permits_decrement_route_and_global_counters() {
    let coordinator = BackpressureCoordinator::new(2, 1);
    let route = route_key("api-a");

    let permit = coordinator
        .checkout(route.clone(), Duration::from_millis(10))
        .await
        .expect("permit granted");

    assert_eq!(
        coordinator.route_snapshot(&route),
        RouteBackpressureSnapshot {
            in_flight: 1,
            waiting: 0,
        }
    );
    assert_eq!(
        coordinator.global_snapshot(),
        RouteBackpressureSnapshot {
            in_flight: 1,
            waiting: 0,
        }
    );

    drop(permit);

    assert_eq!(
        coordinator.route_snapshot(&route),
        RouteBackpressureSnapshot::default()
    );
    assert_eq!(
        coordinator.global_snapshot(),
        RouteBackpressureSnapshot::default()
    );
}

#[tokio::test]
async fn snapshots_expose_route_waiting_and_in_flight_counts() {
    let coordinator = BackpressureCoordinator::new(1, 1);
    let route = route_key("api-a");
    let held = coordinator
        .checkout(route.clone(), Duration::from_millis(10))
        .await
        .expect("first permit granted");

    let waiter = {
        let coordinator = coordinator.clone();
        let route = route.clone();
        tokio::spawn(async move {
            coordinator
                .checkout(route, Duration::from_secs(1))
                .await
                .expect("second permit granted");
        })
    };

    let snapshot = tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            let snapshot = coordinator.route_snapshot(&route);
            if snapshot.waiting == 1 {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("waiter entered queue");

    assert_eq!(
        snapshot,
        RouteBackpressureSnapshot {
            in_flight: 1,
            waiting: 1,
        }
    );
    assert_eq!(
        coordinator.global_snapshot(),
        RouteBackpressureSnapshot {
            in_flight: 1,
            waiting: 0,
        }
    );

    drop(held);
    waiter.await.expect("waiter task completed");
}
