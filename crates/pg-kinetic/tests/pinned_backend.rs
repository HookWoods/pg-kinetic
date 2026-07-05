use pg_kinetic::pin::PinnedBackend;

#[test]
fn starts_without_backend() {
    let pinned = PinnedBackend::default();

    assert!(!pinned.is_pinned());
}

#[test]
fn remembers_backend_id() {
    let mut pinned = PinnedBackend::default();
    pinned.mark_pinned(42);

    assert!(pinned.is_pinned());
    assert_eq!(pinned.backend_id(), Some(42));

    pinned.clear();
    assert!(!pinned.is_pinned());
}
