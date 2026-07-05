use pg_kinetic::{
    backpressure::BackpressureError,
    cleanup::CleanupAction,
    core,
    pin::PinnedBackend,
    pool::PoolError,
    prepare::PreparedCatalog,
    proxy_runtime,
    recovery::RecoveryAction,
    route::RouteKey,
    session::SessionState,
    sql::SqlCommand,
    virtual_session::VirtualSession,
    wire,
    wire::{backend::ReadyStatus, frame::FrontendFrame},
};

#[test]
fn compatibility_reexports_remain_available() {
    let _ = std::mem::size_of::<BackpressureError>();
    let _ = std::mem::size_of::<CleanupAction>();
    let _ = std::mem::size_of::<PinnedBackend>();
    let _ = std::mem::size_of::<RouteKey>();
    let _ = std::mem::size_of::<PoolError>();
    let _ = std::mem::size_of::<PreparedCatalog>();
    let _ = std::mem::size_of::<RecoveryAction>();
    let _ = std::mem::size_of::<SessionState>();
    let _ = std::mem::size_of::<SqlCommand>();
    let _ = std::mem::size_of::<VirtualSession>();
    let _ = std::mem::size_of::<ReadyStatus>();
    let _ = std::mem::size_of::<FrontendFrame>();
    let _ = std::mem::size_of::<core::recovery::RecoveryAction>();
    let _ = std::mem::size_of::<proxy_runtime::pool::PoolError>();
    let _ = std::mem::size_of::<wire::backend::ReadyStatus>();
}
