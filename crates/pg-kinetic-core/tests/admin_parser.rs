use pg_kinetic_core::admin::{parse_admin_command, AdminCommand, AdminView};

#[test]
fn parses_supported_show_commands() {
    assert_eq!(
        parse_admin_command("show clients"),
        AdminCommand::Show(AdminView::Clients)
    );
    assert_eq!(
        parse_admin_command("SHOW POOLS;"),
        AdminCommand::Show(AdminView::Pools)
    );
    assert_eq!(
        parse_admin_command("show prepared"),
        AdminCommand::Show(AdminView::Prepared)
    );
    assert_eq!(
        parse_admin_command("show backpressure"),
        AdminCommand::Show(AdminView::Backpressure)
    );
    assert_eq!(
        parse_admin_command("show route maps"),
        AdminCommand::Show(AdminView::RouteMaps)
    );
    assert_eq!(
        parse_admin_command("show shards"),
        AdminCommand::Show(AdminView::Shards)
    );
    assert_eq!(
        parse_admin_command("show migrations"),
        AdminCommand::Show(AdminView::Migrations)
    );
    assert_eq!(
        parse_admin_command("show limits"),
        AdminCommand::Show(AdminView::Limits)
    );
}

#[test]
fn rejects_non_admin_sql() {
    assert!(matches!(
        parse_admin_command("select 1"),
        AdminCommand::Unknown(_)
    ));
    assert!(matches!(
        parse_admin_command("drop table x"),
        AdminCommand::Unknown(_)
    ));
}

#[test]
fn parses_operational_verbs() {
    assert_eq!(parse_admin_command("PAUSE"), AdminCommand::Pause);
    assert_eq!(parse_admin_command("pause;"), AdminCommand::Pause);
    assert_eq!(parse_admin_command("RESUME"), AdminCommand::Resume);
    assert_eq!(parse_admin_command("RELOAD"), AdminCommand::Reload);
    assert!(matches!(
        parse_admin_command("PAUSE db1"),
        AdminCommand::Unknown(_)
    ));
}
