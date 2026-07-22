use bytes::BytesMut;
use pg_kinetic_wire::admin::{
    build_admin_table_response, build_command_complete, build_command_response, build_data_row,
    build_row_description, AdminWireColumn, AdminWireType,
};

#[test]
fn builds_row_description_data_row_and_ready() {
    let columns = vec![AdminWireColumn::new("client_id", AdminWireType::Int8)];
    let rows = vec![vec!["42".to_string()]];

    let response = build_admin_table_response(&columns, &rows);

    assert_eq!(
        response,
        BytesMut::from(
            &[
                b'T', 0, 0, 0, 34, 0, 1, b'c', b'l', b'i', b'e', b'n', b't', b'_', b'i', b'd', 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 0, 8, 255, 255, 255, 255, 0, 0, b'D', 0, 0, 0, 12,
                0, 1, 0, 0, 0, 2, b'4', b'2', b'C', 0, 0, 0, 9, b'S', b'H', b'O', b'W', 0, b'Z', 0,
                0, 0, 5, b'I',
            ][..]
        ),
    );
}

#[test]
fn empty_table_still_returns_command_complete_and_ready() {
    let response = build_admin_table_response(&[], &[]);

    assert_eq!(
        response,
        BytesMut::from(
            &[
                b'T', 0, 0, 0, 6, 0, 0, b'C', 0, 0, 0, 9, b'S', b'H', b'O', b'W', 0, b'Z', 0, 0, 0,
                5, b'I',
            ][..]
        ),
    );
}

#[test]
fn builds_individual_messages() {
    let columns = vec![
        AdminWireColumn::new("name", AdminWireType::Text),
        AdminWireColumn::new("count", AdminWireType::Int8),
    ];
    let rows = vec![vec!["alpha".to_string(), "7".to_string()]];

    let row_description = build_row_description(&columns);
    let data_rows = build_data_row(&columns, &rows);
    let command_complete = build_command_complete("SELECT 1");

    assert!(row_description.starts_with(b"T"));
    assert!(data_rows.starts_with(b"D"));
    assert!(command_complete.starts_with(b"C"));
}

#[test]
fn builds_command_response() {
    let response = build_command_response("PAUSE");

    assert_eq!(
        response,
        BytesMut::from(
            &[b'C', 0, 0, 0, 10, b'P', b'A', b'U', b'S', b'E', 0, b'Z', 0, 0, 0, 5, b'I'][..]
        ),
    );
}
