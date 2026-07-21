use pg_kinetic_core::{
    routing::{QueryClass, RoutingHint},
    sql_classify::analyze_sql,
};

#[test]
fn analysis_preserves_query_class_and_routing_hint() {
    let analysis = analyze_sql("/* pg-kinetic: replica */ select 1");

    assert_eq!(analysis.query_class(), QueryClass::ReadCandidate);
    assert_eq!(analysis.routing_hint(), RoutingHint::Replica);
}
