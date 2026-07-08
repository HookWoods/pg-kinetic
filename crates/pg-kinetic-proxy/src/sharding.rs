use pg_kinetic_core::{
    routing::FallbackPolicy,
    session::TransactionState,
    shard_extract::{extract_shard_hint, extract_shard_key, ShardExtraction, ShardHint},
    sharding::{
        deterministic_shard_hash, MultiShardPolicy, RouteMapValidationInput, ShardRoute,
        ShardRouteDecision, ShardRouteMap, ShardRouteReason, ShardScope,
    },
    virtual_session::ReadAfterWriteState,
};

use crate::routing::{
    bridge_shard_route_decision, choose_routing_target, ReadRoutingPlanner, RouteHealthSnapshot,
    RoutingContext, RoutingTarget,
};

#[derive(Clone, Debug)]
pub struct ShardRoutingPlanner {
    read_routing: ReadRoutingPlanner,
    sharding_enabled: bool,
    route_map_store: ShardRouteMapStore,
}

impl ShardRoutingPlanner {
    #[must_use]
    pub fn new(
        read_routing: ReadRoutingPlanner,
        sharding_enabled: bool,
        route_map_store: ShardRouteMapStore,
    ) -> Self {
        Self {
            read_routing,
            sharding_enabled,
            route_map_store,
        }
    }

    #[must_use]
    pub fn choose_routing_target(&self, context: ShardRoutingContext<'_>) -> RoutingTarget {
        choose_sharded_routing_target(self, context)
    }

    #[must_use]
    pub fn plan_sharded_route(&self, context: ShardRoutingContext<'_>) -> ShardRouteDecision {
        plan_sharded_route(self, context)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ShardRoutingContext<'a> {
    pub sql: &'a str,
    pub transaction_state: TransactionState,
    pub read_after_write_state: ReadAfterWriteState,
    pub health: &'a RouteHealthSnapshot,
    pub route_map_validation_input: Option<&'a RouteMapValidationInput>,
}

impl<'a> ShardRoutingContext<'a> {
    #[must_use]
    pub const fn new(
        sql: &'a str,
        transaction_state: TransactionState,
        read_after_write_state: ReadAfterWriteState,
        health: &'a RouteHealthSnapshot,
        route_map_validation_input: Option<&'a RouteMapValidationInput>,
    ) -> Self {
        Self {
            sql,
            transaction_state,
            read_after_write_state,
            health,
            route_map_validation_input,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ShardRouteMapStore {
    route_maps: Vec<ShardRouteMap>,
}

impl ShardRouteMapStore {
    #[must_use]
    pub fn new(route_maps: impl Into<Vec<ShardRouteMap>>) -> Self {
        Self {
            route_maps: route_maps.into(),
        }
    }

    #[must_use]
    pub fn route_maps(&self) -> &[ShardRouteMap] {
        &self.route_maps
    }
}

#[must_use]
pub fn plan_sharded_route(
    planner: &ShardRoutingPlanner,
    context: ShardRoutingContext<'_>,
) -> ShardRouteDecision {
    if !planner.sharding_enabled {
        return ShardRouteDecision::new(None, ShardRouteReason::NoMatch, MultiShardPolicy::Reject);
    }

    let explicit_hint = parsed_shard_hint(context.sql);
    let route_map = match planner
        .route_map_store
        .route_maps()
        .iter()
        .find(|route_map| {
            route_map_matches_context(route_map, &context)
                && route_map_matches_explicit_hint(route_map, &explicit_hint)
        }) {
        Some(route_map) => route_map,
        None => match planner
            .route_map_store
            .route_maps()
            .iter()
            .find(|route_map| route_map_matches_context(route_map, &context))
        {
            Some(route_map) => route_map,
            None => {
                return ShardRouteDecision::new(
                    None,
                    ShardRouteReason::NoMatch,
                    MultiShardPolicy::Reject,
                );
            }
        },
    };

    if matches!(explicit_hint, ShardHint::Unknown) && context.sql.contains("pg-kinetic:") {
        return ShardRouteDecision::new(
            None,
            ShardRouteReason::ValidationFailed,
            route_map.policy(),
        );
    }

    if let Some(shard_id) = explicit_shard_id(&explicit_hint) {
        if let Some(route) = select_route_for_shard_id(route_map.routes(), shard_id) {
            let reason = if matches!(explicit_hint, ShardHint::Tenant(_) | ShardHint::Route(_)) {
                ShardRouteReason::AdminOverride
            } else {
                ShardRouteReason::AdminOverride
            };
            return choose_shard_route(route_map, route, reason, planner, context);
        }

        return ShardRouteDecision::new(
            None,
            ShardRouteReason::ValidationFailed,
            route_map.policy(),
        );
    }

    let extraction = match context.route_map_validation_input {
        Some(route_map_validation_input) => {
            extract_shard_key(context.sql, route_map_validation_input)
        }
        None => ShardExtraction::Unknown,
    };

    match extraction {
        ShardExtraction::Key { key, .. } => {
            if let Some(route) =
                select_route_for_shard_key(route_map.routes(), route_map.strategy(), &key)
            {
                let reason = match route_map.strategy() {
                    pg_kinetic_core::sharding::ShardStrategy::Hash => ShardRouteReason::HashMatch,
                    pg_kinetic_core::sharding::ShardStrategy::Range => ShardRouteReason::RangeMatch,
                    pg_kinetic_core::sharding::ShardStrategy::List => ShardRouteReason::ListMatch,
                };

                return choose_shard_route(route_map, route, reason, planner, context);
            }
            ShardRouteDecision::new(None, ShardRouteReason::NoMatch, route_map.policy())
        }
        ShardExtraction::Unknown => {
            ShardRouteDecision::new(None, ShardRouteReason::NoMatch, route_map.policy())
        }
    }
}

#[must_use]
pub fn apply_multi_shard_policy(policy: FallbackPolicy) -> RoutingTarget {
    match policy {
        FallbackPolicy::Primary => RoutingTarget::Primary {
            reason: crate::routing::RoutingReason::FallbackPrimary,
        },
        FallbackPolicy::Reject => RoutingTarget::Reject {
            reason: crate::routing::RoutingReason::FallbackReject,
        },
        FallbackPolicy::Wait => RoutingTarget::Wait {
            reason: crate::routing::RoutingReason::FallbackWait,
        },
    }
}

#[must_use]
pub fn choose_sharded_routing_target(
    planner: &ShardRoutingPlanner,
    context: ShardRoutingContext<'_>,
) -> RoutingTarget {
    if !planner.sharding_enabled {
        return choose_routing_target(
            &planner.read_routing,
            RoutingContext::new(
                context.sql,
                context.transaction_state,
                context.read_after_write_state,
                context.health,
            ),
        );
    }

    let decision = plan_sharded_route(planner, context);
    let _read_routing_decision =
        bridge_shard_route_decision(&decision, context.sql, &planner.read_routing);

    if decision.route().is_some() {
        return choose_routing_target(
            &planner.read_routing,
            RoutingContext::new(
                context.sql,
                context.transaction_state,
                context.read_after_write_state,
                context.health,
            ),
        );
    }

    match decision.reason() {
        ShardRouteReason::ValidationFailed | ShardRouteReason::MultiShardRejected => {
            RoutingTarget::Reject {
                reason: crate::routing::RoutingReason::FallbackReject,
            }
        }
        ShardRouteReason::NoMatch => {
            apply_multi_shard_policy(planner.read_routing.fallback_policy())
        }
        ShardRouteReason::AdminOverride
        | ShardRouteReason::HashMatch
        | ShardRouteReason::RangeMatch
        | ShardRouteReason::ListMatch => {
            apply_multi_shard_policy(planner.read_routing.fallback_policy())
        }
    }
}

fn route_map_matches_context(route_map: &ShardRouteMap, context: &ShardRoutingContext<'_>) -> bool {
    let hint = parsed_shard_hint(context.sql);
    let extraction = context
        .route_map_validation_input
        .map(|route_map_validation_input| {
            extract_shard_key(context.sql, route_map_validation_input)
        })
        .unwrap_or(ShardExtraction::Unknown);

    match route_map.scope() {
        ShardScope::Global => true,
        ShardScope::Tenant(scope) => match hint {
            ShardHint::Tenant(value) | ShardHint::Shard(value) | ShardHint::Route(value) => {
                scope.tenant_id() == value.as_ref()
            }
            ShardHint::None | ShardHint::Unknown => false,
        },
        ShardScope::Table(scope) => match extraction {
            ShardExtraction::Key { schema, table, .. } => {
                let schema_matches = match (schema.as_deref(), scope.schema()) {
                    (Some(actual_schema), expected_schema) => actual_schema == expected_schema,
                    (None, _) => false,
                };

                schema_matches && table.as_ref() == scope.table()
            }
            ShardExtraction::Unknown => false,
        },
    }
}

fn route_map_matches_explicit_hint(route_map: &ShardRouteMap, hint: &ShardHint) -> bool {
    match explicit_shard_id(hint) {
        Some(shard_id) => route_map
            .routes()
            .iter()
            .any(|route| route.target().shard_id().as_str() == shard_id),
        None => true,
    }
}

fn parsed_shard_hint(sql: &str) -> ShardHint {
    extract_shard_hint(sql)
}

fn explicit_shard_id(hint: &ShardHint) -> Option<&str> {
    match hint {
        ShardHint::Shard(value) | ShardHint::Tenant(value) | ShardHint::Route(value) => {
            Some(value.as_ref())
        }
        ShardHint::None | ShardHint::Unknown => None,
    }
}

fn choose_shard_route(
    route_map: &ShardRouteMap,
    route: &ShardRoute,
    reason: ShardRouteReason,
    planner: &ShardRoutingPlanner,
    context: ShardRoutingContext<'_>,
) -> ShardRouteDecision {
    let target_role = choose_routing_target(
        &planner.read_routing,
        RoutingContext::new(
            context.sql,
            context.transaction_state,
            context.read_after_write_state,
            context.health,
        ),
    )
    .target_role();

    let selected_route = route_map
        .routes()
        .iter()
        .find(|candidate| {
            candidate.target().shard_id() == route.target().shard_id()
                && Some(candidate.target().backend_role()) == target_role
        })
        .cloned()
        .or_else(|| {
            route_map
                .routes()
                .iter()
                .find(|candidate| candidate.target().shard_id() == route.target().shard_id())
                .cloned()
        });

    ShardRouteDecision::new(selected_route, reason, route_map.policy())
}

fn select_route_for_shard_id<'a>(
    routes: &'a [ShardRoute],
    shard_id: &str,
) -> Option<&'a ShardRoute> {
    routes
        .iter()
        .find(|route| route.target().shard_id().as_str() == shard_id)
}

fn select_route_for_shard_key<'a>(
    routes: &'a [ShardRoute],
    strategy: pg_kinetic_core::sharding::ShardStrategy,
    key: &pg_kinetic_core::sharding::ShardKey,
) -> Option<&'a ShardRoute> {
    if routes.is_empty() {
        return None;
    }

    match strategy {
        pg_kinetic_core::sharding::ShardStrategy::Hash => {
            let index = (deterministic_shard_hash(key) % routes.len() as u64) as usize;
            routes.get(index)
        }
        pg_kinetic_core::sharding::ShardStrategy::List => match key.as_text() {
            Some(text) => routes
                .iter()
                .find(|route| route.target().shard_id().as_str() == text)
                .or_else(|| (routes.len() == 1).then(|| &routes[0])),
            None => (routes.len() == 1).then(|| &routes[0]),
        },
        pg_kinetic_core::sharding::ShardStrategy::Range => (routes.len() == 1).then(|| &routes[0]),
    }
}
