use super::*;

pub(super) struct RequestPlan<'a> {
    pub(super) sql: Cow<'a, str>,
    pub(super) command: SqlCommand,
    pub(super) analysis: Option<SqlAnalysis>,
    pub(super) updates_transaction_state: bool,
    pub(super) updates_session_state: bool,
}

#[derive(Clone, Debug)]
pub(super) struct CachedSqlPlan {
    pub(super) command: SqlCommand,
    pub(super) analysis: Option<SqlAnalysis>,
}

#[derive(Debug)]
pub(super) struct SqlPlanCache {
    capacity: usize,
    plans: HashMap<Vec<u8>, CachedSqlPlan>,
    insertion_order: VecDeque<Vec<u8>>,
}

impl SqlPlanCache {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            plans: HashMap::new(),
            insertion_order: VecDeque::new(),
        }
    }

    fn get_or_insert(&mut self, sql: &str, needs_analysis: bool) -> CachedSqlPlan {
        if let Some(plan) = self.plans.get(sql.as_bytes()) {
            if !needs_analysis || plan.analysis.is_some() {
                return plan.clone();
            }
        }

        let command = self
            .plans
            .get(sql.as_bytes())
            .map(|plan| plan.command.clone())
            .unwrap_or_else(|| classify(sql));
        let plan = CachedSqlPlan {
            command,
            analysis: needs_analysis.then(|| analyze_sql(sql)),
        };
        if self.capacity == 0 {
            return plan;
        }

        let key = sql.as_bytes().to_vec();
        if !self.plans.contains_key(key.as_slice()) {
            self.insertion_order.push_back(key.clone());
        }
        self.plans.insert(key, plan.clone());
        self.evict_to_capacity();
        plan
    }

    fn evict_to_capacity(&mut self) {
        while self.plans.len() > self.capacity {
            let Some(key) = self.insertion_order.pop_front() else {
                break;
            };
            self.plans.remove(key.as_slice());
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.plans.len()
    }
}

impl<'a> RequestPlan<'a> {
    fn new(
        sql: Cow<'a, str>,
        updates_transaction_state: bool,
        updates_session_state: bool,
        needs_analysis: bool,
        cache: &mut SqlPlanCache,
    ) -> Self {
        let cached = cache.get_or_insert(sql.as_ref(), needs_analysis);
        Self {
            sql,
            command: cached.command,
            analysis: cached.analysis,
            updates_transaction_state,
            updates_session_state,
        }
    }

    fn from_prepared(statement: &pg_kinetic_core::prepare::PreparedStatement) -> Self {
        Self {
            sql: Cow::Owned(statement.query.clone()),
            command: statement.command().clone(),
            analysis: Some(statement.analysis()),
            updates_transaction_state: false,
            updates_session_state: false,
        }
    }

    pub(super) fn analysis(&self) -> SqlAnalysis {
        self.analysis
            .unwrap_or_else(|| analyze_sql(self.sql.as_ref()))
    }
}

pub(super) fn request_plans_for_frames<'frames>(
    prepared: &PreparedCatalog,
    frames: &'frames [FrontendFrame],
    analyze_sql: bool,
    cache: &mut SqlPlanCache,
) -> anyhow::Result<Vec<RequestPlan<'frames>>> {
    let mut plans = Vec::new();
    for frame in frames {
        if let Some(query) = parse_simple_query(frame)? {
            plans.push(RequestPlan::new(
                Cow::Borrowed(query),
                true,
                true,
                analyze_sql,
                cache,
            ));
            continue;
        }

        if let Some(parse) = parse_parse_message(frame)? {
            plans.push(RequestPlan::new(
                Cow::Owned(parse.query),
                true,
                false,
                analyze_sql,
                cache,
            ));
            continue;
        }

        if let Some(statement_name) = parse_bind_statement_name(frame).ok().flatten() {
            if let Some(statement) = prepared.get_for_current_route_map(&statement_name) {
                plans.push(RequestPlan::from_prepared(statement));
                continue;
            }
        }

        if let Some(statement) =
            parse_describe_target(frame)
                .ok()
                .flatten()
                .and_then(|describe_target| match describe_target {
                    DescribeTarget::Statement(statement_name) => prepared
                        .get_for_current_route_map(&statement_name)
                        .map(|statement| statement),
                    _ => None,
                })
        {
            plans.push(RequestPlan::from_prepared(statement));
        }
    }

    Ok(plans)
}

#[cfg(test)]
mod sql_plan_cache_tests {
    use super::*;
    use pg_kinetic_core::routing::{QueryClass as RoutingQueryClass, RoutingHint};

    #[test]
    fn cached_sql_plan_upgrades_to_full_analysis_only_when_needed() {
        let mut cache = SqlPlanCache::new(8);

        let minimal = cache.get_or_insert("select 1", false);
        assert_eq!(minimal.command, SqlCommand::Query);
        assert!(minimal.analysis.is_none());
        assert_eq!(cache.len(), 1);

        let repeated_minimal = cache.get_or_insert("select 1", false);
        assert_eq!(repeated_minimal.command, SqlCommand::Query);
        assert!(repeated_minimal.analysis.is_none());
        assert_eq!(cache.len(), 1);

        let analyzed = cache.get_or_insert("select 1", true);
        assert_eq!(analyzed.command, SqlCommand::Query);
        assert_eq!(
            analyzed.analysis.expect("analysis").query_class(),
            RoutingQueryClass::ReadCandidate
        );
        assert_eq!(cache.len(), 1);

        let repeated_analyzed = cache.get_or_insert("select 1", true);
        assert_eq!(
            repeated_analyzed.analysis.expect("analysis").routing_hint(),
            RoutingHint::None
        );
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn cached_sql_plan_is_bounded() {
        let mut cache = SqlPlanCache::new(2);

        cache.get_or_insert("select 1", true);
        cache.get_or_insert("select 2", true);
        cache.get_or_insert("select 3", true);

        assert_eq!(cache.len(), 2);
        assert!(!cache.plans.contains_key(&b"select 1"[..]));
        assert!(cache.plans.contains_key(&b"select 2"[..]));
        assert!(cache.plans.contains_key(&b"select 3"[..]));
    }
}

pub(super) fn safe_request_to_replay(
    frames: &[FrontendFrame],
    plans: &[RequestPlan<'_>],
    session: &VirtualSession,
) -> bool {
    !frames.is_empty()
        && frames
            .iter()
            .all(|frame| frame.tag == u8::from(FrontendTag::Query))
        && plans.len() == 1
        && plans[0].analysis().query_class().routes_to_replica()
        && session.pin_reason().is_none()
        && !session.has_replayable_settings()
}

pub(super) fn mirror_sql_command_for_request_plan(
    request_plan: Option<&RequestPlan<'_>>,
) -> SqlCommand {
    request_plan
        .map(|plan| plan.command.clone())
        .unwrap_or(SqlCommand::Query)
}
