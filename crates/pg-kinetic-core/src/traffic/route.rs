use std::{
    fmt,
    hash::{Hash, Hasher},
    net::SocketAddr,
    sync::Arc,
};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum QueryClass {
    #[default]
    Default,
    Read,
    Write,
    Maintenance,
}

impl fmt::Display for QueryClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Default => "default",
            Self::Read => "read",
            Self::Write => "write",
            Self::Maintenance => "maintenance",
        };
        formatter.write_str(label)
    }
}

#[derive(Clone, Debug)]
pub struct RouteKey {
    database: Arc<str>,
    user: Arc<str>,
    application_name: Option<Arc<str>>,
    client_addr: Option<SocketAddr>,
    query_class: QueryClass,
    metric_label: Arc<str>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PoolKey {
    database: Arc<str>,
    user: Arc<str>,
    application_name: Option<Arc<str>>,
}

impl PartialEq for RouteKey {
    fn eq(&self, other: &Self) -> bool {
        self.database == other.database
            && self.user == other.user
            && self.application_name == other.application_name
            && self.client_addr == other.client_addr
            && self.query_class == other.query_class
    }
}

impl Eq for RouteKey {}

impl Hash for RouteKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.database.hash(state);
        self.user.hash(state);
        self.application_name.hash(state);
        self.client_addr.hash(state);
        self.query_class.hash(state);
    }
}

impl RouteKey {
    #[must_use]
    pub fn new(
        database: impl Into<Arc<str>>,
        user: impl Into<Arc<str>>,
        application_name: Option<&str>,
        client_addr: Option<SocketAddr>,
        query_class: QueryClass,
    ) -> Self {
        let database = database.into();
        let user = user.into();
        let application_name = application_name.map(Arc::<str>::from);
        let metric_label =
            build_metric_label(&database, &user, application_name.as_deref(), query_class);

        Self {
            database,
            user,
            application_name,
            client_addr,
            query_class,
            metric_label,
        }
    }

    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }

    #[must_use]
    pub fn user(&self) -> &str {
        &self.user
    }

    #[must_use]
    pub fn application_name(&self) -> Option<&str> {
        self.application_name.as_deref()
    }

    #[must_use]
    pub fn client_addr(&self) -> Option<SocketAddr> {
        self.client_addr
    }

    #[must_use]
    pub fn pool_key(&self) -> PoolKey {
        PoolKey {
            database: Arc::clone(&self.database),
            user: Arc::clone(&self.user),
            application_name: self.application_name.clone(),
        }
    }

    #[must_use]
    pub fn selection_key(&self) -> PoolKey {
        PoolKey {
            database: Arc::clone(&self.database),
            user: Arc::clone(&self.user),
            application_name: None,
        }
    }

    #[must_use]
    pub const fn query_class(&self) -> QueryClass {
        self.query_class
    }

    #[must_use]
    pub fn with_application_name(&self, application_name: Option<&str>) -> Self {
        if self.application_name() == application_name {
            return self.clone();
        }

        let application_name = application_name.map(Arc::<str>::from);
        Self {
            database: Arc::clone(&self.database),
            user: Arc::clone(&self.user),
            metric_label: build_metric_label(
                &self.database,
                &self.user,
                application_name.as_deref(),
                self.query_class,
            ),
            application_name,
            client_addr: self.client_addr,
            query_class: self.query_class,
        }
    }

    #[must_use]
    pub fn with_query_class(&self, query_class: QueryClass) -> Self {
        if self.query_class == query_class {
            return self.clone();
        }

        Self {
            database: Arc::clone(&self.database),
            user: Arc::clone(&self.user),
            application_name: self.application_name.clone(),
            client_addr: self.client_addr,
            query_class,
            metric_label: build_metric_label(
                &self.database,
                &self.user,
                self.application_name.as_deref(),
                query_class,
            ),
        }
    }

    #[must_use]
    pub fn metric_label(&self) -> String {
        self.metric_label.to_string()
    }

    #[must_use]
    pub fn metric_label_shared(&self) -> Arc<str> {
        Arc::clone(&self.metric_label)
    }
}

impl PoolKey {
    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }

    #[must_use]
    pub fn user(&self) -> &str {
        &self.user
    }

    #[must_use]
    pub fn application_name(&self) -> Option<&str> {
        self.application_name.as_deref()
    }

    #[must_use]
    pub fn metric_label(&self) -> String {
        build_metric_label(
            &self.database,
            &self.user,
            self.application_name.as_deref(),
            QueryClass::Default,
        )
        .to_string()
    }
}

fn build_metric_label(
    database: &str,
    user: &str,
    application_name: Option<&str>,
    query_class: QueryClass,
) -> Arc<str> {
    Arc::from(format!(
        "{database}/{user}/{}/{query_class}",
        application_name.unwrap_or("<none>")
    ))
}
