use std::{fmt, net::SocketAddr, sync::Arc};

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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RouteKey {
    database: Arc<str>,
    user: Arc<str>,
    application_name: Option<Arc<str>>,
    client_addr: Option<SocketAddr>,
    query_class: QueryClass,
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
        Self {
            database: database.into(),
            user: user.into(),
            application_name: application_name.map(Arc::<str>::from),
            client_addr,
            query_class,
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
    pub const fn query_class(&self) -> QueryClass {
        self.query_class
    }

    #[must_use]
    pub fn metric_label(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.database,
            self.user,
            self.application_name.as_deref().unwrap_or("<none>"),
            self.query_class
        )
    }
}
