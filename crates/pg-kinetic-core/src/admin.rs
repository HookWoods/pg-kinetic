#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminCommand {
    Show(AdminView),
    Unknown(String),
}

#[must_use]
pub fn parse_admin_command(sql: &str) -> AdminCommand {
    let normalized_sql = normalize_admin_sql(sql);
    let mut parts = normalized_sql.split_whitespace();

    match (parts.next(), parts.next(), parts.next()) {
        (Some("show"), Some(view), None) => match parse_admin_view(view) {
            Some(view) => AdminCommand::Show(view),
            None => AdminCommand::Unknown(normalized_sql),
        },
        _ => AdminCommand::Unknown(normalized_sql),
    }
}

impl AdminCommand {
    #[must_use]
    pub fn view(&self) -> Option<AdminView> {
        match self {
            Self::Show(view) => Some(*view),
            Self::Unknown(_) => None,
        }
    }
}

fn normalize_admin_sql(sql: &str) -> String {
    let trimmed = sql.trim();
    let without_semicolon = trimmed.strip_suffix(';').unwrap_or(trimmed);
    without_semicolon.trim().to_ascii_lowercase()
}

fn parse_admin_view(view: &str) -> Option<AdminView> {
    match view {
        "clients" => Some(AdminView::Clients),
        "pools" => Some(AdminView::Pools),
        "servers" => Some(AdminView::Servers),
        "prepared" => Some(AdminView::Prepared),
        "pinning" => Some(AdminView::Pinning),
        "recovery" => Some(AdminView::Recovery),
        "backpressure" => Some(AdminView::Backpressure),
        "routes" => Some(AdminView::Routes),
        "settings" => Some(AdminView::Settings),
        "limits" => Some(AdminView::Limits),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AdminView {
    Clients,
    Pools,
    Servers,
    Prepared,
    Pinning,
    Recovery,
    Backpressure,
    Routes,
    Settings,
    Limits,
}

impl AdminView {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Clients => "clients",
            Self::Pools => "pools",
            Self::Servers => "servers",
            Self::Prepared => "prepared",
            Self::Pinning => "pinning",
            Self::Recovery => "recovery",
            Self::Backpressure => "backpressure",
            Self::Routes => "routes",
            Self::Settings => "settings",
            Self::Limits => "limits",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminColumnType {
    Text,
    Int8,
    Float8,
    Bool,
    Timestamp,
}

impl AdminColumnType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Int8 => "int8",
            Self::Float8 => "float8",
            Self::Bool => "bool",
            Self::Timestamp => "timestamp",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminColumn {
    name: &'static str,
    column_type: AdminColumnType,
}

impl AdminColumn {
    #[must_use]
    pub const fn new(name: &'static str, column_type: AdminColumnType) -> Self {
        Self { name, column_type }
    }

    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    #[must_use]
    pub const fn column_type(&self) -> AdminColumnType {
        self.column_type
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminRow {
    values: Vec<String>,
}

impl AdminRow {
    #[must_use]
    pub fn new(values: impl Into<Vec<String>>) -> Self {
        Self {
            values: values.into(),
        }
    }

    #[must_use]
    pub fn values(&self) -> &[String] {
        &self.values
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminTable {
    view: AdminView,
    columns: Vec<AdminColumn>,
    rows: Vec<AdminRow>,
}

impl AdminTable {
    #[must_use]
    pub fn new(view: AdminView, columns: Vec<AdminColumn>, rows: Vec<AdminRow>) -> Self {
        Self {
            view,
            columns,
            rows,
        }
    }

    #[must_use]
    pub const fn view(&self) -> AdminView {
        self.view
    }

    #[must_use]
    pub fn columns(&self) -> &[AdminColumn] {
        &self.columns
    }

    #[must_use]
    pub fn rows(&self) -> &[AdminRow] {
        &self.rows
    }
}
