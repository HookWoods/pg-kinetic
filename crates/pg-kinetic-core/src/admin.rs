#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminCommand {
    Show(AdminView),
    Unknown(String),
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
