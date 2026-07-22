#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminCommand {
    Show(AdminView),
    Unknown(String),
}

#[must_use]
pub fn parse_admin_command(sql: &str) -> AdminCommand {
    let normalized_sql = normalize_admin_sql(sql);
    let parts = normalized_sql.split_whitespace().collect::<Vec<_>>();

    match parts.as_slice() {
        ["show", "clients"] => AdminCommand::Show(AdminView::Clients),
        ["show", "pools"] => AdminCommand::Show(AdminView::Pools),
        ["show", "servers"] => AdminCommand::Show(AdminView::Servers),
        ["show", "runtime"] => AdminCommand::Show(AdminView::Runtime),
        ["show", "runtime", "shards"] => AdminCommand::Show(AdminView::RuntimeShards),
        ["show", "nodes"] => AdminCommand::Show(AdminView::Nodes),
        ["show", "mirroring"] => AdminCommand::Show(AdminView::Mirroring),
        ["show", "adaptive"] => AdminCommand::Show(AdminView::Adaptive),
        ["show", "benchmarks"] => AdminCommand::Show(AdminView::Benchmarks),
        ["show", "performance"] => AdminCommand::Show(AdminView::Performance),
        ["show", "prepared"] => AdminCommand::Show(AdminView::Prepared),
        ["show", "pinning"] => AdminCommand::Show(AdminView::Pinning),
        ["show", "recovery"] => AdminCommand::Show(AdminView::Recovery),
        ["show", "backpressure"] => AdminCommand::Show(AdminView::Backpressure),
        ["show", "policies"] => AdminCommand::Show(AdminView::Policies),
        ["show", "policy", "decisions"] => AdminCommand::Show(AdminView::PolicyDecisions),
        ["show", "policy", "audit"] => AdminCommand::Show(AdminView::PolicyAudit),
        ["show", "routes"] => AdminCommand::Show(AdminView::Routes),
        ["show", "route", "maps"] => AdminCommand::Show(AdminView::RouteMaps),
        ["show", "shards"] => AdminCommand::Show(AdminView::Shards),
        ["show", "migrations"] => AdminCommand::Show(AdminView::Migrations),
        ["show", "settings"] => AdminCommand::Show(AdminView::Settings),
        ["show", "limits"] => AdminCommand::Show(AdminView::Limits),
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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AdminView {
    Clients,
    Pools,
    Servers,
    Runtime,
    RuntimeShards,
    Nodes,
    Mirroring,
    Adaptive,
    Benchmarks,
    Performance,
    Prepared,
    Pinning,
    Recovery,
    Backpressure,
    Policies,
    PolicyDecisions,
    PolicyAudit,
    Routes,
    RouteMaps,
    Shards,
    Migrations,
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
            Self::Runtime => "runtime",
            Self::RuntimeShards => "runtime shards",
            Self::Nodes => "nodes",
            Self::Mirroring => "mirroring",
            Self::Adaptive => "adaptive",
            Self::Benchmarks => "benchmarks",
            Self::Performance => "performance",
            Self::Prepared => "prepared",
            Self::Pinning => "pinning",
            Self::Recovery => "recovery",
            Self::Backpressure => "backpressure",
            Self::Policies => "policies",
            Self::PolicyDecisions => "policy decisions",
            Self::PolicyAudit => "policy audit",
            Self::Routes => "routes",
            Self::RouteMaps => "route maps",
            Self::Shards => "shards",
            Self::Migrations => "migrations",
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

#[cfg(test)]
mod tests {
    use super::{parse_admin_command, AdminCommand, AdminView};

    #[test]
    fn parses_runtime_shards_view_without_changing_runtime_view() {
        assert_eq!(
            parse_admin_command("SHOW RUNTIME").view(),
            Some(AdminView::Runtime)
        );
        assert_eq!(
            parse_admin_command("SHOW RUNTIME SHARDS").view(),
            Some(AdminView::RuntimeShards)
        );
        assert!(matches!(
            parse_admin_command("SHOW RUNTIME POOLS"),
            AdminCommand::Unknown(_)
        ));
    }
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
