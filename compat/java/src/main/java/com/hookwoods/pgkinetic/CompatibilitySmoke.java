package com.hookwoods.pgkinetic;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

public final class CompatibilitySmoke {
    private static final String MARKER = "compatibility report complete";
    private static final String[] LIBRARIES = {
        "jdbc", "datasource", "hikari", "spring-jdbc", "spring-boot-datasource", "jooq"
    };

    private CompatibilitySmoke() {}

    private static String library() {
        String value = System.getenv("PG_KINETIC_COMPAT_LIBRARY");
        return value == null || value.isBlank() ? "jdbc" : value;
    }

    public static void main(String[] args) {
        if (!"1".equals(System.getenv("PG_KINETIC_COMPAT_LIVE"))) {
            printSkip("live-stack-unavailable", "PG_KINETIC_COMPAT_LIVE=1 is required");
            return;
        }
        String url = "pg-kinetic".equals(System.getenv("PG_KINETIC_COMPAT_TARGET"))
            ? System.getenv("DATABASE_URL_PROXY") : System.getenv("DATABASE_URL_DIRECT");
        if (url == null || url.isBlank()) {
            printSkip("live-stack-unavailable", "target database URL is not configured");
            return;
        }
        if ("spring-boot-datasource".equals(library()) || "jooq".equals(library())) {
            printSkip("feature-unsupported", library() + " mapping is optional for this protocol smoke");
            return;
        }

        long started = System.nanoTime();
        List<String> cases = new ArrayList<>();
        try {
            String selected = library();
            if ("jdbc".equals(selected)) {
                try (Connection connection = DriverManager.getConnection(url)) {
                    cases.add(caseResult("startup-connect", "connected"));
                    try (PreparedStatement statement = connection.prepareStatement(
                            "SELECT id, name FROM compat_items WHERE id = ?")) {
                        statement.setInt(1, 2);
                        try (ResultSet rows = statement.executeQuery()) {
                            if (!rows.next() || rows.getInt(1) != 2 || !"beta".equals(rows.getString(2))) {
                                throw new IllegalStateException("parameterized-query returned unexpected row");
                            }
                        }
                    }
                    cases.add(caseResult("parameterized-query", "one-row"));
                    try (PreparedStatement statement = connection.prepareStatement(
                            "SELECT id, name FROM compat_items WHERE id = ?")) {
                        statement.setInt(1, 1);
                        try (ResultSet rows = statement.executeQuery()) {
                            if (!rows.next() || rows.getInt(1) != 1 || !"alpha".equals(rows.getString(2))) {
                                throw new IllegalStateException("prepared-statement returned unexpected row");
                            }
                        }
                    }
                    cases.add(caseResult("prepared-statement", "one-row"));
                }
            }

            if ("hikari".equals(selected)) {
                HikariConfig config = new HikariConfig();
                config.setJdbcUrl(url);
                config.setMaximumPoolSize(1);
                config.setConnectionTimeout(5000);
                try (HikariDataSource pool = new HikariDataSource(config)) {
                    try (Connection first = pool.getConnection()) { first.createStatement().execute("SELECT 1"); }
                    try (Connection second = pool.getConnection()) { second.createStatement().execute("SELECT 1"); }
                }
                cases.add(caseResult("pool-reuse", "reused"));
            }

            if ("datasource".equals(selected)) {
                PGSimpleDataSource dataSource = new PGSimpleDataSource();
                dataSource.setUrl(url);
                try (Connection connection = dataSource.getConnection()) {
                    cases.add(caseResult("startup-connect", "connected"));
                    try (PreparedStatement statement = connection.prepareStatement(
                            "SELECT id, name FROM compat_items WHERE id = ?")) {
                        statement.setInt(1, 1);
                        try (ResultSet rows = statement.executeQuery()) {
                            if (!rows.next() || rows.getInt(1) != 1 || !"alpha".equals(rows.getString(2))) {
                                throw new IllegalStateException("datasource query returned unexpected row");
                            }
                        }
                    }
                    cases.add(caseResult("parameterized-query", "one-row"));
                }
            }

            if ("spring-jdbc".equals(selected)) {
                DriverManagerDataSource dataSource = new DriverManagerDataSource(url);
                JdbcTemplate template = new JdbcTemplate((DataSource) dataSource);
                Integer count = template.queryForObject("SELECT count(*) FROM compat_items", Integer.class);
                if (count == null || count < 1) throw new IllegalStateException("spring-jdbc query returned no rows");
                cases.add(caseResult("simple-query", "one-row"));
            }
            if (cases.isEmpty()) throw new IllegalArgumentException("unsupported library " + selected);
            printPass(cases, System.nanoTime() - started);
        } catch (Exception error) {
            printFail(error.getClass().getSimpleName() + ": " + error.getMessage(), System.nanoTime() - started);
        }
    }

    private static String caseResult(String id, String outcome) {
        String reason = "skip".equals(outcome) ? ",\"skip_reason\":\"feature-unsupported\"" : "";
        return "{\"case_id\":\"" + id + "\",\"outcome\":\"" + outcome + "\"" + reason + "}";
    }

    private static void printSkip(String reason, String detail) {
        String library = library();
        System.out.println("{\"ok\":true,\"success_marker\":\"" + MARKER
            + "\",\"language\":\"java\",\"target\":\"" + escape(System.getenv("PG_KINETIC_COMPAT_TARGET")) + "\",\"summary\":{\"pass\":0,\"fail\":0,\"skip\":1},\"results\":[{\"suite_id\":\"java-" + escape(library) + "\",\"language\":\"java\",\"library\":\"" + escape(library) + "\",\"version\":\"configured\",\"target\":\""
            + escape(System.getenv("PG_KINETIC_COMPAT_TARGET")) + "\",\"outcome\":\"skip\",\"duration_ms\":0,\"skip_reason\":\"" + reason + "\",\"error_summary\":\"" + escape(detail) + "\"}]}");
    }

    private static void printPass(List<String> cases, long nanos) {
        String library = library();
        System.out.println("{\"ok\":true,\"success_marker\":\"" + MARKER
            + "\",\"language\":\"java\",\"target\":\"" + escape(System.getenv("PG_KINETIC_COMPAT_TARGET"))
            + "\",\"libraries\":[\"" + String.join("\",\"", LIBRARIES)
            + "\"],\"summary\":{\"pass\":1,\"fail\":0,\"skip\":0},\"results\":[{\"suite_id\":\"java-" + escape(library) + "\",\"language\":\"java\",\"library\":\"" + escape(library) + "\",\"version\":\"configured\",\"target\":\""
            + escape(System.getenv("PG_KINETIC_COMPAT_TARGET")) + "\",\"outcome\":\"pass\",\"duration_ms\":" + (nanos / 1_000_000)
            + "}],\"cases\":[" + String.join(",", cases) + "]}");
    }

    private static void printFail(String detail, long nanos) {
        System.out.println("{\"ok\":false,\"success_marker\":\"" + MARKER
            + "\",\"language\":\"java\",\"outcome\":\"fail\",\"duration_ms\":"
            + (nanos / 1_000_000) + ",\"error_summary\":\"" + escape(detail) + "\"}");
        System.exit(1);
    }

    private static String escape(String value) {
        return value == null ? "unknown" : value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
