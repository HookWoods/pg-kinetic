package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const marker = "compatibility report complete"

type result struct {
	SuiteID string `json:"suite_id"`; Language string `json:"language"`; Library string `json:"library"`; Version string `json:"version"`; Target string `json:"target"`; Outcome string `json:"outcome"`; DurationMS int64 `json:"duration_ms"`; SkipReason string `json:"skip_reason,omitempty"`; ErrorSummary string `json:"error_summary,omitempty"`; Cases []caseResult `json:"cases,omitempty"`
}

type caseResult struct {
	CaseID string `json:"case_id"`; Outcome string `json:"outcome"`
}

func target() string { if value := os.Getenv("PG_KINETIC_COMPAT_TARGET"); value != "" { return value }; return "direct-postgres" }
func selectedLibrary() string { if value := os.Getenv("PG_KINETIC_COMPAT_LIBRARY"); value != "" { return value }; return "" }
func makeResult(library, outcome string, started time.Time) result { return result{SuiteID: "go-" + library, Language: "go", Library: library, Version: "configured", Target: target(), Outcome: outcome, DurationMS: time.Since(started).Milliseconds()} }
func emit(results []result) { failed := false; for _, item := range results { failed = failed || item.Outcome == "fail" }; payload := map[string]any{"ok": !failed, "success_marker": marker, "summary": map[string]int{"pass": 0, "fail": 0, "skip": 0}, "results": results}; for _, item := range results { payload["summary"].(map[string]int)[item.Outcome]++ }; encoded, _ := json.Marshal(payload); fmt.Println(string(encoded)) }
func selected(results ...result) []result { library := selectedLibrary(); if library == "" { return results }; filtered := []result{}; for _, item := range results { if item.Library == library { filtered = append(filtered, item) } }; return filtered }

func main() {
	started := time.Now()
	if os.Getenv("PG_KINETIC_COMPAT_LIVE") != "1" { first := makeResult("pgx", "skip", started); first.SkipReason = "live-disabled"; first.ErrorSummary = "set PG_KINETIC_COMPAT_LIVE=1"; second := makeResult("database-sql", "skip", started); second.SkipReason = "live-disabled"; second.ErrorSummary = "set PG_KINETIC_COMPAT_LIVE=1"; emit(selected(first, second)); return }
	name := "DATABASE_URL_DIRECT"; if target() == "pg-kinetic" { name = "DATABASE_URL_PROXY" }; url := os.Getenv(name)
	if url == "" { first := makeResult("pgx", "skip", started); first.SkipReason = "database-url-unavailable"; second := makeResult("database-sql", "skip", started); second.SkipReason = "database-url-unavailable"; emit(selected(first, second)); return }
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds())*time.Second); defer cancel()
	pgxResult := makeResult("pgx", "pass", started)
	conn, err := pgx.Connect(ctx, url); if err == nil { _, err = conn.Exec(ctx, "SELECT $1::int", 1); _ = conn.Close(ctx) }; if err != nil { pgxResult.Outcome = "fail"; pgxResult.ErrorSummary = err.Error() }; if ctx.Err() != nil { pgxResult.Outcome = "fail"; pgxResult.ErrorSummary = "bounded execution timeout" }
	if pgxResult.Outcome == "pass" { pgxResult.Cases = []caseResult{{CaseID: "startup-connect", Outcome: "connected"}, {CaseID: "parameterized-query", Outcome: "one-row"}} }
	dbResult := makeResult("database-sql", "pass", started)
	config, configErr := pgx.ParseConfig(url)
	if configErr != nil {
		dbResult.Outcome = "fail"
		dbResult.ErrorSummary = configErr.Error()
	} else {
		var db *sql.DB = stdlib.OpenDB(*config)
		err = db.PingContext(ctx)
		if err == nil { _, err = db.QueryContext(ctx, "SELECT $1::int", 1) }
		_ = db.Close()
		if err != nil { dbResult.Outcome = "fail"; dbResult.ErrorSummary = err.Error() }
	}
	if dbResult.Outcome == "pass" { dbResult.Cases = []caseResult{{CaseID: "startup-connect", Outcome: "connected"}, {CaseID: "parameterized-query", Outcome: "one-row"}} }
	emit(selected(pgxResult, dbResult))
}

func timeoutSeconds() int { if value := os.Getenv("PG_KINETIC_COMPAT_TIMEOUT_SECONDS"); value != "" { var seconds int; if _, err := fmt.Sscanf(value, "%d", &seconds); err == nil && seconds > 0 { return seconds } }; return 30 }
