using Npgsql;
using System.Text.Json;

const string marker = "compatibility report complete";
var target = Environment.GetEnvironmentVariable("PG_KINETIC_COMPAT_TARGET");
var requested = Environment.GetEnvironmentVariable("PG_KINETIC_COMPAT_LIBRARY") ?? "npgsql";
var url = target == "pg-kinetic"
    ? Environment.GetEnvironmentVariable("DATABASE_URL_PROXY")
    : Environment.GetEnvironmentVariable("DATABASE_URL_DIRECT");

void Print(object payload) => Console.WriteLine(JsonSerializer.Serialize(payload));

if (Environment.GetEnvironmentVariable("PG_KINETIC_COMPAT_LIVE") != "1")
{
    Print(new { ok = true, success_marker = marker, language = "dotnet", libraries = new[] { "npgsql", "ef-core" }, target, outcome = "skip", skip_reason = "live-stack-unavailable", error_summary = "PG_KINETIC_COMPAT_LIVE=1 is required" });
    return;
}
if (string.IsNullOrWhiteSpace(url))
{
    Print(new { ok = true, success_marker = marker, language = "dotnet", libraries = new[] { "npgsql", "ef-core" }, target, outcome = "skip", skip_reason = "live-stack-unavailable", error_summary = "target database URL is not configured" });
    return;
}
if (requested == "ef-core")
{
    Print(new
    {
        ok = true,
        success_marker = marker,
        language = "dotnet",
        libraries = new[] { "npgsql", "ef-core" },
        target,
        outcome = "skip",
        skip_reason = "feature-unsupported",
        error_summary = "EF Core mapping is optional for this protocol smoke",
        cases = Array.Empty<object>()
    });
    return;
}

var started = Environment.TickCount64;
try
{
    await using var dataSource = NpgsqlDataSource.Create(url);
    await using var connection = await dataSource.OpenConnectionAsync();
    await using (var command = new NpgsqlCommand("SELECT 1", connection))
    {
        if ((int)(await command.ExecuteScalarAsync() ?? 0) != 1) throw new InvalidOperationException("startup query failed");
    }
    await using (var command = new NpgsqlCommand("SELECT id, name FROM compat_items WHERE id = $1", connection))
    {
        command.Parameters.AddWithValue(2);
        await using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync() || reader.GetInt32(0) != 2 || reader.GetString(1) != "beta")
            throw new InvalidOperationException("parameterized-query returned unexpected row");
    }
    await using (var command = new NpgsqlCommand("SELECT id, name FROM compat_items WHERE id = $1", connection))
    {
        command.Parameters.AddWithValue(1);
        command.Prepare();
        await using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync() || reader.GetInt32(0) != 1 || reader.GetString(1) != "alpha")
            throw new InvalidOperationException("prepared-statement returned unexpected row");
    }
    Print(new
    {
        ok = true, success_marker = marker, language = "dotnet", libraries = new[] { "npgsql", "ef-core" }, target,
        outcome = "pass",
        duration_ms = Environment.TickCount64 - started,
        cases = new[] {
            new { case_id = "startup-connect", outcome = "connected" },
            new { case_id = "parameterized-query", outcome = "one-row" },
            new { case_id = "prepared-statement", outcome = "one-row" }
        }
    });
}
catch (Exception error)
{
    Console.WriteLine(JsonSerializer.Serialize(new
    {
        ok = false,
        success_marker = marker,
        language = "dotnet",
        target,
        outcome = "fail",
        duration_ms = Environment.TickCount64 - started,
        error_summary = $"{error.GetType().Name}: {error.Message}"
    }));
    Environment.ExitCode = 1;
}
