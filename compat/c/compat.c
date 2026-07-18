#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void skip_report(const char *reason, const char *detail) {
    printf("{\"ok\":true,\"success_marker\":\"compatibility report complete\",\"language\":\"c\",\"outcome\":\"skip\",\"skip_reason\":\"%s\",\"error_summary\":\"%s\"}\n", reason, detail);
}

static void fail_report(const char *detail) {
    printf("{\"ok\":false,\"success_marker\":\"compatibility report complete\",\"language\":\"c\",\"outcome\":\"fail\",\"error_summary\":\"%s\"}\n", detail);
}

int main(void) {
    if (strcmp(getenv("PG_KINETIC_COMPAT_LIVE") ? getenv("PG_KINETIC_COMPAT_LIVE") : "", "1") != 0) {
        skip_report("live-stack-unavailable", "PG_KINETIC_COMPAT_LIVE=1 is required");
        return 0;
    }
    const char *target = getenv("PG_KINETIC_COMPAT_TARGET");
    const char *uri = strcmp(target ? target : "", "pg-kinetic") == 0 ? getenv("DATABASE_URL_PROXY") : getenv("DATABASE_URL_DIRECT");
    if (!uri || !*uri) { skip_report("live-stack-unavailable", "target database URL is not configured"); return 0; }
    PGconn *connection = PQconnectdb(uri);
    if (PQstatus(connection) != CONNECTION_OK) { fail_report(PQerrorMessage(connection)); PQfinish(connection); return 1; }
    PGresult *result = PQexec(connection, "SELECT id, name FROM compat_items WHERE id = 1");
    if (PQresultStatus(result) != PGRES_TUPLES_OK || PQntuples(result) != 1 || strcmp(PQgetvalue(result, 0, 1), "alpha") != 0) {
        fail_report(PQerrorMessage(connection)); PQclear(result); PQfinish(connection); return 1;
    }
    PQclear(result);
    const char *value[] = {"2"};
    result = PQexecParams(connection, "SELECT id, name FROM compat_items WHERE id = $1", 1, NULL, value, NULL, NULL, 0);
    if (PQresultStatus(result) != PGRES_TUPLES_OK || PQntuples(result) != 1 || strcmp(PQgetvalue(result, 0, 1), "beta") != 0) {
        fail_report(PQerrorMessage(connection)); PQclear(result); PQfinish(connection); return 1;
    }
    PQclear(result);
    puts("{\"ok\":true,\"success_marker\":\"compatibility report complete\",\"language\":\"c\",\"libraries\":[\"libpq\"],\"outcome\":\"pass\",\"results\":[{\"case_id\":\"startup-connect\",\"outcome\":\"connected\"},{\"case_id\":\"simple-query\",\"outcome\":\"one-row\"},{\"case_id\":\"parameterized-query\",\"outcome\":\"one-row\"}]}" );
    PQfinish(connection);
    return 0;
}
