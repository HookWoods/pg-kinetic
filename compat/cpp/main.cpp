#include <pqxx/pqxx>
#include <cstdlib>
#include <iostream>
#include <string>

int main() {
    const char *live = std::getenv("PG_KINETIC_COMPAT_LIVE");
    if (!live || std::string(live) != "1") {
        std::cout << "{\"ok\":true,\"success_marker\":\"compatibility report complete\",\"language\":\"cpp\",\"outcome\":\"skip\",\"skip_reason\":\"live-stack-unavailable\",\"error_summary\":\"PG_KINETIC_COMPAT_LIVE=1 is required\"}\n";
        return 0;
    }
    const char *target = std::getenv("PG_KINETIC_COMPAT_TARGET");
    const char *uri = target && std::string(target) == "pg-kinetic" ? std::getenv("DATABASE_URL_PROXY") : std::getenv("DATABASE_URL_DIRECT");
    if (!uri || !*uri) {
        std::cout << "{\"ok\":true,\"success_marker\":\"compatibility report complete\",\"language\":\"cpp\",\"outcome\":\"skip\",\"skip_reason\":\"live-stack-unavailable\",\"error_summary\":\"target database URL is not configured\"}\n";
        return 0;
    }
    try {
        pqxx::connection connection(uri);
        pqxx::work transaction(connection);
        auto row = transaction.exec_params1("SELECT id, name FROM compat_items WHERE id = $1", 1);
        if (row[0].as<int>() != 1 || row[1].as<std::string>() != "alpha") throw std::runtime_error("unexpected query result");
        transaction.commit();
        std::cout << "{\"ok\":true,\"success_marker\":\"compatibility report complete\",\"language\":\"cpp\",\"libraries\":[\"libpqxx\"],\"outcome\":\"pass\",\"results\":[{\"case_id\":\"startup-connect\",\"outcome\":\"connected\"},{\"case_id\":\"parameterized-query\",\"outcome\":\"one-row\"}]}\n";
    } catch (const std::exception &error) {
        std::cerr << error.what() << '\n';
        std::cout << "{\"ok\":false,\"success_marker\":\"compatibility report complete\",\"language\":\"cpp\",\"outcome\":\"fail\",\"error_summary\":\"query failed\"}\n";
        return 1;
    }
    return 0;
}
