#pragma once

#include <crow.h>
#include <nlohmann/json.hpp>
#include <memory>

class DatabaseManager;

class JsonHandler {
public:
    [[nodiscard]] static crow::response handleListDatabases(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager);
    [[nodiscard]] static crow::response handleCreateDatabase(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager);
    [[nodiscard]] static crow::response handleRenameDatabase(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager);
    [[nodiscard]] static crow::response handleDeleteDatabase(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager);
    [[nodiscard]] static crow::response handleQuery(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager);
    [[nodiscard]] static crow::response handleCors(const crow::request& req, const std::string& methods);

    [[nodiscard]] static crow::response handleGetLogs(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager);
    [[nodiscard]] static crow::response handleDownloadLogs(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager);
    [[nodiscard]] static crow::response handleClearLogs(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager);
    [[nodiscard]] static crow::response handleGetLogsByDatabase(const crow::request& req, const std::string& database, std::shared_ptr<DatabaseManager> dbManager);
    [[nodiscard]] static crow::response handleGetLogById(const crow::request& req, int id, std::shared_ptr<DatabaseManager> dbManager);
    [[nodiscard]] static crow::response handleDeleteLog(const crow::request& req, int id, std::shared_ptr<DatabaseManager> dbManager);

    [[nodiscard]] static crow::response handleBulkDeleteLogs(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager);
    [[nodiscard]] static crow::response handleBulkDeleteLogsByDatabase(const crow::request& req, const std::string& database, std::shared_ptr<DatabaseManager> dbManager);

    [[nodiscard]] static crow::response handleGetHistory(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager);

    [[nodiscard]] static crow::response createJsonResponse(int code, const nlohmann::json& body);

private:
    [[nodiscard]] static bool validateDatabaseName(const std::string& name);
    [[nodiscard]] static bool isSelectQuery(const std::string& query);
    [[nodiscard]] static crow::response executeSingleQuery(const std::string& query_str,
                                                          const std::string& database,
                                                          std::shared_ptr<DatabaseManager> dbManager);
};
