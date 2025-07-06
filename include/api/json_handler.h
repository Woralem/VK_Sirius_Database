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

private:
    [[nodiscard]] static crow::response createJsonResponse(int code, const nlohmann::json& body);
    [[nodiscard]] static bool validateDatabaseName(const std::string& name);
};