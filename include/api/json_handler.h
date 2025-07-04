#pragma once

#include <crow.h>
#include <nlohmann/json.hpp>
#include <memory>

class DatabaseManager;

class JsonHandler {
public:
    static crow::response handleListDatabases(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager);
    static crow::response handleCreateDatabase(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager);
    static crow::response handleRenameDatabase(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager);
    static crow::response handleDeleteDatabase(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager);
    static crow::response handleQuery(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager);
    static crow::response handleCors(const crow::request& req, const std::string& methods);

private:
    static crow::response createJsonResponse(int code, const nlohmann::json& body);
    static bool validateDatabaseName(const std::string& name);
};