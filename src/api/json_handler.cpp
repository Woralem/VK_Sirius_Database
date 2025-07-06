#include "api/json_handler.h"
#include "api/database_manager.h"
#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include "utils.h"
#include <format>
#include <ranges>

using json = nlohmann::json;

crow::response JsonHandler::createJsonResponse(int code, const json& body) {
    crow::response res;
    res.code = code;
    res.add_header("Content-Type", "application/json");
    res.add_header("Access-Control-Allow-Origin", "*");
    res.body = body.dump();
    return res;
}

bool JsonHandler::validateDatabaseName(const std::string& name) {
    if (name.empty()) return false;
    return std::ranges::all_of(name, [](char c) {
        return std::isalnum(c) || c == '_';
    });
}

crow::response JsonHandler::handleListDatabases(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager) {
    try {
        auto databases = dbManager->listDatabases();
        return createJsonResponse(200, json{
            {"status", "success"},
            {"databases", databases}
        });
    } catch (const std::exception& e) {
        return createJsonResponse(500, json{
            {"status", "error"},
            {"message", std::format("Failed to list databases: {}", e.what())}
        });
    }
}

crow::response JsonHandler::handleCreateDatabase(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager) {
    try {
        auto body = json::parse(req.body);
        if (!body.contains("name") || !body["name"].is_string()) {
            return createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'name' field"}
            });
        }

        std::string dbName = body["name"];

        if (!validateDatabaseName(dbName)) {
            return createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Invalid database name. Use only alphanumeric characters and underscores."}
            });
        }

        if (dbManager->createDatabase(dbName)) {
            return createJsonResponse(200, json{
                {"status", "success"},
                {"message", "Database created successfully"},
                {"database", dbName}
            });
        } else {
            return createJsonResponse(409, json{
                {"status", "error"},
                {"message", "Database already exists"}
            });
        }

    } catch (const json::parse_error& e) {
        return createJsonResponse(400, json{
            {"status", "error"},
            {"message", std::format("Invalid JSON: {}", e.what())}
        });
    } catch (const std::exception& e) {
        return createJsonResponse(500, json{
            {"status", "error"},
            {"message", std::format("Failed to create database: {}", e.what())}
        });
    }
}

crow::response JsonHandler::handleRenameDatabase(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager) {
    try {
        auto body = json::parse(req.body);
        if (!body.contains("oldName") || !body["oldName"].is_string() ||
            !body.contains("newName") || !body["newName"].is_string()) {
            return createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'oldName' and 'newName' fields"}
            });
        }

        std::string oldName = body["oldName"];
        std::string newName = body["newName"];

        if (!validateDatabaseName(newName)) {
            return createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Invalid database name. Use only alphanumeric characters and underscores."}
            });
        }

        if (dbManager->renameDatabase(oldName, newName)) {
            return createJsonResponse(200, json{
                {"status", "success"},
                {"message", "Database renamed successfully"},
                {"oldName", oldName},
                {"newName", newName}
            });
        } else {
            return createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Failed to rename database. Either the old database doesn't exist, the new name is already taken, or you're trying to rename the default database."}
            });
        }

    } catch (const json::parse_error& e) {
        return createJsonResponse(400, json{
            {"status", "error"},
            {"message", std::format("Invalid JSON: {}", e.what())}
        });
    } catch (const std::exception& e) {
        return createJsonResponse(500, json{
            {"status", "error"},
            {"message", std::format("Failed to rename database: {}", e.what())}
        });
    }
}

crow::response JsonHandler::handleDeleteDatabase(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager) {
    try {
        auto body = json::parse(req.body);
        if (!body.contains("name") || !body["name"].is_string()) {
            return createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'name' field"}
            });
        }

        std::string dbName = body["name"];

        if (dbManager->deleteDatabase(dbName)) {
            return createJsonResponse(200, json{
                {"status", "success"},
                {"message", "Database deleted successfully"}
            });
        } else {
            return createJsonResponse(404, json{
                {"status", "error"},
                {"message", "Database not found or cannot be deleted"}
            });
        }

    } catch (const json::parse_error& e) {
        return createJsonResponse(400, json{
            {"status", "error"},
            {"message", std::format("Invalid JSON: {}", e.what())}
        });
    } catch (const std::exception& e) {
        return createJsonResponse(500, json{
            {"status", "error"},
            {"message", std::format("Failed to delete database: {}", e.what())}
        });
    }
}

crow::response JsonHandler::handleQuery(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager) {
    try {
        auto body = json::parse(req.body);
        if (!body.contains("query") || !body["query"].is_string()) {
            return createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'query' string field."}
            });
        }

        std::string query_str = body["query"];
        std::string database = body.value("database", "default");

        auto executor = dbManager->getExecutor(database);
        if (!executor) {
            return createJsonResponse(404, json{
                {"status", "error"},
                {"message", std::format("Database not found: {}", database)}
            });
        }

        query_engine::Lexer lexer(query_str);
        auto tokens = lexer.tokenize();

        query_engine::Parser parser(std::span<const query_engine::Token>{tokens});

        auto ast = parser.parse();

        if (parser.hasError()) {
            return createJsonResponse(400, json{
                {"status", "error"},
                {"message", "SQL Parse Error"},
                {"errors", parser.getErrors()}
            });
        }

        if (!ast) {
            return createJsonResponse(200, json{
                {"status", "success"},
                {"message", "Empty query executed successfully."}
            });
        }

        json result = executor->execute(ast);
        return createJsonResponse(200, result);

    } catch (const json::parse_error& e) {
        return createJsonResponse(400, json{
            {"status", "error"},
            {"message", std::format("Invalid JSON in request body: {}", e.what())}
        });
    } catch (const std::exception& e) {
        return createJsonResponse(500, json{
            {"status", "error"},
            {"message", std::format("An internal error occurred: {}", e.what())}
        });
    }
}

crow::response JsonHandler::handleCors(const crow::request& req, const std::string& methods) {
    crow::response res;
    res.add_header("Access-Control-Allow-Origin", "*");
    res.add_header("Access-Control-Allow-Headers", "Content-Type");
    res.add_header("Access-Control-Allow-Methods", methods);
    res.code = 204;
    res.end();
    return res;
}