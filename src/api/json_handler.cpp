#include "api/json_handler.h"
#include "api/database_manager.h"
#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include "utils/activity_logger.h"
#include <format>

using json = nlohmann::json;

namespace query_engine {
    std::string astNodeTypeToString(ASTNode::Type type);
}

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

        // Check for special log commands
        std::string upperQuery = query_str;
        std::transform(upperQuery.begin(), upperQuery.end(), upperQuery.begin(), ::toupper);

        if (upperQuery == "SHOW LOGS" || upperQuery == "SELECT * FROM LOGS") {
            auto& logger = ActivityLogger::getInstance();
            logger.logDatabaseAction(ActivityLogger::ActionType::LOG_VIEWED, database,
                                   "Viewed logs via SQL command");
            return createJsonResponse(200, logger.getLogsAsJson(100, 0));
        }

        auto executor = dbManager->getExecutor(database);
        if (!executor) {
            auto& logger = ActivityLogger::getInstance();
            logger.logQuery(database, query_str, {}, {}, false,
                          std::format("Database not found: {}", database));

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
            auto& logger = ActivityLogger::getInstance();
            logger.logQuery(database, query_str, {}, {}, false,
                          parser.getErrors().empty() ? "Parse error" : parser.getErrors()[0]);

            return createJsonResponse(400, json{
                {"status", "error"},
                {"message", "SQL Parse Error"},
                {"errors", parser.getErrors()}
            });
        }

        if (!ast) {
            auto& logger = ActivityLogger::getInstance();
            logger.logQuery(database, query_str, {}, {}, true, "");

            return createJsonResponse(200, json{
                {"status", "success"},
                {"message", "Empty query executed successfully."}
            });
        }

        // Log AST info
        json astInfo = {
            {"type", query_engine::astNodeTypeToString(ast->type)}
        };

        json result = executor->execute(ast);

        // Log successful query
        auto& logger = ActivityLogger::getInstance();
        logger.logQuery(database, query_str, astInfo, result, true);

        return createJsonResponse(200, result);

    } catch (const json::parse_error& e) {
        return createJsonResponse(400, json{
            {"status", "error"},
            {"message", std::format("Invalid JSON in request body: {}", e.what())}
        });
    } catch (const std::exception& e) {
        auto& logger = ActivityLogger::getInstance();
        std::string query = "";
        std::string database = "default";

        try {
            auto body = json::parse(req.body);
            query = body.value("query", "");
            database = body.value("database", "default");
        } catch (...) {}

        logger.logQuery(database, query, {}, {}, false, e.what());

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

// Новые методы для логов
crow::response JsonHandler::handleGetLogs(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager) {
    try {
        auto& logger = ActivityLogger::getInstance();

        // Parse query parameters
        size_t limit = 100;
        size_t offset = 0;
        std::string format = "json";

        if (req.url_params.get("limit")) {
            limit = std::stoul(req.url_params.get("limit"));
        }
        if (req.url_params.get("offset")) {
            offset = std::stoul(req.url_params.get("offset"));
        }
        if (req.url_params.get("format")) {
            format = req.url_params.get("format");
        }

        if (format == "text") {
            crow::response resp(logger.getLogsAsText(limit, offset));
            resp.add_header("Content-Type", "text/plain");
            resp.add_header("Access-Control-Allow-Origin", "*");
            return resp;
        } else if (format == "csv") {
            crow::response resp(logger.getLogsAsCsv(limit, offset));
            resp.add_header("Content-Type", "text/csv");
            resp.add_header("Access-Control-Allow-Origin", "*");
            return resp;
        } else {
            return createJsonResponse(200, logger.getLogsAsJson(limit, offset));
        }

    } catch (const std::exception& e) {
        return createJsonResponse(500, json{
            {"status", "error"},
            {"message", e.what()}
        });
    }
}

crow::response JsonHandler::handleDownloadLogs(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager) {
    try {
        auto& logger = ActivityLogger::getInstance();

        std::string format = "text";
        if (req.url_params.get("format")) {
            format = req.url_params.get("format");
        }

        logger.logDatabaseAction(ActivityLogger::ActionType::LOG_DOWNLOADED, "system",
                               std::format("Downloaded logs in {} format", format));

        crow::response resp;

        if (format == "csv") {
            resp.body = logger.getLogsAsCsv(10000, 0);
            resp.add_header("Content-Type", "text/csv");
            resp.add_header("Content-Disposition", "attachment; filename=\"activity_log.csv\"");
        } else if (format == "json") {
            resp.body = logger.getLogsAsJson(10000, 0).dump(2);
            resp.add_header("Content-Type", "application/json");
            resp.add_header("Content-Disposition", "attachment; filename=\"activity_log.json\"");
        } else {
            resp.body = logger.getLogsAsText(10000, 0);
            resp.add_header("Content-Type", "text/plain");
            resp.add_header("Content-Disposition", "attachment; filename=\"activity_log.txt\"");
        }

        resp.add_header("Access-Control-Allow-Origin", "*");
        return resp;

    } catch (const std::exception& e) {
        return createJsonResponse(500, json{
            {"status", "error"},
            {"message", e.what()}
        });
    }
}

crow::response JsonHandler::handleClearLogs(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager) {
    try {
        auto& logger = ActivityLogger::getInstance();
        logger.clearLogs();

        return createJsonResponse(200, json{
            {"status", "success"},
            {"message", "Logs cleared successfully"}
        });

    } catch (const std::exception& e) {
        return createJsonResponse(500, json{
            {"status", "error"},
            {"message", e.what()}
        });
    }
}