#include "api/json_handler.h"
#include "api/database_manager.h"
#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include "utils/activity_logger.h"
#include <format>
#include <sstream>

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

bool JsonHandler::isSelectQuery(const std::string& query) {
    std::string upperQuery = query;
    std::transform(upperQuery.begin(), upperQuery.end(), upperQuery.begin(), ::toupper);

    auto firstNonSpace = upperQuery.find_first_not_of(" \t\r\n");
    if (firstNonSpace != std::string::npos) {
        upperQuery = upperQuery.substr(firstNonSpace);
    }

    auto lastNonSpace = upperQuery.find_last_not_of(" \t\r\n;");
    if (lastNonSpace != std::string::npos) {
        upperQuery = upperQuery.substr(0, lastNonSpace + 1);
    }

    return upperQuery.starts_with("SELECT") ||
           upperQuery == "SHOW LOGS" ||
           upperQuery == "SELECT * FROM LOGS";
}


crow::response JsonHandler::executeSingleQuery(const std::string& query_str,
                                              const std::string& database,
                                              std::shared_ptr<DatabaseManager> dbManager) {
    std::string upperQuery = query_str;
    std::transform(upperQuery.begin(), upperQuery.end(), upperQuery.begin(), ::toupper);

    auto firstNonSpace = upperQuery.find_first_not_of(" \t\r\n");
    auto lastNonSpace = upperQuery.find_last_not_of(" \t\r\n");
    if (firstNonSpace != std::string::npos && lastNonSpace != std::string::npos) {
        upperQuery = upperQuery.substr(firstNonSpace, lastNonSpace - firstNonSpace + 1);
    }

    if (upperQuery == "SHOW LOGS" || upperQuery == "SELECT * FROM LOGS") {
        auto& logger = ActivityLogger::getInstance();
        logger.logDatabaseAction(ActivityLogger::ActionType::LOG_VIEWED, database,
                               "Viewed logs via SQL command");
        auto result = logger.getLogsAsJson(100, 0);
        result["isSelect"] = true;
        return createJsonResponse(200, result);
    }

    auto executor = dbManager->getExecutor(database);
    if (!executor) {
        auto& logger = ActivityLogger::getInstance();
        logger.logQuery(database, query_str, {}, {}, false,
                      std::format("Database not found: {}", database));

        return createJsonResponse(404, json{
            {"status", "error"},
            {"message", std::format("Database not found: {}", database)},
            {"isSelect", isSelectQuery(query_str)}
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
            {"errors", parser.getErrors()},
            {"isSelect", isSelectQuery(query_str)}
        });
    }

    if (!ast) {
        auto& logger = ActivityLogger::getInstance();
        logger.logQuery(database, query_str, {}, {}, true, "");

        return createJsonResponse(200, json{
            {"status", "success"},
            {"message", "Empty query executed successfully."},
            {"isSelect", false}
        });
    }

    json astInfo = {
        {"type", query_engine::astNodeTypeToString(ast->type)}
    };

    json result = executor->execute(ast);

    bool isSelect = (ast->type == query_engine::ASTNode::Type::SELECT_STMT);
    result["isSelect"] = isSelect;

    auto& logger = ActivityLogger::getInstance();
    logger.logQuery(database, query_str, astInfo, result, true);

    return createJsonResponse(200, result);
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

        // Split queries by newline or semicolon
        std::vector<std::string> queries;
        std::stringstream ss(query_str);
        std::string line;
        std::string current_query;

        while (std::getline(ss, line)) {
            line.erase(0, line.find_first_not_of(" \t\r\n"));
            line.erase(line.find_last_not_of(" \t\r\n") + 1);

            if (!line.empty()) {
                bool endsWithSemicolon = (!line.empty() && line.back() == ';');

                if (!current_query.empty()) {
                    current_query += " ";
                }
                current_query += line;

                if (endsWithSemicolon) {
                    queries.push_back(current_query);
                    current_query.clear();
                }
            }
        }

        if (!current_query.empty()) {
            queries.push_back(current_query);
        }

        if (queries.size() == 1) {
            return executeSingleQuery(queries[0], database, dbManager);
        }

        // Execute multiple queries
        json results = json::array();
        int successCount = 0;
        int totalCount = queries.size();

        for (const auto& single_query : queries) {
            auto response = executeSingleQuery(single_query, database, dbManager);

            json resultJson;
            try {
                resultJson = json::parse(response.body);
            } catch (...) {
                resultJson = {
                    {"status", "error"},
                    {"message", "Failed to parse response"},
                    {"isSelect", isSelectQuery(single_query)}
                };
            }

            if (resultJson.value("status", "") == "success") {
                successCount++;
            }

            results.push_back({
                {"query", single_query},
                {"result", resultJson},
                {"isSelect", isSelectQuery(single_query)}
            });
        }

        return createJsonResponse(200, json{
            {"status", "success"},
            {"message", std::format("Executed {} queries, {} successful", totalCount, successCount)},
            {"results", results},
            {"totalQueries", totalCount},
            {"successfulQueries", successCount}
        });

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

crow::response JsonHandler::handleGetHistory(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager) {
    try {
        auto& logger = ActivityLogger::getInstance();

        size_t limit = 100;
        size_t offset = 0;

        if (req.url_params.get("limit")) {
            limit = std::stoul(req.url_params.get("limit"));
        }
        if (req.url_params.get("offset")) {
            offset = std::stoul(req.url_params.get("offset"));
        }

        return createJsonResponse(200, logger.getHistoryLogs(limit, offset));

    } catch (const std::exception& e) {
        return createJsonResponse(500, json{
            {"status", "error"},
            {"message", e.what()}
        });
    }
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
        if (!body.contains("database") || !body["database"].is_string()) {
            return createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'database' field"}
            });
        }

        std::string dbName = body["database"];

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
        if (!body.contains("database") || !body["database"].is_string()) {
            return createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'database' field"}
            });
        }

        std::string dbName = body["database"];

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

crow::response JsonHandler::handleCors(const crow::request& req, const std::string& methods) {
    crow::response res;
    res.add_header("Access-Control-Allow-Origin", "*");
    res.add_header("Access-Control-Allow-Headers", "Content-Type");
    res.add_header("Access-Control-Allow-Methods", methods);
    res.code = 204;
    res.end();
    return res;
}

crow::response JsonHandler::handleGetLogs(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager) {
    try {
        auto& logger = ActivityLogger::getInstance();

        // Parse query parameters
        size_t limit = 100;
        size_t offset = 0;
        std::string format = "json";
        std::optional<bool> successFilter = std::nullopt;

        if (req.url_params.get("limit")) {
            limit = std::stoul(req.url_params.get("limit"));
        }
        if (req.url_params.get("offset")) {
            offset = std::stoul(req.url_params.get("offset"));
        }
        if (req.url_params.get("format")) {
            format = req.url_params.get("format");
        }
        if (req.url_params.get("success")) {
            std::string successParam = req.url_params.get("success");
            if (successParam == "true") {
                successFilter = true;
            } else if (successParam == "false") {
                successFilter = false;
            }
        }

        if (format == "text") {
            crow::response resp(logger.getLogsAsText(limit, offset, successFilter));
            resp.add_header("Content-Type", "text/plain");
            resp.add_header("Access-Control-Allow-Origin", "*");
            return resp;
        } else if (format == "csv") {
            crow::response resp(logger.getLogsAsCsv(limit, offset, successFilter));
            resp.add_header("Content-Type", "text/csv");
            resp.add_header("Access-Control-Allow-Origin", "*");
            return resp;
        } else {
            return createJsonResponse(200, logger.getLogsAsJson(limit, offset, successFilter));
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
        std::optional<bool> successFilter = std::nullopt;

        if (req.url_params.get("format")) {
            format = req.url_params.get("format");
        }
        if (req.url_params.get("success")) {
            std::string successParam = req.url_params.get("success");
            if (successParam == "true") {
                successFilter = true;
            } else if (successParam == "false") {
                successFilter = false;
            }
        }

        logger.logDatabaseAction(ActivityLogger::ActionType::LOG_DOWNLOADED, "system",
                               std::format("Downloaded logs in {} format", format));

        crow::response resp;

        if (format == "csv") {
            resp.body = logger.getLogsAsCsv(SIZE_MAX, 0, successFilter);
            resp.add_header("Content-Type", "text/csv");
            resp.add_header("Content-Disposition", "attachment; filename=\"activity_log.csv\"");
        } else if (format == "json") {
            resp.body = logger.getLogsAsJson(SIZE_MAX, 0, successFilter).dump(2);
            resp.add_header("Content-Type", "application/json");
            resp.add_header("Content-Disposition", "attachment; filename=\"activity_log.json\"");
        } else {
            resp.body = logger.getLogsAsText(SIZE_MAX, 0, successFilter);
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

crow::response JsonHandler::handleGetLogsByDatabase(const crow::request& req, const std::string& database, std::shared_ptr<DatabaseManager> dbManager) {
    try {
        auto& logger = ActivityLogger::getInstance();
        size_t limit = 100;
        size_t offset = 0;
        std::optional<bool> successFilter = std::nullopt;
        if (req.url_params.get("limit")) {
            limit = std::stoul(req.url_params.get("limit"));
        }
        if (req.url_params.get("offset")) {
            offset = std::stoul(req.url_params.get("offset"));
        }
        if (req.url_params.get("success")) {
            std::string successParam = req.url_params.get("success");
            if (successParam == "true") {
                successFilter = true;
            } else if (successParam == "false") {
                successFilter = false;
            }
        }
        return createJsonResponse(200, logger.getLogsByDatabase(database, limit, offset, successFilter));
    } catch (const std::exception& e) {
        return createJsonResponse(500, json{
            {"status", "error"},
            {"message", e.what()}
        });
    }
}

crow::response JsonHandler::handleGetLogById(const crow::request& req, int id, std::shared_ptr<DatabaseManager> dbManager) {
    try {
        auto& logger = ActivityLogger::getInstance();
        auto log = logger.getLogById(static_cast<size_t>(id));
        if (log.contains("error")) {
            return createJsonResponse(404, json{
                {"status", "error"},
                {"message", "Log not found"}
            });
        }
        return createJsonResponse(200, json{
            {"status", "success"},
            {"log", log}
        });
    } catch (const std::exception& e) {
        return createJsonResponse(500, json{
            {"status", "error"},
            {"message", e.what()}
        });
    }
}

crow::response JsonHandler::handleDeleteLog(const crow::request& req, int id, std::shared_ptr<DatabaseManager> dbManager) {
    try {
        auto& logger = ActivityLogger::getInstance();
        if (logger.deleteLogById(static_cast<size_t>(id))) {
            return createJsonResponse(200, json{
                {"status", "success"},
                {"message", "Log deleted successfully"}
            });
        } else {
            return createJsonResponse(404, json{
                {"status", "error"},
                {"message", "Log not found"}
            });
        }
    } catch (const std::exception& e) {
        return createJsonResponse(500, json{
            {"status", "error"},
            {"message", e.what()}
        });
    }
}

crow::response JsonHandler::handleBulkDeleteLogs(const crow::request& req, std::shared_ptr<DatabaseManager> dbManager) {
    try {
        auto& logger = ActivityLogger::getInstance();

        std::optional<bool> successFilter = std::nullopt;

        if (req.url_params.get("success")) {
            std::string successParam = req.url_params.get("success");
            if (successParam == "true") {
                successFilter = true;
            } else if (successParam == "false") {
                successFilter = false;
            }
        }

        size_t deletedCount = logger.deleteLogsBySuccess(successFilter);

        std::string message;
        if (successFilter.has_value()) {
            message = std::format("Deleted {} {} logs", deletedCount,
                                successFilter.value() ? "successful" : "error");
        } else {
            message = std::format("Deleted all {} logs", deletedCount);
        }

        return createJsonResponse(200, json{
            {"status", "success"},
            {"message", message},
            {"deleted_count", deletedCount}
        });

    } catch (const std::exception& e) {
        return createJsonResponse(500, json{
            {"status", "error"},
            {"message", e.what()}
        });
    }
}

crow::response JsonHandler::handleBulkDeleteLogsByDatabase(const crow::request& req, const std::string& database, std::shared_ptr<DatabaseManager> dbManager) {
    try {
        auto& logger = ActivityLogger::getInstance();

        std::optional<bool> successFilter = std::nullopt;

        if (req.url_params.get("success")) {
            std::string successParam = req.url_params.get("success");
            if (successParam == "true") {
                successFilter = true;
            } else if (successParam == "false") {
                successFilter = false;
            }
        }

        size_t deletedCount = logger.deleteLogsByDatabase(database, successFilter);

        std::string message;
        if (successFilter.has_value()) {
            message = std::format("Deleted {} {} logs from database '{}'", deletedCount,
                                successFilter.value() ? "successful" : "error", database);
        } else {
            message = std::format("Deleted {} logs from database '{}'", deletedCount, database);
        }

        return createJsonResponse(200, json{
            {"status", "success"},
            {"message", message},
            {"deleted_count", deletedCount},
            {"database", database}
        });

    } catch (const std::exception& e) {
        return createJsonResponse(500, json{
            {"status", "error"},
            {"message", e.what()}
        });
    }
}
