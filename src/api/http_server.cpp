#include "api/http_server.h"
#include "api/database_manager.h"
#include "api/json_handler.h"
#include "utils/activity_logger.h"
#include <iostream>
#include <format>

HttpServer::HttpServer() : dbManager(std::make_shared<DatabaseManager>()) {
    // Enable file logging
    auto& logger = ActivityLogger::getInstance();
    logger.setPersistToFile(true, "database_activity.log");

    setupRoutes();
    setupCorsRoutes();
}

void HttpServer::setupRoutes() {
    CROW_ROUTE(app, "/")
    ([](){
        return "Database Server is running! Use POST /api/query to send queries.";
    });

    CROW_ROUTE(app, "/api/db/list")
    .methods(crow::HTTPMethod::Get)
    ([this](const crow::request& req){
        return JsonHandler::handleListDatabases(req, dbManager);
    });

    CROW_ROUTE(app, "/api/db/create")
    .methods(crow::HTTPMethod::Post)
    ([this](const crow::request& req){
        return JsonHandler::handleCreateDatabase(req, dbManager);
    });

    CROW_ROUTE(app, "/api/db/rename")
    .methods(crow::HTTPMethod::Post)
    ([this](const crow::request& req){
        return JsonHandler::handleRenameDatabase(req, dbManager);
    });

    CROW_ROUTE(app, "/api/db/delete")
    .methods(crow::HTTPMethod::Post)
    ([this](const crow::request& req){
        return JsonHandler::handleDeleteDatabase(req, dbManager);
    });

    CROW_ROUTE(app, "/api/query")
    .methods(crow::HTTPMethod::Post)
    ([this](const crow::request& req){
        return JsonHandler::handleQuery(req, dbManager);
    });

    CROW_ROUTE(app, "/api/logs")
    .methods(crow::HTTPMethod::Get)
    ([this](const crow::request& req){
        return JsonHandler::handleGetLogs(req, dbManager);
    });

    CROW_ROUTE(app, "/api/logs/download")
    .methods(crow::HTTPMethod::Get)
    ([this](const crow::request& req){
        return JsonHandler::handleDownloadLogs(req, dbManager);
    });

    CROW_ROUTE(app, "/api/logs/clear")
    .methods(crow::HTTPMethod::Post)
    ([this](const crow::request& req){
        return JsonHandler::handleClearLogs(req, dbManager);
    });

    CROW_ROUTE(app, "/api/logs")
    .methods(crow::HTTPMethod::Delete)
    ([this](const crow::request& req){
        return JsonHandler::handleBulkDeleteLogs(req, dbManager);
    });

    CROW_ROUTE(app, "/api/logs/database/<string>")
    .methods(crow::HTTPMethod::Get)
    ([this](const crow::request& req, const std::string& database){
        return JsonHandler::handleGetLogsByDatabase(req, database, dbManager);
    });

    CROW_ROUTE(app, "/api/logs/database/<string>")
    .methods(crow::HTTPMethod::Delete)
    ([this](const crow::request& req, const std::string& database){
        return JsonHandler::handleBulkDeleteLogsByDatabase(req, database, dbManager);
    });

    CROW_ROUTE(app, "/api/logs/<int>")
    .methods(crow::HTTPMethod::Get)
    ([this](const crow::request& req, int id){
        return JsonHandler::handleGetLogById(req, id, dbManager);
    });

    CROW_ROUTE(app, "/api/logs/<int>")
    .methods(crow::HTTPMethod::Delete)
    ([this](const crow::request& req, int id){
        return JsonHandler::handleDeleteLog(req, id, dbManager);
    });

    CROW_ROUTE(app, "/api/history")
    .methods(crow::HTTPMethod::Get)
    ([this](const crow::request& req){
        return JsonHandler::handleGetHistory(req, dbManager);
    });

    // Database switch logging endpoint
    CROW_ROUTE(app, "/api/db/switch")
    .methods(crow::HTTPMethod::Post)
    ([this](const crow::request& req){
        try {
            auto body = nlohmann::json::parse(req.body);
            std::string fromDb = body.value("from", "");
            std::string toDb = body.value("to", "");

            auto& logger = ActivityLogger::getInstance();
            logger.logDatabaseSwitch(fromDb, toDb);

            return JsonHandler::createJsonResponse(200, nlohmann::json{
                {"status", "success"},
                {"message", "Database switch logged"}
            });
        } catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, nlohmann::json{
                {"status", "error"},
                {"message", e.what()}
            });
        }
    });
}

void HttpServer::setupCorsRoutes() {
    CROW_ROUTE(app, "/api/query")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });

    CROW_ROUTE(app, "/api/db/list")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, OPTIONS");
    });

    CROW_ROUTE(app, "/api/db/create")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });

    CROW_ROUTE(app, "/api/db/rename")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });

    CROW_ROUTE(app, "/api/db/delete")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });

    CROW_ROUTE(app, "/api/logs")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, DELETE, OPTIONS");
    });

    CROW_ROUTE(app, "/api/logs/download")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, OPTIONS");
    });

    CROW_ROUTE(app, "/api/logs/clear")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });

    CROW_ROUTE(app, "/api/db/switch")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });

    CROW_ROUTE(app, "/api/logs/database/<string>")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req, const std::string&){
        return JsonHandler::handleCors(req, "GET, DELETE, OPTIONS");
    });

    CROW_ROUTE(app, "/api/logs/<int>")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req, int){
        return JsonHandler::handleCors(req, "GET, DELETE, OPTIONS");
    });

    CROW_ROUTE(app, "/api/history")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, OPTIONS");
    });
}

void HttpServer::run(int port) {
    CROW_LOG_INFO << "Database Server starting...";
    std::cout << std::format("Database Server is running on http://localhost:{}\n", port);

    auto& logger = ActivityLogger::getInstance();
    logger.logDatabaseAction(ActivityLogger::ActionType::LOG_VIEWED, "system",
                           std::format("Server started on port {}", port));

    app.port(port).multithreaded().run();
}
