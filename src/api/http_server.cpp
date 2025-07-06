#include "api/http_server.h"
#include "api/database_manager.h"
#include "api/json_handler.h"
#include <iostream>
#include <format>

HttpServer::HttpServer() : dbManager(std::make_shared<DatabaseManager>()) {
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
}

void HttpServer::run(int port) {
    CROW_LOG_INFO << "Database Server starting...";
    std::cout << std::format("Database Server is running on http://localhost:{}\n", port);
    app.port(port).multithreaded().run();
}