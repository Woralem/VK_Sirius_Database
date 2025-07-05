#include <crow.h>
#include<cpr/cpr.h>
#include<string>
#include<vector>

#include "http-server.h"
#include "database-handler.cpp.h"
#include "json-handler.h"
#include "log-handler.h"
#include "table-handler.h"

HttpServer::HttpServer() {
    cur_db = "default";
    std::cout << "HTTP Server created." << std::endl;
}

HttpServer::~HttpServer() {
    std::cout << "HTTP Server destroyed." << std::endl;
}

void HttpServer::setupRoutes() {
    CROW_ROUTE(app, "/")
    ([](){
        return "Database Server is running! Use POST /api/query to send queries.";
    });
    CROW_ROUTE(app, "/changeDB").methods(crow::HTTPMethod::Post)
    ([&](const crow::request& req) {
        try {
            return DBhandler::changeDB(cur_db, req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });
    CROW_ROUTE(app, "/DB").methods(crow::HTTPMethod::Post)
    ([&] (const crow::request& req) {
        try{
            return DBhandler::DB(cur_db, req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
    }
});
    CROW_ROUTE(app, "/DB/Table").methods(crow::HTTPMethod::Post)
    ([&](const crow::request& req) {
        try {
            return TableHandler::table(cur_db, req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });
    CROW_ROUTE(app, "/DB/query").methods(crow::HTTPMethod::Post)
    ([&](const crow::request& req) {
        try {
           return TableHandler::makeQuery(cur_db, req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });
    CROW_ROUTE(app, "/DB/query/history/delete").methods(crow::HTTPMethod::Post)
    ([&](const crow::request& req) {
        try {
            return LogHandler::deleteQuery(cur_db, req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });
    CROW_ROUTE(app, "/DB/query/errors/delete").methods(crow::HTTPMethod::Post)
    ([&](const crow::request& req) {
        try {
            return LogHandler::deleteQuery(cur_db, req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });
    CROW_ROUTE(app, "/DB/query/history").methods(crow::HTTPMethod::Get)
    ([&]() {
        try {
            return LogHandler::getQueries(cur_db);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });
    CROW_ROUTE(app, "/DB/query/errors").methods(crow::HTTPMethod::Get)
    ([&]() {
        try {
            return LogHandler::getQueries(cur_db);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });
    CROW_ROUTE(app, "/DB/query/history/delete").methods(crow::HTTPMethod::Get)
    ([&]() {
        try {
            return LogHandler::deleteQueries(cur_db);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });
    CROW_ROUTE(app, "/DB/query/errors/delete").methods(crow::HTTPMethod::Get)
    ([&]() {
        try {
            return LogHandler::deleteErrors(cur_db);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });
    CROW_ROUTE(app, "/DB/remove").methods(crow::HTTPMethod::Get)
    ([&]() {
        try {
            return DBhandler::removeDB(cur_db);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });
    CROW_ROUTE(app, "/DB/list").methods(crow::HTTPMethod::Get)
    ([&]() {
        try {
           return DBhandler::listDB();
        }
        catch (const std::exception& e) {
           return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });

}
void HttpServer::setupCorsRoutes() {
    CROW_ROUTE(app, "/changeDB")
        .methods(crow::HTTPMethod::Options)
        ([](const crow::request& req){
            return JsonHandler::handleCors(req, "POST, OPTIONS");
        });
    CROW_ROUTE(app, "/DB")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });
    CROW_ROUTE(app, "/DB/Table")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });
    CROW_ROUTE(app, "/DB/query")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });
    CROW_ROUTE(app, "/DB/query/history/delete")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });
    CROW_ROUTE(app, "/DB/query/errors/delete")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });
    CROW_ROUTE(app, "/DB/query/history")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, OPTIONS");
    });
    CROW_ROUTE(app, "/DB/query/errors")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, OPTIONS");
    });
    CROW_ROUTE(app, "/DB/remove")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, OPTIONS");
    });
    CROW_ROUTE(app, "/DB/list")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, OPTIONS");
    });
}
void HttpServer::run (int port) {
    CROW_LOG_INFO << "http Server on API starting...";
    std::cout << "Database Server is running on http://localhost:" << port << std::endl;
    HttpServer::setupRoutes();
    CROW_LOG_INFO << "Routers was set up...";
    HttpServer::setupCorsRoutes();
    CROW_LOG_INFO << "Cors was set up...";
    app.port(port).multithreaded().run();
}