#include "api/http_server.h"
#include "api/json_handler.h"
#include "crow.h"
#include <iostream>

HttpServer::HttpServer() {
    std::cout << "HTTP Server created." << std::endl;
}

HttpServer::~HttpServer() {
    std::cout << "HTTP Server destroyed." << std::endl;
}

void HttpServer::run() {
    crow::SimpleApp app;

    CROW_ROUTE(app, "/api/query").methods("POST"_method)
    ([](const crow::request& req) {
        std::string sql_query = req.body;
        if (sql_query.empty()) {
            return crow::response(400, JsonHandler::serializeError("Query cannot be empty."));
        }

        std::cout << "Received query: " << sql_query << std::endl;
        
        // ЗАГЛУШКА: В будущем здесь будет вызов Query Engine

        return crow::response(200, JsonHandler::serializeSuccess("Query received: " + sql_query));
    });

    std::cout << "Starting server on port 18080..." << std::endl;
    app.port(18080).multithreaded().run();
}