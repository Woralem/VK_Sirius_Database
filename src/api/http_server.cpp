#include <crow.h>
#include <iostream>
#include <nlohmann/json.hpp>
#include <memory>

#include "api/json_handler.h"
#include "api/http_server.h"
#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include "storage/optimized_in_memory_storage.h" // Временно пока не реализовн Storage Layer
#include "utils/logger.h"

using json = nlohmann::json;

HttpServer::HttpServer() {
    std::cout << "HTTP Server created." << std::endl;
}

HttpServer::~HttpServer() {
    std::cout << "HTTP Server destroyed." << std::endl;//
}

void HttpServer::run(int port) {
    auto storage = std::make_shared<InMemoryStorage>();
    auto executor = std::make_shared<query_engine::QueryExecutor>(storage);

    CROW_LOG_INFO << "Database Server starting...";
    LOG_INFO("Server", "Database Server starting...");
    CROW_ROUTE(app, "/")
    ([](){
        return "Database Server is running! Use POST /api/query to send queries.";
    });
    //Сам сервер
    CROW_ROUTE(app, "/api/query").methods(crow::HTTPMethod::Post)
    ([&executor](const crow::request& req) {
        //получили запрос от фронта
        crow::response res;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*"); // Разрешаем кросс-доменные запросы для фронтенда

        try {
            auto json_query = crow::json::load(req.body);
            if (!json_query) {
                CROW_LOG_ERROR << "Invalid JSON: " << req.body;
                LOG_ERROR("Server", "Invalid JSON: " + req.body);
                return crow::response(400, JsonHandler::serializeError("Query cannot be empty."));
            }
            if (!json_query.has("query")) {
                CROW_LOG_ERROR << "Missing 'query' field: " << req.body;
                LOG_ERROR("Server", "Missing 'query' field: " + req.body);
                return crow::response(400, JsonHandler::serializeError("Field 'query' is required."));
            }
            std::string sql_query = json_query["query"].s();//в ["query"] лежит посланный запрос вида "select * from table"
            CROW_LOG_INFO << "Processing query: " << sql_query;
            LOG_INFO("Server", "Processing query: " + sql_query);
            //sql_query передается дальше беку
            // 1. Лексический анализ
            query_engine::Lexer lexer(sql_query);
            auto tokens = lexer.tokenize();

            // 2. Синтаксический анализ (Парсинг)
            query_engine::Parser parser(tokens);
            auto ast = parser.parse();

            //Проверка на ошибки парсинга
            if (parser.hasError()) {
            res.code = 400; // Bad Request
            res.body = JsonHandler::serializeError("SQL Parse Error", parser.getErrors());
                LOG_ERROR("Server", "SQL Parse Error " + req.body);
            return res;
            }
            if (!ast) { // Пустой запрос
            res.code = 200;
            res.body = JsonHandler::serializeSuccess("Empty query executed successfully.");
            LOG_SUCCESS("Server", "Empty query executed successfully.");
            return res;
            }

            // 4. Исполнение
            json result = executor->execute(ast);

            res.code = 200;
            res.body = JsonHandler::serializeSuccess("Not empty query executed successfully.");//Возможно, надо поставить статус succes ?
            LOG_SUCCESS("Server", "Not empty query executed successfully.");
            return res;
        }
        catch (const json::parse_error& e) {
            LOG_ERROR("Server", "Invalid JSON in request body: " + req.body);
            CROW_LOG_ERROR << "Invalid JSON in request body: " << e.what();
            res.code = 400;
            res.body = JsonHandler::serializeError ("Invalid JSON in request body: " + std::string(e.what()));
            return res;
        } catch (const std::exception& e) {//!!
            LOG_ERROR("Server", "Internal error: " + req.body);
            CROW_LOG_ERROR << "Internal error: " << e.what();
            res.code = 500; // Internal Server Error
            res.body =  JsonHandler::serializeError ("An internal error occurred: " + std::string(e.what()));
            return res;
        }
});

    CROW_ROUTE(app, "/api/query")
   .methods(crow::HTTPMethod::Options)
   ([](const crow::request& req, crow::response& res){
       res.add_header("Access-Control-Allow-Origin", "*");
       res.add_header("Access-Control-Allow-Headers", "Content-Type");
       res.add_header("Access-Control-Allow-Methods", "POST, OPTIONS");
       res.end();
   });

    std::cout << "Starting server on port " << port << " ..." << std::endl;
    app.port(port).multithreaded().run();
}

void HttpServer::stop() {
    LOG_INFO("Server", "Stopping server...");
    std::cout << "Stopping server..." << std::endl;
    app.stop();
}