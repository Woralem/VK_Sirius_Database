#include <crow.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <memory>

#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include "storage/in_memory_storage.h" // Временно пока не реализовн Storage Layer

using json = nlohmann::json;

int main() {
    crow::SimpleApp app;

    auto storage = std::make_shared<InMemoryStorage>();
    auto executor = std::make_shared<query_engine::QueryExecutor>(storage);

    CROW_LOG_INFO << "Database Server starting...";

    CROW_ROUTE(app, "/")
    ([](){
        return "Database Server is running! Use POST /api/query to send queries.";
    });

    CROW_ROUTE(app, "/api/query")
    .methods(crow::HTTPMethod::Post)
    ([&executor](const crow::request& req){
        crow::response res;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*"); // Разрешаем кросс-доменные запросы для фронтенда

        try {
            auto body = json::parse(req.body);
            if (!body.contains("query") || !body["query"].is_string()) {
                res.code = 400;
                res.body = json{
                    {"status", "error"},
                    {"message", "Request body must be a JSON object with a 'query' string field."}
                }.dump();
                return res;
            }

            std::string query_str = body["query"];

            // 1. Лексический анализ
            query_engine::Lexer lexer(query_str);
            auto tokens = lexer.tokenize();

            // 2. Синтаксический анализ (Парсинг)
            query_engine::Parser parser(tokens);
            auto ast = parser.parse();

            // 3. Проверка на ошибки парсинга
            if (parser.hasError()) {
                res.code = 400; // Bad Request
                res.body = json{
                    {"status", "error"},
                    {"message", "SQL Parse Error"},
                    {"errors", parser.getErrors()}
                }.dump();
                return res;
            }

            if (!ast) { // Пустой запрос
                res.code = 200;
                res.body = json{
                    {"status", "success"},
                    {"message", "Empty query executed successfully."}
                }.dump();
                return res;
            }

            // 4. Исполнение
            json result = executor->execute(ast);

            res.code = 200;
            res.body = result.dump();
            return res;

        } catch (const json::parse_error& e) {
            res.code = 400;
            res.body = json{
                {"status", "error"},
                {"message", "Invalid JSON in request body: " + std::string(e.what())}
            }.dump();
            return res;
        } catch (const std::exception& e) {
            res.code = 500; // Internal Server Error
            res.body = json{
                {"status", "error"},
                {"message", "An internal error occurred: " + std::string(e.what())}
            }.dump();
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

    std::cout << "Database Server is running on http://localhost:8080" << std::endl;
    app.port(8080).multithreaded().run();

    return 0;
}