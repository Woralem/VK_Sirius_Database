#include <crow.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <mutex>

#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include "storage/optimized_in_memory_storage.h"

using json = nlohmann::json;

class DatabaseManager {
private:
    std::unordered_map<std::string, std::pair<
        std::shared_ptr<OptimizedInMemoryStorage>,
        std::shared_ptr<query_engine::OptimizedQueryExecutor>
    >> databases;
    std::mutex db_mutex;

public:
    DatabaseManager() {
        // Создаем дефолтную БД
        createDatabase("default");
    }

    bool createDatabase(const std::string& name) {
        std::lock_guard<std::mutex> lock(db_mutex);

        if (databases.find(name) != databases.end()) {
            return false; // БД уже существует
        }

        auto storage = std::make_shared<OptimizedInMemoryStorage>();
        auto executor = std::make_shared<query_engine::OptimizedQueryExecutor>(storage);
        executor->setLoggingEnabled(false);

        databases[name] = {storage, executor};
        return true;
    }

    bool deleteDatabase(const std::string& name) {
        std::lock_guard<std::mutex> lock(db_mutex);

        if (name == "default") {
            return false; // Нельзя удалить дефолтную БД
        }

        return databases.erase(name) > 0;
    }

    std::vector<std::string> listDatabases() {
        std::lock_guard<std::mutex> lock(db_mutex);
        std::vector<std::string> names;

        for (const auto& pair : databases) {
            names.push_back(pair.first);
        }

        return names;
    }

    std::shared_ptr<query_engine::OptimizedQueryExecutor> getExecutor(const std::string& name) {
        std::lock_guard<std::mutex> lock(db_mutex);

        auto it = databases.find(name);
        if (it == databases.end()) {
            return nullptr;
        }

        return it->second.second;
    }
};

int main() {
    crow::SimpleApp app;

    auto dbManager = std::make_shared<DatabaseManager>();

    CROW_LOG_INFO << "Database Server starting...";

    CROW_ROUTE(app, "/")
    ([](){
        return "Database Server is running! Use POST /api/query to send queries.";
    });

    // Получить список БД
    CROW_ROUTE(app, "/api/db/list")
    .methods(crow::HTTPMethod::Get)
    ([dbManager](const crow::request& req){
        crow::response res;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");

        try {
            auto databases = dbManager->listDatabases();
            res.code = 200;
            res.body = json{
                {"status", "success"},
                {"databases", databases}
            }.dump();
            return res;

        } catch (const std::exception& e) {
            res.code = 500;
            res.body = json{
                {"status", "error"},
                {"message", "Failed to list databases: " + std::string(e.what())}
            }.dump();
            return res;
        }
    });

    // Создать новую БД
    CROW_ROUTE(app, "/api/db/create")
    .methods(crow::HTTPMethod::Post)
    ([dbManager](const crow::request& req){
        crow::response res;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");

        try {
            auto body = json::parse(req.body);
            if (!body.contains("name") || !body["name"].is_string()) {
                res.code = 400;
                res.body = json{
                    {"status", "error"},
                    {"message", "Request body must contain 'name' field"}
                }.dump();
                return res;
            }

            std::string dbName = body["name"];

            // Валидация имени БД
            if (dbName.empty() || dbName.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_") != std::string::npos) {
                res.code = 400;
                res.body = json{
                    {"status", "error"},
                    {"message", "Invalid database name. Use only alphanumeric characters and underscores."}
                }.dump();
                return res;
            }

            if (dbManager->createDatabase(dbName)) {
                res.code = 200;
                res.body = json{
                    {"status", "success"},
                    {"message", "Database created successfully"},
                    {"database", dbName}
                }.dump();
            } else {
                res.code = 409; // Conflict
                res.body = json{
                    {"status", "error"},
                    {"message", "Database already exists"}
                }.dump();
            }
            return res;

        } catch (const json::parse_error& e) {
            res.code = 400;
            res.body = json{
                {"status", "error"},
                {"message", "Invalid JSON: " + std::string(e.what())}
            }.dump();
            return res;
        } catch (const std::exception& e) {
            res.code = 500;
            res.body = json{
                {"status", "error"},
                {"message", "Failed to create database: " + std::string(e.what())}
            }.dump();
            return res;
        }
    });

    // Удалить БД
    CROW_ROUTE(app, "/api/db/delete")
    .methods(crow::HTTPMethod::Post)
    ([dbManager](const crow::request& req){
        crow::response res;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");

        try {
            auto body = json::parse(req.body);
            if (!body.contains("name") || !body["name"].is_string()) {
                res.code = 400;
                res.body = json{
                    {"status", "error"},
                    {"message", "Request body must contain 'name' field"}
                }.dump();
                return res;
            }

            std::string dbName = body["name"];

            if (dbManager->deleteDatabase(dbName)) {
                res.code = 200;
                res.body = json{
                    {"status", "success"},
                    {"message", "Database deleted successfully"}
                }.dump();
            } else {
                res.code = 404;
                res.body = json{
                    {"status", "error"},
                    {"message", "Database not found or cannot be deleted"}
                }.dump();
            }
            return res;

        } catch (const json::parse_error& e) {
            res.code = 400;
            res.body = json{
                {"status", "error"},
                {"message", "Invalid JSON: " + std::string(e.what())}
            }.dump();
            return res;
        } catch (const std::exception& e) {
            res.code = 500;
            res.body = json{
                {"status", "error"},
                {"message", "Failed to delete database: " + std::string(e.what())}
            }.dump();
            return res;
        }
    });

    // Модифицированный эндпоинт для выполнения запросов
    CROW_ROUTE(app, "/api/query")
    .methods(crow::HTTPMethod::Post)
    ([dbManager](const crow::request& req){
        crow::response res;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");

        try {
            auto body = json::parse(req.body);
            if (!body.contains("query") || !body["query"].is_string()) {
                res.code = 400;
                res.body = json{
                    {"status", "error"},
                    {"message", "Request body must contain 'query' string field."}
                }.dump();
                return res;
            }

            std::string query_str = body["query"];
            std::string database = body.value("database", "default");

            // Получаем executor для указанной БД
            auto executor = dbManager->getExecutor(database);
            if (!executor) {
                res.code = 404;
                res.body = json{
                    {"status", "error"},
                    {"message", "Database not found: " + database}
                }.dump();
                return res;
            }

            // 1. Лексический анализ
            query_engine::Lexer lexer(query_str);
            auto tokens = lexer.tokenize();

            // 2. Синтаксический анализ (Парсинг)
            query_engine::Parser parser(tokens);
            auto ast = parser.parse();

            // 3. Проверка на ошибки парсинга
            if (parser.hasError()) {
                res.code = 400;
                res.body = json{
                    {"status", "error"},
                    {"message", "SQL Parse Error"},
                    {"errors", parser.getErrors()}
                }.dump();
                return res;
            }

            if (!ast) {
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
            res.code = 500;
            res.body = json{
                {"status", "error"},
                {"message", "An internal error occurred: " + std::string(e.what())}
            }.dump();
            return res;
        }
    });

    // OPTIONS для CORS - для всех эндпоинтов
    CROW_ROUTE(app, "/api/query")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        crow::response res;
        res.add_header("Access-Control-Allow-Origin", "*");
        res.add_header("Access-Control-Allow-Headers", "Content-Type");
        res.add_header("Access-Control-Allow-Methods", "POST, OPTIONS");
        res.code = 204;
        res.end();
        return res;
    });

    CROW_ROUTE(app, "/api/db/list")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        crow::response res;
        res.add_header("Access-Control-Allow-Origin", "*");
        res.add_header("Access-Control-Allow-Headers", "Content-Type");
        res.add_header("Access-Control-Allow-Methods", "GET, OPTIONS");
        res.code = 204;
        res.end();
        return res;
    });

    CROW_ROUTE(app, "/api/db/create")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        crow::response res;
        res.add_header("Access-Control-Allow-Origin", "*");
        res.add_header("Access-Control-Allow-Headers", "Content-Type");
        res.add_header("Access-Control-Allow-Methods", "POST, OPTIONS");
        res.code = 204;
        res.end();
        return res;
    });

    CROW_ROUTE(app, "/api/db/delete")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        crow::response res;
        res.add_header("Access-Control-Allow-Origin", "*");
        res.add_header("Access-Control-Allow-Headers", "Content-Type");
        res.add_header("Access-Control-Allow-Methods", "POST, OPTIONS");
        res.code = 204;
        res.end();
        return res;
    });

    std::cout << "Database Server is running on http://localhost:8080" << std::endl;
    app.port(8080).multithreaded().run();

    return 0;
}