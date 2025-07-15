#include "http-server.h"
#include "json-handler.h"
#include <iostream>
#include <format>

const std::string HttpServer::BACKEND_URL = "http://localhost:8080";

HttpServer::HttpServer() {
    std::cout << "API Layer Server created!" << std::endl;
    setupCorsRoutes();
    setupRoutes();
}

HttpServer::~HttpServer() {
    std::cout << "API Layer Server destroyed." << std::endl;
}

void HttpServer::setupRoutes() {
    // Основной маршрут
    CROW_ROUTE(app, "/")
    ([](){
        return "API Layer is running! All requests are proxied to backend.";
    });


    // Список баз данных
    CROW_ROUTE(app, "/api/db/list").methods(crow::HTTPMethod::Get)
    ([this](const crow::request& req){
        return JsonHandler::proxyRequest(req, BACKEND_URL, "GET", "/api/db/list");
    });

    // Создание базы данных
    CROW_ROUTE(app, "/api/db/create").methods(crow::HTTPMethod::Post)
    ([this](const crow::request& req){
        return JsonHandler::proxyRequest(req, BACKEND_URL, "POST", "/api/db/create");
    });

    // Переименование базы данных
    CROW_ROUTE(app, "/api/db/rename").methods(crow::HTTPMethod::Post)
    ([this](const crow::request& req){
        return JsonHandler::proxyRequest(req, BACKEND_URL, "POST", "/api/db/rename");
    });

    // Удаление базы данных
    CROW_ROUTE(app, "/api/db/delete").methods(crow::HTTPMethod::Post)
    ([this](const crow::request& req){
        return JsonHandler::proxyRequest(req, BACKEND_URL, "POST", "/api/db/delete");
    });

    // Переключение базы данных (для логирования)
    CROW_ROUTE(app, "/api/db/switch").methods(crow::HTTPMethod::Post)
    ([this](const crow::request& req){
        return JsonHandler::proxyRequest(req, BACKEND_URL, "POST", "/api/db/switch");
    });

    // SQL запросы
    CROW_ROUTE(app, "/api/query").methods(crow::HTTPMethod::Post)
    ([this](const crow::request& req){
        return JsonHandler::proxyRequest(req, BACKEND_URL, "POST", "/api/query");
    });

    // История запросов
    CROW_ROUTE(app, "/api/history").methods(crow::HTTPMethod::Get)
    ([this](const crow::request& req){
        return JsonHandler::proxyRequest(req, BACKEND_URL, "GET", "/api/history");
    });

    // Получить логи
    CROW_ROUTE(app, "/api/logs").methods(crow::HTTPMethod::Get)
    ([this](const crow::request& req){
        return JsonHandler::proxyRequest(req, BACKEND_URL, "GET", "/api/logs");
    });

    // Массовое удаление логов
    CROW_ROUTE(app, "/api/logs").methods(crow::HTTPMethod::Delete)
    ([this](const crow::request& req){
        return JsonHandler::proxyRequest(req, BACKEND_URL, "DELETE", "/api/logs");
    });

    // Скачать логи
    CROW_ROUTE(app, "/api/logs/download").methods(crow::HTTPMethod::Get)
    ([this](const crow::request& req){
        return JsonHandler::proxyRequest(req, BACKEND_URL, "GET", "/api/logs/download");
    });

    // Очистить логи
    CROW_ROUTE(app, "/api/logs/clear").methods(crow::HTTPMethod::Post)
    ([this](const crow::request& req){
        return JsonHandler::proxyRequest(req, BACKEND_URL, "POST", "/api/logs/clear");
    });

    // Логи по базе данных
    CROW_ROUTE(app, "/api/logs/database/<string>").methods(crow::HTTPMethod::Get)
    ([this](const crow::request& req, const std::string& database){
        std::string path = std::format("/api/logs/database/{}", database);
        return JsonHandler::proxyRequest(req, BACKEND_URL, "GET", path);
    });

    // Удалить логи по базе данных
    CROW_ROUTE(app, "/api/logs/database/<string>").methods(crow::HTTPMethod::Delete)
    ([this](const crow::request& req, const std::string& database){
        std::string path = std::format("/api/logs/database/{}", database);
        return JsonHandler::proxyRequest(req, BACKEND_URL, "DELETE", path);
    });

    // Получить лог по ID
    CROW_ROUTE(app, "/api/logs/<int>").methods(crow::HTTPMethod::Get)
    ([this](const crow::request& req, int id){
        std::string path = std::format("/api/logs/{}", id);
        return JsonHandler::proxyRequest(req, BACKEND_URL, "GET", path);
    });

    // Удалить лог по ID
    CROW_ROUTE(app, "/api/logs/<int>").methods(crow::HTTPMethod::Delete)
    ([this](const crow::request& req, int id){
        std::string path = std::format("/api/logs/{}", id);
        return JsonHandler::proxyRequest(req, BACKEND_URL, "DELETE", path);
    });
}

void HttpServer::setupCorsRoutes() {
    auto& cors = app.get_middleware<crow::CORSHandler>();
    cors
      .global()
      .headers("*")
      .methods("GET"_method, "POST"_method, "PUT"_method, "DELETE"_method, "OPTIONS"_method)
      .origin("*");

    CROW_ROUTE(app, "/api/db/list").methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, OPTIONS");
    });

    CROW_ROUTE(app, "/api/db/create").methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });

    CROW_ROUTE(app, "/api/db/rename").methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });

    CROW_ROUTE(app, "/api/db/delete").methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });

    CROW_ROUTE(app, "/api/db/switch").methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });

    CROW_ROUTE(app, "/api/query").methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });

    CROW_ROUTE(app, "/api/history").methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, OPTIONS");
    });

    CROW_ROUTE(app, "/api/logs").methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, DELETE, OPTIONS");
    });

    CROW_ROUTE(app, "/api/logs/download").methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, OPTIONS");
    });

    CROW_ROUTE(app, "/api/logs/clear").methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });

    CROW_ROUTE(app, "/api/logs/database/<string>").methods(crow::HTTPMethod::Options)
    ([](const crow::request& req, const std::string&){
        return JsonHandler::handleCors(req, "GET, DELETE, OPTIONS");
    });

    CROW_ROUTE(app, "/api/logs/<int>").methods(crow::HTTPMethod::Options)
    ([](const crow::request& req, int){
        return JsonHandler::handleCors(req, "GET, DELETE, OPTIONS");
    });
}

void HttpServer::run(int port) {
    std::cout << "API Layer starting on port " << port << "..." << std::endl;
    app.port(port).multithreaded().run();
}
