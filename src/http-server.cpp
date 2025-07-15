#include <crow.h>
#include<cpr/cpr.h>
#include<string>
//#include<vector>

#include "http-server.h"
#include "database-handler.cpp.h"
#include "json-handler.h"
#include "log-handler.h"
#include "table-handler.h"
#include "windowManager.h"

HttpServer::HttpServer() {
    cur_db = "default";
    std::cout << "HTTP Server created!" << std::endl;
}

HttpServer::~HttpServer() {
    std::cout << "HTTP Server destroyed." << std::endl;
}

void HttpServer::setupRoutes() {
    CROW_ROUTE(app, "/")
    ([](){
        return "Database Server is running! Use POST /DB/query to send queries.";
    });

    //Сменить текущую БД
    CROW_ROUTE(app, "/changeDB").methods(crow::HTTPMethod::Post)
    ([&](const crow::request& req) {
        try {
            return DBhandler::changeDB(cur_db, req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });

    //Взаимодействие с БД через UI
    CROW_ROUTE(app, "/DB").methods(crow::HTTPMethod::Post)
    ([&] (const crow::request& req) {
        try{
            return DBhandler::DB(cur_db, req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
    }
});

    //Взаимодействие с таблицой через UI
    CROW_ROUTE(app, "/DB/Table").methods(crow::HTTPMethod::Post)
    ([&](const crow::request& req) {
        try {
            return TableHandler::table(cur_db, req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });

    //SQL-запрос
    CROW_ROUTE(app, "/DB/query").methods(crow::HTTPMethod::Post)
    ([&](const crow::request& req) {
        try {
           return TableHandler::makeQuery(cur_db, req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "API Internal error: " + (std::string)e.what());
        }
    });

    CROW_ROUTE(app, "/DB/query/history/delete").methods(crow::HTTPMethod::Post)
    ([&](const crow::request& req) {
        try {
            return LogHandler::deleteQuery(cur_db, req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "API Internal error: " + (std::string)e.what());
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

    //Получить пару по id (id окна, содержимое окна)
    CROW_ROUTE(app, "/get").methods(crow::HTTPMethod::Post)
    ([&](const crow::request& req) {
        try {
            return wm.get(req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });

    //Удалить окно по id
    CROW_ROUTE(app, "/remove").methods(crow::HTTPMethod::Post)
    ([&](const crow::request& req) {
        try {
            return wm.remove(req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });

    //Добавить окно
    CROW_ROUTE(app, "/add").methods(crow::HTTPMethod::Post)
    ([&](const crow::request& req) {
        try {
            return wm.add(req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });

    //Обновить содержимое окна по id
    CROW_ROUTE(app, "/update").methods(crow::HTTPMethod::Post)
    ([&](const crow::request& req) {
        try {
            return wm.update(req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });

    //Сменить окно
    CROW_ROUTE(app, "/change").methods(crow::HTTPMethod::Post)//сменить окно
    ([&](const crow::request& req) {
        try {
            return wm.changeWindow(req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });

    //Обновить текущее окно
    CROW_ROUTE(app, "/update/current").methods(crow::HTTPMethod::Post)//сменить окно
    ([&](const crow::request& req) {
        try {
            return wm.updateCurrent(req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });


    CROW_ROUTE(app, "/DB/query/history").methods(crow::HTTPMethod::Get)
    ([&]() {
        try {
            CROW_LOG_INFO<<"query history was getted";
            return LogHandler::getQueries(cur_db);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });

    CROW_ROUTE(app, "/DB/query/errors").methods(crow::HTTPMethod::Get)
    ([&]() {
        try {
            return LogHandler::getErrors(cur_db);
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
            return JsonHandler::createJsonResponse(500, "API Internal error: " + (std::string)e.what());
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

    //Удалить бд по id
    CROW_ROUTE(app, "/DB/remove").methods(crow::HTTPMethod::Delete)
    ([&](const crow::request& req) {
        try {
            return DBhandler::removeDB(cur_db, req.body);
        }
        catch (const std::exception& e) {
            return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });

    //Получить список названий существующих бд
    CROW_ROUTE(app, "/DB/list").methods(crow::HTTPMethod::Get)
    ([&]() {
        try {
           return DBhandler::listDB();
        }
        catch (const std::exception& e) {
           return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });

    ///Получить все пары ключ-значение json -ом (для окон)
    CROW_ROUTE(app, "/get").methods(crow::HTTPMethod::Get)
    ([&]() {
        try {
           return wm.get();
        }
        catch (const std::exception& e) {
           return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });

    //Удалить все окна
    CROW_ROUTE(app, "/remove").methods(crow::HTTPMethod::Get)
   ([&]() {
       try {
          return wm.remove();
       }
       catch (const std::exception& e) {
          return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
       }
   });

    //Получить список id существующих окон
    CROW_ROUTE(app, "/get/list").methods(crow::HTTPMethod::Get)
    ([&]() {
        try {
           return wm.getList();
        }
        catch (const std::exception& e) {
           return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });

    //Получить информацию о текущем окне
    CROW_ROUTE(app, "/get/current").methods(crow::HTTPMethod::Get)//получение содержимого активного окна
    ([&]() {
        try {
           return wm.getCurrent();
        }
        catch (const std::exception& e) {
           return JsonHandler::createJsonResponse(500, "Internal error: " + (std::string)e.what());
        }
    });
}
void HttpServer::setupCorsRoutes() {
    auto& cors = app.get_middleware<crow::CORSHandler>();
    // Разрешаем все origins, методы и заголовки (аналог CORS(app) в Flask)
    cors
      .global()
      .headers("*")
      .methods("GET"_method, "POST"_method, "PUT"_method, "DELETE"_method)
      .origin("*");
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
        return JsonHandler::handleCors(req, "POST, GET, OPTIONS");
    });
    CROW_ROUTE(app, "/DB/query/errors/delete")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, GET, OPTIONS");
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
        return JsonHandler::handleCors(req, "DELETE, OPTIONS");
    });
    CROW_ROUTE(app, "/DB/list")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, OPTIONS");
    });
    CROW_ROUTE(app, "/get")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, POST, OPTIONS");
    });
    CROW_ROUTE(app, "/remove")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, POST, OPTIONS");
    });
    CROW_ROUTE(app, "/add")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });
    CROW_ROUTE(app, "/update")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });
    CROW_ROUTE(app, "/change")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });
    CROW_ROUTE(app, "/update/current")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "POST, OPTIONS");
    });
    CROW_ROUTE(app, "/get/current")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, OPTIONS");
    });
    CROW_ROUTE(app, "/get/list")
    .methods(crow::HTTPMethod::Options)
    ([](const crow::request& req){
        return JsonHandler::handleCors(req, "GET, OPTIONS");
    });

}
void HttpServer::run (int port) {
    CROW_LOG_INFO << "http Server on API starting...";
    HttpServer::setupCorsRoutes();
    CROW_LOG_INFO << "Cors was set up...";
    HttpServer::setupRoutes();
    CROW_LOG_INFO << "Routers was set up...";
    app.port(port).multithreaded().run();
    std::cout << "Database Server is running on http://localhost:" << port << std::endl;
}