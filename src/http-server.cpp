#include <crow.h>
#include<cpr/cpr.h>
#include<string>
#include<vector>
#include "http-server.h"
#include "logger.h"



HttpServer::HttpServer() {
    cur_bd = "default";
    //создание System_logs
    crow::json::wvalue db_req;
    db_req["name"] = "__System_logs";
    cpr::Response db_res = cpr::Post(
                cpr::Url{"http://localhost:8080/api/db/create"},
                cpr::Body{db_req.dump()},
                cpr::Header{"Content-Type", "application/json"});
    std::cout << "HTTP Server created." << std::endl;
}

HttpServer::~HttpServer() {
    //удаление System_logs
    crow::json::wvalue db_req;
    db_req["name"] = "__System_logs";
    cpr::Response db_res = cpr::Post(
                cpr::Url{"http://localhost:8080/api/db/delete"},
                cpr::Body{db_req.dump()},
                cpr::Header{"Content-Type", "application/json"});
    std::cout << "HTTP Server destroyed." << std::endl;
}

void HttpServer::run(int port) {
    crow::SimpleApp app;
    CROW_LOG_INFO << "http Server on API starting...";
    LOG_INFO("Server", "http Server on API starting...");
    CROW_ROUTE(app, "/")
    ([](){
        return "Database Server is running! Use POST /api/query to send queries.";
    });
    CROW_ROUTE(app, "/changeBD").methods(crow::HTTPMethod::Post)
    ([&](crow::request& req) {
        crow::response res;
        try {
            crow::json::rvalue json_request = crow::json::load(req.body);
            if (!json_request || !json_request["bd_name"]) {
                CROW_LOG_ERROR << "Invalid JSON request";
                LOG_ERROR("Server", "Invalid JSON request");
                crow::json::wvalue response;
                response["status"] = "error";
                response["error"] = "Query cannot be empty";
                return crow::response(400, response.dump());
            }
            std::string request_type_str = json_request["bd_name"].s();
            cur_bd = request_type_str;
            crow::json::wvalue response;
            response["status"] = "success";
            return crow::response(200, response.dump());

        }
        catch (const std::exception& e) {
            LOG_ERROR("Server", "Internal error: " + req.body);
            CROW_LOG_ERROR << "Internal error: " << e.what();
            crow::json::wvalue response;
            response["status"] = "error";
            response["error"] = "An internal error occurred: "+ std::string(e.what());
            return crow::response(500, response.dump());
        }
    });
    CROW_ROUTE(app, "/DB").methods(crow::HTTPMethod::Post)
    ([&] (crow::request& req) {
        crow::response res;
        try {
            crow::json::rvalue json_request = crow::json::load(req.body);
            if (!json_request || !json_request.has("type")) {
                CROW_LOG_ERROR << "Invalid JSON: " << req.body;
                LOG_ERROR("Server", "Invalid JSON: " + req.body);
                crow::json::wvalue response;
                response["status"] = "error";
                response["error"] = "Query cannot be empty";
                return crow::response(400, response.dump());
            }
            std::string request_type_str = json_request["type"].s();
            HttpServer::DB_POST request_type = HttpServer::db_post(request_type_str);

            if (!json_request.has("data") || !json_request["data"]) {
                CROW_LOG_ERROR << "Missing 'data' field: " << req.body;
                LOG_ERROR("Server", "Missing 'data' field: " + req.body);
                crow::json::wvalue response;
                response["status"] = "error";
                response["error"] = "Missing 'data' field";
                return crow::response(400, response.dump());
            }
            crow::json::rvalue data = json_request["data"];
            switch (request_type) {
                case HttpServer::DB_POST::CREATE: {
                    if (!data.has("db_name")) {
                        CROW_LOG_ERROR << "Missing 'db_name' field: " << req.body;
                        LOG_ERROR("Server", "Missing 'db_name' field: " + req.body);
                        crow::json::wvalue response;
                        response["status"] = "error";
                        response["error"] = "Missing 'db_name' field";
                        return crow::response (400, response.dump());
                    }
                    //создание БД
                    std::string db_name = data["db_name"].s();
                    crow::json::wvalue db_request;
                    db_request["name"] = db_name;
                    cpr::Response db_res = cpr::Post(
                        cpr::Url{"http://localhost:8080/api/db/create"},
                        cpr::Body{db_request.dump()},
                        cpr::Header{"Content-Type", "application/json"});
                    res.code = db_res.status_code;
                    //установка заголовков
                    for (const auto& [key, value] : db_res.header) {
                        res.add_header(key, value);
                    }
                    res.body = db_res.text;
                    //создание Таблицы запросов
                    db_request = crow::json::wvalue {};
                    db_request["name"] = "__System_logs";
                    db_request["query"] = "CREATE TABLE queries_" + db_name + " (" +
                        " id INTENGER, time TIMESTAMP, query VARCHAR" ");";
                    db_res = cpr::Post(
                        cpr::Url{"http://localhost:8080/api/query"},
                        cpr::Body{db_request.dump()},
                        cpr::Header{"Content-Type", "application/json"});
                    //создание Таблицы ошибок
                    db_request["query"] = "CREATE TABLE errors_" + db_name +
                        " (" + "id INTENGER time TIMESTAMP errors VARCHAR" ");";
                    db_res = cpr::Post(
                        cpr::Url{"http://localhost:8080/api/query"},
                        cpr::Body{db_request.dump()},
                        cpr::Header{"Content-Type", "application/json"});
                    LOG_SUCCESS("Server", "Post created successfully");
                    return res;
                }
                case HttpServer::DB_POST::RENAME: {
                    if (!data.has("new_db_name")) {
                        CROW_LOG_ERROR << "Missing 'new_db_name' field: " << req.body;
                        LOG_ERROR("Server", "Missing 'new_db_name' field: " + req.body);
                        crow::json::wvalue response;
                        response["status"] = "error";
                        response["error"] = "Missing 'new_db_name' field";
                        return crow::response(400, response.dump());
                    }
                    std::string old_db_name = HttpServer::cur_bd;
                    std::string new_db_name = data["new_db_name"].s();
                    crow::json::wvalue db_req;
                    db_req["oldName"] = old_db_name;
                    db_req["newName"] = new_db_name;
                    cpr::Response db_res = cpr::Post(
                        cpr::Url{"http://localhost:8080/api/bd/rename"},
                        cpr::Body{db_req.dump()},
                        cpr::Header{"Content-Type", "application/json"});
                    HttpServer::cur_bd = new_db_name;
                    res.code = db_res.status_code;
                    for (const auto& [key, value] : db_res.header) {
                        res.add_header(key, value);
                    }
                    res.body = db_res.text;
                    //Изменение таблиц системных логов
                    db_req = crow::json::wvalue {};
                    db_req["name"] = "__System_logs";
                    db_req["query"] = "ALTER TABLE queries_" + old_db_name + " RENAME TO queries_" + new_db_name + ";";
                    db_res = cpr::Post(
                        cpr::Url{"http://localhost:8080/api/query"},
                        cpr::Body{db_req.dump()},
                        cpr::Header{"Content-Type", "application/json"});
                    db_req["query"] = "ALTER TABLE queries_" + old_db_name + " RENAME TO errors_" + new_db_name + ";";
                    db_res = cpr::Post(
                        cpr::Url{"http://localhost:8080/api/query"},
                        cpr::Body{db_req.dump()},
                        cpr::Header{"Content-Type", "application/json"});
                    LOG_SUCCESS("Server", "Post created successfully");
                    return res;
                    break;
                }
                case HttpServer::DB_POST::ERR: {
                    CROW_LOG_ERROR << "Invalid request type: " << request_type_str;
                    LOG_ERROR("Server", "Invalid request type: " + request_type_str);
                    crow::json::wvalue response;
                    response["status"] = "error";
                    response["error"] = "Invalid request type" + request_type_str;
                    return crow::response(400, response.dump());
                }
                default:
                    CROW_LOG_ERROR << "Invalid request type: " << request_type_str;
                    LOG_ERROR("Server", "Invalid request type: " + request_type_str);
                    crow::json::wvalue response;
                    response["status"] = "error";
                    response["error"] = "Invalid request type" + request_type_str;
                    return crow::response(400, response.dump());
            }
        }
        catch (const std::exception& e) {
            LOG_ERROR("Server", "Internal error: " + req.body);
            CROW_LOG_ERROR << "Internal error: " << e.what();
            crow::json::wvalue response;
            response["status"] = "error";
            response["error"] = "An internal error occurred: " + std::string(e.what());
            return crow::response(500, response.dump());
    }
});
    CROW_ROUTE(app, "/DB/Table").methods(crow::HTTPMethod::Post)
    ([&](crow::request& req) {
        crow::response res;
        try {
            crow::json::rvalue json_request = crow::json::load(req.body);
            if (!json_request) {
                CROW_LOG_ERROR << "Invalid JSON: " << req.body;
                LOG_ERROR("Server", "Invalid JSON: " + req.body);
                crow::json::wvalue response;
                response["status"] = "error";
                response["error"] = "Query cannot be empty " + req.body;
                return crow::response(400, response.dump());
            }
            if (!json_request.has("type")) {
                CROW_LOG_ERROR << "Missing 'type' field: " << req.body;
                LOG_ERROR("Server", "Missing 'type' field: " + req.body);
                crow::json::wvalue response;
                response["status"] = "error";
                response["error"] = "Missing 'type' field";
                return crow::response(400, response.dump());
            }
            std::string request_type_str = json_request["type"].s();
            if (!json_request.has("data") || !json_request["data"]) {
                CROW_LOG_ERROR << "Missing 'data' field: " << req.body;
                LOG_ERROR("Server", "Missing 'data' field: " + req.body);
                crow::json::wvalue response;
                response["status"] = "error";
                response["error"] = "Missing 'data' field";
                return crow::response(400, response.dump());
            }
            crow::json::rvalue data = json_request["data"];
            std::string sql_query;//везде, кроме ERR
            switch (request_type_str) {
                case HttpServer::DB_Table_POST::RENAME_COLUMN: {
                    if (!data.has("table_name") || !data["table_name"]) {
                        CROW_LOG_ERROR << "Missing 'table_name' field: " << req.body;
                        LOG_ERROR("Server", "Missing 'table_name' field: " + req.body);
                        crow::json::wvalue response;
                        response["status"] = "error";
                        response["error"] = "Missing 'table_name' field";
                        return crow::response(400, response.dump());
                    }
                    if (!data.has("old_column_name") || !data["old_column_name"]) {
                        CROW_LOG_ERROR << "Missing 'old_column_name' field: " << req.body;
                        LOG_ERROR("Server", "Missing 'old_column_name' field: " + req.body);
                        crow::json::wvalue response;
                        response["status"] = "error";
                        response["error"] = "Missing 'old_column_name' field";
                        return crow::response(400, response.dump());
                    }
                    if (!data.has("new_column_name") || !data["new_column_name"]) {
                        CROW_LOG_ERROR << "Missing 'new_column_name' field: " << req.body;
                        LOG_ERROR("Server", "Missing 'new_column_name' field: " + req.body);
                        crow::json::wvalue response;
                        response["status"] = "error";
                        response["error"] = "Missing 'new_column_name' field";
                        return crow::response(400, response.dump());
                    }
                    crow::json::wvalue db_req;
                    db_req["name"] = cur_bd;
                    db_req["query"] = "ALTER TABLE " + cur_bd +" RENAME COLUMN " +
                        (std::string)data["old_column_name"].s() + " TO " +
                            (std::string)data["new_column_name"].s() + ";";
                    db_req["isUser"] = true;
                    cpr::Response db_res = cpr::Post(
                        cpr::Url{"http://localhost:8080/api/query"},
                        cpr::Body{db_req.dump()},
                        cpr::Header{"Content-Type", "application/json"});
                        res.code = db_res.status_code;
                        for (const auto& [key, value] : db_res.header) {
                            res.add_header(key, value);
                        }
                        res.body = db_res.text;
                        LOG_SUCCESS("Server", "Post created successfully");
                        return res;
                    break;
                }
                case HttpServer::DB_Table_POST::RETYPE_COLUMN: {
                    if (!data.has("table_name") || !data["table_name"]) {
                        CROW_LOG_ERROR << "Missing 'table_name' field: " << req.body;
                        LOG_ERROR("Server", "Missing 'table_name' field: " + req.body);
                        crow::json::wvalue response;
                        response["status"] = "error";
                        response["error"] = "Missing 'table_name' field";
                        return crow::response(400, response.dump());
                    }
                    if (!data.has("column_name") || !data["column_name"]) {
                        CROW_LOG_ERROR << "Missing 'column_name' field: " << req.body;
                        LOG_ERROR("Server", "Missing 'column_name' field: " + req.body);
                        crow::json::wvalue response;
                        response["status"] = "error";
                        response["error"] = "Missing 'column_name' field";
                        return crow::response(400, response.dump());
                    }
                    if (!data.has("new_type") || !data["new_type"]) {
                        CROW_LOG_ERROR << "Missing 'new_type' field: " << req.body;
                        LOG_ERROR("Server", "Missing 'new_type' field: " + req.body);
                        crow::json::wvalue response;
                        response["status"] = "error";
                        response["error"] = "Missing 'new_type' field";
                        return crow::response(400, response.dump());
                    }
                    crow::json::wvalue db_req;
                    db_req["name"] = cur_bd;
                    db_req["query"] = "ALTER TABLE " + cur_bd + " ALTER COLUMN " +
                        (std::string)data["column_name"].s() + " TYPE " + (std::string)data["new_type"].s() + ";";
                    db_req["isUser"] = true;
                    cpr::Response db_res = cpr::Post(
                        cpr::Url{"http://localhost:8080/api/query"},
                        cpr::Body{db_req.dump()},
                        cpr::Header{"Content-Type", "application/json"});
                        res.code = db_res.status_code;
                        for (const auto& [key, value] : db_res.header) {
                            res.add_header(key, value);
                        }
                        res.body = db_res.text;
                        LOG_SUCCESS("Server", "Post created successfully");
                        return res;
                    break;
                }
                case HttpServer::DB_Table_POST::REVAL_CELL: {
                    if (!data.has("table_name") || !data["table_name"]) {
                        CROW_LOG_ERROR << "Missing 'table_name' field: " << req.body;
                        LOG_ERROR("Server", "Missing 'table_name' field: " + req.body);
                        crow::json::wvalue response;
                        response["status"] = "error";
                        response["error"] = "Missing 'table_name' field";
                        return crow::response(400, response.dump());
                    }
                    if (!data.has("row") || !data["row"]) {
                        CROW_LOG_ERROR << "Missing 'row' field: " << req.body;
                        LOG_ERROR("Server", "Missing 'row' field: " + req.body);
                        crow::json::wvalue response;
                        response["status"] = "error";
                        response["error"] = "Missing 'row' field";
                        return crow::response(400, response.dump());
                    }
                    if (!data.has("column_name") || !data["column_name"]) {
                        CROW_LOG_ERROR << "Missing 'column_name' field: " << req.body;
                        LOG_ERROR("Server", "Missing 'column_name' field: " + req.body);
                        crow::json::wvalue response;
                        response["status"] = "error";
                        response["error"] = "Missing 'column_name' field";
                        return crow::response(400, response.dump());
                    }
                    if (!data.has("new_value")) {
                        CROW_LOG_ERROR << "Missing 'new_value' field: " << req.body;
                        LOG_ERROR("Server", "Missing 'new_value' field: " + req.body);
                        crow::json::wvalue response;
                        response["status"] = "error";
                        response["error"] = "Missing 'new_value' field";
                        return crow::response(400, response.dump());
                    }
                    std::string db_name = cur_bd;
                    std::string table = data["table_name"].s();
                    std::string column = data["column_name"].s();
                    std::string new_value = data["new_value"].s();
                    sql_query = "UPDATE " + table + " SET " + column + " = " + new_value + " WHERE";
                    try {
                        const crow::json::rvalue& row = data["row"];
                        for (size_t i = 0; i < row.size(); ++i) {
                            const crow::json::rvalue& cell = row[i];
                            std::string col = cell.begin()->key();
                            std::string val = cell.begin()->s();
                            sql_query += col + " = " + val + " AND";
                        }
                        //удаляем последнее: "AND "
                        for (int i = 0; i < 4; ++i) {
                            sql_query.pop_back();
                        }
                        sql_query.push_back(';');
                        crow::json::wvalue db_req;
                        db_req["name"] = db_name;
                        db_req["query"] = sql_query;
                        cpr::Response db_res = cpr::Post(
                        cpr::Url{"http://localhost:8080/api/query"},
                        cpr::Body{db_req.dump()},
                        cpr::Header{"Content-Type", "application/json"});
                        res.code = db_res.status_code;
                        for (const auto& [key, value] : db_res.header) {
                            res.add_header(key, value);
                        }
                        res.body = db_res.text;
                        LOG_SUCCESS("Server", "Post created successfully");
                        return res;
                    }
                    catch (const std::exception& e) {
                        CROW_LOG_ERROR << "'row' must be an array of objects " << e.what();
                        LOG_ERROR("Server", "'row' must be an array of objects");
                        crow::json::wvalue response;
                        response["status"] = "error";
                        response["error"] = "'row' must be an array of objects";
                        return crow::response(400, response.dump());
                    }
                    break;
                }
                case HttpServer::DB_Table_POST::ERR: {
                    CROW_LOG_ERROR << "Invalid request type: " << request_type_str;
                    LOG_ERROR("Server", "Invalid request type: " + request_type_str);
                    crow::json::wvalue response;
                    response["status"] = "error";
                    response["error"] = "Invalid request type" + request_type_str;
                    return crow::response(400, response.dump());
                    break;
                }
            }
        }
        catch (const std::exception& e) {
            LOG_ERROR("Server", "Internal error: " + req.body);
            CROW_LOG_ERROR << "Internal error: " << e.what();
            crow::json::wvalue response;
            response["status"] = "error";
            response["error"] = "An internal error occurred: " + std::string(e.what());
            return crow::response(500, response.dump());
        }
    });
    CROW_ROUTE(app, "/DB/query").methods(crow::HTTPMethod::Post)
    ([&](crow::request& req) {
        crow::response res;
        try {
            crow::json::rvalue json_request = crow::json::load(req.body);
            if (!json_request) {
                CROW_LOG_ERROR << "Invalid JSON: " << req.body;
                LOG_ERROR("Server", "Invalid JSON: " + req.body);
                crow::json::wvalue response;
                response["status"] = "error";
                response["error"] = "Query cannot be empty " + req.body;
                return crow::response(400, response.dump());
            }
            if (!json_request.has("query")) {
                CROW_LOG_ERROR << "Missing 'type' field: " << req.body;
                LOG_ERROR("Server", "Missing 'type' field: " + req.body);
                crow::json::wvalue response;
                response["status"] = "error";
                response["error"] = "Missing 'type' field";
                return crow::response(400, response.dump());
            }
            std::string request_type_str = json_request["query"].s();
            crow::json::wvalue db_req;
            std::string query = json_request["query"].s();
            crow::json::wvalue db_request;
            db_request["name"] = HttpServer::cur_bd;
            db_request["query"] = query;
            db_request["isUser"] = true;
            cpr::Response db_res = cpr::Post(
                cpr::Url{"http://localhost:8080/api/query"},
                cpr::Body{db_request.dump()},
                cpr::Header{"Content-Type", "application/json"});
            res.code = db_res.status_code;
            //установка заголовков
            for (const auto& [key, value] : db_res.header) {
                res.add_header(key, value);
            }
            res.body = db_res.text;
            LOG_SUCCESS("Server", "Post created successfully");
            return res;
        }
        catch (const std::exception& e) {
            LOG_ERROR("Server", "Internal error: " + req.body);
            CROW_LOG_ERROR << "Internal error: " << e.what();
            crow::json::wvalue response;
            response["status"] = "error";
            response["error"] = "An internal error occurred: " + std::string(e.what());
            return crow::response(500, response.dump());
        }
    });
    CROW_ROUTE(app, "/DB/query/history").methods(crow::HTTPMethod::Post)//ВООБЩЕ НЕ ГОТОВО - просмотреть код ещё раз!!!
    ([&](crow::request& req) {
        crow::response res;
        try {
            crow::json::rvalue json_request = crow::json::load(req.body);
            if (!json_request) {
                CROW_LOG_ERROR << "Invalid JSON: " << req.body;
                LOG_ERROR("Server", "Invalid JSON: " + req.body);
                crow::json::wvalue response;
                response["status"] = "error";
                response["error"] = "Query cannot be empty " + req.body;
                return crow::response(400, response.dump());
            }
            if (!json_request.has("type")) {
                CROW_LOG_ERROR << "Missing 'type' field: " << req.body;
                LOG_ERROR("Server", "Missing 'type' field: " + req.body);
                crow::json::wvalue response;
                response["status"] = "error";
                response["error"] = "Missing 'type' field";
                return crow::response(400, response.dump());
            }
            std::string request_type_str = json_request["type"].s();
            if (!json_request.has("data") || !json_request["data"]) {
                CROW_LOG_ERROR << "Missing 'data' field: " << req.body;
                LOG_ERROR("Server", "Missing 'data' field: " + req.body);
                crow::json::wvalue response;
                response["status"] = "error";
                response["error"] = "Missing 'data' field";
                return crow::response(400, response.dump());
            }
            crow::json::rvalue data = json_request["data"];
            std::string sql_query;//везде, кроме ERR
        }
        catch (const std::exception& e) {}
    });
    CROW_ROUTE(app, "DB/remove").methods(crow::HTTPMethod::Get)
    ([&]() {
        crow::response res;
        try {
            crow::json::wvalue db_req;
            db_req["name"] = cur_bd;
            cpr::Response db_res = cpr::Post(
                cpr::Url{"http://localhost:8080/api/bd/delete"},
                cpr::Body{db_req.dump()},
                cpr::Header{"Content-Type", "application/json"});
            res.code = db_res.status_code;
            for (const auto& [key, value] : db_res.header) {
                res.add_header(key, value);
            }
            res.body = db_res.text;
            //Удаление таблицы логов
            db_req = crow::json::wvalue {};
            db_req["name"] = "__System_logs";
            db_req["query"] = "DELETE TABLE queries_" + cur_bd + ";";
            db_res = cpr::Post(
                cpr::Url{"http://localhost:8080/api/query"},
                cpr::Body{db_req.dump()},
                cpr::Header{"Content-Type", "application/json"});
            db_req["query"] = "DELETE TABLE errors_" + cur_bd + ";";
            db_res = cpr::Post(
                cpr::Url{"http://localhost:8080/api/query"},
                cpr::Body{db_req.dump()},
                cpr::Header{"Content-Type", "application/json"});
            cur_bd = "default";
            LOG_SUCCESS("Server", "Get created successfully");
            return res;
        }
        catch (const std::exception& e) {
            LOG_ERROR("Server", "Internal error");
            CROW_LOG_ERROR << "Internal error: " << e.what();
            crow::json::wvalue response;
            response["status"] = "error";
            response["error"] = "An internal error occurred: " + std::string(e.what());
            return crow::response(500, response.dump());
        }
    });
}

