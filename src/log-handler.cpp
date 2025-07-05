#include <crow.h>
#include<string>
#include<cpr/cpr.h>
#include<nlohmann/json.hpp>

#include "log-handler.h"
#include "json-handler.h"

namespace LogHandler {
    using json = nlohmann::json;
    crow::response getQueries(const std::string& cur_db) {
        crow::response res;
        json db_request;
        db_request["name"] = cur_db;
        cpr::Response db_res = cpr::Post(
            cpr::Url{"http://localhost:8080/api/ЕНДПОИНТ ДЛЯ ПОЛУЧЕНИЯ ТАБЛИЦЫ ЗАПРОСОВ"},
            cpr::Body{db_request.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        //установка заголовков
        for (const auto& [key, value] : db_res.header) {
            res.add_header(key, value);
        }
        res.body = db_res.text;
        return res;
    }
    crow::response getErrors(const std::string& cur_db) {
        crow::response res;
        json db_request;
        db_request["name"] = cur_db;
        cpr::Response db_res = cpr::Post(
            cpr::Url{"http://localhost:8080/api/ЕНДПОИНТ ДЛЯ ПОЛУЧЕНИЯ ТАБЛИЦЫ ОШИБОК"},
            cpr::Body{db_request.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        //установка заголовков
        for (const auto& [key, value] : db_res.header) {
            res.add_header(key, value);
        }
        res.body = db_res.text;
        return res;
    }
    crow::response deleteQuery(const std::string& cur_db, const std::string& req) {
        crow::response res;
        json json_request = json::parse(req);
        if (!json_request.contains("id") || !json_request["id"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'id' field"}
            });
        }
        std::string id = json_request["id"].get<std::string>();
        json db_request;
        db_request["name"] = cur_db;
        db_request["id"] = id;
        cpr::Response db_res = cpr::Post(
            cpr::Url{"http://localhost:8080/api/УДАЛЕНИЕ ЗАПРОСА ПО ID"},
            cpr::Body{db_request.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        //установка заголовков
        for (const auto& [key, value] : db_res.header) {
            res.add_header(key, value);
        }
        res.body = db_res.text;
        return res;

    }
    crow::response deleteQueries(const std::string& cur_db) {
        crow::response res;
        json db_request;
        db_request["name"] = cur_db;
        cpr::Response db_res = cpr::Post(
            cpr::Url{"http://localhost:8080/api/ЕНДПОИН ДЛЯ УДАЛЕНИЕ ВСЕХ ЗАПРОСОВ"},
            cpr::Body{db_request.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        //установка заголовков
        for (const auto& [key, value] : db_res.header) {
            res.add_header(key, value);
        }
        res.body = db_res.text;
        return res;
    }
    crow::response deleteError(const std::string& cur_db, const std::string& req) {
        crow::response res;
        json json_request = json::parse(req);
        if (!json_request.contains("id") || !json_request["id"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'id' field"}
            });
        }
        std::string id = json_request["id"].get<std::string>();
        json db_request;
        db_request["name"] = cur_db;
        db_request["id"] = id;
        cpr::Response db_res = cpr::Post(
            cpr::Url{"http://localhost:8080/api/ЕНДПОИНТ УДАЛЕНИЕ ОШИБКИ ПО ID"},
            cpr::Body{db_request.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        //установка заголовков
        for (const auto& [key, value] : db_res.header) {
            res.add_header(key, value);
        }
        res.body = db_res.text;
        return res;
    }
    crow::response deleteErrors(const std::string& cur_db) {
        crow::response res;
        json db_request;
        db_request["name"] = cur_db;
        cpr::Response db_res = cpr::Post(
            cpr::Url{"http://localhost:8080/api/ЕНДПОИНТ УДАЛЕНИЯ ВСЕХ ОШИБОК"},
            cpr::Body{db_request.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        //установка заголовков
        for (const auto& [key, value] : db_res.header) {
            res.add_header(key, value);
        }
        res.body = db_res.text;
        return res;
    }
};