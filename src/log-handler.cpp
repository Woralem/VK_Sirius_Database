#include <crow.h>
#include<string>
#include<format>
#include<cpr/cpr.h>
#include<nlohmann/json.hpp>

#include "log-handler.h"
#include "json-handler.h"

namespace LogHandler {
    using json = nlohmann::json;
    crow::response getQueries(const std::string& cur_db) {
        crow::response res;
        json db_request;
        cpr::Response db_res = cpr::Get(
            cpr::Url{std::format("http://localhost:8080/api/logs/database/{}", cur_db)},
            cpr::Parameters{
                {"success", "true"}
            });

        res.code = db_res.status_code;
        //установка заголовков
        for (const auto& [key, value] : db_res.header) {
            res.add_header(key, value);
        }
        res.add_header("Access-Control-Allow-Origin", "*");
        res.body = db_res.text;
        return res;
    }
    crow::response getErrors(const std::string& cur_db) {
        crow::response res;
        cpr::Response db_res = cpr::Get(
            cpr::Url{std::format("http://localhost:8080/api/logs/database/{}", cur_db)},
            cpr::Parameters{
                {"success", "false"}
            });
        res.code = db_res.status_code;
        //установка заголовков
        for (const auto& [key, value] : db_res.header) {
            res.add_header(key, value);
        }
        res.add_header("Access-Control-Allow-Origin", "*");
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
        cpr::Response db_res = cpr::Delete(
            cpr::Url{std::format("http://localhost:8080/api/logs/database/{}", id)});
        res.code = db_res.status_code;
        //установка заголовков
        for (const auto& [key, value] : db_res.header) {
            res.add_header(key, value);
        }
        res.add_header("Access-Control-Allow-Origin", "*");
        res.body = db_res.text;
        return res;

    }
    crow::response deleteQueries(const std::string& cur_db) {
        crow::response res;
        cpr::Response db_res = cpr::Delete(
            cpr::Url{std::format("http://localhost:8080/api/logs/database/{}", cur_db)},
            cpr::Parameters{{"success", "true"}});
        res.code = db_res.status_code;
        //установка заголовков
        for (const auto& [key, value] : db_res.header) {
            res.add_header(key, value);
        }
        res.add_header("Access-Control-Allow-Origin", "*");
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
        cpr::Response db_res = cpr::Delete(
            cpr::Url{std::format("http://localhost:8080/api/logs/database/{}", id)});
        res.code = db_res.status_code;
        //установка заголовков
        for (const auto& [key, value] : db_res.header) {
            res.add_header(key, value);
        }
        res.add_header("Access-Control-Allow-Origin", "*");
        res.body = db_res.text;
        return res;
    }
    crow::response deleteErrors(const std::string& cur_db) {
        crow::response res;
        cpr::Response db_res = cpr::Delete(
            cpr::Url{std::format("http://localhost:8080/api/logs/database/{}", cur_db)},
            cpr::Parameters{{"success", "false"}});
        res.code = db_res.status_code;
        //установка заголовков
        for (const auto& [key, value] : db_res.header) {
            res.add_header(key, value);
        }
        res.add_header("Access-Control-Allow-Origin", "*");
        res.body = db_res.text;
        return res;
    }
};