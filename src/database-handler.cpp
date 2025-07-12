#include <nlohmann/json.hpp>
#include <crow.h>
#include <string>
#include <cpr/cpr.h>

#include "database-handler.cpp.h"
#include "json-handler.h"

namespace DBhandler {
    using json = nlohmann::json;
    crow::response createDB(const json& json_request) {
        CROW_LOG_INFO << "Creating DB";
        crow::response res;
        if (!json_request.contains("database") || !json_request["database"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'db_name' field"}
            });
        }

        cpr::Response db_res = cpr::Post(
            cpr::Url{"http://localhost:8080/api/db/create"},
            cpr::Body{json_request.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        CROW_LOG_INFO << "Created DB";
        res.code = db_res.status_code;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");
        res.body=db_res.text;
        return res;
    }
    crow::response renameDB(std::string& db_name, json& json_request) {
        crow::response res;
        if (!json_request.contains("newName") || !json_request["newName"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'newName' field"}
            });
        }
        json_request["oldName"] = db_name;
        db_name = json_request["newName"].get<std::string>();
        cpr::Response db_res = cpr::Post(
            cpr::Url{"http://localhost:8080/api/db/rename"},
            cpr::Body{json_request.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");
        res.body=db_res.text;
        return res;
    }
    crow::response removeDB(std::string& db_name) {
        crow::response res;
        cpr::Response db_res = cpr::Post(
            cpr::Url{"http://localhost:8080/api/db/delete"},
            cpr::Body{json{{"database", db_name}}.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        db_name = "default";
        res.code = db_res.status_code;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");
        res.body=db_res.text;
        return res;
    }
    crow::response listDB() {
        crow::response res;
        cpr::Response db_res = cpr::Get(
                cpr::Url{"http://localhost:8080/api/db/list"});
        res.code = db_res.status_code;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");
        res.body=db_res.text;
        return res;
    }
    crow::response DB(std:: string& cur_db, const std::string& req) {
        json json_request = json::parse(req);
        if (!json_request.contains("type") || !json_request["type"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'type' field"}
            });
        }
        if (!json_request.contains("data")) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'data' field"}
            });
        }
        DB_POST type = db_post(json_request["type"].get<std::string>());
        switch (type) {
            case DB_POST::CREATE:
                return createDB(json_request["data"]);
            case DB_POST::RENAME:
                return renameDB(cur_db, json_request["data"]);
            case DB_POST::ERR:
                return JsonHandler::createJsonResponse(400, json{
                    {"status", "error"},
                {"error", "Invalid request type"}});
        }

    }
    crow::response changeDB(std::string& cur_db, const std::string& req) {
        json json_request = json::parse(req);
        if (!json_request.contains("db_name") || !json_request["db_name"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'db_name' field"}
            });
        }
        json db_req;
        db_req["from"] = cur_db;
        cur_db = json_request["db_name"].get<std::string>();
        db_req["to"] = cur_db;
        cpr::Response db_res = cpr::Post(
            cpr::Url{"http://localhost:8080/api/db/switch}"},
            cpr::Body{db_req.dump()});
        cur_db = json_request["db_name"].get<std::string>();
        return JsonHandler::createJsonResponse(200, json{
            {"status", "success"},
            {"message", "Database was changed successfully"}
        });
    }
}