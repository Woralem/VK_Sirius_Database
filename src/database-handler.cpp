#include <nlohmann/json.hpp>
#include <crow.h>
#include <string>
#include <cpr/cpr.h>
#include <algorithm>
#include "database-handler.cpp.h"
#include "http-server.h"
#include "json-handler.h"

namespace DBhandler {
    using json = nlohmann::json;
    crow::response createDB(std::string& cur_db, const json& json_request) {
        CROW_LOG_INFO << "Creating DB";
        crow::response res;
        if (!json_request.contains("database") || !json_request["database"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'db_name' field"}
            });
        }
        cpr::Response db_res = cpr::Post(
            cpr::Url{std::format("{}/api/db/create", HttpServer::getServerURL())},
            cpr::Body{json_request.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");
        res.body=db_res.text;
        CROW_LOG_INFO << "Created DB";

        //Смена текущей БД и логирование этого
        json db_req;
        db_req["from"] = cur_db;
        cur_db = json_request["database"].get<std::string>();
        db_req["to"] = cur_db;
        cpr::Response db_res_log = cpr::Post(
            cpr::Url{std::format("{}/api/db/switch", HttpServer::getServerURL())},
            cpr::Body{db_req.dump()});
        cur_db = json_request["database"].get<std::string>();
        CROW_LOG_INFO << "Changed active DB";

        return res;
    }
    crow::response renameDB(std::string& cur_db, json& json_request) {
        crow::response res;
        if (!json_request.contains("newName") || !json_request["newName"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'newName' field"}
            });
        }
        json_request["oldName"] = cur_db;
        cur_db = json_request["newName"].get<std::string>();
        cpr::Response db_res = cpr::Post(
            cpr::Url{std::format("{}/api/db/rename", HttpServer::getServerURL())},
            cpr::Body{json_request.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");
        res.body=db_res.text;
        return res;
    }
    crow::response removeDB(std::string& cur_db, const std::string& database) {
        std::string db_name = database;
        crow::response res;
        cpr::Response db_res = cpr::Post(
            cpr::Url{std::format("{}/api/db/delete", HttpServer::getServerURL())},
            cpr::Body{json{{"database", db_name}}.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");
        res.body=db_res.text;
        CROW_LOG_INFO << "Deleted DB";

        //Если удалили текущую, то смена текущей
        if (db_name == cur_db) {
            CROW_LOG_INFO << "Change active DB to default";
            json db_req_log;
            db_req_log["from"] = cur_db;
            cur_db = "default";
            db_req_log["to"] = cur_db;
            cpr::Response db_res = cpr::Post(
                cpr::Url{std::format("{}/api/db/switch", HttpServer::getServerURL())},
                cpr::Body{db_req_log.dump()});
        }
        return res;
    }
    crow::response listDB() {
        crow::response res;
        cpr::Response db_res = cpr::Get(
                cpr::Url{std::format("{}/api/db/list", HttpServer::getServerURL())});
        res.code = db_res.status_code;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");
        json dblist = json::parse(db_res.text);

        std::vector<std::string> vec {"default"};
        for (const auto& item : dblist["databases"]) {
            if (item.get<std::string>() != "default") {
                vec.push_back(item.get<std::string>());
            }
        }
        std::sort(vec.begin() + 1, vec.end());

        res.body = json{{"databases", vec}}.dump();
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
                return createDB(cur_db, json_request["data"]);
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
            cpr::Url{std::format("{}/api/db/switch", HttpServer::getServerURL())},
            cpr::Body{db_req.dump()});
        cur_db = json_request["db_name"].get<std::string>();
        return JsonHandler::createJsonResponse(200, json{
            {"status", "success"},
            {"message", "Database was changed successfully"}
        });
    }
}