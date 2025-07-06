#include <nlohmann/json.hpp>
#include <crow.h>
#include<string>
#include<cpr/cpr.h>

#include "table-handler.h"
#include "json-handler.h"

namespace TableHandler{
    using json = nlohmann::json;
    crow::response revalCell(const std::string& cur_db, const json& json_request) {
        crow::response res;
        if (!json_request.contains("table_name") || !json_request["table_name"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'table_name' field"}
            });
        }
        if (!json_request.contains("column_name") || !json_request["column_name"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'column' field"}
            });
        }
        if (!json_request.contains("new_value") || !json_request["new_value"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'new_value' field"}
            });
        }
        if (!json_request.contains("row") || !json_request["row"]) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'row' field"}
            });
        }
        std::string db_name = cur_db;
        std::string table = json_request["table_name"].get<std::string>();
        std::string column = json_request["column_name"].get<std::string>();
        std::string new_value = json_request["new_value"].get<std::string>();
        std::string sql_query = "UPDATE " + table + " SET " + column + " = " + new_value + " WHERE";
        const auto& row = json_request["row"];
        for (size_t i = 0; i < row.size(); ++i) {
            const auto& cell = row[i];
            std::string col = cell.begin().key();
            std::string val = cell.begin().value();
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
        cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        for (const auto& [key, value] : db_res.header) {
            res.add_header(key, value);
        }
        res.body = db_res.text;
        return res;
    }
    crow::response retypeColumn(const std::string& cur_db, const json& json_request) {
        crow::response res;
        if (!json_request.contains("table_name") || !json_request["table_name"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'table_name' field"}
            });
        }
        if (!json_request.contains("column_name") || !json_request["column_name"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'column_name' field"}
            });
        }
        if (!json_request.contains("new_type") || !json_request["new_type"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'new_type' field"}
            });
        }
        json db_req;
        db_req["name"] = cur_db;
        db_req["query"] = "ALTER TABLE " + cur_db + " ALTER COLUMN " +
            (std::string)json_request["column_name"].get<std::string>() + " TYPE " +
                (std::string)json_request["new_type"].get<std::string>() + ";";
        cpr::Response db_res = cpr::Post(
            cpr::Url{"http://localhost:8080/api/query"},
            cpr::Body{db_req.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        for (const auto& [key, value] : db_res.header) {
            res.add_header(key, value);
        }
        res.body = db_res.text;
        return res;
    }
    crow::response renameColumn(const std::string& cur_db, const json& json_request) {
        crow::response res;
        if (!json_request.contains("table_name") || !json_request["table_name"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'table_name' field"}
            });
        }
        if (!json_request.contains("old_column_name") || !json_request["old_column_name"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'old_column_name' field"}
            });
        }
        if (!json_request.contains("new_column_name") || !json_request["new_column_name"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'new_column_name' field"}
            });
        }
        json db_req;
        db_req["name"] = cur_db;
        db_req["query"] = "ALTER TABLE " + json_request["table_name"].get<std::string>()
        +" RENAME COLUMN " +json_request["old_column_name"].get<std::string>() + " TO " +
                json_request["new_column_name"].get<std::string>() + ";";
        cpr::Response db_res = cpr::Post(
            cpr::Url{"http://localhost:8080/api/query"},
            cpr::Body{db_req.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        for (const auto& [key, value] : db_res.header) {
            res.add_header(key, value);
        }
        res.body = db_res.text;
        return res;
    }
    crow::response makeQuery(const std::string& cur_db, const std::string& req) {
        crow::response res;
        json json_request = json::parse(req);
        if (!json_request.contains("query") || !json_request["query"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'query' field"}
            });
        }
        json db_req;
        db_req["name"] = cur_db;
        db_req["query"] = json_request["query"].get<std::string>();
        cpr::Response db_res = cpr::Post(
            cpr::Url{"http://localhost:8080/api/query"},
            cpr::Body{db_req.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        //установка заголовков
        for (const auto& [key, value] : db_res.header) {
            res.add_header(key, value);
        }
        res.body = db_res.text;
        return res;
    }
    crow::response table(const std::string& cur_db, const std::string& req) {
        json json_request = json::parse(req);
        if (!json_request.contains("type") || !json_request["type"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'type' field"}
            });
        }
        if (json_request.contains("data") && json_request["data"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'data' field"}
            });
        }
        DB_Table_POST type = db_table_post(json_request["type"].get<std::string>());
        switch (type) {
            case DB_Table_POST::RETYPE_COLUMN:
                return retypeColumn(cur_db, json_request["data"]);
            case DB_Table_POST::RENAME_COLUMN:
                return renameColumn(cur_db, json_request["data"]);
            case DB_Table_POST::REVAL_CELL:
                return revalCell(cur_db, json_request["data"]);
            case DB_Table_POST::ERR:
                return JsonHandler::createJsonResponse(400, json{
                    {"status", "error"},
                {"error", "Invalid request type"}});
        }
    }
};