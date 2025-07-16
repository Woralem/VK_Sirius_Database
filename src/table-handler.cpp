#include <nlohmann/json.hpp>
#include <crow.h>
#include<string>
#include<cpr/cpr.h>

#include "table-handler.h"

#include "http-server.h"
#include "json-handler.h"

namespace TableHandler{
    using json = nlohmann::json;
    crow::response revalCell(const std::string& cur_db, const std::string& cur_table,
        const json& cur_headers, const json& json_request) {
        crow::response res;
        if (!json_request.contains("cell_id") || !json_request["cell_id"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"error", "Request body must contain 'cell_id' field"}
            });
        }
        if (!json_request.contains("new_value") || !json_request["new_value"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"error", "Request body must contain 'new_value' field"}
            });
        }
        if (!json_request.contains("row") || !json_request["row"]) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"error", "Request body must contain 'row' field"}
            });
        }
        std::string db_name = cur_db;
        if (cur_table == "") {
            return JsonHandler::createJsonResponse(200, json{
                {"status", "success"},
                {"message", "There are no active table. Nothing was changed"}
            });
        }
        std::string table = cur_table;
        //извлекаем из id номер колонки
        int column_id = parse_column_number_from_cell_id(json_request["cell_id"].get<std::string>());
        //получаем название колонки
        std::string column = cur_headers[column_id].get<std::string>();
        std::string new_value = json_request["new_value"].get<std::string>();
        std::string sql_query = "UPDATE " + table + " SET " + column + " = " + new_value + " WHERE";
        const auto& row = json_request["row"];
        for (size_t i = 0; i < row.size(); ++i) {
            std::string col = cur_headers[i].get<std::string>();
            std::string val = row[i]["content"].get<std::string>();
            sql_query += col + " = " + val + " AND";
        }
            //удаляем последнее: "AND "
        for (int i = 0; i < 4; ++i) {
            sql_query.pop_back();
        }
        sql_query.push_back(';');
        crow::json::wvalue db_req;
        db_req["database"] = db_name;
        db_req["query"] = sql_query;
        cpr::Response db_res = cpr::Post(
        cpr::Url{std::format("{}/api/query", HttpServer::getServerURL())},
        cpr::Body{db_req.dump()},
        cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");
        res.body=db_res.text;
        return res;
    }
    crow::response retypeColumn(const std::string& cur_db, const std::string& cur_table, const json& json_request) {
        crow::response res;
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
        db_req["database"] = cur_db;
        db_req["query"] = "ALTER TABLE " + cur_table + " ALTER COLUMN " +
            (std::string)json_request["column_name"].get<std::string>() + " TYPE " +
                (std::string)json_request["new_type"].get<std::string>() + ";";
        cpr::Response db_res = cpr::Post(
            cpr::Url{std::format("{}/api/query", HttpServer::getServerURL())},
            cpr::Body{db_req.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");
        res.body=db_res.text;
        return res;
    }
    crow::response renameColumn(const std::string& cur_db,  const std::string& cur_table,
        json& cur_headers, const json& json_request) {
        crow::response res;
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
        std::string old_col_name = json_request["old_column_name"].get<std::string>();
        std::string new_col_name = json_request["new_column_name"].get<std::string>();
        json db_req;
        db_req["database"] = cur_db;
        db_req["query"] = "ALTER TABLE " + cur_table +
        +" RENAME COLUMN " + old_col_name + " TO " + new_col_name + ";";
        cpr::Response db_res = cpr::Post(
            cpr::Url{std::format("{}/api/query", HttpServer::getServerURL())},
            cpr::Body{db_req.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");
        res.body=db_res.text;
        //Обновление имени колонки
        for (auto& col_name : cur_headers) {
            if (col_name == old_col_name) {
                col_name = new_col_name;
            }
        }
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
        db_req["database"] = cur_db;
        db_req["query"] = json_request["query"].get<std::string>();
        cpr::Response db_res = cpr::Post(
            cpr::Url{std::format("{}/api/query", HttpServer::getServerURL())},
            cpr::Body{db_req.dump()},
            cpr::Header{{"Content-Type", "application/json"}});
        res.code = db_res.status_code;
        //установка заголовков
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");
        res.body=db_res.text;
        //Обновление cur_table при надобности:

        return res;
    }
    crow::response table(const std::string& cur_db, const std::string& cur_table,
         json& cur_headers, const std::string& req) {
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
        DB_Table_POST type = db_table_post(json_request["type"].get<std::string>());
        switch (type) {
            case DB_Table_POST::RETYPE_COLUMN:
                return retypeColumn(cur_db, cur_table, json_request["data"]);
            case DB_Table_POST::RENAME_COLUMN:
                return renameColumn(cur_db, cur_table, cur_headers, json_request["data"]);
            case DB_Table_POST::REVAL_CELL:
                return revalCell(cur_db, cur_table, cur_headers, json_request["data"]);
            case DB_Table_POST::ERR:
                return JsonHandler::createJsonResponse(400, json{
                    {"status", "error"},
                {"error", "Invalid request type"}});
        }
    }
    //Внимательно просмотреть
    int parse_column_number_from_cell_id(std::string_view cell_id) {
        // Находим последнее подчеркивание
        size_t last_underscore = cell_id.rfind('_');
        if (last_underscore == std::string_view::npos) {
            throw std::invalid_argument("Invalid cell_id format: no underscore found");
        }

        // Извлекаем подстроку после последнего подчеркивания
        std::string_view number_part = cell_id.substr(last_underscore + 1);

        // Преобразуем в число
        char* end;
        long column_number = std::strtol(number_part.data(), &end, 10);

        // Проверяем, что преобразование прошло успешно
        if (end != number_part.data() + number_part.size()) {
            throw std::invalid_argument("Invalid cell_id format: non-numeric column index");
        }

        return static_cast<int>(column_number);
    }
};