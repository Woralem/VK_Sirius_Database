#include <nlohmann/json.hpp>
#include <crow.h>
#include<string>
#include<cpr/cpr.h>

#include "table-handler.h"

#include "http-server.h"
#include "json-handler.h"

namespace TableHandler{
    using json = nlohmann::json;
    //Возможно с одиночными кавычками ошибка будет!!
    crow::response revalCell(const std::string& cur_db, const std::string& cur_table,
        const json& cur_headers, const json& cur_types, const json& json_request) {
        crow::response res;
        CROW_LOG_INFO << "cell-handler started";
        if (!json_request.contains("cell_id") || !json_request["cell_id"].is_string()) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"error", "Request body must contain 'cell_id' field"}
            });
        }
        CROW_LOG_INFO << "1";
        if (!json_request.contains("new_value")) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"error", "Request body must contain 'new_value' field"}
            });
        }
        CROW_LOG_INFO << "2";
        if (!json_request.contains("row")) {
            return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"error", "Request body must contain 'row' field"}
            });
        }
        CROW_LOG_INFO << "3";
        std::string db_name = cur_db;
        if (cur_table == "") {
            return JsonHandler::createJsonResponse(200, json{
                {"status", "success"},
                {"message", "There are no active table. Nothing was changed"}
            });
        }
        CROW_LOG_INFO << "4";
        std::string table = cur_table;
        //извлекаем из id номер колонки
        CROW_LOG_INFO << "5";
        int column_id = parse_column_number_from_cell_id(json_request["cell_id"].get<std::string>());
        //получаем название колонки
        CROW_LOG_INFO << "6 " << column_id << '\n' << cur_headers;
        std::string column = cur_headers[column_id].get<std::string>();
        CROW_LOG_INFO << "id was parsed";
        std::string new_value = json_request["new_value"].get<std::string>();
        if (cur_types[column_id] == "VARCHAR") {
            new_value = "'" + new_value + "'";
        }
        /*
        if (json_request["new_value"].is_string()) {
            new_value = "'"+ json_request["new_value"].get<std::string>() + "'";
        } else if (json_request["new_value"].is_number_integer()) {
            new_value = std::to_string(json_request["new_value"].get<int>());
        } else if (json_request["new_value"].is_number_float()) {
            new_value = std::to_string(json_request["new_value"].get<double>());
        } else if (json_request["new_value"].is_boolean()) {
            new_value = json_request["new_value"].get<bool>()  ? "TRUE" : "FALSE";
        } else if (json_request["new_value"].is_null()) {
            new_value = "NULL";
        }*/
        CROW_LOG_INFO << "new_value was parsed";
        std::string sql_query = "UPDATE " + table + " SET " + column + " = " + new_value + " WHERE";
        const auto& row = json_request["row"];
        for (size_t i = 0; i < row.size(); ++i) {
            std::string col = cur_headers[i].get<std::string>();
            std::string val;
            if (row[i]["content"].is_string()) {
                val = "'" + row[i]["content"].get<std::string>() + "'";
            } else if (row[i]["content"].is_number_integer()) {
                val = std::to_string(row[i]["content"].get<int>());
            }else if (row[i]["content"].is_number_float()){
                val = std::to_string(row[i]["content"].get<double>());
            } else if (row[i]["content"].is_boolean()) {
                val = row[i]["content"].get<bool>() ? "TRUE" : "FALSE";
            } else if (row[i]["content"].is_null()) {
                val = "NULL";
            }
            sql_query += " " + col + " = " + val + " AND";
        }
            //удаляем последнее: "AND "
        for (int i = 0; i < 4; ++i) {
            sql_query.pop_back();
        }
        CROW_LOG_INFO << "row was parsed";
        sql_query.push_back(';');
        json db_req;
        CROW_LOG_INFO<< "SQL query: " << sql_query;
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
    crow::response retypeColumn(const std::string& cur_db, const std::string& cur_table,
        const json& cur_headers, json& cur_types, const json& json_request) {
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
        for (int i = 0; i < cur_headers.size(); ++i) {
            if (json_request["column_name"].get<std::string>() == cur_headers[i].get<std::string>()) {
                cur_types[i] = json_request["new_type"];
                break;
            }
        }
        json db_req;
        db_req["database"] = cur_db;
        db_req["query"] = "ALTER TABLE " + cur_table + " ALTER COLUMN " +
            (std::string)json_request["column_name"].get<std::string>() + " TYPE " +
                (std::string)json_request["new_type"].get<std::string>() + ";";
        CROW_LOG_INFO<< "SQL query: " << db_req["query"];
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
    crow::response renameColumn(const std::string& cur_db, const std::string& cur_table,
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
        CROW_LOG_INFO<< "SQL query: " << db_req["query"];
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
                break;
            }
        }
        return res;
    }
    crow::response makeQuery(const std::string& cur_db, std::string& cur_table,
        json& cur_headers, json& cur_types, const std::string& req) {
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
        json json_response = json::parse(res.body);
        if (json_response["status"] == "success" && json_response["isSelect"].get<bool>() == true) {
            cur_table = json_response["table_name"].get<std::string>();
            cur_headers = json::array();
            cur_types = json::array();
            for (const auto& item : json_response["header"]) {
                cur_headers.push_back(item["content"].get<std::string>());
                cur_types.push_back(item["type"].get<std::string>());
            }
            CROW_LOG_INFO << "Table changed";
            CROW_LOG_INFO<< "Header: " << cur_headers;
            CROW_LOG_INFO<< "Types: " << cur_headers;
        }
        CROW_LOG_INFO<< "Table: " << cur_table;
        return res;
    }
    crow::response table(const std::string& cur_db, const std::string& cur_table,
         json& cur_headers, json& cur_types, const std::string& req) {
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
                return retypeColumn(cur_db, cur_table, cur_headers, cur_types, json_request["data"]);
            case DB_Table_POST::RENAME_COLUMN:
                return renameColumn(cur_db, cur_table, cur_headers, json_request["data"]);
            case DB_Table_POST::REVAL_CELL:
                return revalCell(cur_db, cur_table, cur_headers, cur_types, json_request["data"]);
            case DB_Table_POST::ERR:
                return JsonHandler::createJsonResponse(400, json{
                    {"status", "error"},
                {"error", "Invalid request type"}});
        }
    }
    int parse_column_number_from_cell_id(std::string_view cell_id) {
        // Находим последнее подчеркивание
        size_t last_underscore = cell_id.rfind('_');
        // Извлекаем подстроку после последнего подчеркивания
        std::string_view number_part = cell_id.substr(last_underscore + 1);

        // Преобразуем в число
        char* end;
        long column_number = std::strtol(number_part.data(), &end, 10);

        return static_cast<int>(column_number);
    }
};