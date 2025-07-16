#pragma once
#include <crow.h>
#include<string>
#include<nlohmann/json.hpp>
using json = nlohmann::json;
namespace TableHandler {
    enum class DB_Table_POST {
        REVAL_CELL,
        RETYPE_COLUMN,
        RENAME_COLUMN,
        ERR
    };
    DB_Table_POST inline db_table_post(const std::string& request) {
        if (request == "REVAL_CELL") { return DB_Table_POST::REVAL_CELL;}
        if (request=="RENAME_COLUMN") { return DB_Table_POST::RENAME_COLUMN;}
        if (request=="RETYPE_COLUMN") {return DB_Table_POST::RETYPE_COLUMN;}
        return DB_Table_POST::ERR;
    }
    crow::response table(const std::string& cur_db, const std::string& cur_table,
         json& cur_headers, const std::string& req);
    crow::response revalCell(const std::string& cur_db, const std::string& cur_table,
        const json& cur_headers, const json& json_request);
    crow::response retypeColumn(const std::string& cur_db, const std::string& cur_table, const json& json_request);
    crow::response renameColumn(const std::string& cur_db, const std::string& cur_table,
        json& cur_headers, const json& json_request);
    crow::response makeQuery(const std::string& cur_db, const std::string& req);
    //Внимательно посмотреть функцию
    int parse_column_number_from_cell_id(std::string_view cell_id);

};