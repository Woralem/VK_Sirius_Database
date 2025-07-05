#include <string>
#include<crow.h>
#include <nlohmann/json.hpp>
using json=nlohmann::json;
namespace DBhandler {
    enum class DB_POST {
        CREATE,
        REMOVE,
        RENAME,
        ERR
    };
    DB_POST inline db_post(const std::string& request) {
        if (request == "CREATE") { return DB_POST::CREATE; }
        if (request == "RENAME") { return DB_POST::RENAME; }
        return DB_POST::ERR;
    }
    crow::response createDB(const json& req);
    crow::response removeDB(std::string& name);
    crow::response renameDB(std::string& name, json& req);
    crow::response listDB();
    crow::response DB(std::string& db_name, const std::string& req);
    crow::response changeDB(std::string& db_name, const std::string& req);
};
