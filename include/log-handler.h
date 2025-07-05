#include<string>
#include<crow.h>
namespace LogHandler {
    crow::response getQueries(const std::string& cur_db);
    crow::response getErrors(const std::string& cur_db);
    crow::response deleteQuery(const std::string& cur_db, const std::string& req);
    crow::response deleteQueries(const std::string& cur_db);
    crow::response deleteError(const std::string& cur_db, const std::string& req);
    crow::response deleteErrors(const std::string& cur_db);
}