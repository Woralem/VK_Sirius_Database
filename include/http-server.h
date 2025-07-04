#define HTTP_SERVER_H
#ifdef HTTP_SERVER_H
#include <string>
class HttpServer {
public:
    HttpServer();
    ~HttpServer();
    std::string cur_bd;
    std::string cur_tab;//Это для работы с окнами: пока не знаю, как
    enum class DB_POST {
        CREATE,
        REMOVE,
        RENAME,//??
        ERR
    };
    enum class DB_Table_POST {
        RENAME,//??
        REVAL_CELL,
        RETYPE_COLUMN,
        RENAME_COLUMN,
        ERR
    };
    void run(int port = 8080);
    bool in_table(int id, const std::string& table_name);
    int find_id(std::string table_name);
    static DB_POST db_post(const std::string& request) {
        if (request == "CREATE") { return DB_POST::CREATE; }
        if (request == "REMOVE") { return DB_POST::REMOVE; }
        if (request == "RENAME") { return DB_POST::RENAME; }
        return DB_POST::ERR;
    }
    static DB_Table_POST db_table_post(const std::string& request) {
        if (request == "RENAME") { return DB_Table_POST::RENAME; }
        if (request == "REVAL_CELL") { return DB_Table_POST::REVAL_CELL;}
        if (request=="RENAME_COLUMN") { return DB_Table_POST::RENAME_COLUMN;}
        return DB_Table_POST::ERR;
    }
};
#endif //HTTP_SERVER_H
