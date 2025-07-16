#pragma once
#include <string>
#include<crow.h>
#include<crow/middlewares/cors.h>
#include"windowManager.h"
#include <nlohmann/json.hpp>
using json = nlohmann::json;

class HttpServer {
public:
    static const std::string& getServerURL() {
        static const std::string instance = "http://database_server:8080";
        return instance;
    }
    HttpServer();
    ~HttpServer();
    void setupRoutes();
    void setupCorsRoutes();
    void run(int port = 8090);
private:
    crow::App<crow::CORSHandler> app;
    //уйдет в Session:
    std::string cur_db;
    std::string cur_table = "";
    json cur_headers;
    WindowManager wm;
};
