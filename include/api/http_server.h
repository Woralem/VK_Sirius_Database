#pragma once

#include <crow.h>
#include <memory>

class DatabaseManager;

class HttpServer {
private:
    crow::SimpleApp app;
    std::shared_ptr<DatabaseManager> dbManager;

    void setupRoutes();
    void setupCorsRoutes();

public:
    HttpServer();
    void run(int port = 8080);
};