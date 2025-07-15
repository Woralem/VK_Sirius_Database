#pragma once
#include <crow.h>
#include <crow/middlewares/cors.h>
#include <string>

class HttpServer {
public:
    HttpServer();
    ~HttpServer();
    void run(int port = 8090);

private:
    void setupRoutes();
    void setupCorsRoutes();

    crow::App<crow::CORSHandler> app;
    static const std::string BACKEND_URL;
};
