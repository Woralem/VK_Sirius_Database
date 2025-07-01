#pragma once
#include<crow.h>
class HttpServer {
public:
    HttpServer();
    ~HttpServer();

    void run(int port = 8080);
    void stop();
private:
    crow::SimpleApp app;
};