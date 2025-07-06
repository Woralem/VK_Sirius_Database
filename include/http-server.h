#define HTTP_SERVER_H
#ifdef HTTP_SERVER_H
#include <string>
#include<crow.h>

#include"windowManager.h"

class HttpServer {
public:
    HttpServer();
    ~HttpServer();
    void setupRoutes();
    void setupCorsRoutes();
    void run(int port = 8090);
private:
    crow::SimpleApp app;
    std::string cur_db;
    WindowManager wm;
};
#endif //HTTP_SERVER_H
