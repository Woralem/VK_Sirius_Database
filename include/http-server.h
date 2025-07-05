#define HTTP_SERVER_H
#ifdef HTTP_SERVER_H
#include <string>
#include<crow.h>
class HttpServer {
public:
    HttpServer();
    ~HttpServer();
    std::string cur_tab;//Это для работы с окнами: пока не знаю, как
    void setupRoutes();
    void setupCorsRoutes();
    void run(int port = 8090);
private:
    crow::SimpleApp app;
    std::string cur_db;
};
#endif //HTTP_SERVER_H
