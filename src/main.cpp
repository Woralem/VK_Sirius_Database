#include "api/http_server.h"

int main() {
    HttpServer server;
    server.run(8080);
    return 0;
}