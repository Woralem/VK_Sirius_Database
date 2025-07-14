#include"http-server.h"
#include <sodium.h>

int main() {
    HttpServer server;
    server.run();
    return 0;
}