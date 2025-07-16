#include"http-server.h"
#include <sodium.h>

int main() {
    if (sodium_init() < 0) {
        std::cerr << "Libsodium is not initialized!\n";
        return 1;
    }
    HttpServer server;
    server.run();
    return 0;
}