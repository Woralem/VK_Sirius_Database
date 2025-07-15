#include "http-server.h"
#include <iostream>

int main() {
    try {
        HttpServer server;
        server.run(8090);
    } catch (const std::exception& e) {
        std::cerr << "Server error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
