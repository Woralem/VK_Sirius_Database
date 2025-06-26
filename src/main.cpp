#include <crow.h>
#include <nlohmann/json.hpp>
#include <iostream>

using json = nlohmann::json;

int main() {
    crow::SimpleApp app;

    CROW_ROUTE(app, "/")
    ([](){
        return "Database Server is running!";
    });

    CROW_ROUTE(app, "/api/query")
    .methods(crow::HTTPMethod::Post)
    ([](const crow::request& req){
        try {
            auto body = json::parse(req.body);
            std::string query = body["query"];

            json response = {
                {"status", "success"},
                {"message", "Query received: " + query}
            };

            return crow::response(response.dump());
        } catch (const std::exception& e) {
            json error = {
                {"status", "error"},
                {"message", e.what()}
            };
            return crow::response(400, error.dump());
        }
    });

    std::cout << "Database Server is running on http://localhost:8080" << std::endl;
    app.port(8080).multithreaded().run();
    return 0;
}