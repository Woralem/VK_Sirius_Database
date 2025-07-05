#include <crow.h>
#include <string>
#include <nlohmann/json.hpp>

#include "json-handler.h"

namespace JsonHandler {
    using json = nlohmann::json;
    crow::response createJsonResponse(int code, const json& body) {
        crow::response res;
        res.code = code;
        res.add_header("Content-Type", "application/json");
        res.add_header("Access-Control-Allow-Origin", "*");
        res.body = body.dump();
        return res;
    }
    crow::response handleCors(const crow::request& req, const std::string& methods) {
        crow::response res;
        res.add_header("Access-Control-Allow-Origin", "*");
        res.add_header("Access-Control-Allow-Headers", "Content-Type");
        res.add_header("Access-Control-Allow-Methods", methods);
        res.code = 204;
        res.end();
        return res;
    }

}