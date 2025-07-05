#pragma once
#include <string>
#include <nlohmann/json.hpp>
#include<crow.h>
using json = nlohmann::json;
namespace JsonHandler {
    crow::response createJsonResponse(int code, const json& body);
    crow::response handleCors(const crow::request& req, const std::string& methods);
}
