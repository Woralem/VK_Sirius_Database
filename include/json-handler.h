#pragma once
#include <crow.h>
#include <nlohmann/json.hpp>

namespace JsonHandler {
    crow::response createJsonResponse(int code, const nlohmann::json& body);
    crow::response handleCors(const crow::request& req, const std::string& methods);
    crow::response proxyRequest(const crow::request& req,
                               const std::string& backendUrl,
                               const std::string& method,
                               const std::string& path);
}
