#include "json-handler.h"
#include <cpr/cpr.h>
#include <format>
#include <iostream>

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
        res.add_header("Access-Control-Allow-Headers", "Content-Type, Authorization");
        res.add_header("Access-Control-Allow-Methods", methods);
        res.add_header("Access-Control-Max-Age", "86400");
        res.code = 204;
        return res;
    }

    crow::response proxyRequest(const crow::request& req,
                               const std::string& backendUrl,
                               const std::string& method,
                               const std::string& path) {
        try {
            std::string fullUrl = backendUrl + path;
            cpr::Response backend_response;

            // Подготовка параметров запроса
            cpr::Parameters params;
            auto query_params = crow::query_string(req.url);
            for (auto key : query_params.keys()) {
                params.Add({key, query_params.get(key)});
            }

            // Выполнение запроса в зависимости от метода
            if (method == "GET") {
                backend_response = cpr::Get(
                    cpr::Url{fullUrl},
                    params
                );
            } else if (method == "POST") {
                backend_response = cpr::Post(
                    cpr::Url{fullUrl},
                    cpr::Body{req.body},
                    cpr::Header{{"Content-Type", "application/json"}}
                );
            } else if (method == "DELETE") {
                backend_response = cpr::Delete(
                    cpr::Url{fullUrl},
                    params
                );
            } else if (method == "PUT") {
                backend_response = cpr::Put(
                    cpr::Url{fullUrl},
                    cpr::Body{req.body},
                    cpr::Header{{"Content-Type", "application/json"}}
                );
            }

            // Формирование ответа
            crow::response res;
            res.code = backend_response.status_code;
            res.add_header("Content-Type", "application/json");
            res.add_header("Access-Control-Allow-Origin", "*");
            res.body = backend_response.text;

            // Логирование для отладки
            std::cout << std::format("[PROXY] {} {} -> Backend: {} (Status: {})",
                                   method, path, fullUrl, res.code) << std::endl;

            return res;

        } catch (const std::exception& e) {
            std::cerr << "Proxy error: " << e.what() << std::endl;
            return createJsonResponse(500, json{
                {"status", "error"},
                {"message", std::format("Proxy error: {}", e.what())}
            });
        }
    }
}
