#include <string>
#include <unordered_map>
#include <nlohmann/json.hpp>
#include <crow.h>

#include "windowManager.h"
#include "json-handler.h"

using json = nlohmann::json;
crow::response WindowManager::get() {
    return JsonHandler::createJsonResponse(200, json{
        {"status", "success"},
        {"data", WindowManager::manager}
    });

}//Получить все пары ключ-значение json строкой
crow::response WindowManager::get(const std::string& req) {
    json json_request = json::parse(req);
    if (!json_request.contains("id") || !json_request["id"].is_string()) {
        return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'id' field"}
            });
    }
    std::string id = json_request["id"].get<std::string>();
    if (!WindowManager::manager.contains(id)) {
        return JsonHandler::createJsonResponse(409, json{
            {"status", "error"},
            {"error", "Unknown id: " + id}
        });
    }
    return JsonHandler::createJsonResponse(200, json{
        {"status", "success"},
        {"data", {
            {id, WindowManager::manager[id]}
            }
        }
    });
}//Получить пару по id
crow::response WindowManager::remove() {
    manager.clear();
    return JsonHandler::createJsonResponse(200, json{
        {"status", "success"}
    });
}//Удалить всё
crow::response WindowManager::remove(const std::string& req) {
    json json_request = json::parse(req);
    if (!json_request.contains("id") || !json_request["id"].is_string()) {
        return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'id' field"}
            });
    }
    std::string id = json_request["id"].get<std::string>();
    if (!WindowManager::manager.contains(id)) {
        return JsonHandler::createJsonResponse(409, json{
            {"status", "error"},
            {"error", ("Unknown id: " + id)}
            });//Conflict
    }
    manager.erase(id);
    return JsonHandler::createJsonResponse(200, json{
        {"status", "success"}
    });
}//Удалить по id
crow::response WindowManager::add(const std::string& req) {
    json json_request = json::parse(req);
    if (!json_request.contains("id") || !json_request["id"].is_string()) {
        return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'id' field"}
            });
    }
    if (!json_request.contains("value") || !json_request["value"].is_string()) {
        return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'value' field"}
            });
    }
    std::string id = json_request["id"].get<std::string>();
    if (WindowManager::manager.contains(id)) {
        return JsonHandler::createJsonResponse(409, json{
            {"status", "error"},
            {"error", ("id: " + id + " is already in use: ")}
            });//Conflict
    }
    WindowManager::manager[id] = json_request["value"].get<std::string>();
    return JsonHandler::createJsonResponse(200, json{
        {"status", "success"},
    });
}//Добавить окно
crow::response WindowManager::update (const std::string& req) {
    json json_request = json::parse(req);
    if (!json_request.contains("id") || !json_request["id"].is_string()) {
        return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'id' field"}
            });
    }
    if (!json_request.contains("value") || !json_request["value"].is_string()) {
        return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'value' field"}
            });
    }
    std::string id = json_request["id"].get<std::string>();
    if (!WindowManager::manager.contains(id)) {
        return JsonHandler::createJsonResponse(409, json{
            {"status", "error"},
            {"error", ("Unknown id: " + id)}
            });//Conflict
    }
    WindowManager::manager[id] = json_request["value"].get<std::string>();
    return JsonHandler::createJsonResponse(200, json{
        {"status", "success"},
    });

}//Обновить существующее окно
crow::response WindowManager::getList () {
    json windows = json::array();
    for (auto& [id, window] : WindowManager::manager) {
        windows.push_back(id);
    }
    return JsonHandler::createJsonResponse(200, json{
        {"status", "success"},
        {"data", windows}
    });
}//Получить список id существующих окон