#include <string>
#include <unordered_map>
#include <nlohmann/json.hpp>
#include <crow.h>

#include "windowManager.h"
#include "json-handler.h"

using json = nlohmann::json;

//Получить все пары ключ-значение json строкой
crow::response WindowManager::get() {
    return JsonHandler::createJsonResponse(200, json{
        {"status", "success"},
        {"data", WindowManager::manager}
    });
}

//Получить пару по id
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
}

//Удалить всё
crow::response WindowManager::remove() {
    manager.clear();
    cur_window = "";
    return JsonHandler::createJsonResponse(200, json{
        {"status", "success"}
    });
}

//Удалить по id
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
            });
    }
    manager.erase(id);
    if (cur_window == id && !manager.empty()) {
        cur_window = manager.begin()->first;
    } else {
        cur_window = "";
    }
    return JsonHandler::createJsonResponse(200, json{
        {"status", "success"},
        {"currentWindow", cur_window}
    });
}

//Добавить окно
crow::response WindowManager::add(const std::string& req) {
    json json_request = json::parse(req);
    if (!json_request.contains("value") || !json_request["value"].is_string()) {
        return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'value' field"}
            });
    }
    std::string id = WindowManager::generate_next();
    WindowManager::cur_window = id;
    if (WindowManager::manager.contains(id)) {
        return JsonHandler::createJsonResponse(409, json{
            {"status", "error"},
            {"error", ("id: " + id + " is already in use: ")}
            });
    }
    WindowManager::manager[id] = json_request["value"].get<std::string>();
    return JsonHandler::createJsonResponse(200, json{
        {"status", "success"},
    });
}

//Обновить содержимое окна по id
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
            });
    }
    WindowManager::manager[id] = json_request["value"].get<std::string>();
    return JsonHandler::createJsonResponse(200, json{
        {"status", "success"},
    });

}

//Сменить окно
crow::response WindowManager::changeWindow(const std::string& req) {
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
            });
    }
    std::string oldWindow = WindowManager::cur_window;
    WindowManager::cur_window = id;
    return JsonHandler::createJsonResponse(200, json{
        {"status", "success"},
        {"newWindow", id},
        {"oldWindow", oldWindow},
        {"data", WindowManager::manager[id]}
    });
}

//Получить список id существующих окон
crow::response WindowManager::getList () {
    json windows = json::array();
    for (auto& [id, window] : WindowManager::manager) {
        windows.push_back(id);
    }
    return JsonHandler::createJsonResponse(200, json{
        {"status", "success"},
        {"data", windows}
    });
}

//получить информацию о текущем окне
crow::response WindowManager::getCurrent() {
    if (cur_window == "") {
        return JsonHandler::createJsonResponse(400, json{
            {"status", "error"},
            {"error", "There are no active window"}
        });
    }
    return JsonHandler::createJsonResponse(200, json{
            {"status", "success"},
            {"id", WindowManager::cur_window },
            {"data", WindowManager::manager[WindowManager::cur_window]}
    });
}

//Обновить текущее окно
crow::response WindowManager:: updateCurrent(const std::string& req) {
    json json_request = json::parse(req);
    if (!json_request.contains("value")) {
        return JsonHandler::createJsonResponse(400, json{
                {"status", "error"},
                {"message", "Request body must contain 'value' field"}
            });
    }
    std::string id = WindowManager::cur_window;
    if (id == " ") {
        return JsonHandler::createJsonResponse(409, json{
            {"status", "error"},
            {"error", ("There are no active window")}
            });
    }
    if (!WindowManager::manager.contains(id)) {
        return JsonHandler::createJsonResponse(409, json{
            {"status", "error"},
            {"error", ("Internal error with cur_window")}
            });
    }
    WindowManager::manager[id] = json_request["value"].get<std::string>();
    return JsonHandler::createJsonResponse(200, json{
        {"status", "success"},
    });
}

std::string WindowManager::generate_next() {
    WindowManager::max_id += 1;
    return std::format("File_{}", WindowManager::max_id);
}
