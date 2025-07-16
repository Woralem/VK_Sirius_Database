#pragma once
#include "windowManager.h"
#include <string>
#include <nlohmann/json.hpp>
#include<unordered_map>
#include<crow.h>
#include <chrono>

using json = nlohmann::json;
struct Session {
    WindowManager wm;
    std::string cur_table;
    json cur_headers;//список названий колонок
    std::string cur_db;
    std::chrono::time_point<std::chrono::system_clock> last_activity; // Для очистки
};
class SessionHandler {
    std::unordered_map<int, Session> sessions;
    std::mutex sessions_mutex;
public:
    crow::response addSession();
    void changeSession(int id, const Session& new_session);
    Session getSession(int id);
};