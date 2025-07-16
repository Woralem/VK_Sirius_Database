#pragma once
#include "windowManager.h"
#include <string>
#include <nlohmann/json.hpp>
#include<unordered_map>
#include<crow.h>
#include <chrono>
#include <memory>
#include <thread>
using json = nlohmann::json;
struct Session {
    WindowManager wm;
    std::string cur_table;
    json cur_headers;//список названий колонок
    std::string cur_db;
    std::chrono::time_point<std::chrono::system_clock> last_activity; // Для очистки
    //Добавить User user;
};
class SessionManager {
    std::unordered_map<int, std::unique_ptr<Session>> sessions;
    std::mutex sessions_mutex;
    void cleanupThreadFunc();
    std::atomic<bool> running{false};
    std::thread cleanup_thread;
public:
    SessionManager();
    ~SessionManager();
    crow::response addSession(const std::string& req);
    Session* getSession(int id);
    void removeSession(int id);
    void startCleanup();
    void stopCleanup();
    void cleanupExpiredSessions();
    int generateSessionId();
};