#include "sessionManager.h"
#include "windowManager.h"
#include "json-handler.h"
#include <string>
#include <nlohmann/json.hpp>
#include<unordered_map>
#include<crow.h>
#include <chrono>

using json = nlohmann::json;
    SessionManager::SessionManager() {
        startCleanup();
    }
    SessionManager::~SessionManager() {
        stopCleanup();
    }
    void SessionManager::cleanupThreadFunc() {
        while (running) {
            std::this_thread::sleep_for(std::chrono::minutes(10));//Периодичность очистки
            cleanupExpiredSessions();
        }
    }
    crow::response SessionManager::addSession(const std::string& req) {
        // где-то здесь ещё вызываю функцию авторизации
        int sessionId = generateSessionId();
        sessions[sessionId] = std::make_unique<Session>();
        sessions[sessionId]->cur_db = "default";
        sessions[sessionId]->cur_table = "";
        sessions[sessionId]->last_activity = std::chrono::system_clock::now();
        //sessions[sessionId]->wm = WindowManager();// Раскомментить после удаления мутекса в WindowManager
        return JsonHandler::createJsonResponse(200, json{
            {"status", "success"},
            {"message", "authorization was successfully completed"},
            {"session_id", sessionId}
        });
    }
    Session* SessionManager::getSession(int id) {
        std::lock_guard<std::mutex> lock(sessions_mutex);
        auto it = sessions.find(id);
        if (it == sessions.end()) {
            return nullptr;
        }
        it->second->last_activity = std::chrono::system_clock::now();
        return it->second.get();
    }
    void SessionManager::removeSession(int id) {
        std::lock_guard<std::mutex> lock(sessions_mutex);
        sessions.erase(id);
        return;
    }
    void SessionManager::startCleanup() {
        if (running) return;
        running = true;
        cleanup_thread = std::thread(&SessionManager::cleanupThreadFunc, this);
    }
    void SessionManager::stopCleanup() {
        running = false;
        if (cleanup_thread.joinable()) {
            cleanup_thread.join();
        }
    }
    void SessionManager::cleanupExpiredSessions() {
        std::lock_guard<std::mutex> lock(sessions_mutex);
        auto now = std::chrono::system_clock::now();
        auto prev = now - std::chrono::hours(1);

        for (auto it = sessions.begin(); it != sessions.end(); ) {
            if (it->second->last_activity < prev) {
                it = sessions.erase(it);
            } else {
                ++it;
            }
        }
    }
    int SessionManager::generateSessionId() {
        int id = 0;
        while (sessions.contains(id)) {
            ++id;
        }
        return id;
    }
