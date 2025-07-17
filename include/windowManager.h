#pragma once
#include <string>
#include <map>
#include <crow.h>
#include <mutex>

class WindowManager {
    static constexpr auto compareByNum = [](const std::string& a, const std::string& b) -> bool {
        int A = std::stoi(a.substr(5));
        int B = std::stoi(b.substr(5));
        return A < B;
    };
    std::map<std::string, std::string, decltype(compareByNum)> manager;
    std::string cur_window;
    std::mutex mtx;
    int max_id = 1;
    std::string generate_next();
public:
    WindowManager();
    ~WindowManager() = default;
    crow::response get();//Получить все пары ключ-значение json строкой
    crow::response get(const std::string& req);//Получить пару по id
    crow::response add(const std::string& req);//Добавить окно, если оно уже есть - ошибка
    crow::response remove();//Удалить всё
    crow::response remove(const std::string& req);//Удалить по id
    crow::response update(const std::string& req);//Обновить значение текущего окна
    crow::response changeWindow(const std::string& req);//Сменить окно
    crow::response getList();//Получить список id существующих окон
    crow::response getCurrent();//Получить содержимое активного окна

};