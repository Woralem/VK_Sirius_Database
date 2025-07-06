#pragma once
#include <string>
#include <unordered_map>
#include<crow.h>

class WindowManager {
std::unordered_map<std::string, std::string> manager;
public:
    WindowManager() {}
    ~WindowManager() {}
    crow::response get();//Получить все пары ключ-значение json строкой
    crow::response get(const std::string& req);//Получить пару по id
    crow::response add(const std::string& req);//Добавить окно, если оно уже есть - ошибка
    crow::response remove();//Удалить всё
    crow::response remove(const std::string& req);//Удалить по id
    crow::response update(const std::string& req);//Обновить значение, если его нет - ошибка
    crow::response getList();//Получить список id существующих окон
};