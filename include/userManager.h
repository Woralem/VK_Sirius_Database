#pragma once
#include <string>
#include <vector>
#include <crow.h>
#include <nlohmann/json.hpp>


struct DB {
    std::string identificator;
    int accessLevel; //1 2 3
};
// Удалила поле 'password' из структуры User тк небезопасно
struct User {
    std::string login;
    std::string code;
    std::vector<DB> dataBases;
};


class UserManager {
public:
    std::string loginUser(const std::string& jsonBody, std::string& statusMessage, User& user);
    crow::response registerUser(const std::string& jsonBody);

    // проверка существования пользователя
    bool userExists(const std::string& username);
    // создание пользователя
    bool createUser(const std::string& username, const std::string& password);
    // валидация пользователя (пароля)
    bool validateUser(const std::string& username, const std::string& password);
    // генерация токена (если он используется)
    std::string generateAuthToken(const std::string& username);
    // получение данных пользователя по логину
    bool getUserByLogin(const std::string& login, User& user);

private:
    // Эти методы должны стать частью реализации UserManager,
    // а не просто свободными функциями.
    //метод для проверки существования логина в БД
    bool doesLoginExist(const std::string& login);

    //метод для добавления пользователя в БД
    void addUser(User& person, const std::string& hashedPassword); // Добавляем hashedPassword

    //метод для получения пользователя
    void getUser(User& person);

    //метод для проверки пароля на корректность
    bool validPassword(const std::string& password);

    //метод для генерации персонального кода
    std::string generateCode();

    //метод для хэширования пароля
    std::string hashingPassword(const std::string& password);
};
