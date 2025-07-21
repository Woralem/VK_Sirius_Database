#pragma once

#include <string>
#include <vector>
#include <optional>
#include <algorithm> // Для std::find_if
#include <stdexcept> // Для std::runtime_error
#include <sodium.h> // Для libsodium

// Внешние зависимости, если они используются в Crow или nlohmann/json
#include "crow.h" // Предполагается, что Crow установлен и доступен
#include "nlohmann/json.hpp" // Предполагается, что nlohmann/json установлен и доступен

// Структура для представления данных о доступной базе данных для пользователя
struct DB {
    std::string dbName;      // Имя базы данных
    std::string dbCode;      // Уникальный код базы данных
    int accessLevel;         // Уровень доступа (1: полный, 2: ограниченный, 3: только чтение)
};

// Структура для представления данных пользователя
struct User {
    std::string login;
    std::string hashedPassword;
    std::string userCode;                          // Уникальный код пользователя
    std::vector<DB> accessibleDBs;                 // Список баз данных, к которым у пользователя есть доступ
};

// Структура для глобальных метаданных о базах данных (на бэкенде)
struct GlobalDBMetadata {
    std::string dbName;
    std::string ownerLogin; // Логин пользователя, создавшего БД
    std::string dbCode;     // Уникальный код самой базы данных
};


class UserManager {
public:
    UserManager() = default; // Конструктор по умолчанию

    // Методы API (доступные через HTTP-запросы)
    crow::response registerUser(const std::string& jsonBody);
    std::string loginUser(const std::string& jsonBody, std::string& statusMessage, User& user);
    crow::response createDatabase(const std::string& jsonBody);
    crow::response modifyDatabase(const std::string& jsonBody);
    crow::response getAccessibleDatabases(const std::string& jsonBody);
    crow::response manageDatabaseAccess(const std::string& jsonBody);
    crow::response deleteUser(const std::string& jsonBody);


private:
    // Вспомогательные методы для работы с пользователями и БД (внутренние)
    bool doesUserExistInDB(const std::string& login);
    bool isValidLogin(const std::string& login);
    bool isValidPassword(const std::string& password);
    bool addUserToDB(const User& user, const std::string& hashedPassword);
    std::optional<User> getUserFromDB(const std::string& login);
    bool updateUserInDB(const User& user);
    bool deleteUserFromDB(const std::string& login);

    std::string hashPassword(const std::string& password);
    bool verifyPassword(const std::string& password, const std::string& hashedPassword);
    std::string generateSecureCode(); // Генерирует уникальный 8-байтовый код

    // Вспомогательные методы для работы с базами данных на бэкенде
    bool doesDatabaseExistBackend(const std::string& dbName);
    bool doesTableExistInDatabase(const std::string& dbName, const std::string& tableName);
    bool createDatabaseBackend(const std::string& dbName);
    bool createUsersTableInDatabase(const std::string& dbName);
    bool writeUserDataToDatabaseTable(const std::string& dbName, const User& user);
    std::optional<User> getUserDataFromDatabaseTable(const std::string& dbName, const std::string& login);
    bool updateUserDataInDatabaseTable(const std::string& dbName, const User& user);
    bool deleteUserRowFromDatabaseTable(const std::string& dbName, const std::string& login);


    // Временное хранилище метаданных о базах данных.
    // В реальном приложении это будет извлекаться из БД
    std::vector<GlobalDBMetadata> globalDBs;
};
