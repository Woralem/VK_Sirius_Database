#include "userManager.h"
#include <iostream>
#include <random> // Для генерации случайных чисел
#include <algorithm> // Для std::find_if
#include <chrono> // Для std::chrono::system_clock
#include <stdexcept> // Для std::runtime_error
#include <sodium.h> // Для libsodium (хэширование паролей, генерация случайных байт)

// Предполагаем наличие файла json-handler.h для создания JSON-ответов
// Если его нет, вам потребуется реализовать вспомогательные функции для формирования crow::response
// Например:
namespace JsonHandler {
    crow::response createJsonResponse(int status, const nlohmann::json& data) {
        crow::response res;
        res.code = status;
        res.set_header("Content-Type", "application/json");
        res.body = data.dump();
        return res;
    }
}


// --- Вспомогательные методы UserManager ---

// Метод для проверки существования пользователя по логину в БД
// Имитирует запрос к центральной базе данных или хранилищу пользователей
bool UserManager::doesUserExistInDB(const std::string& login) {
    // ЗАГЛУШКА: В реальном приложении здесь будет запрос к вашей СУБД.
    // Например, к таблице 'main_users_registry' или подобной,
    // где хранятся все зарегистрированные логины.
    // Для примера, имитируем, что пользователь "existing_user" существует.
    return (login == "existing_user");
}

// Метод для проверки корректности логина пользователя
bool UserManager::isValidLogin(const std::string& login) {
    // 1. Не более 16 символов
    if (login.length() > 16) {
        return false;
    }
    // 2. Только буквы английского алфавита, цифры, тире и нижнее подчеркивание
    for (char c : login) {
        if (!std::isalnum(static_cast<unsigned char>(c)) && c != '-' && c != '_') {
            return false;
        }
    }
    // 3. Нижнее подчеркивание не может стоять в конце
    if (!login.empty() && login.back() == '_') {
        return false;
    }
    return true;
}

// Метод для проверки корректности пароля пользователя
bool UserManager::isValidPassword(const std::string& password) {
    // Проверка на минимальную длину
    if (password.length() < 8) {
        return false;
    }

    // Проверка на наличие символов в диапазоне ASCII от 33 до 126
    for (char c : password) {
        if (static_cast<unsigned char>(c) < 33 || static_cast<unsigned char>(c) > 126) {
            return false; // Символ вне допустимого диапазона
        }
    }

    // Проверка на сложность пароля (буквы верхнего/нижнего регистра, цифры, спецсимволы)
    bool hasUpper = false, hasLower = false, hasDigit = false, hasSpecial = false;
    for (char c : password) {
        if (std::isupper(static_cast<unsigned char>(c))) hasUpper = true;
        else if (std::islower(static_cast<unsigned char>(c))) hasLower = true;
        else if (std::isdigit(static_cast<unsigned char>(c))) hasDigit = true;
        else if (static_cast<unsigned char>(c) >= 33 && static_cast<unsigned char>(c) <= 126 && // Проверяем, что символ ASCII и не буквенно-цифровой
                 !std::isalnum(static_cast<unsigned char>(c))) hasSpecial = true;
    }
    return hasUpper && hasLower && hasDigit && hasSpecial;
}

// Метод для добавления нового пользователя в БД
// В реальной системе это будет вставка в центральную таблицу пользователей
bool UserManager::addUserToDB(const User& user, const std::string& hashedPassword) {
    // Генерируем SQL-запрос INSERT на основе документации
    [cite_start]// INSERT INTO users (login, hashedPassword, userCode) VALUES ('user.login', 'hashedPassword', 'user.userCode'); [cite: 360, 375]
    std::string sqlQuery = "INSERT INTO main_users_registry (login, hashed_password, user_code) VALUES ('" +
                           user.login + "', '" + hashedPassword + "', '" + user.userCode + "');";
    std::cout << "Generated SQL for addUserToDB: " << sqlQuery << std::endl;
    // В реальном приложении здесь будет выполнение этого SQL-запроса
    return true; // Имитация успешного добавления
}

// Метод для извлечения данных пользователя из БД по логину
// Возвращает std::optional<User>
std::optional<User> UserManager::getUserFromDB(const std::string& login) {
    // Генерируем SQL-запрос SELECT на основе документации
    [cite_start]// SELECT * FROM users WHERE login = 'login'; [cite: 362, 376]
    std::string sqlQuery = "SELECT login, hashed_password, user_code FROM main_users_registry WHERE login = '" + login + "';";
    std::cout << "Generated SQL for getUserFromDB: " << sqlQuery << std::endl;

    // ЗАГЛУШКА: В реальном приложении здесь будет выполнение SQL-запроса SELECT
    // из вашей центральной таблицы пользователей.
    // Если пользователь найден, заполняем объект User и возвращаем его.
    // Для примера, если логин "existing_user", возвращаем тестовые данные.
    if (login == "existing_user") {
        User u;
        u.login = "existing_user";
        u.hashedPassword = hashPassword("TestPassword1!"); // Имитация хэшированного пароля
        u.userCode = "user_code_existing";
        // Предположим, у этого пользователя уже есть доступ к тестовой БД
        u.accessibleDBs.push_back({"test_db_1", "db_code_1", 1});
        return u;
    }
    return std::nullopt; // Пользователь не найден
}

// Метод для обновления данных пользователя в БД
bool UserManager::updateUserInDB(const User& user) {
    // Генерируем SQL-запрос UPDATE на основе документации
    [cite_start]// UPDATE users SET hashedPassword = 'hashedPassword', userCode = 'userCode' WHERE login = 'user.login'; [cite: 363]
    std::string sqlQuery = "UPDATE main_users_registry SET hashed_password = '" + user.hashedPassword +
                           "', user_code = '" + user.userCode + "' WHERE login = '" + user.login + "';";
    std::cout << "Generated SQL for updateUserInDB: " << sqlQuery << std::endl;
    // В реальном приложении здесь будет выполнение этого SQL-запроса
    return true; // Имитация успешного обновления
}

// Метод для удаления пользователя из БД
bool UserManager::deleteUserFromDB(const std::string& login) {
    // Генерируем SQL-запрос DELETE на основе документации
    [cite_start]// DELETE FROM users WHERE login = 'login'; [cite: 363]
    std::string sqlQuery = "DELETE FROM main_users_registry WHERE login = '" + login + "';";
    std::cout << "Generated SQL for deleteUserFromDB: " << sqlQuery << std::endl;
    // В реальном приложении здесь будет выполнение этого SQL-запроса
    return true; // Имитация успешного удаления
}


// Метод для хэширования пароля
std::string UserManager::hashPassword(const std::string& password) {
    // Инициализация libsodium, если еще не инициализирована
    if (sodium_init() == -1) {
        throw std::runtime_error("libsodium initialization failed");
    }

    char hashed_password[crypto_pwhash_STRBYTES];
    // Используем crypto_pwhash_str для получения строкового представления хэша
    if (crypto_pwhash_str(hashed_password, password.c_str(), password.length(),
                          crypto_pwhash_OPSLIMIT_MODERATE, crypto_pwhash_MEMLIMIT_MODERATE) != 0) {
        return ""; // Возвращаем пустую строку в случае ошибки
    }
    return std::string(hashed_password);
}

// Метод для проверки соответствия введенного пароля хэшированному
bool UserManager::verifyPassword(const std::string& password, const std::string& hashedPassword) {
    // Инициализация libsodium
    if (sodium_init() == -1) {
        return false;
    }
    // crypto_pwhash_str_verify возвращает 0 при успешной проверке
    return crypto_pwhash_str_verify(hashedPassword.c_str(), password.c_str(), password.length()) == 0;
}

// Метод для генерации надёжного 8-байтового кода
std::string UserManager::generateSecureCode() {
    // Инициализация libsodium для генерации случайных байт
    if (sodium_init() == -1) {
        throw std::runtime_error("libsodium initialization failed");
    }

    const int CODE_LENGTH = 8; // 8 байт
    unsigned char random_bytes[CODE_LENGTH];
    randombytes_buf(random_bytes, CODE_LENGTH); // Генерируем криптографически стойкие случайные байты

    // Преобразуем байты в шестнадцатеричную строку для удобства хранения и использования
    std::string code_hex;
    code_hex.reserve(CODE_LENGTH * 2); // Каждый байт - 2 шестнадцатеричных символа
    char buf[3];
    for (int i = 0; i < CODE_LENGTH; ++i) {
        sprintf(buf, "%02x", random_bytes[i]); // Форматируем байт в 2-значное шестнадцатеричное число
        code_hex += buf;
    }
    return code_hex; // Например, "a1b2c3d4e5f6g7h8"
}

// Метод для проверки существования базы данных (на бэкенде)
bool UserManager::doesDatabaseExistBackend(const std::string& dbName) {
    // ЗАГЛУШКА: В реальном приложении здесь будет запрос к вашей СУБД,
    // чтобы проверить, существует ли база данных с таким именем.
    // Ищем в списке глобальных метаданных о БД
    return std::any_of(globalDBs.begin(), globalDBs.end(),
                       [&](const GlobalDBMetadata& meta) { return meta.dbName == dbName; });
}

// Метод для проверки существования таблицы 'users' в указанной БД
bool UserManager::doesTableExistInDatabase(const std::string& dbName, const std::string& tableName) {
    // ЗАГЛУШКА: В реальном приложении здесь будет запрос к вашей СУБД,
    // чтобы проверить наличие таблицы 'users' в конкретной базе данных.
    // Для простоты, допустим, что таблица 'users' всегда существует, если существует БД.
    return doesDatabaseExistBackend(dbName);
}

// Метод для создания базы данных на бэкенде
bool UserManager::createDatabaseBackend(const std::string& dbName) {
    // Генерируем SQL-команду CREATE DATABASE. Это DDL, не напрямую в документации, но логично.
    [cite_start]// На основе CREATE TABLE синтаксиса [cite: 359] можно предположить CREATE DATABASE.
    std::string sqlQuery = "CREATE DATABASE " + dbName + ";";
    std::cout << "Generated SQL for createDatabaseBackend: " << sqlQuery << std::endl;

    // Имитируем создание БД и добавляем её в наш список глобальных метаданных
    if (doesDatabaseExistBackend(dbName)) {
        return false; // БД уже существует
    }
    // Предполагаем, что владелец пока не определен или это системный владелец
    // Этот код БД пока не привязан к конкретному пользователю, он будет привязан при создании
    // пользователем.
    GlobalDBMetadata newDB;
    newDB.dbName = dbName;
    newDB.ownerLogin = ""; // Владелец пока не установлен
    newDB.dbCode = generateSecureCode(); // Генерируем код для самой БД
    globalDBs.push_back(newDB);
    std::cout << "DEBUG: Создана новая база данных: " << dbName << " с кодом: " << newDB.dbCode << std::endl;
    return true; // Имитация успешного создания
}

// Метод для создания таблицы 'users' в указанной БД
bool UserManager::createUsersTableInDatabase(const std::string& dbName) {
    // Генерируем SQL-команду CREATE TABLE для таблицы 'users' с базовыми столбцами.
    [cite_start]// CREATE TABLE users (id INTEGER PRIMARY KEY, login VARCHAR, hashed_password VARCHAR, user_code VARCHAR); [cite: 368, 369]
    std::string sqlQuery = "CREATE TABLE " + dbName + ".users (id INTEGER PRIMARY KEY, login VARCHAR, hashed_password VARCHAR, user_code VARCHAR);";
    std::cout << "Generated SQL for createUsersTableInDatabase: " << sqlQuery << std::endl;
    // В реальном приложении здесь будет выполнение этого SQL-запроса
    std::cout << "DEBUG: Создана таблица 'users' в базе данных: " << dbName << std::endl;
    return true; // Имитация успешного создания
}

// Метод для записи данных пользователя в таблицу 'users' в указанной БД
bool UserManager::writeUserDataToDatabaseTable(const std::string& dbName, const User& user) {
    // Генерируем SQL-запрос INSERT или UPDATE для таблицы 'users' в конкретной БД.
    // Если пользователь уже существует, UPDATE, иначе INSERT. Для простоты, здесь будет INSERT.
    [cite_start]// INSERT INTO dbName.users (login, hashed_password, user_code) VALUES ('user.login', 'user.hashedPassword', 'user.userCode'); [cite: 360, 375]
    std::string sqlQuery = "INSERT INTO " + dbName + ".users (login, hashed_password, user_code) VALUES ('" +
                           user.login + "', '" + user.hashedPassword + "', '" + user.userCode + "');";
    std::cout << "Generated SQL for writeUserDataToDatabaseTable: " << sqlQuery << std::endl;
    // В реальном приложении здесь будет выполнение этого SQL-запроса
    std::cout << "DEBUG: Запись/обновление данных пользователя " << user.login
              << " в таблице 'users' базы данных: " << dbName << std::endl;
    return true;
}

// Метод для получения данных пользователя из конкретной БД (таблицы 'users')
std::optional<User> UserManager::getUserDataFromDatabaseTable(const std::string& dbName, const std::string& login) {
    // Генерируем SQL-запрос SELECT на основе документации
    [cite_start]// SELECT login, hashed_password, user_code FROM dbName.users WHERE login = 'login'; [cite: 362, 376]
    std::string sqlQuery = "SELECT login, hashed_password, user_code FROM " + dbName + ".users WHERE login = '" + login + "';";
    std::cout << "Generated SQL for getUserDataFromDatabaseTable: " << sqlQuery << std::endl;

    // ЗАГЛУШКА: В реальном приложении здесь будет выполнение SQL-запроса SELECT
    // из таблицы 'users' конкретной базы данных.
    // Имитируем, что пользователь найден, если dbName соответствует нашей заглушке.
    if (dbName == "created_db_by_user" && login == "new_user") {
        User u;
        u.login = "new_user";
        u.hashedPassword = hashPassword("NewUserPass1!");
        u.userCode = "new_user_code_1";
        // Предположим, что в этой БД у него есть доступ к этой самой БД
        u.accessibleDBs.push_back({dbName, "db_code_new_user", 1});
        return u;
    }
    return std::nullopt;
}

// Метод для обновления данных пользователя в конкретной БД (таблице 'users')
bool UserManager::updateUserDataInDatabaseTable(const std::string& dbName, const User& user) {
    // Генерируем SQL-запрос UPDATE на основе документации
    [cite_start]// UPDATE dbName.users SET hashed_password = 'user.hashedPassword', user_code = 'user.userCode' WHERE login = 'user.login'; [cite: 363]
    std::string sqlQuery = "UPDATE " + dbName + ".users SET hashed_password = '" + user.hashedPassword +
                           "', user_code = '" + user.userCode + "' WHERE login = '" + user.login + "';";
    std::cout << "Generated SQL for updateUserDataInDatabaseTable: " << sqlQuery << std::endl;
    // В реальном приложении здесь будет выполнение этого SQL-запроса
    std::cout << "DEBUG: Обновление данных пользователя " << user.login
              << " в таблице 'users' базы данных: " << dbName << std::endl;
    return true;
}

// Метод для удаления строки пользователя из таблицы 'users' в указанной БД
bool UserManager::deleteUserRowFromDatabaseTable(const std::string& dbName, const std::string& login) {
    // Генерируем SQL-запрос DELETE на основе документации
    [cite_start]// DELETE FROM dbName.users WHERE login = 'login'; [cite: 363]
    std::string sqlQuery = "DELETE FROM " + dbName + ".users WHERE login = '" + login + "';";
    std::cout << "Generated SQL for deleteUserRowFromDatabaseTable: " << sqlQuery << std::endl;
    // В реальном приложении здесь будет выполнение этого SQL-запроса
    std::cout << "DEBUG: Удаление строки пользователя " << login
              << " из таблицы 'users' базы данных: " << dbName << std::endl;
    return true;
}


// --- Методы API UserManager ---

// Метод для регистрации нового пользователя
crow::response UserManager::registerUser(const std::string& jsonBody) {
    try {
        nlohmann::json json = nlohmann::json::parse(jsonBody);
        // Проверка наличия обязательных полей
        if (!json.contains("username") || !json.contains("password")) {
            return JsonHandler::createJsonResponse(400, nlohmann::json{
                {"status", "error"},
                {"message", "You must specify username and password"}
            });
        }

        std::string username = json["username"].get<std::string>();
        std::string password = json["password"].get<std::string>();
        // Проверка валидности логина
        if (!isValidLogin(username)) {
            return JsonHandler::createJsonResponse(400, nlohmann::json{
                {"status", "error"},
                {"message", "Invalid username format. It must be no more than 16 characters, contain only English letters, numbers, hyphens, and underscores. Underscore cannot be at the end."}
            });
        }

        // Проверка валидности пароля
        if (!isValidPassword(password)) {
            return JsonHandler::createJsonResponse(400, nlohmann::json{
                {"status", "error"},
                {"message", "Invalid password format. Password must be at least 8 characters long and contain uppercase, lowercase, digit, and special characters. All characters must be within ASCII range 33-126."}
            });
        }

        // Проверка существования пользователя в центральной системе
        if (doesUserExistInDB(username)) {
            return JsonHandler::createJsonResponse(409, nlohmann::json{
                {"status", "error"},
                {"message", "A user with that name already exists"}
            });
        }

        // Хэширование пароля
        std::string hashedPassword = hashPassword(password);
        if (hashedPassword.empty()) {
            return JsonHandler::createJsonResponse(500, nlohmann::json{
                {"status", "error"},
                {"message", "Error hashing password"}
            });
        }

        // Создание объекта User и генерация уникального кода пользователя
        User newUser;
        newUser.login = username;
        newUser.hashedPassword = hashedPassword;
        newUser.userCode = generateSecureCode(); // Генерируем уникальный код для пользователя

        // Добавление пользователя в центральную БД
        if (!addUserToDB(newUser, hashedPassword)) {
            return JsonHandler::createJsonResponse(500, nlohmann::json{
                {"status", "error"},
                {"message", "Error when creating a user in the database"}
            });
        }

        return JsonHandler::createJsonResponse(201, nlohmann::json{
            {"status", "success"},
            {"message", "The user has been successfully registered"},
            {"user_code", newUser.userCode} // Возвращаем сгенерированный код пользователя
        });
    }
    catch (const nlohmann::json::parse_error& e) {
        return JsonHandler::createJsonResponse(400, nlohmann::json{
            {"status", "error"},
            {"message", "Invalid JSON format: " + std::string(e.what())}
        });
    }
    catch (const std::exception& e) {
        return JsonHandler::createJsonResponse(500, nlohmann::json{
            {"status", "error"},
            {"message", "An internal server error occurred: " + std::string(e.what())}
        });
    }
}

// Метод для входа пользователя в систему
std::string UserManager::loginUser(const std::string& jsonBody, std::string& statusMessage, User& user) {
    try {
        auto json = nlohmann::json::parse(jsonBody);
        if (!json.contains("username") || !json.contains("password")) {
            statusMessage = "error";
            return "You must specify username and password";
        }

        std::string username = json["username"];
        std::string password = json["password"];

        // Получаем данные пользователя из центральной БД
        std::optional<User> foundUser = getUserFromDB(username);
        // Проверяем существование пользователя
        if (!foundUser) {
            statusMessage = "error";
            return "The user was not found";
        }

        // Проверяем пароль
        if (!verifyPassword(password, foundUser->hashedPassword)) {
            statusMessage = "error";
            return "Invalid password";
        }

        // При успешной проверке, копируем найденные данные пользователя
        user = *foundUser;
        // Генерация токена (заглушка)
        std::string token = generateSecureCode();
        // Имитируем токен как безопасный код
        statusMessage = "success";
        // Возвращаем JSON с данными пользователя и токеном
        nlohmann::json responseJson;
        responseJson["message"] = "Successful login";
        responseJson["username"] = user.login;
        responseJson["user_code"] = user.userCode; // Возвращаем уникальный код пользователя
        responseJson["token"] = token;
        // Добавляем информацию о доступных базах данных
        nlohmann::json dbs_array = nlohmann::json::array();
        for (const auto& db : user.accessibleDBs) {
            dbs_array.push_back({
                {"db_name", db.dbName},
                {"db_code", db.dbCode},
                {"access_level", db.accessLevel}
            });
        }
        responseJson["accessible_databases"] = dbs_array;

        return responseJson.dump();
    }
    catch (const nlohmann::json::parse_error& e) {
        statusMessage = "error";
        return "Invalid JSON format: " + std::string(e.what());
    }
    catch (const std::exception& e) {
        statusMessage = "error";
        return "An internal server error occurred: " + std::string(e.what());
    }
}

// Метод для создания новой базы данных пользователем
crow::response UserManager::createDatabase(const std::string& jsonBody) {
    try {
        nlohmann::json json = nlohmann::json::parse(jsonBody);
        // Проверка наличия обязательных полей
        if (!json.contains("username") || !json.contains("db_name")) {
            return JsonHandler::createJsonResponse(400, nlohmann::json{
                {"status", "error"},
                {"message", "You must specify username and db_name"}
            });
        }

        std::string username = json["username"].get<std::string>();
        std::string dbName = json["db_name"].get<std::string>();
        // 1. Проверяем существование базы данных на бэкенде
        if (doesDatabaseExistBackend(dbName)) {
            return JsonHandler::createJsonResponse(409, nlohmann::json{
                {"status", "error"},
                {"message", "Database with this name already exists"}
            });
        }

        // 2. Создаем базу данных на бэкенде
        if (!createDatabaseBackend(dbName)) {
            return JsonHandler::createJsonResponse(500, nlohmann::json{
                {"status", "error"},
                {"message", "Failed to create database on backend"}
            });
        }

        // Получаем сгенерированный код для новой БД (из GlobalDBMetadata)
        std::string newDbCode;
        auto it = std::find_if(globalDBs.begin(), globalDBs.end(),
                               [&](const GlobalDBMetadata& meta) { return meta.dbName == dbName; });
        if (it != globalDBs.end()) {
            newDbCode = it->dbCode;
        } else {
            // Этого не должно произойти, если createDatabaseBackend отработал успешно
            return JsonHandler::createJsonResponse(500, nlohmann::json{
                {"status", "error"},
                {"message", "Failed to retrieve database code after creation"}
            });
        }


        // 3. Создаем таблицу 'users' в новой базе данных
        if (!createUsersTableInDatabase(dbName)) {
            return JsonHandler::createJsonResponse(500, nlohmann::json{
                {"status", "error"},
                {"message", "Failed to create 'users' table in the new database"}
            });
        }

        // 4. Получаем данные пользователя из центральной БД
        std::optional<User> currentUserOpt = getUserFromDB(username);
        if (!currentUserOpt) {
            return JsonHandler::createJsonResponse(404, nlohmann::json{
                {"status", "error"},
                {"message", "User not found"}
            });
        }
        User currentUser = *currentUserOpt;
        // 5. Привязываем код новой БД к пользователю с максимальным уровнем доступа (1)
        // Проверяем, не превышено ли количество баз данных (до 1000)
        if (currentUser.accessibleDBs.size() >= 1000) {
            return JsonHandler::createJsonResponse(403, nlohmann::json{
                {"status", "error"},
                {"message", "User has reached the maximum limit of 1000 databases"}
            });
        }

        DB newAccessibleDb;
        newAccessibleDb.dbName = dbName;
        newAccessibleDb.dbCode = newDbCode; // Привязываем сгенерированный код БД
        newAccessibleDb.accessLevel = 1; // Максимальный уровень доступа

        currentUser.accessibleDBs.push_back(newAccessibleDb);
        // 6. Обновляем данные пользователя в центральной БД (добавляем новую БД в список доступных)
        if (!updateUserInDB(currentUser)) {
            return JsonHandler::createJsonResponse(500, nlohmann::json{
                {"status", "error"},
                {"message", "Failed to update user's accessible databases"}
            });
        }

        // 7. Записываем данные создателя в таблицу 'users' новой БД с уровнем доступа 1
        // (Это подразумевает, что таблица 'users' в этой новой БД будет хранить
        // информацию о всех пользователях, имеющих к ней доступ).
        // Если пользователь является создателем, он является и первым пользователем в этой БД.
        if (!writeUserDataToDatabaseTable(dbName, currentUser)) {
            return JsonHandler::createJsonResponse(500, nlohmann::json{
                {"status", "error"},
                {"message", "Failed to write user data to the new database's users table"}
            });
        }

        // Обновляем владельца в GlobalDBMetadata
        auto it_meta = std::find_if(globalDBs.begin(), globalDBs.end(), [&](const GlobalDBMetadata& meta) { return meta.dbName == dbName; });
        if (it_meta != globalDBs.end()) {
            it_meta->ownerLogin = username;
        }

        return JsonHandler::createJsonResponse(200, nlohmann::json{
            {"status", "success"},
            {"message", "Database '" + dbName + "' created successfully and access granted to user '" + username + "'"},
            {"db_code", newDbCode}, // Возвращаем код созданной БД
            {"access_level", 1}
        });
    }
    catch (const nlohmann::json::parse_error& e) {
        return JsonHandler::createJsonResponse(400, nlohmann::json{
            {"status", "error"},
            {"message", "Invalid JSON format: " + std::string(e.what())}
        });
    }
    catch (const std::exception& e) {
        return JsonHandler::createJsonResponse(500, nlohmann::json{
            {"status", "error"},
            {"message", "An internal server error occurred: " + std::string(e.what())}
        });
    }
}

// Метод для изменения данных в базе данных (проверка уровня доступа)
crow::response UserManager::modifyDatabase(const std::string& jsonBody) {
    try {
        nlohmann::json json = nlohmann::json::parse(jsonBody);
        // Проверка наличия обязательных полей: логин пользователя, название БД, тип операции, данные для изменения
        if (!json.contains("username") || !json.contains("db_name") || !json.contains("operation_type") || !json.contains("data")) {
            return JsonHandler::createJsonResponse(400, nlohmann::json{
                {"status", "error"},
                {"message", "Missing required fields: username, db_name, operation_type, data"}
            });
        }

        std::string username = json["username"].get<std::string>();
        std::string dbName = json["db_name"].get<std::string>();
        std::string operationType = json["operation_type"].get<std::string>();
        nlohmann::json dataToModify = json["data"]; // Данные для изменения

        // 1. Получаем данные пользователя из центральной БД
        std::optional<User> currentUserOpt = getUserFromDB(username);
        if (!currentUserOpt) {
            return JsonHandler::createJsonResponse(404, nlohmann::json{
                {"status", "error"},
                {"message", "User not found"}
            });
        }
        User currentUser = *currentUserOpt;

        // 2. Ищем доступ пользователя к указанной БД
        auto it = std::find_if(currentUser.accessibleDBs.begin(), currentUser.accessibleDBs.end(), [&](const DB& db) { return db.dbName == dbName; });
        if (it == currentUser.accessibleDBs.end()) {
            return JsonHandler::createJsonResponse(403, nlohmann::json{
                {"status", "error"},
                {"message", "User does not have access to this database or database not found"}
            });
        }
        int accessLevel = it->accessLevel;

        // 3. Проверяем уровень доступа пользователя
        // Уровень 1: может все
        // Уровень 2: не может удалять, не может создавать таблицы
        // Уровень 3: может только читать
        if (accessLevel == 3 && operationType != "read") {
            return JsonHandler::createJsonResponse(403, nlohmann::json{
                {"status", "error"},
                {"message", "Access Denied: User has read-only access to this database."}
            });
        }
        if (accessLevel == 2 && (operationType == "delete" || operationType == "create_table")) {
            return JsonHandler::createJsonResponse(403, nlohmann::json{
                {"status", "error"},
                {"message", "Access Denied: User cannot delete or create tables in this database."}
            });
        }

        // 4. Генерируем и имитируем выполнение операции с БД
        std::string sqlQuery;
        std::string message;

        if (operationType == "read") {
            [cite_start]// SELECT * FROM dbName.tableName WHERE condition; [cite: 362]
            std::string tableName = dataToModify.value("table_name", "default_table");
            std::string condition = dataToModify.value("condition", "");
            sqlQuery = "SELECT * FROM " + dbName + "." + tableName;
            if (!condition.empty()) {
                sqlQuery += " WHERE " + condition;
            }
            sqlQuery += ";";
            message = "Read operation successful in database '" + dbName + "'. SQL: " + sqlQuery;
        } else if (operationType == "write" || operationType == "insert") {
            if (accessLevel == 3) {
                return JsonHandler::createJsonResponse(403, nlohmann::json{ {"status", "error"}, {"message", "Access Denied: User has read-only access."} });
            }
            [cite_start]// INSERT INTO dbName.tableName (columns) VALUES (values); [cite: 360]
            std::string tableName = dataToModify.value("table_name", "default_table");
            nlohmann::json columns = dataToModify.value("columns", nlohmann::json::array());
            nlohmann::json values = dataToModify.value("values", nlohmann::json::array());

            std::string cols_str = "";
            if (!columns.empty()) {
                for (size_t i = 0; i < columns.size(); ++i) {
                    cols_str += columns[i].get<std::string>();
                    if (i < columns.size() - 1) cols_str += ", ";
                }
            }

            std::string vals_str = "";
            if (!values.empty()) {
                for (size_t i = 0; i < values.size(); ++i) {
                    if (values[i].is_string()) {
                        vals_str += "'" + values[i].get<std::string>() + "'";
                    } else {
                        vals_str += values[i].dump();
                    }
                    if (i < values.size() - 1) vals_str += ", ";
                }
            }
            sqlQuery = "INSERT INTO " + dbName + "." + tableName;
            if (!cols_str.empty()) {
                sqlQuery += " (" + cols_str + ")";
            }
            sqlQuery += " VALUES (" + vals_str + ");";
            message = "Write/Insert operation successful in database '" + dbName + "'. SQL: " + sqlQuery;
        } else if (operationType == "update") {
            if (accessLevel == 3) {
                return JsonHandler::createJsonResponse(403, nlohmann::json{ {"status", "error"}, {"message", "Access Denied: User has read-only access."} });
            }
            [cite_start]// UPDATE dbName.tableName SET column1 = value1, ... WHERE condition; [cite: 363]
            std::string tableName = dataToModify.value("table_name", "default_table");
            nlohmann::json assignments = dataToModify.value("assignments", nlohmann::json::object());
            std::string condition = dataToModify.value("condition", "");

            std::string set_str = "";
            bool first_assignment = true;
            for (auto it = assignments.begin(); it != assignments.end(); ++it) {
                if (!first_assignment) set_str += ", ";
                set_str += it.key() + " = ";
                if (it.value().is_string()) {
                    set_str += "'" + it.value().get<std::string>() + "'";
                } else {
                    set_str += it.value().dump();
                }
                first_assignment = false;
            }
            sqlQuery = "UPDATE " + dbName + "." + tableName + " SET " + set_str;
            if (!condition.empty()) {
                sqlQuery += " WHERE " + condition;
            }
            sqlQuery += ";";
            message = "Update operation successful in database '" + dbName + "'. SQL: " + sqlQuery;
        } else if (operationType == "delete") {
            if (accessLevel >= 2) {
                return JsonHandler::createJsonResponse(403, nlohmann::json{ {"status", "error"}, {"message", "Access Denied: User cannot delete in this database."} });
            }
            [cite_start]// DELETE FROM dbName.tableName WHERE condition; [cite: 363]
            std::string tableName = dataToModify.value("table_name", "default_table");
            std::string condition = dataToModify.value("condition", "");
            sqlQuery = "DELETE FROM " + dbName + "." + tableName;
            if (!condition.empty()) {
                sqlQuery += " WHERE " + condition;
            }
            sqlQuery += ";";
            message = "Delete operation successful in database '" + dbName + "'. SQL: " + sqlQuery;
        } else if (operationType == "create_table") {
            if (accessLevel >= 2) {
                return JsonHandler::createJsonResponse(403, nlohmann::json{ {"status", "error"}, {"message", "Access Denied: User cannot create tables in this database."} });
            }
            [cite_start]// CREATE TABLE dbName.tableName (column_definition, ...); [cite: 359]
            std::string tableName = dataToModify.value("table_name", "new_table");
            nlohmann::json columns_definitions = dataToModify.value("columns", nlohmann::json::array());

            std::string cols_def_str = "";
            for (size_t i = 0; i < columns_definitions.size(); ++i) {
                cols_def_str += columns_definitions[i].get<std::string>();
                if (i < columns_definitions.size() - 1) cols_def_str += ", ";
            }
            sqlQuery = "CREATE TABLE " + dbName + "." + tableName + " (" + cols_def_str + ");";
            message = "Create table operation successful in database '" + dbName + "'. SQL: " + sqlQuery;
        } else {
            return JsonHandler::createJsonResponse(400, nlohmann::json{
                {"status", "error"},
                {"message", "Invalid operation type specified."}
            });
        }

        std::cout << "Generated SQL for modifyDatabase: " << sqlQuery << std::endl; // Вывод сгенерированного SQL

        return JsonHandler::createJsonResponse(200, nlohmann::json{
            {"status", "success"},
            {"message", message}
        });
    }
    catch (const nlohmann::json::parse_error& e) {
        return JsonHandler::createJsonResponse(400, nlohmann::json{
            {"status", "error"},
            {"message", "Invalid JSON format: " + std::string(e.what())}
        });
    }
    catch (const std::exception& e) {
        return JsonHandler::createJsonResponse(500, nlohmann::json{
            {"status", "error"},
            {"message", "An internal server error occurred: " + std::string(e.what())}
        });
    }
}


// Метод для получения списка доступных баз данных для пользователя
crow::response UserManager::getAccessibleDatabases(const std::string& jsonBody) {
    try {
        nlohmann::json json = nlohmann::json::parse(jsonBody);
        if (!json.contains("username")) {
            return JsonHandler::createJsonResponse(400, nlohmann::json{
                {"status", "error"},
                {"message", "You must specify username"}
            });
        }

        std::string username = json["username"].get<std::string>();

        std::optional<User> currentUserOpt = getUserFromDB(username);
        if (!currentUserOpt) {
            return JsonHandler::createJsonResponse(404, nlohmann::json{
                {"status", "error"},
                {"message", "User not found"}
            });
        }
        User currentUser = *currentUserOpt;

        nlohmann::json dbs_array = nlohmann::json::array();
        for (const auto& db : currentUser.accessibleDBs) {
            dbs_array.push_back({
                {"db_name", db.dbName},
                {"db_code", db.dbCode},
                {"access_level", db.accessLevel}
            });
        }

        return JsonHandler::createJsonResponse(200, nlohmann::json{
            {"status", "success"},
            {"username", username},
            {"accessible_databases", dbs_array}
        });
    }
    catch (const nlohmann::json::parse_error& e) {
        return JsonHandler::createJsonResponse(400, nlohmann::json{
            {"status", "error"},
            {"message", "Invalid JSON format: " + std::string(e.what())}
        });
    }
    catch (const std::exception& e) {
        return JsonHandler::createJsonResponse(500, nlohmann::json{
            {"status", "error"},
            {"message", "An internal server error occurred: " + std::string(e.what())}
        });
    }
}

// Метод для предоставления/отзыва доступа пользователя к БД
crow::response UserManager::manageDatabaseAccess(const std::string& jsonBody) {
    try {
        nlohmann::json json = nlohmann::json::parse(jsonBody);
        if (!json.contains("admin_username") || !json.contains("target_username") ||
            !json.contains("db_name") || !json.contains("access_level")) {
            return JsonHandler::createJsonResponse(400, nlohmann::json{
                {"status", "error"},
                {"message", "Missing required fields: admin_username, target_username, db_name, access_level"}
            });
        }

        std::string adminUsername = json["admin_username"].get<std::string>();
        std::string targetUsername = json["target_username"].get<std::string>();
        std::string dbName = json["db_name"].get<std::string>();
        int newAccessLevel = json["access_level"].get<int>();

        if (newAccessLevel < 0 || newAccessLevel > 3) {
            return JsonHandler::createJsonResponse(400, nlohmann::json{
                {"status", "error"},
                {"message", "Invalid access_level. Must be 0 (revoke), 1 (full), 2 (limited), or 3 (read-only)."}
            });
        }

        // Проверяем, что администратор имеет полный доступ (уровень 1) к этой БД
        std::optional<User> adminUserOpt = getUserFromDB(adminUsername);
        if (!adminUserOpt) {
            return JsonHandler::createJsonResponse(404, nlohmann::json{
                {"status", "error"},
                {"message", "Admin user not found"}
            });
        }
        User adminUser = *adminUserOpt;
        auto adminDbAccess = std::find_if(adminUser.accessibleDBs.begin(), adminUser.accessibleDBs.end(),
                                          [&](const DB& db) { return db.dbName == dbName; });
        if (adminDbAccess == adminUser.accessibleDBs.end() || adminDbAccess->accessLevel != 1) {
            return JsonHandler::createJsonResponse(403, nlohmann::json{
                {"status", "error"},
                {"message", "Access Denied: Only users with full access (level 1) can manage database access."}
            });
        }

        // Получаем данные целевого пользователя
        std::optional<User> targetUserOpt = getUserFromDB(targetUsername);
        if (!targetUserOpt) {
            return JsonHandler::createJsonResponse(404, nlohmann::json{
                {"status", "error"},
                {"message", "Target user not found"}
            });
        }
        User targetUser = *targetUserOpt;

        // Ищем запись о доступе целевого пользователя к данной БД
        auto targetDbAccess = std::find_if(targetUser.accessibleDBs.begin(), targetUser.accessibleDBs.end(),
                                           [&](const DB& db) { return db.dbName == dbName; });

        // Если новый уровень доступа 0 (отзыв)
        if (newAccessLevel == 0) {
            if (targetDbAccess != targetUser.accessibleDBs.end()) {
                targetUser.accessibleDBs.erase(targetDbAccess); // Удаляем доступ
                // Также удаляем запись пользователя из таблицы 'users' в этой конкретной БД
                if (!deleteUserRowFromDatabaseTable(dbName, targetUsername)) {
                    // Это может быть просто предупреждение, если основная запись пользователя уже удалена
                    std::cerr << "WARNING: Failed to delete user " << targetUsername
                              << " from database " << dbName << " users table." << std::endl;
                }
            } else {
                return JsonHandler::createJsonResponse(404, nlohmann::json{
                    {"status", "error"},
                    {"message", "User does not currently have access to this database."}
                });
            }
        } else { // Предоставление или изменение уровня доступа
            if (targetDbAccess != targetUser.accessibleDBs.end()) {
                targetDbAccess->accessLevel = newAccessLevel; // Обновляем уровень доступа
            } else {
                // Проверяем, не превышено ли количество баз данных (до 1000)
                if (targetUser.accessibleDBs.size() >= 1000) {
                    return JsonHandler::createJsonResponse(403, nlohmann::json{
                        {"status", "error"},
                        {"message", "Target user has reached the maximum limit of 1000 databases"}
                    });
                }
                // Если доступа нет, добавляем новый
                DB newAccessibleDb;
                newAccessibleDb.dbName = dbName;
                // Находим dbCode из globalDBs
                auto globalDbIt = std::find_if(globalDBs.begin(), globalDBs.end(),
                                               [&](const GlobalDBMetadata& meta) { return meta.dbName == dbName; });
                if (globalDbIt != globalDBs.end()) {
                    newAccessibleDb.dbCode = globalDbIt->dbCode;
                } else {
                    return JsonHandler::createJsonResponse(404, nlohmann::json{
                        {"status", "error"},
                        {"message", "Database not found in global registry."}
                    });
                }
                newAccessibleDb.accessLevel = newAccessLevel;
                targetUser.accessibleDBs.push_back(newAccessibleDb);

                // Записываем данные целевого пользователя в таблицу 'users' новой БД
                if (!writeUserDataToDatabaseTable(dbName, targetUser)) {
                    std::cerr << "WARNING: Failed to write target user " << targetUsername
                              << " data to database " << dbName << " users table." << std::endl;
                }
            }
        }

        // Обновляем данные целевого пользователя в центральной БД
        if (!updateUserInDB(targetUser)) {
            return JsonHandler::createJsonResponse(500, nlohmann::json{
                {"status", "error"},
                {"message", "Failed to update target user's accessible databases."}
            });
        }

        return JsonHandler::createJsonResponse(200, nlohmann::json{
            {"status", "success"},
            {"message", "Access for user '" + targetUsername + "' to database '" + dbName + "' set to level " + std::to_string(newAccessLevel)}
        });
    }
    catch (const nlohmann::json::parse_error& e) {
        return JsonHandler::createJsonResponse(400, nlohmann::json{
            {"status", "error"},
            {"message", "Invalid JSON format: " + std::string(e.what())}
        });
    }
    catch (const std::exception& e) {
        return JsonHandler::createJsonResponse(500, nlohmann::json{
            {"status", "error"},
            {"message", "An internal server error occurred: " + std::string(e.what())}
        });
    }
}

// Метод для удаления пользователя
crow::response UserManager::deleteUser(const std::string& jsonBody) {
    try {
        nlohmann::json json = nlohmann::json::parse(jsonBody);
        if (!json.contains("admin_username") || !json.contains("username")) {
            return JsonHandler::createJsonResponse(400, nlohmann::json{
                {"status", "error"},
                {"message", "You must specify admin_username and username to delete"}
            });
        }

        std::string adminUsername = json["admin_username"].get<std::string>();
        std::string username = json["username"].get<std::string>();

        // Для простоты, предположим, что любой admin_username имеет право удалять.
        // В реальной системе здесь будет более строгая проверка прав администратора.

        if (!doesUserExistInDB(username)) {
            return JsonHandler::createJsonResponse(404, nlohmann::json{
                {"status", "error"},
                {"message", "User to delete not found"}
            });
        }

        // Получаем информацию о пользователе, чтобы удалить его записи из доступных БД
        std::optional<User> userToDeleteOpt = getUserFromDB(username);
        if (userToDeleteOpt) {
            User userToDelete = *userToDeleteOpt;
            for (const auto& db : userToDelete.accessibleDBs) {
                // Удаляем запись пользователя из каждой доступной ему БД
                if (!deleteUserRowFromDatabaseTable(db.dbName, username)) {
                    // Это может быть просто предупреждение, если основная запись пользователя уже удалена
                    std::cerr << "WARNING: Failed to delete user " << username
                              << " from database " << db.dbName << " users table." << std::endl;
                }
            }
        }

        // Удаляем пользователя из центральной БД
        if (!deleteUserFromDB(username)) {
            return JsonHandler::createJsonResponse(500, nlohmann::json{
                {"status", "error"},
                {"message", "Failed to delete user from central database"}
            });
        }

        return JsonHandler::createJsonResponse(200, nlohmann::json{
            {"status", "success"},
            {"message", "User '" + username + "' deleted successfully"}
        });
    }
    catch (const nlohmann::json::parse_error& e) {
        return JsonHandler::createJsonResponse(400, nlohmann::json{
            {"status", "error"},
            {"message", "Invalid JSON format: " + std::string(e.what())}
        });
    }
    catch (const std::exception& e) {
        return JsonHandler::createJsonResponse(500, nlohmann::json{
            {"status", "error"},
            {"message", "An internal server error occurred: " + std::string(e.what())}
        });
    }
}
