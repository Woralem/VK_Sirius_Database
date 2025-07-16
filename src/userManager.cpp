/*#include <string>
#include <vector>
#include "userManager.h"
#include <crow.h>
#include <nlohmann/json.hpp>
#include "json-handler.h"
#include <sodium.h>
#include <unordered_set> // Для validPassword

// Передаем UserManager по ссылке
crow::response registerUser(UserManager& userManager, const std::string& jsonBody){
    try {
        // Парсим JSON-тело запроса
        nlohmann::json json = nlohmann::json::parse(jsonBody);

        // Проверяем наличие обязательных полей
        if (!json.contains("username") || !json.contains("password")) {
            return JsonHandler::createJsonResponse(400, nlohmann::json{
            {"status", "error"},
            {"message", "You must specify username and password"}
            });
        }

        std::string username = json["username"].get<std::string>();
        std::string password = json["password"].get<std::string>();

        // Проверяем, не существует ли уже такой пользователь
        if (userManager.userExists(username)) { // Используем переданный экземпляр
            return JsonHandler::createJsonResponse(409, nlohmann::json{
                {"status", "error"},
                {"message", "A user with that name already exists"}
                });
        }

        // Создаем нового пользователя
        bool success = userManager.createUser(username, password); // Используем переданный экземпляр

        if (!success) {
            return JsonHandler::createJsonResponse(500, nlohmann::json{
                {"status", "error"},
                {"message", "Error when creating a user"}
                });
        }

        // Возвращаем успешный ответ
        /*nlohmann::json responseJson;
        responseJson["message"] = "The user has been successfully registered";
        responseJson["username"] = username;
        return crow::response(201, responseJson.dump())
        #1#

        return JsonHandler::createJsonResponse(201, nlohmann::json{
                {"status", "success"},
                {"message", "The user has been successfully registered"}
                });

    }
    catch (const std::exception& e) {
        // Обработка ошибок парсинга JSON
        return JsonHandler::createJsonResponse(400, nlohmann::json{
            {"status", "error"},
            {"message", "Invalid JSON format: " + std::string(e.what())}
            });
    }
}

// Новая сигнатура loginUser
std::string UserManager::loginUser(const std::string& jsonBody, std::string& statusMessage, User& user) {
    try {
        // Парсим JSON-тело запроса
        auto json = nlohmann::json::parse(jsonBody);

        // Проверяем наличие обязательных полей
        if (!json.contains("username") || !json.contains("password")) {
            statusMessage = "error";
            return JsonHandler::createJsonResponse(400, nlohmann::json{
                {"status", "error"},
                {"message", "You must specify username and password"}
            }).body; // Возвращаем только тело сообщения
        }

        std::string username = json["username"];
        std::string password = json["password"];

        // Проверяем существование пользователя
        if (!userExists(username)) { // Используем переданный экземпляр
            statusMessage = "error";
            return JsonHandler::createJsonResponse(404, nlohmann::json{
                {"status", "error"},
                {"message", "The user was not found"}
            }).body;
        }

        // Проверяем пароль
        if (!userManager.validateUser(username, password)) { // Используем переданный экземпляр
            statusMessage = "error";
            return JsonHandler::createJsonResponse(401, nlohmann::json{
                {"status", "error"},
                {"message", "Invalid password"}
            }).body;
        }


        // Получаем данные пользователя
        if (!userManager.getUserByLogin(username, user)) { // Записываем данные в переданный User& user
             statusMessage = "error";
             return JsonHandler::createJsonResponse(500, nlohmann::json{
                {"status", "error"},
                {"message", "Failed to retrieve user data"}
            }).body;
        }

        // Генерируем токен (если используется аутентификация через токены)
        std::string token = userManager.generateAuthToken(username); // Используем переданный экземпляр

        statusMessage = "success";
        nlohmann::json responseJson;
        responseJson["message"] = "Successful login";
        responseJson["username"] = username;
        responseJson["token"] = token; // Добавляем токен в ответ
        responseJson["user_data"] = nlohmann::json::object();
        responseJson["user_data"]["login"] = user.login;
        responseJson["user_data"]["code"] = user.code;
        // Здесь можно добавить другие поля из user, если они нужны в ответе

        return responseJson.dump(); // Возвращаем JSON-строку
    } catch (const std::exception& e) {
        // Обработка ошибок парсинга JSON
        statusMessage = "error";
        return JsonHandler::createJsonResponse(400, nlohmann::json{
            {"status", "error"},
            {"message", "Invalid JSON format: " + std::string(e.what())}
        }).body;
    }
}

// Реализация методов UserManager (переносим их внутрь класса)
// Примерные реализации, вам нужно будет адаптировать их к вашей логике БД и хеширования

bool UserManager::doesLoginExist(const std::string& login){
    // Здесь должна быть логика проверки существования логина в вашей БД
    // Например:
    // return database.query("SELECT COUNT(*) FROM users WHERE login = ?", login) > 0;
    return false; // Заглушка
}

void UserManager::addUser(User& person, const std::string& hashedPassword){
    // Здесь должна быть логика добавления пользователя в вашу БД
    // Убедитесь, что вы сохраняете хешированный пароль, а не исходный
    // database.execute("INSERT INTO users (login, password_hash, code) VALUES (?, ?, ?)", person.login, hashedPassword, person.code);
}

void UserManager::getUser(User& person){
    // Здесь должна быть логика получения данных пользователя из вашей БД
    // Например, по login, и заполнение полей person
    // database.queryUserByLogin(person.login, person);
}

bool UserManager::validPassword(const std::string& password){
    // Примеры правил для пароля:
    // 1. Минимальная длина
    // 2. Наличие заглавных и строчных букв
    // 3. Наличие цифр
    // 4. Наличие специальных символов
    // 5. Все символы должны находиться в мн-ве допустимых символов

    if (password.length() < 8) { // Минимальная длина 8 символов
        return false;
    }
    // Используйте std::string для символов в unordered_set или std::set<char>
    std::unordered_set<char> alphabet = {
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '_', '+', '=', '{', '}', '[', ']', '|', ':', ';', '<', '>', ',', '.', '?', '/', '`', '~'
    };

    bool hasUpper = false;
    bool hasLower = false;
    bool hasDigit = false;
    bool hasSpecial = false;
    bool hasBadChars = false;

    for (char c : password) {
        if (alphabet.find(c) == alphabet.end()) {
            hasBadChars = true;
            break;
        }
        if (std::isupper(c)) {
            hasUpper = true;
        } else if (std::islower(c)) {
            hasLower = true;
        } else if (std::isdigit(c)) {
            hasDigit = true;
        } else if (std::ispunct(c)) { // ispunct включает много символов, возможно, лучше создать свой набор
            hasSpecial = true;
        }
    }


    // Пароль должен содержать как минимум по одному из каждого типа символов
    return hasUpper && hasLower && hasDigit && hasSpecial && !hasBadChars;
}

std::string UserManager::generateCode(){
    // Здесь должна быть логика генерации уникального персонального кода
    // Например, UUID или случайная строка
    return "some_generated_code"; // Заглушка
}

std::string UserManager::hashingPassword(const std::string& password){
    // Использование libsodium для хеширования пароля
    // Убедитесь, что libsodium установлен и подключен к вашему проекту
    // Пример:
    char hashed_password[crypto_pwhash_STRBYTES];
    if (crypto_pwhash_str(hashed_password, password.c_str(), password.length(),
                          crypto_pwhash_OPSLIMIT_MODERATE, crypto_pwhash_MEMLIMIT_MODERATE) != 0) {
        // Ошибка хеширования
        return "";
    }
    return std::string(hashed_password);
}

// Реализация новых публичных методов UserManager
bool UserManager::userExists(const std::string& username) {
    return doesLoginExist(username); // Используем приватный метод
}

bool UserManager::createUser(const std::string& username, const std::string& password) {
    if (!validPassword(password)) {
        return false;
    }
    std::string hashedPassword = hashingPassword(password);
    if (hashedPassword.empty()) {
        return false;
    }

    User newUser;
    newUser.login = username;
    newUser.code = generateCode(); // Генерируем код для нового пользователя
    // newUser.dataBases - инициализация данных о БД, если необходимо

    addUser(newUser, hashedPassword); // Добавляем пользователя в БД с хешированным паролем
    return true;
}

bool UserManager::validateUser(const std::string& username, const std::string& password) {
    // Здесь должна быть логика получения хеша пароля из БД для данного пользователя
    // и его сравнения с предоставленным паролем
    // Пример с libsodium:
    // std::string storedHashedPassword = getHashedPasswordFromDB(username);
    // if (storedHashedPassword.empty()) {
    //     return false; // Пользователь не найден или пароль не сохранен
    // }
    // return crypto_pwhash_str_verify(storedHashedPassword.c_str(), password.c_str(), password.length()) == 0;
    return true; // Заглушка: всегда успешно для примера
}

std::string UserManager::generateAuthToken(const std::string& username) {
    // Здесь должна быть логика генерации токена аутентификации
    // Например, JWT
    return "some_auth_token_for_" + username; // Заглушка
}

bool UserManager::getUserByLogin(const std::string& login, User& user) {
    // Здесь должна быть логика получения всех данных пользователя по логину из БД
    // и заполнения переданного объекта user.
    // Убедитесь, что вы НЕ извлекаете пароль из БД в этот объект User.
    // Пример:
    // database.queryUserByLoginWithoutPassword(login, user);
    user.login = login;
    user.code = "example_code_123";
    user.dataBases = {}; // Заполните реальными данными
    return true; // Заглушка
}*/