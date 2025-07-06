#include <iostream>
#include <vector>
#include <string>
#include <stdexcept>
#include <chrono>
#include <filesystem>
#include <random>
#include <numeric> // для std::accumulate

#ifdef _WIN32
#include <windows.h> // для SetConsoleOutputCP
#endif

#include "storage_engine.h" // Подключаем наш движок

// --- Вспомогательные функции для тестов ---

void printTestResult(bool success, const std::string& test_name) {
    std::cout << "[ " << (success ? "PASS" : "FAIL") << " ] " << test_name << std::endl;
    if (!success) {
        // Завершаем программу при первом же провале, чтобы не получать каскад ошибок
        std::cerr << "      |!| Тест провален. Дальнейшее выполнение остановлено." << std::endl;
        exit(1);
    }
}

std::string generateRandomTableName(int length) {
    static const std::string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-";
    static std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<int> distribution(0, chars.length() - 1);

    std::string random_string;
    random_string.reserve(length);
    for (int i = 0; i < length; ++i) {
        random_string += chars[distribution(generator)];
    }

    if (random_string.back() == '_') {
        random_string.back() = 'a';
    }
    if (random_string.find_first_not_of('-') == std::string::npos) {
        random_string[0] = 'a';
    }
    return random_string;
}

// --- Блоки тестов ---

void runFunctionalTests() {
    std::cout << "\n--- 1. Запуск функциональных тестов ---" << std::endl;
    const std::string db_path = "functional_test_db";
    if (std::filesystem::exists(db_path)) {
        std::filesystem::remove_all(db_path);
    }
    
    StorageEngine engine(db_path);

    const std::vector<ColumnDef> test_columns = {
        {"id", DataType::Integer, true, true},
        {"name", DataType::VarChar, false, true}
    };

    // 1. Успешное создание
    try {
        engine.createTable("users", test_columns);
        printTestResult(true, "Успешное создание валидной таблицы 'users'");
    } catch (const std::exception& e) {
        std::cerr << "      | Ошибка: " << e.what() << std::endl;
        printTestResult(false, "Успешное создание валидной таблицы 'users'");
    }

    // 2. Дубликат в памяти
    try {
        engine.createTable("users", test_columns);
        printTestResult(false, "Попытка создать дубликат таблицы 'users' (ожидалась ошибка)");
    } catch (const std::runtime_error&) {
        printTestResult(true, "Попытка создать дубликат таблицы 'users' (ожидалась ошибка)");
    }

    // 3. Невалидные имена
    std::vector<std::pair<std::string, std::string>> invalid_names = {
        {"слишком_длинное_имя_больше_16_символов", "Имя длиннее 16 символов"},
        {"invalid_name_", "Имя заканчивается на '_'"}, {"", "Пустое имя"},
        {"----", "Имя состоит только из '-'"}, {"name$!@", "Имя содержит недопустимые символы"}
    };
    for (const auto& test_case : invalid_names) {
        try {
            engine.createTable(test_case.first, test_columns);
            printTestResult(false, "Тест на невалидное имя: " + test_case.second);
        } catch (const std::invalid_argument&) {
            printTestResult(true, "Тест на невалидное имя: " + test_case.second);
        }
    }
    std::cout << "[ INFO ] Функциональные тесты завершены успешно." << std::endl;
}

// Объединяем тесты производительности и персистентности, так как они логически связаны
void runPerfAndPersistenceTests(const std::string& db_path, std::vector<std::string>& created_names) {
    std::cout << "\n--- 2. Запуск тестов производительности и персистентности ---" << std::endl;
    if (std::filesystem::exists(db_path)) {
        std::filesystem::remove_all(db_path);
    }
    
    StorageEngine engine(db_path);

    const int num_tables_to_create = 10000;
    const std::vector<ColumnDef> test_columns = {{"id", DataType::Integer}};

    created_names.clear();
    created_names.reserve(num_tables_to_create);
    for(int i = 0; i < num_tables_to_create; ++i) {
        created_names.push_back(generateRandomTableName(12));
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    for (const auto& name : created_names) {
        try {
            engine.createTable(name, test_columns);
        } catch (const std::exception& e) {
            std::cerr << "      | Ошибка: " << e.what() << std::endl;
            printTestResult(false, "Критическая ошибка в тесте производительности при создании таблицы '" + name + "'");
        }
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration_ms = end_time - start_time;

    std::cout << "[ INFO ] Тест производительности 'createTable':" << std::endl;
    std::cout << "    - Создано таблиц: " << num_tables_to_create << std::endl;
    std::cout << "    - Общее время: " << duration_ms.count() << " мс" << std::endl;
    std::cout << "    - Среднее время на операцию: " << duration_ms.count() / num_tables_to_create << " мс" << std::endl;
    std::cout << "    - Операций в секунду (TPS): " << num_tables_to_create / (duration_ms.count() / 1000.0) << std::endl;
    
    printTestResult(true, "Тест производительности и сохранения на диск");
    std::cout << "[ INFO ] Данные сохранены на диск. Движок будет перезапущен для проверки загрузки." << std::endl;
}

// Новый, самый важный тест!
void runLoadAndIntegrityTests(const std::string& db_path, const std::vector<std::string>& created_names) {
    std::cout << "\n--- 3. Запуск тестов загрузки данных и целостности ---" << std::endl;

    if (!std::filesystem::exists(db_path)) {
         printTestResult(false, "Каталог БД не существует перед тестом загрузки. Ошибка в предыдущем тесте.");
    }
    if (created_names.empty()) {
        printTestResult(false, "Список созданных таблиц пуст. Ошибка в предыдущем тесте.");
    }

    // 1. Замеряем время создания движка, что включает загрузку каталога с диска
    auto start_time = std::chrono::high_resolution_clock::now();
    StorageEngine engine(db_path);
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration_ms = end_time - start_time;

    std::cout << "[ INFO ] Время инициализации движка (загрузка " << created_names.size() << " таблиц): " << duration_ms.count() << " мс" << std::endl;
    
    // 2. Проверяем, что ранее созданная таблица теперь вызывает ошибку дубликата
    // Мы берем имя из середины списка для надежности
    const std::string& existing_table = created_names[created_names.size() / 2];
    const std::vector<ColumnDef> test_columns = {{"id", DataType::Integer}};
    
    try {
        engine.createTable(existing_table, test_columns);
        printTestResult(false, "Проверка целостности: создание дубликата '" + existing_table + "' после перезапуска");
    } catch (const std::runtime_error&) {
        printTestResult(true, "Проверка целостности: создание дубликата '" + existing_table + "' после перезапуска");
    }

    // 3. Дополнительная проверка: создаем новую таблицу, чтобы убедиться, что система все еще работает
    std::string new_table_name = "final_check";
    try {
        engine.createTable(new_table_name, test_columns);
        printTestResult(true, "Проверка целостности: создание новой таблицы '" + new_table_name + "' после перезапуска");
    } catch (const std::exception& e) {
        std::cerr << "      | Ошибка: " << e.what() << std::endl;
        printTestResult(false, "Проверка целостности: создание новой таблицы '" + new_table_name + "' после перезапуска");
    }

    std::cout << "[ INFO ] Тесты загрузки и целостности завершены успешно." << std::endl;
}


int main() {
#ifdef _WIN32
    SetConsoleOutputCP(CP_UTF8);
#endif

    std::cout << "====== ЗАПУСК ТЕСТИРОВАНИЯ STORAGE ENGINE ======" << std::endl;
    
    // Блок 1: Базовая функциональность
    runFunctionalTests();

    // Блок 2 и 3: Производительность, сохранение и загрузка
    const std::string perf_db_path = "performance_db";
    std::vector<std::string> created_table_names; // Этот вектор передается между тестами

    runPerfAndPersistenceTests(perf_db_path, created_table_names);
    runLoadAndIntegrityTests(perf_db_path, created_table_names);

    std::cout << "\n====== ВСЕ ТЕСТЫ УСПЕШНО ЗАВЕРШЕНЫ ======" << std::endl;

    return 0;
}