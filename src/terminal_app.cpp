#include <iostream>
#include <string>
#include <memory>
#include <sstream>
#include <iomanip>
#include <regex>
#include <format>
#include <ranges>
#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include "utils/logger.h"

using namespace query_engine;
using namespace utils;
using json = nlohmann::json;

class MockStorage : public StorageInterface {
private:
    std::map<std::string, json> tables;
    std::map<std::string, std::vector<ColumnDef>> schemas;

    [[nodiscard]] bool isValidInteger(std::string_view str) const {
        if (str.empty()) return false;
        static const std::regex intRegex("^[-+]?\\d+$");
        return std::regex_match(str.begin(), str.end(), intRegex);
    }

    [[nodiscard]] bool isValidDouble(std::string_view str) const {
        if (str.empty()) return false;
        static const std::regex doubleRegex("^[-+]?\\d*\\.?\\d+([eE][-+]?\\d+)?$");
        return std::regex_match(str.begin(), str.end(), doubleRegex);
    }

    [[nodiscard]] bool isValidBoolean(std::string_view str) const {
        std::string lower(str);
        std::ranges::transform(lower, lower.begin(), ::tolower);
        return lower == "true" || lower == "false" || lower == "1" || lower == "0" ||
               lower == "yes" || lower == "no" || lower == "on" || lower == "off";
    }

    [[nodiscard]] bool stringToBoolean(std::string_view str) const {
        std::string lower(str);
        std::ranges::transform(lower, lower.begin(), ::tolower);
        return lower == "true" || lower == "1" || lower == "yes" || lower == "on";
    }

    [[nodiscard]] json convertValue(const json& value, DataType fromType, DataType toType) const {
        if (value.is_null()) {
            return nullptr;
        }

        try {
            switch (toType) {
                case DataType::INT: {
                    switch (fromType) {
                        case DataType::INT:
                            return value;
                        case DataType::DOUBLE:
                            return static_cast<int>(std::round(value.get<double>()));
                        case DataType::VARCHAR: {
                            std::string_view str = value.get<std::string>();
                            if (isValidInteger(str)) {
                                return std::stoi(std::string(str));
                            } else if (isValidDouble(str)) {
                                return static_cast<int>(std::round(std::stod(std::string(str))));
                            } else {
                                LOG_WARNING("Storage", std::format("Cannot convert '{}' to INT, setting to NULL", str));
                                return nullptr;
                            }
                        }
                        case DataType::BOOLEAN:
                            return value.get<bool>() ? 1 : 0;
                        default:
                            return nullptr;
                    }
                }

                case DataType::DOUBLE: {
                    switch (fromType) {
                        case DataType::INT:
                            return static_cast<double>(value.get<int>());
                        case DataType::DOUBLE:
                            return value;
                        case DataType::VARCHAR: {
                            std::string_view str = value.get<std::string>();
                            if (isValidDouble(str) || isValidInteger(str)) {
                                return std::stod(std::string(str));
                            } else {
                                LOG_WARNING("Storage", std::format("Cannot convert '{}' to DOUBLE, setting to NULL", str));
                                return nullptr;
                            }
                        }
                        case DataType::BOOLEAN:
                            return value.get<bool>() ? 1.0 : 0.0;
                        default:
                            return nullptr;
                    }
                }

                case DataType::VARCHAR: {
                    switch (fromType) {
                        case DataType::INT:
                            return std::to_string(value.get<int>());
                        case DataType::DOUBLE:
                            return std::format("{}", value.get<double>());
                        case DataType::VARCHAR:
                            return value;
                        case DataType::BOOLEAN:
                            return value.get<bool>() ? "true" : "false";
                        default:
                            return value.dump();
                    }
                }

                case DataType::BOOLEAN: {
                    switch (fromType) {
                        case DataType::INT:
                            return value.get<int>() != 0;
                        case DataType::DOUBLE:
                            return value.get<double>() != 0.0;
                        case DataType::VARCHAR: {
                            std::string_view str = value.get<std::string>();
                            if (isValidBoolean(str)) {
                                return stringToBoolean(str);
                            } else {
                                LOG_WARNING("Storage", std::format("Cannot convert '{}' to BOOLEAN, setting to NULL", str));
                                return nullptr;
                            }
                        }
                        case DataType::BOOLEAN:
                            return value;
                        default:
                            return nullptr;
                    }
                }

                default:
                    LOG_ERROR("Storage", std::format("Unsupported target type: {}", dataTypeToString(toType)));
                    return nullptr;
            }
        } catch (const std::exception& e) {
            LOG_ERROR("Storage", std::format("Error converting value: {}, setting to NULL", e.what()));
            return nullptr;
        }
    }

    [[nodiscard]] DataType jsonTypeToDataType(const json& value) const {
        if (value.is_null()) return DataType::UNKNOWN_TYPE;
        if (value.is_number_integer()) return DataType::INT;
        if (value.is_number_float()) return DataType::DOUBLE;
        if (value.is_string()) return DataType::VARCHAR;
        if (value.is_boolean()) return DataType::BOOLEAN;
        return DataType::UNKNOWN_TYPE;
    }

    void setJsonValue(json& row, std::string_view key, const Value& value) {
        std::visit([&row, &key](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, std::monostate>) {
                row[std::string(key)] = nullptr;
            } else {
                row[std::string(key)] = arg;
            }
        }, value);
    }

public:
    [[nodiscard]] bool createTable(const std::string& tableName,
                    const std::vector<ColumnDef*>& columns,
                    const TableOptions& options = TableOptions()) override {
        LOG_INFO("Storage", std::format("Creating table '{}' in storage", tableName));

        schemas[tableName].clear();
        for (auto* col : columns) {
            schemas[tableName].push_back(*col);
        }

        tables[tableName] = json::array();

        LOG_SUCCESS("Storage", std::format("Table '{}' created in storage", tableName));
        return true;
    }

    [[nodiscard]] bool insertRow(const std::string& tableName,
                  const std::vector<std::string>& columns,
                  const std::vector<Value>& values) override {
        LOG_INFO("Storage", std::format("Inserting row into table '{}'", tableName));

        if (!tables.contains(tableName)) {
            LOG_ERROR("Storage", std::format("Table '{}' does not exist", tableName));
            return false;
        }

        json row;

        if (columns.empty()) {
            for (size_t i = 0; i < values.size() && i < schemas[tableName].size(); i++) {
                std::string colName = schemas[tableName][i].name;
                setJsonValue(row, colName, values[i]);
            }
        } else {
            for (size_t i = 0; i < columns.size() && i < values.size(); i++) {
                setJsonValue(row, columns[i], values[i]);
            }
        }

        tables[tableName].push_back(row);

        LOG_SUCCESS("Storage", "Row inserted successfully");
        LOG_DEBUG("Storage", std::format("Row data: {}", row.dump()));
        return true;
    }

    [[nodiscard]] json selectRows(const std::string& tableName,
                   const std::vector<std::string>& columns,
                   std::function<bool(const json&)> predicate) override {
        LOG_INFO("Storage", std::format("Selecting from table '{}'", tableName));

        json result;
        result["status"] = "success";
        result["data"] = json::array();

        if (!tables.contains(tableName)) {
            LOG_ERROR("Storage", std::format("Table '{}' does not exist", tableName));
            result["status"] = "error";
            result["message"] = "Table does not exist";
            return result;
        }

        int totalRows = 0;
        int matchedRows = 0;

        for (const auto& row : tables[tableName]) {
            totalRows++;
            if (predicate(row)) {
                matchedRows++;
                if (columns.empty()) {
                    result["data"].push_back(row);
                } else {
                    json filteredRow;
                    for (const auto& col : columns) {
                        if (row.contains(col)) {
                            filteredRow[col] = row[col];
                        }
                    }
                    result["data"].push_back(filteredRow);
                }
            }
        }

        LOG_SUCCESS("Storage", std::format("Selected {} out of {} rows", matchedRows, totalRows));
        return result;
    }

    [[nodiscard]] int updateRows(const std::string& tableName,
                  const std::vector<std::pair<std::string, Value>>& assignments,
                  std::function<bool(const json&)> predicate) override {
        LOG_INFO("Storage", std::format("Updating rows in table '{}'", tableName));

        if (!tables.contains(tableName)) {
            LOG_ERROR("Storage", std::format("Table '{}' does not exist", tableName));
            return 0;
        }

        int updatedCount = 0;

        for (auto& row : tables[tableName]) {
            if (predicate(row)) {
                for (const auto& [col, val] : assignments) {
                    setJsonValue(row, col, val);
                }
                updatedCount++;
                LOG_DEBUG("Storage", std::format("Updated row: {}", row.dump()));
            }
        }

        LOG_SUCCESS("Storage", std::format("Updated {} rows", updatedCount));
        return updatedCount;
    }

    [[nodiscard]] int deleteRows(const std::string& tableName,
                  std::function<bool(const json&)> predicate) override {
        LOG_INFO("Storage", std::format("Deleting rows from table '{}'", tableName));

        if (!tables.contains(tableName)) {
            LOG_ERROR("Storage", std::format("Table '{}' does not exist", tableName));
            return 0;
        }

        int deletedCount = 0;
        auto& tableData = tables[tableName];

        tableData.erase(
            std::remove_if(tableData.begin(), tableData.end(),
                [&predicate, &deletedCount](const json& row) {
                    if (predicate(row)) {
                        LOG_DEBUG("Storage", std::format("Deleting row: {}", row.dump()));
                        deletedCount++;
                        return true;
                    }
                    return false;
                }),
            tableData.end()
        );

        LOG_SUCCESS("Storage", std::format("Deleted {} rows", deletedCount));
        return deletedCount;
    }

    [[nodiscard]] bool renameTable(const std::string& oldName, const std::string& newName) override {
        LOG_INFO("Storage", std::format("Renaming table '{}' to '{}'", oldName, newName));

        auto it = tables.find(oldName);
        if (it == tables.end()) {
            LOG_ERROR("Storage", std::format("Table '{}' does not exist", oldName));
            return false;
        }

        if (tables.contains(newName)) {
            LOG_ERROR("Storage", std::format("Table '{}' already exists", newName));
            return false;
        }

        auto node = tables.extract(oldName);
        node.key() = newName;
        tables.insert(std::move(node));

        auto schemaNode = schemas.extract(oldName);
        schemaNode.key() = newName;
        schemas.insert(std::move(schemaNode));

        LOG_SUCCESS("Storage", "Table renamed successfully");
        return true;
    }

    [[nodiscard]] bool renameColumn(const std::string& tableName, const std::string& oldColumnName, const std::string& newColumnName) override {
        LOG_INFO("Storage", std::format("Renaming column '{}' to '{}' in table '{}'", oldColumnName, newColumnName, tableName));

        if (!tables.contains(tableName)) {
            LOG_ERROR("Storage", std::format("Table '{}' does not exist", tableName));
            return false;
        }

        // Обновляем схему
        for (auto& col : schemas[tableName]) {
            if (col.name == oldColumnName) {
                col.name = newColumnName;
                break;
            }
        }

        // Обновляем данные
        for (auto& row : tables[tableName]) {
            if (row.contains(oldColumnName)) {
                row[newColumnName] = row[oldColumnName];
                row.erase(oldColumnName);
            }
        }

        LOG_SUCCESS("Storage", "Column renamed successfully");
        return true;
    }

    [[nodiscard]] bool alterColumnType(const std::string& tableName, const std::string& columnName, DataType newType) override {
        LOG_INFO("Storage", std::format("Changing type of column '{}' to {}", columnName, dataTypeToString(newType)));

        if (!tables.contains(tableName)) {
            LOG_ERROR("Storage", std::format("Table '{}' does not exist", tableName));
            return false;
        }

        ColumnDef* colDef = nullptr;
        DataType oldType = DataType::UNKNOWN_TYPE;
        for (auto& col : schemas[tableName]) {
            if (col.name == columnName) {
                colDef = &col;
                oldType = col.parsedType;
                break;
            }
        }

        if (!colDef) {
            LOG_ERROR("Storage", std::format("Column '{}' does not exist", columnName));
            return false;
        }

        LOG_INFO("Storage", std::format("Converting from {} to {}", dataTypeToString(oldType), dataTypeToString(newType)));

        int convertedCount = 0;
        int nullCount = 0;
        int totalCount = 0;

        for (auto& row : tables[tableName]) {
            if (row.contains(columnName)) {
                totalCount++;
                json oldValue = row[columnName];

                if (oldValue.is_null()) {
                    continue;
                }

                DataType actualOldType = jsonTypeToDataType(oldValue);
                json newValue = convertValue(oldValue, actualOldType, newType);

                if (newValue.is_null() && !oldValue.is_null()) {
                    nullCount++;
                } else {
                    convertedCount++;
                }

                row[columnName] = newValue;
            }
        }

        colDef->parsedType = newType;
        colDef->dataType = std::string(dataTypeToString(newType));

        LOG_SUCCESS("Storage", "Column type changed successfully!");
        LOG_INFO("Storage", std::format("Total rows: {}, Converted: {}, Set to NULL: {}",
                                       totalCount, convertedCount, nullCount));

        return true;
    }

    [[nodiscard]] bool dropColumn(const std::string& tableName, const std::string& columnName) override {
        LOG_INFO("Storage", std::format("Dropping column '{}' from table '{}'", columnName, tableName));

        if (!tables.contains(tableName)) {
            LOG_ERROR("Storage", std::format("Table '{}' does not exist", tableName));
            return false;
        }

        bool columnExists = std::ranges::any_of(schemas[tableName], [&columnName](const auto& col) {
            return col.name == columnName;
        });

        if (!columnExists) {
            LOG_ERROR("Storage", std::format("Column '{}' does not exist", columnName));
            return false;
        }

        if (schemas[tableName].size() <= 1) {
            LOG_ERROR("Storage", "Cannot drop the last column from table");
            return false;
        }

        schemas[tableName].erase(
            std::ranges::remove_if(schemas[tableName], [&columnName](const ColumnDef& col) {
                return col.name == columnName;
            }).begin(),
            schemas[tableName].end()
        );

        for (auto& row : tables[tableName]) {
            if (row.contains(columnName)) {
                row.erase(columnName);
            }
        }

        LOG_SUCCESS("Storage", std::format("Column '{}' dropped successfully", columnName));
        return true;
    }

    [[nodiscard]] bool dropTable(const std::string& tableName) override {
        LOG_INFO("Storage", std::format("Dropping table '{}'", tableName));

        auto it = tables.find(tableName);
        if (it == tables.end()) {
            LOG_ERROR("Storage", std::format("Table '{}' does not exist", tableName));
            return false;
        }

        tables.erase(it);
        schemas.erase(tableName);

        LOG_SUCCESS("Storage", std::format("Table '{}' dropped successfully", tableName));
        return true;
    }
};

void printResult(const json& result) {
    std::cout << "\n\033[1;32m=== QUERY RESULT ===\033[0m\n";

    if (result.contains("error")) {
        std::cout << std::format("\033[91mERROR: {}\033[0m\n", result["error"].get<std::string>());
        return;
    }

    if (result.contains("data") && result["data"].is_array()) {
        auto& data = result["data"];

        if (data.empty()) {
            std::cout << "\033[93mNo rows returned\033[0m\n";
            return;
        }

        std::vector<std::string> columns;
        for (auto& [key, value] : data[0].items()) {
            columns.push_back(key);
        }

        std::cout << "\033[96m";
        for (const auto& col : columns) {
            std::cout << std::format("{:15} ", col);
        }
        std::cout << "\033[0m\n";

        std::cout << "\033[90m";
        for (const auto& col : columns) {
            std::cout << std::format("{:-<15} ", "");
        }
        std::cout << "\033[0m\n";

        for (const auto& row : data) {
            for (const auto& col : columns) {
                std::string value_str;
                if (row.contains(col)) {
                    if (row[col].is_null()) {
                        value_str = "NULL";
                    } else if (row[col].is_string()) {
                        value_str = row[col].get<std::string>();
                    } else {
                        value_str = row[col].dump();
                    }
                } else {
                    value_str = "NULL";
                }
                std::cout << std::format("{:15} ", value_str);
            }
            std::cout << "\n";
        }

        std::cout << std::format("\033[92m{} row(s) returned\033[0m\n", data.size());
    } else if (result.contains("rows_affected")) {
        std::cout << std::format("\033[92m{} row(s) affected\033[0m\n", result["rows_affected"].get<int>());
    } else if (result.contains("message")) {
        std::cout << std::format("\033[92m{}\033[0m\n", result["message"].get<std::string>());
    }
}

void printHelp() {
    std::cout << "\033[1;36m=== SQL COMMANDS ===\033[0m\n";
    std::cout << "CREATE TABLE table_name (column_name data_type [constraints], ...)\n";
    std::cout << "INSERT INTO table_name [(columns)] VALUES (values), ...\n";
    std::cout << "SELECT * | columns FROM table_name [WHERE condition]\n";
    std::cout << "UPDATE table_name SET column = value, ... [WHERE condition]\n";
    std::cout << "DELETE FROM table_name [WHERE condition]\n";
    std::cout << "ALTER TABLE table_name RENAME TO new_table_name\n";
    std::cout << "ALTER TABLE table_name RENAME COLUMN old_col TO new_col\n";
    std::cout << "ALTER TABLE table_name ALTER COLUMN col_name TYPE new_type\n";
    std::cout << "ALTER TABLE table_name DROP COLUMN column_name\n";
    std::cout << "DROP TABLE [IF EXISTS] table_name\n";
    std::cout << "\n";
    std::cout << "\033[1;36m=== PATTERN MATCHING ===\033[0m\n";
    std::cout << "WHERE column LIKE 'pattern'\n";
    std::cout << "  % - matches any sequence of characters\n";
    std::cout << "  _ - matches any single character\n";
    std::cout << "Examples:\n";
    std::cout << "  name LIKE 'John%'     - names starting with 'John'\n";
    std::cout << "  email LIKE '%@gmail.com' - Gmail addresses\n";
    std::cout << "  phone LIKE '555-___-____' - phone numbers with 555 area code\n";
    std::cout << "\n";
    std::cout << "\033[1;36m=== SPECIAL COMMANDS ===\033[0m\n";
    std::cout << "\\h or \\help - Show this help\n";
    std::cout << "\\q or \\quit - Exit the program\n";
    std::cout << "\\c or \\clear - Clear the screen\n";
}

int main() {
    // Clear screen and show welcome message
    #ifdef _WIN32
        system("cls");
    #else
        system("clear");
    #endif

    std::cout << "\033[1;35m";
    std::cout << "╔══════════════════════════════════════════════════════════════╗\n";
    std::cout << "║           DATABASE QUERY ENGINE - TERMINAL INTERFACE         ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n";
    std::cout << "\033[0m\n";

    std::cout << "Type \\h for help, \\q to quit\n\n";

    auto storage = std::make_shared<MockStorage>();
    auto executor = std::make_unique<QueryExecutor>(storage);

    std::string input;

    while (true) {
        std::cout << "\n\033[1;33mSQL> \033[0m";
        std::getline(std::cin, input);

        // Trim whitespace
        auto trimmed = input | std::views::drop_while(::isspace) | std::views::reverse
                             | std::views::drop_while(::isspace) | std::views::reverse;
        input = std::string(trimmed.begin(), trimmed.end());

        if (input.empty()) continue;

        if (input == "\\q" || input == "\\quit") {
            std::cout << "\033[1;32mGoodbye!\033[0m\n";
            break;
        } else if (input == "\\h" || input == "\\help") {
            printHelp();
            continue;
        } else if (input == "\\c" || input == "\\clear") {
            #ifdef _WIN32
                system("cls");
            #else
                system("clear");
            #endif
            continue;
        }

        Logger::header("PROCESSING QUERY");
        Logger::printBox("Input Query", input);

        try {
            Logger::header("LEXICAL ANALYSIS");
            Lexer lexer(input);
            auto tokens = lexer.tokenize();

            Logger::header("PARSING");
            Parser parser(std::span{tokens});
            auto ast = parser.parse();

            if (parser.hasError()) {
                std::cout << "\n\033[91m=== PARSE ERRORS ===\033[0m\n";
                for (const auto& error : parser.getErrors()) {
                    std::cout << std::format("\033[91m• {}\033[0m\n", error);
                }
                continue;
            }

            Logger::header("EXECUTION");
            json result = executor->execute(ast);

            printResult(result);

        } catch (const std::exception& e) {
            std::cout << std::format("\n\033[91mFATAL ERROR: {}\033[0m\n", e.what());
        }
    }

    return 0;
}