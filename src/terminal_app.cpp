#include <iostream>
#include <string>
#include <memory>
#include <sstream>
#include <iomanip>
#include <regex>
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

    bool isValidInteger(const std::string& str) {
        if (str.empty()) return false;
        std::regex intRegex("^[-+]?\\d+$");
        return std::regex_match(str, intRegex);
    }

    bool isValidDouble(const std::string& str) {
        if (str.empty()) return false;
        std::regex doubleRegex("^[-+]?\\d*\\.?\\d+([eE][-+]?\\d+)?$");
        return std::regex_match(str, doubleRegex);
    }

    bool isValidBoolean(const std::string& str) {
        std::string lower = str;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
        return lower == "true" || lower == "false" || lower == "1" || lower == "0" ||
               lower == "yes" || lower == "no" || lower == "on" || lower == "off";
    }

    bool stringToBoolean(const std::string& str) {
        std::string lower = str;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
        return lower == "true" || lower == "1" || lower == "yes" || lower == "on";
    }

    json convertValue(const json& value, DataType fromType, DataType toType) {
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
                            std::string str = value.get<std::string>();
                            if (isValidInteger(str)) {
                                return std::stoi(str);
                            } else if (isValidDouble(str)) {
                                return static_cast<int>(std::round(std::stod(str)));
                            } else {
                                LOG_WARNING("Storage", "Cannot convert '" + str + "' to INT, setting to NULL");
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
                            std::string str = value.get<std::string>();
                            if (isValidDouble(str) || isValidInteger(str)) {
                                return std::stod(str);
                            } else {
                                LOG_WARNING("Storage", "Cannot convert '" + str + "' to DOUBLE, setting to NULL");
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
                        case DataType::DOUBLE: {
                            double d = value.get<double>();
                            std::ostringstream oss;
                            oss << d;
                            return oss.str();
                        }
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
                            std::string str = value.get<std::string>();
                            if (isValidBoolean(str)) {
                                return stringToBoolean(str);
                            } else {
                                LOG_WARNING("Storage", "Cannot convert '" + str + "' to BOOLEAN, setting to NULL");
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
                    LOG_ERROR("Storage", "Unsupported target type: " + dataTypeToString(toType));
                    return nullptr;
            }
        } catch (const std::exception& e) {
            LOG_ERROR("Storage", "Error converting value: " + std::string(e.what()) + ", setting to NULL");
            return nullptr;
        }
    }

    DataType jsonTypeToDataType(const json& value) {
        if (value.is_null()) return DataType::UNKNOWN_TYPE;
        if (value.is_number_integer()) return DataType::INT;
        if (value.is_number_float()) return DataType::DOUBLE;
        if (value.is_string()) return DataType::VARCHAR;
        if (value.is_boolean()) return DataType::BOOLEAN;
        return DataType::UNKNOWN_TYPE;
    }

public:
    bool createTable(const std::string& tableName,
                    const std::vector<ColumnDef*>& columns,
                    const TableOptions& options = TableOptions()) override {
        LOG_INFO("Storage", "Creating table '" + tableName + "' in storage");

        schemas[tableName].clear();
        for (auto* col : columns) {
            schemas[tableName].push_back(*col);
        }

        tables[tableName] = json::array();

        LOG_SUCCESS("Storage", "Table '" + tableName + "' created in storage");
        return true;
    }

    bool insertRow(const std::string& tableName,
                  const std::vector<std::string>& columns,
                  const std::vector<Value>& values) override {
        LOG_INFO("Storage", "Inserting row into table '" + tableName + "'");

        if (tables.find(tableName) == tables.end()) {
            LOG_ERROR("Storage", "Table '" + tableName + "' does not exist");
            return false;
        }

        json row;

        if (columns.empty()) {
            for (size_t i = 0; i < values.size() && i < schemas[tableName].size(); i++) {
                std::string colName = schemas[tableName][i].name;
                if (std::holds_alternative<int>(values[i])) {
                    row[colName] = std::get<int>(values[i]);
                } else if (std::holds_alternative<double>(values[i])) {
                    row[colName] = std::get<double>(values[i]);
                } else if (std::holds_alternative<std::string>(values[i])) {
                    row[colName] = std::get<std::string>(values[i]);
                } else {
                    row[colName] = nullptr;
                }
            }
        } else {
            for (size_t i = 0; i < columns.size() && i < values.size(); i++) {
                if (std::holds_alternative<int>(values[i])) {
                    row[columns[i]] = std::get<int>(values[i]);
                } else if (std::holds_alternative<double>(values[i])) {
                    row[columns[i]] = std::get<double>(values[i]);
                } else if (std::holds_alternative<std::string>(values[i])) {
                    row[columns[i]] = std::get<std::string>(values[i]);
                } else {
                    row[columns[i]] = nullptr;
                }
            }
        }

        tables[tableName].push_back(row);

        LOG_SUCCESS("Storage", "Row inserted successfully");
        LOG_DEBUG("Storage", "Row data: " + row.dump());
        return true;
    }

    json selectRows(const std::string& tableName,
                   const std::vector<std::string>& columns,
                   std::function<bool(const json&)> predicate) override {
        LOG_INFO("Storage", "Selecting from table '" + tableName + "'");

        json result;
        result["status"] = "success";
        result["data"] = json::array();

        if (tables.find(tableName) == tables.end()) {
            LOG_ERROR("Storage", "Table '" + tableName + "' does not exist");
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

        LOG_SUCCESS("Storage", "Selected " + std::to_string(matchedRows) +
                              " out of " + std::to_string(totalRows) + " rows");

        return result;
    }

    int updateRows(const std::string& tableName,
                  const std::vector<std::pair<std::string, Value>>& assignments,
                  std::function<bool(const json&)> predicate) override {
        LOG_INFO("Storage", "Updating rows in table '" + tableName + "'");

        if (tables.find(tableName) == tables.end()) {
            LOG_ERROR("Storage", "Table '" + tableName + "' does not exist");
            return 0;
        }

        int updatedCount = 0;

        for (auto& row : tables[tableName]) {
            if (predicate(row)) {
                for (const auto& [col, val] : assignments) {
                    if (std::holds_alternative<int>(val)) {
                        row[col] = std::get<int>(val);
                    } else if (std::holds_alternative<double>(val)) {
                        row[col] = std::get<double>(val);
                    } else if (std::holds_alternative<std::string>(val)) {
                        row[col] = std::get<std::string>(val);
                    } else {
                        row[col] = nullptr;
                    }
                }
                updatedCount++;
                LOG_DEBUG("Storage", "Updated row: " + row.dump());
            }
        }

        LOG_SUCCESS("Storage", "Updated " + std::to_string(updatedCount) + " rows");
        return updatedCount;
    }

    int deleteRows(const std::string& tableName,
                  std::function<bool(const json&)> predicate) override {
        LOG_INFO("Storage", "Deleting rows from table '" + tableName + "'");

        if (tables.find(tableName) == tables.end()) {
            LOG_ERROR("Storage", "Table '" + tableName + "' does not exist");
            return 0;
        }

        int deletedCount = 0;
        auto& tableData = tables[tableName];

        for (auto it = tableData.begin(); it != tableData.end();) {
            if (predicate(*it)) {
                LOG_DEBUG("Storage", "Deleting row: " + it->dump());
                it = tableData.erase(it);
                deletedCount++;
            } else {
                ++it;
            }
        }

        LOG_SUCCESS("Storage", "Deleted " + std::to_string(deletedCount) + " rows");
        return deletedCount;
    }

    // ALTER TABLE operations
    bool renameTable(const std::string& oldName, const std::string& newName) override {
        LOG_INFO("Storage", "Renaming table '" + oldName + "' to '" + newName + "'");

        auto it = tables.find(oldName);
        if (it == tables.end()) {
            LOG_ERROR("Storage", "Table '" + oldName + "' does not exist");
            return false;
        }

        if (tables.count(newName) > 0) {
            LOG_ERROR("Storage", "Table '" + newName + "' already exists");
            return false;
        }

        tables[newName] = std::move(it->second);
        schemas[newName] = std::move(schemas[oldName]);
        tables.erase(oldName);
        schemas.erase(oldName);

        LOG_SUCCESS("Storage", "Table renamed successfully");
        return true;
    }

    bool renameColumn(const std::string& tableName, const std::string& oldColumnName, const std::string& newColumnName) override {
        LOG_INFO("Storage", "Renaming column '" + oldColumnName + "' to '" + newColumnName + "' in table '" + tableName + "'");

        if (tables.find(tableName) == tables.end()) {
            LOG_ERROR("Storage", "Table '" + tableName + "' does not exist");
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

    bool alterColumnType(const std::string& tableName, const std::string& columnName, DataType newType) override {
        LOG_INFO("Storage", "Changing type of column '" + columnName + "' to " + dataTypeToString(newType));

        if (tables.find(tableName) == tables.end()) {
            LOG_ERROR("Storage", "Table '" + tableName + "' does not exist");
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
            LOG_ERROR("Storage", "Column '" + columnName + "' does not exist");
            return false;
        }

        LOG_INFO("Storage", "Converting from " + dataTypeToString(oldType) + " to " + dataTypeToString(newType));

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
        colDef->dataType = dataTypeToString(newType);

        LOG_SUCCESS("Storage", "Column type changed successfully!");
        LOG_INFO("Storage", "Total rows: " + std::to_string(totalCount) +
                           ", Converted: " + std::to_string(convertedCount) +
                           ", Set to NULL: " + std::to_string(nullCount));

        return true;
    }
};

void printResult(const json& result) {
    std::cout << "\n\033[1;32m=== QUERY RESULT ===\033[0m\n";

    if (result.contains("error")) {
        std::cout << "\033[91mERROR: " << result["error"] << "\033[0m\n";
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
            std::cout << std::left << std::setw(15) << col << " ";
        }
        std::cout << "\033[0m\n";

        std::cout << "\033[90m";
        for (const auto& col : columns) {
            std::cout << std::string(15, '-') << " ";
        }
        std::cout << "\033[0m\n";

        for (const auto& row : data) {
            for (const auto& col : columns) {
                std::stringstream ss;
                if (row.contains(col)) {
                    if (row[col].is_null()) {
                        ss << "NULL";
                    } else if (row[col].is_string()) {
                        ss << row[col].get<std::string>();
                    } else {
                        ss << row[col];
                    }
                } else {
                    ss << "NULL";
                }
                std::cout << std::left << std::setw(15) << ss.str() << " ";
            }
            std::cout << "\n";
        }

        std::cout << "\033[92m" << data.size() << " row(s) returned\033[0m\n";
    } else if (result.contains("rows_affected")) {
        std::cout << "\033[92m" << result["rows_affected"] << " row(s) affected\033[0m\n";
    } else if (result.contains("message")) {
        std::cout << "\033[92m" << result["message"] << "\033[0m\n";
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

        input.erase(0, input.find_first_not_of(" \t\n\r"));
        input.erase(input.find_last_not_of(" \t\n\r") + 1);

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
            Parser parser(tokens);
            auto ast = parser.parse();

            if (parser.hasError()) {
                std::cout << "\n\033[91m=== PARSE ERRORS ===\033[0m\n";
                for (const auto& error : parser.getErrors()) {
                    std::cout << "\033[91m• " << error << "\033[0m\n";
                }
                continue;
            }

            Logger::header("EXECUTION");
            json result = executor->execute(ast);

            printResult(result);

        } catch (const std::exception& e) {
            std::cout << "\n\033[91mFATAL ERROR: " << e.what() << "\033[0m\n";
        }
    }

    return 0;
}