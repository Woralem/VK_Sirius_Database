#pragma once

#include "query_engine/executor.h"
#include <nlohmann/json.hpp>
#include <unordered_map>
#include <string>
#include <vector>
#include <iostream>
#include <functional>
#include <algorithm>
#include <chrono>
#include <sstream>
#include <regex>

using json = nlohmann::json;
using namespace query_engine;

class OptimizedInMemoryStorage : public StorageInterface {
private:
    struct Table {
        json data = json::array();
        std::vector<ColumnDef> schema;
        TableOptions options;
        std::unordered_map<std::string, std::unordered_map<std::string, std::vector<size_t>>> indexes;
        std::chrono::system_clock::time_point lastGC;
    };

    std::unordered_map<std::string, Table> tables;

    void setJsonValue(json& row, const std::string& key, const Value& value) {
        if (std::holds_alternative<int>(value)) {
            row[key] = std::get<int>(value);
        } else if (std::holds_alternative<double>(value)) {
            row[key] = std::get<double>(value);
        } else if (std::holds_alternative<std::string>(value)) {
            row[key] = std::get<std::string>(value);
        } else if (std::holds_alternative<bool>(value)) {
            row[key] = std::get<bool>(value);
        } else {
            row[key] = nullptr;
        }
    }

    std::string valueToIndexKey(const json& value) {
        if (value.is_null()) return "__NULL__";
        if (value.is_string()) return value.get<std::string>();
        return value.dump();
    }

    bool validateColumnName(const std::string& name, const TableOptions& options) {
        if (name.length() > options.maxColumnNameLength) return false;
        for (char c : name) {
            if (std::isalnum(c) || c == '_' || options.additionalNameChars.count(c)) continue;
            return false;
        }
        return true;
    }

    const ColumnDef* getColumnDef(const Table& table, const std::string& colName) {
        for (const auto& col : table.schema) {
            if (col.name == colName) return &col;
        }
        return nullptr;
    }

    bool validateValueForColumn(const Value& value, const ColumnDef& colDef) {
        if (colDef.notNull && std::holds_alternative<std::monostate>(value)) {
            std::cout << "\033[91m[VALIDATION ERROR]\033[0m Column '" << colDef.name << "' cannot be null.\n";
            return false;
        }
        if (std::holds_alternative<std::monostate>(value)) return true;
        bool type_ok = false;
        switch (colDef.parsedType) {
            case DataType::INT:       type_ok = std::holds_alternative<int>(value); break;
            case DataType::DOUBLE:    type_ok = std::holds_alternative<double>(value) || std::holds_alternative<int>(value); break;
            case DataType::VARCHAR:   type_ok = std::holds_alternative<std::string>(value); break;
            case DataType::BOOLEAN:   type_ok = std::holds_alternative<bool>(value); break;
            default: type_ok = true; break;
        }
        if (!type_ok) {
            std::cout << "\033[91m[VALIDATION ERROR]\033[0m Type mismatch for column '" << colDef.name << "'.\n";
            return false;
        }
        return true;
    }

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
                                std::cout << "\033[93m[WARNING]\033[0m Cannot convert '" << str << "' to INT, setting to NULL\n";
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
                                std::cout << "\033[93m[WARNING]\033[0m Cannot convert '" << str << "' to DOUBLE, setting to NULL\n";
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
                                std::cout << "\033[93m[WARNING]\033[0m Cannot convert '" + str + "' to BOOLEAN, setting to NULL\n";
                                return nullptr;
                            }
                        }
                        case DataType::BOOLEAN:
                            return value;
                        default:
                            return nullptr;
                    }
                }

                case DataType::DATE:
                case DataType::TIMESTAMP: {
                    if (fromType == DataType::VARCHAR) {
                        return value;
                    } else {
                        return convertValue(value, fromType, DataType::VARCHAR);
                    }
                }

                default:
                    std::cout << "\033[91m[ERROR]\033[0m Unsupported target type: " << dataTypeToString(toType) << "\n";
                    return nullptr;
            }
        } catch (const std::exception& e) {
            std::cout << "\033[91m[ERROR]\033[0m Error converting value: " << e.what() << ", setting to NULL\n";
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
    bool createTable(const std::string& tableName, const std::vector<ColumnDef*>& columns, const TableOptions& options = TableOptions()) override {
        if (tables.count(tableName)) return false;
        auto& table = tables[tableName];
        table.options = options;
        table.lastGC = std::chrono::system_clock::now();
        for (auto* col : columns) {
            if (col->parsedType == DataType::UNKNOWN_TYPE || !validateColumnName(col->name, options) ||
                (!options.allowedTypes.empty() && !options.allowedTypes.count(col->parsedType))) {
                tables.erase(tableName);
                return false;
            }
            table.schema.push_back(*col);
            if(col->primaryKey) table.indexes[col->name];
        }
        return true;
    }

    bool insertRow(const std::string& tableName, const std::vector<std::string>& columns, const std::vector<Value>& values) override {
        auto table_it = tables.find(tableName);
        if (table_it == tables.end()) return false;
        auto& table = table_it->second;

        std::vector<std::pair<std::string, Value>> valueMap;
        if (columns.empty()) {
            if (values.size() != table.schema.size()) return false;
            for (size_t i = 0; i < values.size(); ++i) valueMap.emplace_back(table.schema[i].name, values[i]);
        } else {
            if (columns.size() != values.size()) return false;
            for (size_t i = 0; i < columns.size(); ++i) valueMap.emplace_back(columns[i], values[i]);
        }

        for (const auto& [colName, value] : valueMap) {
            const ColumnDef* colDef = getColumnDef(table, colName);
            if (!colDef || (std::holds_alternative<std::string>(value) && std::get<std::string>(value).length() > table.options.maxStringLength) || !validateValueForColumn(value, *colDef)) return false;
            if (colDef->primaryKey) {
                json tempJson; setJsonValue(tempJson, colName, value);
                if(table.indexes.count(colName) && table.indexes.at(colName).count(valueToIndexKey(tempJson[colName]))) return false;
            }
        }

        json row;
        for (const auto& [colName, value] : valueMap) setJsonValue(row, colName, value);
        size_t rowIndex = table.data.size();
        for (auto& [colName, index] : table.indexes) {
            if (row.contains(colName)) index[valueToIndexKey(row[colName])].push_back(rowIndex);
        }
        table.data.push_back(std::move(row));
        return true;
    }

    int updateRows(const std::string& tableName, const std::vector<std::pair<std::string, Value>>& assignments, std::function<bool(const json&)> predicate) override {
        auto it = tables.find(tableName);
        if (it == tables.end()) return 0;
        auto& table = it->second;
        int updatedCount = 0;
        std::vector<size_t> rowsToUpdate;
        for (size_t i = 0; i < table.data.size(); ++i) if (predicate(table.data[i])) rowsToUpdate.push_back(i);

        for (size_t i : rowsToUpdate) {
            bool valid = true;
            for (const auto& [colName, value] : assignments) {
                const ColumnDef* colDef = getColumnDef(table, colName);
                if (!colDef || !validateValueForColumn(value, *colDef)) { valid = false; break; }
                if (std::holds_alternative<std::string>(value) && std::get<std::string>(value).length() > table.options.maxStringLength) { valid = false; break; }
                if (colDef->primaryKey) {
                    json tempJson; setJsonValue(tempJson, colName, value);
                    auto key_to_check = valueToIndexKey(tempJson[colName]);
                    if(table.indexes.at(colName).count(key_to_check) && !table.indexes.at(colName).at(key_to_check).empty() && table.indexes.at(colName).at(key_to_check)[0] != i) {
                        std::cout << "\033[91m[VALIDATION ERROR]\033[0m UPDATE violates PRIMARY KEY constraint for key '" << colName << "'.\n";
                        valid = false; break;
                    }
                }
            }
            if (!valid) continue;
            auto& row = table.data[i];
            for (const auto& [col, val] : assignments) {
                if (table.indexes.count(col) && row.contains(col)) {
                    auto& oldIndex = table.indexes[col][valueToIndexKey(row[col])];
                    oldIndex.erase(std::remove(oldIndex.begin(), oldIndex.end(), i), oldIndex.end());
                }
            }
            for (const auto& [col, val] : assignments) {
                setJsonValue(row, col, val);
                if (table.indexes.count(col)) table.indexes[col][valueToIndexKey(row[col])].push_back(i);
            }
            updatedCount++;
        }
        return updatedCount;
    }

    int deleteRows(const std::string& tableName, std::function<bool(const json&)> predicate) override {
        auto it = tables.find(tableName);
        if (it == tables.end()) return 0;
        auto& table = it->second;

        std::vector<size_t> toDelete;
        for (size_t i = 0; i < table.data.size(); ++i) {
            if (predicate(table.data[i])) {
                toDelete.push_back(i);
            }
        }
        if (toDelete.empty()) return 0;

        std::sort(toDelete.rbegin(), toDelete.rend());

        for (size_t idx_to_delete : toDelete) {
            size_t last_idx = table.data.size() - 1;

            if (idx_to_delete != last_idx) {
                const auto& row_to_move = table.data.back();
                for (auto& [colName, index] : table.indexes) {
                    if (row_to_move.contains(colName)) {
                        auto key = valueToIndexKey(row_to_move[colName]);
                        if (index.count(key)) {
                            auto& indices = index[key];
                            for (size_t& idx_ref : indices) {
                                if (idx_ref == last_idx) {
                                    idx_ref = idx_to_delete;
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            const auto& row_to_delete = table.data[idx_to_delete];
            for (auto& [colName, index] : table.indexes) {
                if (row_to_delete.contains(colName)) {
                    auto key = valueToIndexKey(row_to_delete[colName]);
                    if (index.count(key)) {
                        auto& indices = index[key];
                        indices.erase(std::remove(indices.begin(), indices.end(), idx_to_delete), indices.end());
                    }
                }
            }

            if (idx_to_delete != last_idx) {
                table.data[idx_to_delete] = std::move(table.data.back());
            }

            if (!table.data.empty()) {
                table.data.erase(table.data.size() - 1);
            }
        }

        return toDelete.size();
    }

    json selectRows(const std::string& tableName, const std::vector<std::string>& columns, std::function<bool(const json&)> predicate) override {
        json result;
        result["status"] = "success";
        result["data"] = json::array();
        auto it = tables.find(tableName);
        if (it == tables.end()) {
            result["status"] = "error";
            result["message"] = "Table '" + tableName + "' does not exist";
            return result;
        }
        const auto& tableData = it->second.data;
        for (const auto& row : tableData) {
            if (predicate(row)) {
                if (columns.empty() || (columns.size() == 1 && columns[0] == "*")) {
                    result["data"].push_back(row);
                } else {
                    json filteredRow;
                    for (const auto& col_name : columns) {
                        if (row.contains(col_name)) filteredRow[col_name] = row[col_name];
                        else filteredRow[col_name] = nullptr;
                    }
                    result["data"].push_back(std::move(filteredRow));
                }
            }
        }
        return result;
    }

    // ALTER TABLE operations
    bool renameTable(const std::string& oldName, const std::string& newName) override {
        auto it = tables.find(oldName);
        if (it == tables.end()) return false;
        if (tables.count(newName) > 0) return false; // новое имя уже существует

        auto node = tables.extract(it);
        node.key() = newName;
        tables.insert(std::move(node));
        return true;
    }

    bool renameColumn(const std::string& tableName, const std::string& oldColumnName, const std::string& newColumnName) override {
        auto it = tables.find(tableName);
        if (it == tables.end()) return false;

        auto& table = it->second;

        // Проверяем, существует ли старая колонка
        bool columnExists = false;
        for (auto& col : table.schema) {
            if (col.name == oldColumnName) {
                columnExists = true;
                break;
            }
        }
        if (!columnExists) return false;

        // Проверяем, что новое имя не занято
        for (const auto& col : table.schema) {
            if (col.name == newColumnName) return false;
        }

        // Проверяем валидность нового имени
        if (!validateColumnName(newColumnName, table.options)) return false;

        // Обновляем схему
        for (auto& col : table.schema) {
            if (col.name == oldColumnName) {
                col.name = newColumnName;
                break;
            }
        }

        // Обновляем данные
        for (auto& row : table.data) {
            if (row.contains(oldColumnName)) {
                row[newColumnName] = row[oldColumnName];
                row.erase(oldColumnName);
            }
        }

        // Обновляем индексы
        if (table.indexes.count(oldColumnName)) {
            auto node = table.indexes.extract(oldColumnName);
            node.key() = newColumnName;
            table.indexes.insert(std::move(node));
        }

        return true;
    }

    bool alterColumnType(const std::string& tableName, const std::string& columnName, DataType newType) override {
        auto it = tables.find(tableName);
        if (it == tables.end()) {
            std::cout << "\033[91m[ERROR]\033[0m Table '" << tableName << "' does not exist.\n";
            return false;
        }

        auto& table = it->second;

        // Находим колонку
        ColumnDef* colDef = nullptr;
        for (auto& col : table.schema) {
            if (col.name == columnName) {
                colDef = &col;
                break;
            }
        }
        if (!colDef) {
            std::cout << "\033[91m[ERROR]\033[0m Column '" << columnName << "' does not exist.\n";
            return false;
        }

        if (!table.options.allowedTypes.empty() && !table.options.allowedTypes.count(newType)) {
            std::cout << "\033[91m[ERROR]\033[0m Type '" << dataTypeToString(newType) << "' is not allowed for this table.\n";
            return false;
        }

        DataType oldType = colDef->parsedType;

        std::cout << "\033[96m[INFO]\033[0m Converting column '" << columnName << "' from "
                  << dataTypeToString(oldType) << " to " << dataTypeToString(newType) << "...\n";

        int convertedCount = 0;
        int nullCount = 0;
        int totalCount = 0;

        for (auto& row : table.data) {
            if (row.contains(columnName)) {
                totalCount++;
                json oldValue = row[columnName];

                if (oldValue.is_null()) {
                    // NULL остается NULL
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

        // Обновляем тип в схеме
        colDef->parsedType = newType;
        colDef->dataType = dataTypeToString(newType);

        std::cout << "\033[92m[SUCCESS]\033[0m Column type changed successfully!\n";
        std::cout << "\033[96m[STATS]\033[0m Total rows: " << totalCount
                  << ", Converted: " << convertedCount
                  << ", Set to NULL: " << nullCount << "\n";

        return true;
    }

    bool dropColumn(const std::string& tableName, const std::string& columnName) override {
        auto it = tables.find(tableName);
        if (it == tables.end()) {
            std::cout << "\033[91m[ERROR]\033[0m Table '" << tableName << "' does not exist.\n";
            return false;
        }

        auto& table = it->second;

        bool columnExists = false;
        for (const auto& col : table.schema) {
            if (col.name == columnName) {
                columnExists = true;
                break;
            }
        }

        if (!columnExists) {
            std::cout << "\033[91m[ERROR]\033[0m Column '" << columnName << "' does not exist.\n";
            return false;
        }

        if (table.schema.size() <= 1) {
            std::cout << "\033[91m[ERROR]\033[0m Cannot drop the last column from table.\n";
            return false;
        }

        std::cout << "\033[96m[INFO]\033[0m Dropping column '" << columnName << "' from table '" << tableName << "'...\n";

        table.schema.erase(
            std::remove_if(table.schema.begin(), table.schema.end(),
                [&columnName](const ColumnDef& col) {
                    return col.name == columnName;
                }),
            table.schema.end()
        );

        for (auto& row : table.data) {
            if (row.contains(columnName)) {
                row.erase(columnName);
            }
        }

        if (table.indexes.count(columnName)) {
            table.indexes.erase(columnName);
        }

        std::cout << "\033[92m[SUCCESS]\033[0m Column '" << columnName << "' dropped successfully!\n";
        return true;
    }

    // DROP TABLE operation
    bool dropTable(const std::string& tableName) override {
        auto it = tables.find(tableName);
        if (it == tables.end()) {
            return false;  // Таблица не существует
        }

        tables.erase(it);
        std::cout << "\033[92m[SUCCESS]\033[0m Table '" << tableName << "' dropped successfully.\n";
        return true;
    }

private:
    void performGarbageCollection(Table& table) {
        for (auto& [colName, index] : table.indexes) {
            for (auto it = index.begin(); it != index.end();) {
                if (it->second.empty()) it = index.erase(it);
                else ++it;
            }
        }
    }
};