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
#include <format>
#include <concepts>
#include <ranges>

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

    void setJsonValue(json& row, std::string_view key, const Value& value);
    [[nodiscard]] std::string valueToIndexKey(const json& value) const;

    // Объявления методов без реализации
    [[nodiscard]] bool validateColumnName(std::string_view name, const TableOptions& options) const;
    [[nodiscard]] const ColumnDef* getColumnDef(const Table& table, std::string_view colName) const;
    [[nodiscard]] bool validateValueForColumn(const Value& value, const ColumnDef& colDef) const;
    [[nodiscard]] bool isValidInteger(std::string_view str) const;
    [[nodiscard]] bool isValidDouble(std::string_view str) const;
    [[nodiscard]] bool isValidBoolean(std::string_view str) const;
    [[nodiscard]] bool stringToBoolean(std::string_view str) const;
    [[nodiscard]] json convertValue(const json& value, DataType fromType, DataType toType) const;
    [[nodiscard]] DataType jsonTypeToDataType(const json& value) const;

public:
    [[nodiscard]] bool createTable(const std::string& tableName, const std::vector<ColumnDef*>& columns, const TableOptions& options = TableOptions()) override;
    [[nodiscard]] bool insertRow(const std::string& tableName, const std::vector<std::string>& columns, const std::vector<Value>& values) override;
    [[nodiscard]] int updateRows(const std::string& tableName, const std::vector<std::pair<std::string, Value>>& assignments, std::function<bool(const json&)> predicate) override;
    [[nodiscard]] int deleteRows(const std::string& tableName, std::function<bool(const json&)> predicate) override;
    [[nodiscard]] json selectRows(const std::string& tableName, const std::vector<std::string>& columns, std::function<bool(const json&)> predicate) override;

    // ALTER TABLE operations
    [[nodiscard]] bool renameTable(const std::string& oldName, const std::string& newName) override;
    [[nodiscard]] bool renameColumn(const std::string& tableName, const std::string& oldColumnName, const std::string& newColumnName) override;
    [[nodiscard]] bool alterColumnType(const std::string& tableName, const std::string& columnName, DataType newType) override;
    [[nodiscard]] bool dropColumn(const std::string& tableName, const std::string& columnName) override;
    [[nodiscard]] bool addColumn(const std::string& tableName, const ColumnDef* columnDef) override;

    // DROP TABLE operation
    [[nodiscard]] bool dropTable(const std::string& tableName) override;

private:
    void performGarbageCollection(Table& table);
};