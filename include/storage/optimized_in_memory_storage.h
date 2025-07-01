#pragma once

#include "query_engine/executor.h"
#include <nlohmann/json.hpp>
#include <map>
#include <string>
#include <vector>
#include <functional>
#include <algorithm>

using json = nlohmann::json;
using namespace query_engine;

// Полноценная реализация хранилища в оперативной памяти.
// Данные будут храниться, пока сервер запущен. P.S. Сделано пока Глеб не сделает Storage layer
class InMemoryStorage : public StorageInterface {
private:
    std::map<std::string, json> tables;
    std::map<std::string, std::vector<ColumnDef>> schemas;

    void setJsonValue(json& row, const std::string& key, const Value& value) {
        if (std::holds_alternative<int>(value)) {
            row[key] = std::get<int>(value);
        } else if (std::holds_alternative<double>(value)) {
            row[key] = std::get<double>(value);
        } else if (std::holds_alternative<std::string>(value)) {
            row[key] = std::get<std::string>(value);
        } else {
            row[key] = nullptr;
        }
    }

public:
    bool createTable(const std::string& tableName,
                    const std::vector<ColumnDef*>& columns) override {
        if (schemas.count(tableName)) {
            return false;
        }
        schemas[tableName].clear();
        for (auto* col : columns) {
            schemas[tableName].push_back(*col);
        }
        tables[tableName] = json::array();
        return true;
    }

    bool insertRow(const std::string& tableName,
                  const std::vector<std::string>& columns,
                  const std::vector<Value>& values) override {
        if (tables.find(tableName) == tables.end()) {
            return false;
        }

        json row;
        if (columns.empty()) {
            const auto& schema_cols = schemas[tableName];
            if (values.size() != schema_cols.size()) {
                 return false;
            }
            for (size_t i = 0; i < values.size(); i++) {
                setJsonValue(row, schema_cols[i].name, values[i]);
            }
        } else {
            if (columns.size() != values.size()) {
                return false;
            }
            for (size_t i = 0; i < columns.size(); i++) {
                setJsonValue(row, columns[i], values[i]);
            }
        }

        tables[tableName].push_back(row);
        return true;
    }

    json selectRows(const std::string& tableName,
                   const std::vector<std::string>& columns,
                   std::function<bool(const json&)> predicate) override {
        json result;
        if (tables.find(tableName) == tables.end()) {
            result["status"] = "error";
            result["message"] = "Table '" + tableName + "' does not exist";
            return result;
        }

        result["status"] = "success";
        result["data"] = json::array();

        for (const auto& row : tables[tableName]) {
            if (predicate(row)) {
                // Если SELECT *
                if (columns.empty()) {
                    result["data"].push_back(row);
                } else { // Если выбраны конкретные колонки
                    json filteredRow;
                    for (const auto& col_name : columns) {
                        if (row.contains(col_name)) {
                            filteredRow[col_name] = row[col_name];
                        } else {
                            filteredRow[col_name] = nullptr; // колонка есть в запросе, но нет в строке
                        }
                    }
                    result["data"].push_back(filteredRow);
                }
            }
        }
        return result;
    }

    int updateRows(const std::string& tableName,
                  const std::vector<std::pair<std::string, Value>>& assignments,
                  std::function<bool(const json&)> predicate) override {
        if (tables.find(tableName) == tables.end()) {
            return 0;
        }
        int updatedCount = 0;
        for (auto& row : tables[tableName]) {
            if (predicate(row)) {
                for (const auto& [col, val] : assignments) {
                    setJsonValue(row, col, val);
                }
                updatedCount++;
            }
        }
        return updatedCount;
    }

    int deleteRows(const std::string& tableName,
                  std::function<bool(const json&)> predicate) override {
        if (tables.find(tableName) == tables.end()) {
            return 0;
        }
        int deletedCount = 0;
        auto& tableData = tables[tableName];
        auto it = std::remove_if(tableData.begin(), tableData.end(), 
            [&](const json& row) {
                if (predicate(row)) {
                    deletedCount++;
                    return true;
                }
                return false;
            });
        tableData.erase(it, tableData.end());
        return deletedCount;
    }
};