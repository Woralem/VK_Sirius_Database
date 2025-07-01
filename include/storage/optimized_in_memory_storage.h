#pragma once

#include "query_engine/executor.h"
#include <nlohmann/json.hpp>
#include <unordered_map>
#include <string>
#include <vector>
#include <functional>
#include <algorithm>

using json = nlohmann::json;
using namespace query_engine;

class OptimizedInMemoryStorage : public StorageInterface {
private:
    struct Table {
        json data = json::array();
        std::vector<ColumnDef> schema;
        std::unordered_map<std::string, std::unordered_map<std::string, std::vector<size_t>>> indexes;
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

public:
    bool createTable(const std::string& tableName,
                    const std::vector<ColumnDef*>& columns) override {
        if (tables.count(tableName)) {
            return false;
        }

        auto& table = tables[tableName];
        table.schema.reserve(columns.size());

        for (auto* col : columns) {
            table.schema.push_back(*col);
            table.indexes[col->name];
        }

        return true;
    }

    bool insertRow(const std::string& tableName,
                  const std::vector<std::string>& columns,
                  const std::vector<Value>& values) override {
        auto it = tables.find(tableName);
        if (it == tables.end()) {
            return false;
        }

        auto& table = it->second;
        json row;

        if (columns.empty()) {
            if (values.size() != table.schema.size()) {
                return false;
            }
            for (size_t i = 0; i < values.size(); i++) {
                const auto& colName = table.schema[i].name;
                setJsonValue(row, colName, values[i]);
            }
        } else {
            if (columns.size() != values.size()) {
                return false;
            }
            for (size_t i = 0; i < columns.size(); i++) {
                setJsonValue(row, columns[i], values[i]);
            }
        }

        size_t rowIndex = table.data.size();
        for (auto& [colName, index] : table.indexes) {
            if (row.contains(colName)) {
                std::string key = valueToIndexKey(row[colName]);
                index[key].push_back(rowIndex);
            }
        }

        table.data.push_back(std::move(row));
        return true;
    }

    json selectRows(const std::string& tableName,
                   const std::vector<std::string>& columns,
                   std::function<bool(const json&)> predicate) override {
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
        std::vector<size_t> selectedIndices;
        selectedIndices.reserve(tableData.size() / 10); // Assume ~10% selectivity

        for (size_t i = 0; i < tableData.size(); ++i) {
            if (predicate(tableData[i])) {
                selectedIndices.push_back(i);
            }
        }

        auto& resultData = result["data"];
        resultData.get_ref<json::array_t&>().reserve(selectedIndices.size());

        for (size_t idx : selectedIndices) {
            const auto& row = tableData[idx];
            if (columns.empty()) {
                resultData.push_back(row);
            } else {
                json filteredRow;
                for (const auto& col_name : columns) {
                    if (row.contains(col_name)) {
                        filteredRow[col_name] = row[col_name];
                    } else {
                        filteredRow[col_name] = nullptr;
                    }
                }
                resultData.push_back(std::move(filteredRow));
            }
        }

        return result;
    }

    int updateRows(const std::string& tableName,
                  const std::vector<std::pair<std::string, Value>>& assignments,
                  std::function<bool(const json&)> predicate) override {
        auto it = tables.find(tableName);
        if (it == tables.end()) {
            return 0;
        }

        auto& table = it->second;
        int updatedCount = 0;

        for (size_t i = 0; i < table.data.size(); ++i) {
            auto& row = table.data[i];
            if (predicate(row)) {
                for (const auto& [col, val] : assignments) {
                    if (table.indexes.count(col) && row.contains(col)) {
                        std::string oldKey = valueToIndexKey(row[col]);
                        auto& oldIndex = table.indexes[col][oldKey];
                        oldIndex.erase(std::remove(oldIndex.begin(), oldIndex.end(), i), oldIndex.end());
                    }
                }

                // Update values
                for (const auto& [col, val] : assignments) {
                    setJsonValue(row, col, val);
                }

                // Add to new indexes
                for (const auto& [col, val] : assignments) {
                    if (table.indexes.count(col)) {
                        std::string newKey = valueToIndexKey(row[col]);
                        table.indexes[col][newKey].push_back(i);
                    }
                }

                updatedCount++;
            }
        }

        return updatedCount;
    }

    int deleteRows(const std::string& tableName,
                  std::function<bool(const json&)> predicate) override {
        auto it = tables.find(tableName);
        if (it == tables.end()) {
            return 0;
        }

        auto& table = it->second;
        std::vector<size_t> toDelete;
        toDelete.reserve(table.data.size() / 10);

        for (size_t i = 0; i < table.data.size(); ++i) {
            if (predicate(table.data[i])) {
                toDelete.push_back(i);
            }
        }

        std::sort(toDelete.begin(), toDelete.end(), std::greater<size_t>());

        for (size_t idx : toDelete) {
            const auto& row = table.data[idx];

            for (auto& [colName, index] : table.indexes) {
                if (row.contains(colName)) {
                    std::string key = valueToIndexKey(row[colName]);
                    auto& idxList = index[key];
                    idxList.erase(std::remove(idxList.begin(), idxList.end(), idx), idxList.end());
                }
            }

            table.data.erase(table.data.begin() + idx);

            for (auto& [colName, index] : table.indexes) {
                for (auto& [val, indices] : index) {
                    for (auto& rowIdx : indices) {
                        if (rowIdx > idx) {
                            rowIdx--;
                        }
                    }
                }
            }
        }

        return toDelete.size();
    }
};