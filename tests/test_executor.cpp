#include "test_framework.h"
#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include <nlohmann/json.hpp>

using namespace tests;
using namespace query_engine;
using json = nlohmann::json;

class TestStorageInterface : public StorageInterface {
private:
    std::map<std::string, json> tables;
    std::map<std::string, std::vector<ColumnDef>> schemas;
    std::map<std::string, TableOptions> tableOptions;

public:
    bool createTable(const std::string& tableName,
                    const std::vector<ColumnDef*>& columns,
                    const TableOptions& options = TableOptions()) override {
        schemas[tableName].clear();
        for (auto* col : columns) {
            schemas[tableName].push_back(*col);
        }
        tables[tableName] = json::array();
        tableOptions[tableName] = options;
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
        return true;
    }

    json selectRows(const std::string& tableName,
                   const std::vector<std::string>& columns,
                   std::function<bool(const json&)> predicate) override {
        json result;
        result["status"] = "success";
        result["data"] = json::array();

        if (tables.find(tableName) == tables.end()) {
            result["status"] = "error";
            result["message"] = "Table does not exist";
            return result;
        }

        for (const auto& row : tables[tableName]) {
            if (predicate(row)) {
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

        for (auto it = tableData.begin(); it != tableData.end();) {
            if (predicate(*it)) {
                it = tableData.erase(it);
                deletedCount++;
            } else {
                ++it;
            }
        }
        return deletedCount;
    }

private:
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
};

struct ExecutorTestCase {
    std::string name;
    std::vector<std::string> setup_queries;
    std::string test_query;
    std::function<bool(const json&)> validate_func;
};

bool run_executor_test_cases() {
    const std::vector<ExecutorTestCase> test_cases = {
        {
            "CREATE TABLE",
            {},
            "CREATE TABLE users (id INT, name VARCHAR)",
            [](const json& result) {
                return result["status"] == "success" && result["message"] == "Table created successfully";
            }
        },
        {
            "INSERT single row",
            {"CREATE TABLE users (id INT, name VARCHAR)"},
            "INSERT INTO users VALUES (1, 'John')",
            [](const json& result) {
                return result["status"] == "success" && result["rows_affected"] == 1;
            }
        },
        {
            "SELECT *",
            {
                "CREATE TABLE users (id INT, name VARCHAR)",
                "INSERT INTO users VALUES (1, 'John'), (2, 'Jane')"
            },
            "SELECT * FROM users",
            [](const json& result) {
                return result["status"] == "success" && result["data"].size() == 2;
            }
        },
        {
            "SELECT with WHERE",
            {
                "CREATE TABLE products (id INT, price INT)",
                "INSERT INTO products VALUES (1, 10), (2, 25), (3, 30)"
            },
            "SELECT * FROM products WHERE price > 20",
            [](const json& result) {
                return result["status"] == "success" && result["data"].size() == 2;
            }
        },
        {
            "UPDATE with WHERE",
            {
                "CREATE TABLE users (id INT, age INT)",
                "INSERT INTO users VALUES (1, 25), (2, 30)"
            },
            "UPDATE users SET age = 26 WHERE id = 1",
            [](const json& result) {
                return result["status"] == "success" && result["rows_affected"] == 1;
            }
        },
        {
            "DELETE with WHERE",
            {
                "CREATE TABLE users (id INT, name VARCHAR)",
                "INSERT INTO users VALUES (1, 'John'), (2, 'Jane')"
            },
            "DELETE FROM users WHERE name = 'John'",
            [](const json& result) {
                return result["status"] == "success" && result["rows_affected"] == 1;
            }
        },
        {
            "DELETE all rows (no WHERE)",
            {
                "CREATE TABLE users (id INT)",
                "INSERT INTO users VALUES (1), (2), (3)"
            },
            "DELETE FROM users",
            [](const json& result) {
                return result["status"] == "success" && result["rows_affected"] == 3;
            }
        },
    };

    bool all_passed = true;
    for (const auto& tc : test_cases) {
        auto storage = std::make_shared<TestStorageInterface>();
        QueryExecutor executor(storage);

        bool setup_ok = true;
        for (const auto& query : tc.setup_queries) {
            Lexer lexer(query);
            auto tokens = lexer.tokenize();
            Parser parser(tokens);
            auto ast = parser.parse();
            if (!ast || parser.hasError()) {
                std::cerr << "TC '" << tc.name << "' FAILED during setup: Invalid query '" << query << "'" << std::endl;
                setup_ok = false;
                break;
            }
            executor.execute(ast);
        }
        if (!setup_ok) {
            all_passed = false;
            continue;
        }

        Lexer lexer(tc.test_query);
        auto tokens = lexer.tokenize();
        Parser parser(tokens);
        auto ast = parser.parse();
        if (!ast || parser.hasError()) {
            std::cerr << "TC '" << tc.name << "' FAILED: Could not parse test query." << std::endl;
            all_passed = false;
            continue;
        }

        json result = executor.execute(ast);

        if (!tc.validate_func(result)) {
            std::cerr << "TC '" << tc.name << "' FAILED validation. Result was: " << result.dump(2) << std::endl;
            all_passed = false;
        }
    }
    return all_passed;
}


void addExecutorTests(TestFramework& framework) {
    framework.addTest("Executor: Data-Driven Tests", run_executor_test_cases,
                     "Run a suite of data-driven tests for the executor.");
}