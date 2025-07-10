#include "test_framework.h"
#include "test_utils.h"
#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include "storage/optimized_in_memory_storage.h"
#include <nlohmann/json.hpp>

using namespace query_engine;
using json = nlohmann::json;

class DataTypeTestSuite {
private:
    std::shared_ptr<OptimizedInMemoryStorage> storage;
    std::shared_ptr<OptimizedQueryExecutor> executor;

public:
    DataTypeTestSuite() {
        storage = std::make_shared<OptimizedInMemoryStorage>();
        executor = std::make_shared<OptimizedQueryExecutor>(storage);
        executor->setLoggingEnabled(false);
    }

    bool executeQuery(const std::string& query) {
        try {
            Lexer lexer(query);
            auto tokens = lexer.tokenize();
            Parser parser(tokens);
            auto ast = parser.parse();
            if (parser.hasError()) return false;
            auto result = executor->execute(ast);
            return result.contains("status") && result["status"] == "success";
        } catch (...) {
            return false;
        }
    }

    json queryResult(const std::string& query) {
        try {
            Lexer lexer(query);
            auto tokens = lexer.tokenize();
            Parser parser(tokens);
            auto ast = parser.parse();
            if (parser.hasError()) {
                return json{{"status", "error"}};
            }
            return executor->execute(ast);
        } catch (...) {
            return json{{"status", "error"}};
        }
    }

    void reset() {
        storage = std::make_shared<OptimizedInMemoryStorage>();
        executor = std::make_shared<OptimizedQueryExecutor>(storage);
        executor->setLoggingEnabled(false);
    }
};

void addDataTypeTests(tests::TestFramework& framework) {
    auto suite = std::make_shared<DataTypeTestSuite>();

    framework.addTest("DATATYPE_01_INT_Type", [suite]() {
        logTestStart("INT Data Type");
        suite->reset();

        logStep("Creating table with INT columns");
        ASSERT_TRUE(suite->executeQuery("CREATE TABLE int_test (id INT PRIMARY KEY, positive_num INT, negative_num INT, zero_num INT)"));
        logSuccess("Table created successfully");

        logStep("Inserting INT values");
        std::string insertQuery = "INSERT INTO int_test VALUES (1, 42, -15, 0)";
        logDebug("Executing query: " + insertQuery);
        auto result = suite->queryResult(insertQuery);
        logDebug("Insert result: " + result.dump());
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        logSuccess("INT values inserted successfully");

        logStep("Reading INT values back");
        auto selectResult = suite->queryResult("SELECT * FROM int_test");
        logDebug("Select result: " + selectResult.dump());
        ASSERT_TRUE(selectResult.contains("status") && selectResult["status"] == "success");
        ASSERT_TRUE(selectResult.contains("cells") && selectResult["cells"].is_array());
        ASSERT_EQ(1, selectResult["cells"].size());

        logSuccess("INT data type works correctly");
        return true;
    }, "INT data type functionality");

    framework.addTest("DATATYPE_02_DOUBLE_Type", [suite]() {
        logTestStart("DOUBLE Data Type");
        suite->reset();

        logStep("Creating table with DOUBLE columns");
        ASSERT_TRUE(suite->executeQuery("CREATE TABLE double_test (id INT PRIMARY KEY, price DOUBLE, rate DOUBLE, scientific DOUBLE)"));
        logSuccess("Table created successfully");

        logStep("Inserting DOUBLE values");
        ASSERT_TRUE(suite->executeQuery("INSERT INTO double_test VALUES (1, 99.99, 0.05, 1.23456789)"));
        ASSERT_TRUE(suite->executeQuery("INSERT INTO double_test VALUES (2, 0.0, -15.75, 3.14159265)"));
        logSuccess("DOUBLE values inserted successfully");

        logSuccess("DOUBLE data type works correctly");
        return true;
    }, "DOUBLE data type functionality");

    framework.addTest("DATATYPE_03_VARCHAR_Type", [suite]() {
        logTestStart("VARCHAR Data Type");
        suite->reset();
        ASSERT_TRUE(suite->executeQuery("CREATE TABLE varchar_test (id INT PRIMARY KEY, name VARCHAR, description VARCHAR, empty_field VARCHAR)"));

        ASSERT_TRUE(suite->executeQuery("INSERT INTO varchar_test VALUES (1, 'John Doe', 'Software Engineer', '')"));
        ASSERT_TRUE(suite->executeQuery("INSERT INTO varchar_test VALUES (2, 'Jane Smith', 'Data Scientist', 'Test')"));
        ASSERT_TRUE(suite->executeQuery("INSERT INTO varchar_test VALUES (3, 'Bob O''Connor', 'Designer & Artist', 'Special chars: @#$%')"));

        auto result = suite->queryResult("SELECT name, description FROM varchar_test");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        if (result.contains("header")) {
            for (const auto& col : result["header"]) {
                ASSERT_EQ("VARCHAR", col["type"]);
            }
        }

        logSuccess("VARCHAR data type works correctly");
        return true;
    }, "VARCHAR data type functionality");

    framework.addTest("DATATYPE_04_BOOLEAN_Type", [suite]() {
        logTestStart("BOOLEAN Data Type");
        suite->reset();
        ASSERT_TRUE(suite->executeQuery("CREATE TABLE boolean_test (id INT PRIMARY KEY, is_active BOOLEAN, is_admin BOOLEAN)"));

        ASSERT_TRUE(suite->executeQuery("INSERT INTO boolean_test VALUES (1, TRUE, FALSE)"));
        ASSERT_TRUE(suite->executeQuery("INSERT INTO boolean_test VALUES (2, FALSE, TRUE)"));

        auto result = suite->queryResult("SELECT is_active, is_admin FROM boolean_test");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        if (result.contains("header")) {
            for (const auto& col : result["header"]) {
                ASSERT_EQ("BOOLEAN", col["type"]);
            }
        }

        logSuccess("BOOLEAN data type works correctly");
        return true;
    }, "BOOLEAN data type functionality");

    framework.addTest("DATATYPE_05_NULL_Values", [suite]() {
        logTestStart("NULL Values");
        suite->reset();
        ASSERT_TRUE(suite->executeQuery("CREATE TABLE null_test (id INT PRIMARY KEY, optional_name VARCHAR, optional_age INT)"));

        ASSERT_TRUE(suite->executeQuery("INSERT INTO null_test (id, optional_name) VALUES (1, 'John')"));
        ASSERT_TRUE(suite->executeQuery("INSERT INTO null_test (id, optional_age) VALUES (2, 25)"));

        auto result = suite->queryResult("SELECT * FROM null_test");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");

        logSuccess("NULL values handled correctly");
        return true;
    }, "NULL value handling");

    framework.addTest("DATATYPE_06_Mixed_Types", [suite]() {
        logTestStart("Mixed Data Types");
        suite->reset();
        ASSERT_TRUE(suite->executeQuery("CREATE TABLE mixed_types (id INT PRIMARY KEY, name VARCHAR NOT NULL, score DOUBLE, is_winner BOOLEAN, attempts INT)"));

        ASSERT_TRUE(suite->executeQuery("INSERT INTO mixed_types VALUES (1, 'Player One', 95.5, TRUE, 3)"));
        ASSERT_TRUE(suite->executeQuery("INSERT INTO mixed_types VALUES (2, 'Player Two', 87.2, FALSE, 5)"));

        auto result = suite->queryResult("SELECT * FROM mixed_types");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        if (result.contains("header")) {
            ASSERT_EQ("INT", result["header"][0]["type"]);
            ASSERT_EQ("VARCHAR", result["header"][1]["type"]);
            ASSERT_EQ("DOUBLE", result["header"][2]["type"]);
            ASSERT_EQ("BOOLEAN", result["header"][3]["type"]);
            ASSERT_EQ("INT", result["header"][4]["type"]);
        }

        ASSERT_TRUE(suite->queryResult("SELECT * FROM mixed_types WHERE attempts > 2")["status"] == "success");
        ASSERT_TRUE(suite->queryResult("SELECT * FROM mixed_types WHERE score > 90.0")["status"] == "success");
        ASSERT_TRUE(suite->queryResult("SELECT * FROM mixed_types WHERE is_winner = TRUE")["status"] == "success");
        ASSERT_TRUE(suite->queryResult("SELECT * FROM mixed_types WHERE name = 'Player One'")["status"] == "success");

        logSuccess("Mixed data types work correctly");
        return true;
    }, "Mixed data types in single table");
}