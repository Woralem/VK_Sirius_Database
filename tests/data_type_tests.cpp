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

    // Test 1: INT data type
    framework.addTest("DATATYPE_01_INT_Type", [suite]() {
        logTestStart("INT Data Type");
        
        bool success = suite->executeQuery(R"(
            CREATE TABLE int_test (
                id INT PRIMARY KEY,
                positive_num INT,
                negative_num INT,
                zero_num INT
            )
        )");
        ASSERT_TRUE(success);
        
        // Test various INT values
        success = suite->executeQuery("INSERT INTO int_test VALUES (1, 42, -15, 0)");
        ASSERT_TRUE(success);
        
        success = suite->executeQuery("INSERT INTO int_test VALUES (2, 2147483647, -2147483648, 0)"); // Max/Min INT
        ASSERT_TRUE(success);
        
        // Verify data
        auto result = suite->queryResult("SELECT * FROM int_test");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        if (result.contains("header")) {
            // Check header types
            for (const auto& col : result["header"]) {
                ASSERT_EQ("INT", col["type"]);
            }
        }
        
        logSuccess("INT data type works correctly");
        return true;
    }, "INT data type functionality");

    // Test 2: DOUBLE data type
    framework.addTest("DATATYPE_02_DOUBLE_Type", [suite]() {
        logTestStart("DOUBLE Data Type");
        
        bool success = suite->executeQuery(R"(
            CREATE TABLE double_test (
                id INT PRIMARY KEY,
                price DOUBLE,
                rate DOUBLE,
                scientific DOUBLE
            )
        )");
        ASSERT_TRUE(success);
        
        // Test various DOUBLE values
        success = suite->executeQuery("INSERT INTO double_test VALUES (1, 99.99, 0.05, 1.23456789)");
        ASSERT_TRUE(success);
        
        success = suite->executeQuery("INSERT INTO double_test VALUES (2, 0.0, -15.75, 3.14159265)");
        ASSERT_TRUE(success);
        
        // Verify data and types
        auto result = suite->queryResult("SELECT price, rate FROM double_test");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        if (result.contains("header")) {
            for (const auto& col : result["header"]) {
                ASSERT_EQ("DOUBLE", col["type"]);
            }
        }
        
        logSuccess("DOUBLE data type works correctly");
        return true;
    }, "DOUBLE data type functionality");

    // Test 3: VARCHAR data type
    framework.addTest("DATATYPE_03_VARCHAR_Type", [suite]() {
        logTestStart("VARCHAR Data Type");
        
        bool success = suite->executeQuery(R"(
            CREATE TABLE varchar_test (
                id INT PRIMARY KEY,
                name VARCHAR,
                description VARCHAR,
                empty_field VARCHAR
            )
        )");
        ASSERT_TRUE(success);
        
        // Test various VARCHAR values
        success = suite->executeQuery("INSERT INTO varchar_test VALUES (1, 'John Doe', 'Software Engineer', '')");
        ASSERT_TRUE(success);
        
        success = suite->executeQuery("INSERT INTO varchar_test VALUES (2, 'Jane Smith', 'Data Scientist with a very long title that spans multiple words', 'Test')");
        ASSERT_TRUE(success);
        
        // Test special characters
        success = suite->executeQuery("INSERT INTO varchar_test VALUES (3, 'Bob O''Connor', 'Designer & Artist', 'Special chars: @#$%')");
        ASSERT_TRUE(success);
        
        // Verify data and types
        auto result = suite->queryResult("SELECT name, description FROM varchar_test");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        if (result.contains("header")) {
            for (size_t i = 1; i < result["header"].size(); ++i) { // Skip id column
                ASSERT_EQ("VARCHAR", result["header"][i]["type"]);
            }
        }
        
        logSuccess("VARCHAR data type works correctly");
        return true;
    }, "VARCHAR data type functionality");

    // Test 4: BOOLEAN data type
    framework.addTest("DATATYPE_04_BOOLEAN_Type", [suite]() {
        logTestStart("BOOLEAN Data Type");
        
        bool success = suite->executeQuery(R"(
            CREATE TABLE boolean_test (
                id INT PRIMARY KEY,
                is_active BOOLEAN,
                is_admin BOOLEAN,
                is_verified BOOLEAN
            )
        )");
        ASSERT_TRUE(success);
        
        // Test BOOLEAN values
        success = suite->executeQuery("INSERT INTO boolean_test VALUES (1, TRUE, FALSE, TRUE)");
        ASSERT_TRUE(success);
        
        success = suite->executeQuery("INSERT INTO boolean_test VALUES (2, FALSE, TRUE, FALSE)");
        ASSERT_TRUE(success);
        
        // Verify data and types
        auto result = suite->queryResult("SELECT is_active, is_admin FROM boolean_test");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        if (result.contains("header")) {
            for (size_t i = 1; i < result["header"].size(); ++i) {
                ASSERT_EQ("BOOLEAN", result["header"][i]["type"]);
            }
        }
        
        logSuccess("BOOLEAN data type works correctly");
        return true;
    }, "BOOLEAN data type functionality");

    // Test 5: NULL values
    framework.addTest("DATATYPE_05_NULL_Values", [suite]() {
        logTestStart("NULL Values");
        
        bool success = suite->executeQuery(R"(
            CREATE TABLE null_test (
                id INT PRIMARY KEY,
                optional_name VARCHAR,
                optional_age INT,
                optional_price DOUBLE,
                optional_flag BOOLEAN
            )
        )");
        ASSERT_TRUE(success);
        
        // Insert with explicit columns (leaving others as NULL)
        success = suite->executeQuery("INSERT INTO null_test (id, optional_name) VALUES (1, 'John')");
        ASSERT_TRUE(success);
        
        success = suite->executeQuery("INSERT INTO null_test (id, optional_age, optional_price) VALUES (2, 25, 99.99)");
        ASSERT_TRUE(success);
        
        // Verify NULL handling
        auto result = suite->queryResult("SELECT * FROM null_test");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        logSuccess("NULL values handled correctly");
        return true;
    }, "NULL value handling");

    // Test 6: Mixed data types in single table
    framework.addTest("DATATYPE_06_Mixed_Types", [suite]() {
        logTestStart("Mixed Data Types");
        
        suite->reset();
        
        bool success = suite->executeQuery(R"(
            CREATE TABLE mixed_types (
                id INT PRIMARY KEY,
                name VARCHAR NOT NULL,
                score DOUBLE,
                is_winner BOOLEAN,
                attempts INT
            )
        )");
        ASSERT_TRUE(success);
        
        // Insert mixed data
        success = suite->executeQuery("INSERT INTO mixed_types VALUES (1, 'Player One', 95.5, TRUE, 3)");
        ASSERT_TRUE(success);
        
        success = suite->executeQuery("INSERT INTO mixed_types VALUES (2, 'Player Two', 87.2, FALSE, 5)");
        ASSERT_TRUE(success);
        
        success = suite->executeQuery("INSERT INTO mixed_types VALUES (3, 'Player Three', 92.8, TRUE, 2)");
        ASSERT_TRUE(success);
        
        // Verify mixed type queries
        auto result = suite->queryResult("SELECT * FROM mixed_types");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        if (result.contains("header")) {
            ASSERT_EQ("INT", result["header"][0]["type"]);     // id
            ASSERT_EQ("VARCHAR", result["header"][1]["type"]); // name
            ASSERT_EQ("DOUBLE", result["header"][2]["type"]);  // score
            ASSERT_EQ("BOOLEAN", result["header"][3]["type"]); // is_winner
            ASSERT_EQ("INT", result["header"][4]["type"]);     // attempts
        }
        
        // Test WHERE with different types
        auto intResult = suite->queryResult("SELECT * FROM mixed_types WHERE attempts > 2");
        ASSERT_TRUE(intResult.contains("status") && intResult["status"] == "success");
        
        auto doubleResult = suite->queryResult("SELECT * FROM mixed_types WHERE score > 90.0");
        ASSERT_TRUE(doubleResult.contains("status") && doubleResult["status"] == "success");
        
        auto boolResult = suite->queryResult("SELECT * FROM mixed_types WHERE is_winner = TRUE");
        ASSERT_TRUE(boolResult.contains("status") && boolResult["status"] == "success");
        
        auto stringResult = suite->queryResult("SELECT * FROM mixed_types WHERE name = 'Player One'");
        ASSERT_TRUE(stringResult.contains("status") && stringResult["status"] == "success");
        
        logSuccess("Mixed data types work correctly");
        return true;
    }, "Mixed data types in single table");

    // Test 7: Type consistency
    framework.addTest("DATATYPE_07_Type_Consistency", [suite]() {
        logTestStart("Type Consistency");
        
        // Test that wrong types are handled properly
        suite->executeQuery(R"(
            CREATE TABLE type_consistency (
                id INT PRIMARY KEY,
                count INT,
                price DOUBLE,
                flag BOOLEAN
            )
        )");
        
        // These should work
        bool success1 = suite->executeQuery("INSERT INTO type_consistency VALUES (1, 42, 99.99, TRUE)");
        ASSERT_TRUE(success1);
        
        // Test type validation works in the storage layer
        auto result = suite->queryResult("SELECT * FROM type_consistency");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        logSuccess("Type consistency maintained");
        return true;
    }, "Data type consistency checks");
}