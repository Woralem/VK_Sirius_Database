#include "test_framework.h"
#include "test_utils.h"
#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include "storage/optimized_in_memory_storage.h"
#include <nlohmann/json.hpp>

using namespace query_engine;
using json = nlohmann::json;

class BasicTestSuite {
private:
    std::shared_ptr<OptimizedInMemoryStorage> storage;
    std::shared_ptr<OptimizedQueryExecutor> executor;

public:
    BasicTestSuite() {
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

            if (parser.hasError()) {
                logError("Parser error in query: " + query);
                for (const auto& error : parser.getErrors()) {
                    logError("  " + error);
                }
                return false;
            }

            auto result = executor->execute(ast);
            bool success = result.contains("status") && result["status"] == "success";

            if (!success && result.contains("message")) {
                logError("Execution error: " + result["message"].get<std::string>());
            }
            return success;
        } catch (const std::exception& e) {
            logError("Exception: " + std::string(e.what()));
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
                return json{{"status", "error"}, {"message", "Parser error"}};
            }

            return executor->execute(ast);
        } catch (const std::exception& e) {
            return json{{"status", "error"}, {"message", e.what()}};
        }
    }

    int getRowCount(const std::string& query) {
        auto result = queryResult(query);
        
        if (result.contains("status") && result["status"] == "error") {
            return -1;
        }

        // Поддержка нового формата
        if (result.contains("cells") && result["cells"].is_array()) {
            return result["cells"].size();
        }

        // Поддержка старого формата
        if (result.contains("data") && result["data"].is_array()) {
            return result["data"].size();
        }

        return -1;
    }

    void reset() {
        storage = std::make_shared<OptimizedInMemoryStorage>();
        executor = std::make_shared<OptimizedQueryExecutor>(storage);
        executor->setLoggingEnabled(false);
    }
};

void addBasicOperationTests(tests::TestFramework& framework) {
    auto suite = std::make_shared<BasicTestSuite>();

    // Test 1: CREATE TABLE basic
    framework.addTest("BASIC_01_Create_Table", [suite]() {
        logTestStart("Create Table Basic");
        
        bool success = suite->executeQuery(R"(
            CREATE TABLE users (
                id INT PRIMARY KEY,
                name VARCHAR NOT NULL,
                age INT,
                active BOOLEAN
            )
        )");
        
        ASSERT_TRUE(success);
        logSuccess("Table created successfully");
        return true;
    }, "Basic CREATE TABLE functionality");

    // Test 2: INSERT basic
    framework.addTest("BASIC_02_Insert_Data", [suite]() {
        logTestStart("Insert Data Basic");
        
        bool success = suite->executeQuery("INSERT INTO users VALUES (1, 'John Doe', 25, TRUE)");
        ASSERT_TRUE(success);
        
        success = suite->executeQuery("INSERT INTO users VALUES (2, 'Jane Smith', 30, FALSE)");
        ASSERT_TRUE(success);
        
        logSuccess("Data inserted successfully");
        return true;
    }, "Basic INSERT functionality");

    // Test 3: SELECT all
    framework.addTest("BASIC_03_Select_All", [suite]() {
        logTestStart("Select All Data");
        
        int count = suite->getRowCount("SELECT * FROM users");
        ASSERT_EQ(2, count);
        
        logSuccess("SELECT * returned correct count");
        return true;
    }, "Basic SELECT * functionality");

    // Test 4: SELECT specific columns
    framework.addTest("BASIC_04_Select_Columns", [suite]() {
        logTestStart("Select Specific Columns");
        
        auto result = suite->queryResult("SELECT name, age FROM users");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        // Проверяем новый формат
        if (result.contains("header")) {
            ASSERT_EQ(2, result["header"].size());
            ASSERT_EQ("name", result["header"][0]["content"]);
            ASSERT_EQ("age", result["header"][1]["content"]);
        }
        
        logSuccess("SELECT with specific columns works");
        return true;
    }, "SELECT specific columns");

    // Test 5: UPDATE data
    framework.addTest("BASIC_05_Update_Data", [suite]() {
        logTestStart("Update Data");
        
        bool success = suite->executeQuery("UPDATE users SET age = 26 WHERE name = 'John Doe'");
        ASSERT_TRUE(success);
        
        // Verify update
        auto result = suite->queryResult("SELECT age FROM users WHERE name = 'John Doe'");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        logSuccess("UPDATE executed successfully");
        return true;
    }, "Basic UPDATE functionality");

    // Test 6: DELETE data
    framework.addTest("BASIC_06_Delete_Data", [suite]() {
        logTestStart("Delete Data");
        
        bool success = suite->executeQuery("DELETE FROM users WHERE name = 'Jane Smith'");
        ASSERT_TRUE(success);
        
        // Verify deletion
        int count = suite->getRowCount("SELECT * FROM users");
        ASSERT_EQ(1, count);
        
        logSuccess("DELETE executed successfully");
        return true;
    }, "Basic DELETE functionality");

    // Test 7: WHERE conditions
    framework.addTest("BASIC_07_Where_Conditions", [suite]() {
        logTestStart("WHERE Conditions");
        
        // Add more test data
        suite->executeQuery("INSERT INTO users VALUES (3, 'Bob Johnson', 35, TRUE)");
        suite->executeQuery("INSERT INTO users VALUES (4, 'Alice Brown', 28, FALSE)");
        
        // Test different WHERE conditions
        int count1 = suite->getRowCount("SELECT * FROM users WHERE age > 30");
        ASSERT_EQ(1, count1);
        
        int count2 = suite->getRowCount("SELECT * FROM users WHERE active = TRUE");
        ASSERT_EQ(2, count2);
        
        int count3 = suite->getRowCount("SELECT * FROM users WHERE name = 'Bob Johnson'");
        ASSERT_EQ(1, count3);
        
        logSuccess("WHERE conditions work correctly");
        return true;
    }, "WHERE clause functionality");

    // Test 8: Multiple INSERTs
    framework.addTest("BASIC_08_Multiple_Inserts", [suite]() {
        logTestStart("Multiple Inserts");
        
        suite->reset(); // Start fresh
        
        // Recreate table
        suite->executeQuery(R"(
            CREATE TABLE products (
                id INT PRIMARY KEY,
                name VARCHAR,
                price DOUBLE,
                available BOOLEAN
            )
        )");
        
        // Insert multiple records
        std::vector<std::string> inserts = {
            "INSERT INTO products VALUES (1, 'Laptop', 999.99, TRUE)",
            "INSERT INTO products VALUES (2, 'Mouse', 29.99, TRUE)", 
            "INSERT INTO products VALUES (3, 'Keyboard', 79.99, FALSE)",
            "INSERT INTO products VALUES (4, 'Monitor', 299.99, TRUE)"
        };
        
        for (const auto& insert : inserts) {
            bool success = suite->executeQuery(insert);
            ASSERT_TRUE(success);
        }
        
        int count = suite->getRowCount("SELECT * FROM products");
        ASSERT_EQ(4, count);
        
        logSuccess("Multiple inserts completed");
        return true;
    }, "Multiple INSERT operations");

    // Test 9: Complex SELECT
    framework.addTest("BASIC_09_Complex_Select", [suite]() {
        logTestStart("Complex SELECT");
        
        // Test SELECT with calculations and conditions
        auto result = suite->queryResult("SELECT name, price FROM products WHERE price > 50.0");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        int expensiveCount = suite->getRowCount("SELECT * FROM products WHERE price > 50.0");
        ASSERT_EQ(3, expensiveCount);
        
        int availableCount = suite->getRowCount("SELECT * FROM products WHERE available = TRUE");
        ASSERT_EQ(3, availableCount);
        
        logSuccess("Complex SELECT operations work");
        return true;
    }, "Complex SELECT queries");

    // Test 10: Edge cases
    framework.addTest("BASIC_10_Edge_Cases", [suite]() {
        logTestStart("Edge Cases");
        
        // Empty result set
        int emptyCount = suite->getRowCount("SELECT * FROM products WHERE price > 9999.0");
        ASSERT_EQ(0, emptyCount);
        
        // SELECT from empty table
        suite->executeQuery("CREATE TABLE empty_table (id INT)");
        int emptyTableCount = suite->getRowCount("SELECT * FROM empty_table");
        ASSERT_EQ(0, emptyTableCount);
        
        // UPDATE with no matches
        bool updateSuccess = suite->executeQuery("UPDATE products SET price = 0 WHERE name = 'NonExistent'");
        ASSERT_TRUE(updateSuccess); // Should succeed but affect 0 rows
        
        // DELETE with no matches
        bool deleteSuccess = suite->executeQuery("DELETE FROM products WHERE price < 0");
        ASSERT_TRUE(deleteSuccess); // Should succeed but affect 0 rows
        
        logSuccess("Edge cases handled correctly");
        return true;
    }, "Edge case scenarios");
}