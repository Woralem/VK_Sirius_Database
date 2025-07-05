#include "test_framework.h"
#include "test_utils.h"
#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include "storage/optimized_in_memory_storage.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace query_engine;
using json = nlohmann::json;

class DropTestSuite {
private:
    std::shared_ptr<OptimizedInMemoryStorage> storage;
    std::shared_ptr<OptimizedQueryExecutor> executor;

public:
    DropTestSuite() {
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
                std::cout << "Parser error in query: " << query << "\n";
                for (const auto& error : parser.getErrors()) {
                    std::cout << "  Error: " << error << "\n";
                }
                return false;
            }

            auto result = executor->execute(ast);
            bool success = result.contains("status") && result["status"] == "success";

            if (!success && result.contains("message")) {
                std::cout << "Execution error: \"" << result["message"] << "\"\n";
            }
            return success;
        } catch (const std::exception& e) {
            std::cout << "Exception in query: " << e.what() << "\n";
            return false;
        } catch (...) {
            std::cout << "Unknown exception in query\n";
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
                return json{{"status", "error"}, {"data", json::array()}};
            }

            return executor->execute(ast);
        } catch (...) {
            return json{{"status", "error"}, {"data", json::array()}};
        }
    }

    int getRowCount(const std::string& query) {
        auto result = queryResult(query);

        // Сначала проверяем статус - если ошибка, возвращаем -1
        if (result.contains("status") && result["status"] == "error") {
            return -1;
        }

        // Если успех, проверяем данные
        if (result.contains("data") && result["data"].is_array()) {
            return result["data"].size();
        }

        return -1;
    }

    bool tableExists(const std::string& tableName) {
        auto result = queryResult("SELECT * FROM " + tableName);
        return result.contains("status") && result["status"] == "success";
    }

    bool setupTestData() {
        // Создаем несколько таблиц для тестирования
        std::vector<std::string> createQueries = {
            R"(CREATE TABLE users (
                id INT PRIMARY KEY,
                name VARCHAR NOT NULL,
                email VARCHAR,
                phone VARCHAR,
                age INT,
                salary DOUBLE,
                active BOOLEAN
            ))",
            R"(CREATE TABLE products (
                id INT PRIMARY KEY,
                name VARCHAR NOT NULL,
                price DOUBLE,
                category VARCHAR
            ))",
            R"(CREATE TABLE orders (
                id INT PRIMARY KEY,
                user_id INT,
                product_id INT,
                quantity INT,
                total DOUBLE
            ))",
            R"(CREATE TABLE single_column_table (
                id INT PRIMARY KEY
            ))"
        };

        for (const auto& query : createQueries) {
            if (!executeQuery(query)) {
                return false;
            }
        }

        // Добавляем тестовые данные в users
        std::vector<std::string> insertQueries = {
            "INSERT INTO users VALUES (1, 'John Smith', 'john@gmail.com', '555-1234', 25, 50000.0, TRUE)",
            "INSERT INTO users VALUES (2, 'Jane Doe', 'jane@yahoo.com', '555-5678', 30, 65000.0, TRUE)",
            "INSERT INTO users VALUES (3, 'Bob Johnson', 'bob@gmail.com', '555-9012', 28, 55000.0, FALSE)"
        };

        for (const auto& query : insertQueries) {
            if (!executeQuery(query)) {
                return false;
            }
        }

        // Добавляем данные в products
        std::vector<std::string> productInserts = {
            "INSERT INTO products VALUES (1, 'Laptop', 999.99, 'Electronics')",
            "INSERT INTO products VALUES (2, 'Mouse', 29.99, 'Electronics')",
            "INSERT INTO products VALUES (3, 'Book', 19.99, 'Education')"
        };

        for (const auto& query : productInserts) {
            if (!executeQuery(query)) {
                return false;
            }
        }

        return true;
    }
};

void addDropTableTests(tests::TestFramework& framework) {
    auto suite = std::make_shared<DropTestSuite>();

    // Setup test data
    framework.addTest("DROP_TABLE_00_Setup", [suite]() {
        return suite->setupTestData();
    }, "Setup test data for DROP TABLE operations");

    // Test 1: Basic DROP TABLE
    framework.addTest("DROP_TABLE_01_Basic_Drop", [suite]() {
        // Verify orders table exists first
        ASSERT_TRUE(suite->tableExists("orders"));

        // Drop orders table
        bool success = suite->executeQuery("DROP TABLE orders");
        ASSERT_TRUE(success);

        // Verify table is gone
        ASSERT_FALSE(suite->tableExists("orders"));

        return true;
    }, "Test basic DROP TABLE functionality");

    // Test 2: DROP TABLE with data
    framework.addTest("DROP_TABLE_02_Drop_With_Data", [suite]() {
        // Verify products table has data
        int initialCount = suite->getRowCount("SELECT * FROM products");
        ASSERT_EQ(3, initialCount);

        // Drop products table
        bool success = suite->executeQuery("DROP TABLE products");
        ASSERT_TRUE(success);

        // Verify table is gone
        ASSERT_FALSE(suite->tableExists("products"));

        return true;
    }, "Test DROP TABLE with existing data");

    // Test 3: DROP TABLE IF EXISTS - existing table
    framework.addTest("DROP_TABLE_03_IF_EXISTS_True", [suite]() {
        // Verify users table exists first
        ASSERT_TRUE(suite->tableExists("users"));

        // Drop users table with IF EXISTS
        bool success = suite->executeQuery("DROP TABLE IF EXISTS users");
        ASSERT_TRUE(success);

        // Verify table is gone
        ASSERT_FALSE(suite->tableExists("users"));

        return true;
    }, "Test DROP TABLE IF EXISTS for existing table");

    // Test 4: DROP TABLE IF EXISTS - non-existing table
    framework.addTest("DROP_TABLE_04_IF_EXISTS_False", [suite]() {
        // Try to drop non-existing table with IF EXISTS (should succeed without error)
        bool success = suite->executeQuery("DROP TABLE IF EXISTS nonexistent_table");
        ASSERT_TRUE(success);

        return true;
    }, "Test DROP TABLE IF EXISTS for non-existing table");

    // Test 5: DROP TABLE without IF EXISTS - non-existing table
    framework.addTest("DROP_TABLE_05_Drop_Nonexistent", [suite]() {
        // Try to drop non-existing table without IF EXISTS (should fail)
        bool success = suite->executeQuery("DROP TABLE another_nonexistent_table");
        ASSERT_FALSE(success);

        return true;
    }, "Test DROP TABLE failure for non-existing table");

    // Test 6: Verify remaining table still works
    framework.addTest("DROP_TABLE_06_Verify_Remaining", [suite]() {
        // single_column_table should still exist and work
        ASSERT_TRUE(suite->tableExists("single_column_table"));

        int count = suite->getRowCount("SELECT * FROM single_column_table");
        ASSERT_EQ(0, count); // Empty but should work

        // Should be able to insert
        bool success = suite->executeQuery("INSERT INTO single_column_table VALUES (1)");
        ASSERT_TRUE(success);

        // Verify insertion
        int newCount = suite->getRowCount("SELECT * FROM single_column_table");
        ASSERT_EQ(1, newCount);

        return true;
    }, "Verify remaining tables still function after drops");
}

void addDropColumnTests(tests::TestFramework& framework) {
    auto suite = std::make_shared<DropTestSuite>();

    // Setup for column tests
    framework.addTest("DROP_COLUMN_00_Setup", [suite]() {
        // Create a test table with multiple columns
        bool success = suite->executeQuery(R"(
            CREATE TABLE test_table (
                id INT PRIMARY KEY,
                name VARCHAR NOT NULL,
                email VARCHAR,
                phone VARCHAR,
                age INT,
                salary DOUBLE,
                active BOOLEAN,
                description VARCHAR
            )
        )");
        ASSERT_TRUE(success);

        // Insert test data
        std::vector<std::string> inserts = {
            "INSERT INTO test_table VALUES (1, 'John', 'john@test.com', '555-1234', 25, 50000.0, TRUE, 'Manager')",
            "INSERT INTO test_table VALUES (2, 'Jane', 'jane@test.com', '555-5678', 30, 65000.0, FALSE, 'Developer')",
            "INSERT INTO test_table VALUES (3, 'Bob', 'bob@test.com', '555-9012', 28, 55000.0, TRUE, 'Designer')"
        };

        for (const auto& insert : inserts) {
            if (!suite->executeQuery(insert)) {
                return false;
            }
        }

        // Verify initial data
        int count = suite->getRowCount("SELECT * FROM test_table");
        ASSERT_EQ(3, count);

        return true;
    }, "Setup test table for DROP COLUMN operations");

    // Test 7: Basic DROP COLUMN
    framework.addTest("DROP_COLUMN_01_Basic_Drop", [suite]() {
        // Drop description column
        bool success = suite->executeQuery("ALTER TABLE test_table DROP COLUMN description");
        ASSERT_TRUE(success);

        // Verify data is still there
        int count = suite->getRowCount("SELECT * FROM test_table");
        ASSERT_EQ(3, count);

        // Verify we can still select other columns
        int nameCount = suite->getRowCount("SELECT name FROM test_table");
        ASSERT_EQ(3, nameCount);

        return true;
    }, "Test basic DROP COLUMN functionality");

    // Test 8: DROP COLUMN and verify remaining columns work
    framework.addTest("DROP_COLUMN_02_Verify_Remaining", [suite]() {
        // Drop phone column
        bool success = suite->executeQuery("ALTER TABLE test_table DROP COLUMN phone");
        ASSERT_TRUE(success);

        // Verify we can still select other columns
        int count = suite->getRowCount("SELECT name, email, age FROM test_table");
        ASSERT_EQ(3, count);

        // Verify we can still insert (without dropped columns)
        success = suite->executeQuery("INSERT INTO test_table (id, name, email, age, salary, active) VALUES (4, 'Alice', 'alice@test.com', 35, 70000.0, TRUE)");
        ASSERT_TRUE(success);

        int newCount = suite->getRowCount("SELECT * FROM test_table");
        ASSERT_EQ(4, newCount);

        return true;
    }, "Verify remaining columns work after DROP COLUMN");

    // Test 9: DROP COLUMN - non-existing column
    framework.addTest("DROP_COLUMN_03_Nonexistent_Column", [suite]() {
        // Try to drop non-existing column
        bool success = suite->executeQuery("ALTER TABLE test_table DROP COLUMN nonexistent_column");
        ASSERT_FALSE(success);

        return true;
    }, "Test DROP COLUMN failure for non-existing column");

    // Test 10: DROP COLUMN - table doesn't exist
    framework.addTest("DROP_COLUMN_04_Nonexistent_Table", [suite]() {
        // Try to drop column from non-existing table
        bool success = suite->executeQuery("ALTER TABLE nonexistent_table DROP COLUMN some_column");
        ASSERT_FALSE(success);

        return true;
    }, "Test DROP COLUMN failure for non-existing table");

    // Test 11: DROP COLUMN - attempt to drop last column
    framework.addTest("DROP_COLUMN_05_Last_Column_Protection", [suite]() {
        // Create table with single column
        bool success = suite->executeQuery("CREATE TABLE single_col (id INT)");
        ASSERT_TRUE(success);

        // Try to drop the only column (should fail)
        success = suite->executeQuery("ALTER TABLE single_col DROP COLUMN id");
        ASSERT_FALSE(success);

        return true;
    }, "Test protection against dropping the last column");

    // Test 12: Multiple DROP COLUMN operations
    framework.addTest("DROP_COLUMN_06_Multiple_Drops", [suite]() {
        // Drop multiple columns one by one
        bool success1 = suite->executeQuery("ALTER TABLE test_table DROP COLUMN salary");
        ASSERT_TRUE(success1);

        bool success2 = suite->executeQuery("ALTER TABLE test_table DROP COLUMN active");
        ASSERT_TRUE(success2);

        // Verify table still works with remaining columns
        int count = suite->getRowCount("SELECT id, name, email, age FROM test_table");
        ASSERT_EQ(4, count); // Should have 4 rows from previous tests

        return true;
    }, "Test multiple DROP COLUMN operations");

    // Test 13: DROP COLUMN with UPDATE/SELECT operations
    framework.addTest("DROP_COLUMN_07_Operations_After_Drop", [suite]() {
        // Update remaining data
        bool success = suite->executeQuery("UPDATE test_table SET age = 40 WHERE name = 'John'");
        ASSERT_TRUE(success);

        // Verify update worked
        auto result = suite->queryResult("SELECT age FROM test_table WHERE name = 'John'");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        ASSERT_TRUE(result.contains("data") && result["data"].is_array() && !result["data"].empty());

        // Delete a row
        success = suite->executeQuery("DELETE FROM test_table WHERE name = 'Bob'");
        ASSERT_TRUE(success);

        // Verify deletion
        int count = suite->getRowCount("SELECT * FROM test_table");
        ASSERT_EQ(3, count); // Should be 3 rows now

        return true;
    }, "Test table operations after DROP COLUMN");

    // Test 14: DROP COLUMN and recreate table with same name
    framework.addTest("DROP_COLUMN_08_Recreate_After_Drop", [suite]() {
        // Drop the test table completely
        bool success = suite->executeQuery("DROP TABLE test_table");
        ASSERT_TRUE(success);

        // Recreate with different schema
        success = suite->executeQuery(R"(
            CREATE TABLE test_table (
                user_id INT PRIMARY KEY,
                username VARCHAR NOT NULL,
                status BOOLEAN
            )
        )");
        ASSERT_TRUE(success);

        // Insert new data
        success = suite->executeQuery("INSERT INTO test_table VALUES (1, 'newuser', TRUE)");
        ASSERT_TRUE(success);

        int count = suite->getRowCount("SELECT * FROM test_table");
        ASSERT_EQ(1, count);

        return true;
    }, "Test recreating table after dropping columns and table");
}

void addDropTests(tests::TestFramework& framework) {
    addDropTableTests(framework);
    addDropColumnTests(framework);
}