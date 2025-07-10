#include "test_framework.h"
#include "test_utils.h"
#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include "storage/optimized_in_memory_storage.h"
#include <nlohmann/json.hpp>
#include <format>

using namespace query_engine;
using json = nlohmann::json;

class SubqueryTestSuite {
private:
    std::shared_ptr<OptimizedInMemoryStorage> storage;
    std::shared_ptr<OptimizedQueryExecutor> executor;

public:
    SubqueryTestSuite() {
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

            if (!success) {
                logError("Execution failed for query: " + query);
                if (result.contains("message")) {
                    logError("Error: " + result["message"].get<std::string>());
                }
                if (result.contains("error")) {
                    logError("Error details: " + result["error"].get<std::string>());
                }
            }
            return success;
        } catch (const std::exception& e) {
            logError("Exception in query '" + query + "': " + std::string(e.what()));
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
        if (result.contains("cells") && result["cells"].is_array()) {
            return result["cells"].size();
        }
        return -1;
    }

    void reset() {
        storage = std::make_shared<OptimizedInMemoryStorage>();
        executor = std::make_shared<OptimizedQueryExecutor>(storage);
        executor->setLoggingEnabled(false);
    }
};

void addSubqueryTests(tests::TestFramework& framework) {
    auto suite = std::make_shared<SubqueryTestSuite>();

    framework.addTest("SUBQUERY_01_Setup_Tables", [suite]() {
        logTestStart("Setup Tables for Subquery Tests");
        suite->reset();

        ASSERT_TRUE(suite->executeQuery("CREATE TABLE categories (id INT PRIMARY KEY, name VARCHAR NOT NULL, active BOOLEAN)"));
        ASSERT_TRUE(suite->executeQuery("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR NOT NULL, category_id INT, price DOUBLE)"));
        ASSERT_TRUE(suite->executeQuery("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR, city VARCHAR, vip BOOLEAN)"));
        ASSERT_TRUE(suite->executeQuery("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, product_id INT, quantity INT, order_date VARCHAR)"));

        logSuccess("All tables created successfully");
        return true;
    }, "Setup tables for subquery testing");

    framework.addTest("SUBQUERY_02_Insert_Data", [suite]() {
        logTestStart("Insert Test Data");

        suite->executeQuery("INSERT INTO categories VALUES (1, 'Electronics', true)");
        suite->executeQuery("INSERT INTO categories VALUES (2, 'Books', true)");
        suite->executeQuery("INSERT INTO categories VALUES (3, 'Clothing', false)");
        suite->executeQuery("INSERT INTO categories VALUES (4, 'Food', true)");

        suite->executeQuery("INSERT INTO products VALUES (1, 'Laptop', 1, 999.99)");
        suite->executeQuery("INSERT INTO products VALUES (2, 'Phone', 1, 599.99)");
        suite->executeQuery("INSERT INTO products VALUES (3, 'Tablet', 1, 399.99)");
        suite->executeQuery("INSERT INTO products VALUES (4, 'Novel', 2, 19.99)");
        suite->executeQuery("INSERT INTO products VALUES (5, 'Textbook', 2, 79.99)");
        suite->executeQuery("INSERT INTO products VALUES (6, 'T-Shirt', 3, 29.99)");
        suite->executeQuery("INSERT INTO products VALUES (7, 'Jeans', 3, 59.99)");
        suite->executeQuery("INSERT INTO products VALUES (8, 'Apple', 4, 0.99)");

        suite->executeQuery("INSERT INTO customers VALUES (1, 'John Doe', 'New York', true)");
        suite->executeQuery("INSERT INTO customers VALUES (2, 'Jane Smith', 'Los Angeles', false)");
        suite->executeQuery("INSERT INTO customers VALUES (3, 'Bob Johnson', 'New York', true)");
        suite->executeQuery("INSERT INTO customers VALUES (4, 'Alice Brown', 'Chicago', false)");

        suite->executeQuery("INSERT INTO orders VALUES (1, 1, 1, 1, '2024-01-01')");
        suite->executeQuery("INSERT INTO orders VALUES (2, 1, 2, 2, '2024-01-02')");
        suite->executeQuery("INSERT INTO orders VALUES (3, 2, 4, 1, '2024-01-03')");
        suite->executeQuery("INSERT INTO orders VALUES (4, 3, 1, 1, '2024-01-04')");
        suite->executeQuery("INSERT INTO orders VALUES (5, 3, 3, 1, '2024-01-05')");

        logSuccess("Test data inserted successfully");
        return true;
    }, "Insert test data for subquery tests");

    framework.addTest("SUBQUERY_03_Basic_IN", [suite]() {
        logTestStart("Basic IN Subquery");
        int count = suite->getRowCount("SELECT * FROM products WHERE category_id IN (SELECT id FROM categories WHERE active = true)");
        ASSERT_EQ(6, count); // Electronics (3) + Books (2) + Food (1) = 6
        logSuccess("Basic IN subquery works correctly");
        return true;
    }, "Basic IN subquery functionality");

    framework.addTest("SUBQUERY_04_IN_String", [suite]() {
        logTestStart("IN Subquery with String Comparison");
        int count = suite->getRowCount("SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE city = 'New York')");
        ASSERT_EQ(4, count); // Orders from customers 1 and 3
        logSuccess("IN subquery with string comparison works");
        return true;
    }, "IN subquery with string columns");

    framework.addTest("SUBQUERY_05_Empty_Subquery", [suite]() {
        logTestStart("IN Subquery Returning Empty Set");
        int count = suite->getRowCount("SELECT * FROM products WHERE category_id IN (SELECT id FROM categories WHERE name = 'NonExistent')");
        ASSERT_EQ(0, count);
        logSuccess("Empty subquery result handled correctly");
        return true;
    }, "IN subquery with empty result set");

    framework.addTest("SUBQUERY_06_Complex_WHERE", [suite]() {
        logTestStart("Complex WHERE with IN Subquery");
        int count = suite->getRowCount("SELECT * FROM products WHERE price > 50 AND category_id IN (SELECT id FROM categories WHERE active = true)");
        ASSERT_EQ(4, count); // Laptop, Phone, Tablet, Textbook
        logSuccess("Complex WHERE clause with IN subquery works");
        return true;
    }, "Complex WHERE conditions with IN subquery");

    framework.addTest("SUBQUERY_07_Select_Columns", [suite]() {
        logTestStart("Select Specific Columns with IN Subquery");
        auto result = suite->queryResult("SELECT name, price FROM products WHERE category_id IN (SELECT id FROM categories WHERE name = 'Electronics')");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        ASSERT_TRUE(result.contains("cells") && result["cells"].is_array());
        ASSERT_EQ(3, result["cells"].size());
        if (result.contains("header")) {
            ASSERT_EQ(2, result["header"].size());
            ASSERT_EQ("name", result["header"][0]["content"]);
            ASSERT_EQ("price", result["header"][1]["content"]);
        }
        logSuccess("Column selection with IN subquery works");
        return true;
    }, "Select specific columns with IN subquery");

    framework.addTest("SUBQUERY_08_Boolean_Values", [suite]() {
        logTestStart("IN Subquery with Boolean Values");
        int count = suite->getRowCount("SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE vip = true)");
        ASSERT_EQ(4, count); // Orders from VIP customers (1 and 3)
        logSuccess("IN subquery with boolean values works");
        return true;
    }, "IN subquery with boolean conditions");

    framework.addTest("SUBQUERY_09_Multiple_Columns_Error", [suite]() {
        logTestStart("Error: Subquery Returns Multiple Columns");
        std::string query = "SELECT * FROM products WHERE category_id IN (SELECT id, name FROM categories)";
        logDebug("Testing query: " + query);
        auto result = suite->queryResult(query);
        logDebug("Query result: " + result.dump(2));
        ASSERT_TRUE(result.contains("status") && result["status"] == "error");
        if (result.contains("message")) {
            std::string errorMsg = result["message"].get<std::string>();
            ASSERT_TRUE(errorMsg.find("exactly one column") != std::string::npos);
        }
        logSuccess("Multiple column error correctly detected");
        return true;
    }, "Error handling for multi-column subquery");

    framework.addTest("SUBQUERY_10_Nested_Subquery", [suite]() {
        logTestStart("Nested Subquery");
        int count = suite->getRowCount(R"(
            SELECT * FROM products
            WHERE category_id IN (
                SELECT category_id FROM products
                WHERE id IN (
                    SELECT product_id FROM orders
                    WHERE customer_id IN (
                        SELECT id FROM customers WHERE city = 'New York'
                    )
                )
            )
        )");
        ASSERT_EQ(3, count); // All Electronics products (category 1)
        logSuccess("Nested subqueries work correctly");
        return true;
    }, "Nested subquery functionality");

    framework.addTest("SUBQUERY_11_NULL_Handling", [suite]() {
        logTestStart("IN Subquery with NULL Values");
        suite->executeQuery("INSERT INTO categories VALUES (5, NULL, true)");
        int count = suite->getRowCount("SELECT * FROM categories WHERE name IN (SELECT name FROM categories WHERE active = true)");
        ASSERT_EQ(3, count); // Electronics, Books, Food (NULL is not matched against itself)
        logSuccess("NULL values handled correctly in subquery");
        return true;
    }, "NULL value handling in subqueries");

    framework.addTest("SUBQUERY_12_Performance", [suite]() {
        logTestStart("Performance Test with Larger Dataset");
        suite->executeQuery("CREATE TABLE large_table (id INT PRIMARY KEY, category INT, value DOUBLE)");
        for (int i = 1; i <= 100; i++) {
            suite->executeQuery(std::format("INSERT INTO large_table VALUES ({}, {}, {})", i, (i % 10) + 1, i * 1.5));
        }
        int count = suite->getRowCount("SELECT * FROM large_table WHERE category IN (SELECT id FROM categories WHERE active = true)");
        ASSERT_TRUE(count > 0);
        logSuccess("Performance test completed successfully");
        return true;
    }, "Performance test with larger dataset");

    framework.addTest("SUBQUERY_13_Type_Compatibility", [suite]() {
        logTestStart("Type Compatibility in Subqueries");
        suite->executeQuery("CREATE TABLE test_types (id INT, value DOUBLE)");
        suite->executeQuery("INSERT INTO test_types VALUES (1, 1.0)");
        suite->executeQuery("INSERT INTO test_types VALUES (2, 2.5)");
        int count = suite->getRowCount("SELECT * FROM test_types WHERE id IN (SELECT value FROM test_types WHERE value = 1.0)");
        ASSERT_EQ(1, count);
        logSuccess("Type compatibility handled correctly");
        return true;
    }, "Type compatibility in subquery comparisons");

    framework.addTest("SUBQUERY_14_Parser_Test", [suite]() {
        logTestStart("Parser Test for IN Token");
        std::string query = "SELECT * FROM products WHERE category_id IN (SELECT id FROM categories)";
        Lexer lexer(query);
        auto tokens = lexer.tokenize();
        bool foundInToken = false;
        for (const auto& token : tokens) {
            if (token.type == TokenType::IN_TOKEN) {
                foundInToken = true;
                ASSERT_EQ("IN", token.lexeme);
                break;
            }
        }
        ASSERT_TRUE(foundInToken);
        logSuccess("IN token parsed correctly");
        return true;
    }, "Parser correctly handles IN token");

    framework.addTest("SUBQUERY_15_All_NULL_Subquery", [suite]() {
        logTestStart("Subquery Returning All NULL Values");
        suite->executeQuery("CREATE TABLE null_table (id INT, null_col VARCHAR)");
        suite->executeQuery("INSERT INTO null_table VALUES (1, NULL)");
        suite->executeQuery("INSERT INTO null_table VALUES (2, NULL)");
        int count = suite->getRowCount("SELECT * FROM products WHERE name IN (SELECT null_col FROM null_table)");
        ASSERT_EQ(0, count);
        logSuccess("All NULL subquery handled correctly");
        return true;
    }, "Subquery with all NULL values");
}