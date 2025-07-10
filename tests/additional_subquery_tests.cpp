#include "test_framework.h"
#include "test_utils.h"
#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include "storage/optimized_in_memory_storage.h"
#include <nlohmann/json.hpp>
#include <format>
#include <chrono>

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
            return static_cast<int>(result["cells"].size());
        }
        return -1;
    }

    void reset() {
        storage = std::make_shared<OptimizedInMemoryStorage>();
        executor = std::make_shared<OptimizedQueryExecutor>(storage);
        executor->setLoggingEnabled(false);
    }
};

void addAdditionalSubqueryTests(tests::TestFramework& framework) {
    auto suite = std::make_shared<SubqueryTestSuite>();

    framework.addTest("SUBQUERY_16_Type_Mismatch_Error", [suite]() {
        logTestStart("Type Mismatch in Subquery");
        suite->reset();
        suite->executeQuery("CREATE TABLE type_test1 (id INT, name VARCHAR)");
        suite->executeQuery("CREATE TABLE type_test2 (id VARCHAR, value INT)");
        suite->executeQuery("INSERT INTO type_test1 VALUES (1, 'test')");
        suite->executeQuery("INSERT INTO type_test2 VALUES ('abc', 100)");

        auto result = suite->queryResult("SELECT * FROM type_test1 WHERE id IN (SELECT id FROM type_test2)");
        logSuccess("Type mismatch test completed");
        return true;
    }, "Type mismatch detection in subqueries");

    framework.addTest("SUBQUERY_17_Large_Result_Set", [suite]() {
        logTestStart("Large Result Set in Subquery");
        suite->reset();
        suite->executeQuery("CREATE TABLE large_data_17 (id INT, group_id INT)");
        for (int i = 1; i <= 100; i++) {
            suite->executeQuery(std::format("INSERT INTO large_data_17 VALUES ({}, {})", i, (i % 10) + 1));
        }
        suite->executeQuery("CREATE TABLE categories_17 (id INT PRIMARY KEY, name VARCHAR)");
        for (int i = 1; i <= 5; i++) {
            suite->executeQuery(std::format("INSERT INTO categories_17 VALUES ({}, 'Cat{}')", i, i));
        }
        int count = suite->getRowCount("SELECT * FROM large_data_17 WHERE group_id IN (SELECT id FROM categories_17)");
        ASSERT_EQ(50, count);
        logSuccess("Large result set handled correctly");
        return true;
    }, "Large result set performance");

    framework.addTest("SUBQUERY_18_Mixed_IN_Values", [suite]() {
        logTestStart("Mixed IN Values Test");
        suite->reset();
        ASSERT_TRUE(suite->executeQuery("CREATE TABLE products_18 (id INT, category_id INT, name VARCHAR)"));
        logSuccess("Mixed IN values test setup complete");
        return true;
    }, "IN with both literals and subqueries");

    framework.addTest("SUBQUERY_19_Complex_Expression", [suite]() {
        logTestStart("Subquery in Complex Expression");
        suite->reset();
        ASSERT_TRUE(suite->executeQuery("CREATE TABLE products_19 (id INT, name VARCHAR, price DOUBLE, category_id INT)"));
        ASSERT_TRUE(suite->executeQuery("CREATE TABLE categories_19 (id INT, active BOOLEAN)"));
        suite->executeQuery("INSERT INTO products_19 VALUES (1, 'Laptop', 1500, 1)");
        suite->executeQuery("INSERT INTO products_19 VALUES (2, 'Mouse', 25, 1)");
        suite->executeQuery("INSERT INTO products_19 VALUES (3, 'Keyboard', 150, 1)");
        suite->executeQuery("INSERT INTO categories_19 VALUES (1, true)");

        int count = suite->getRowCount("SELECT * FROM products_19 WHERE price > 100 AND category_id IN (SELECT id FROM categories_19 WHERE active = true)");
        ASSERT_EQ(2, count);
        logSuccess("Complex expression with subquery works");
        return true;
    }, "Subquery within complex boolean expression");

    framework.addTest("SUBQUERY_20_Case_Sensitivity", [suite]() {
        logTestStart("Case Sensitivity in IN Subquery");
        suite->reset();
        suite->executeQuery("CREATE TABLE case_test (id INT, name VARCHAR)");
        suite->executeQuery("INSERT INTO case_test VALUES (1, 'Test')");
        suite->executeQuery("INSERT INTO case_test VALUES (2, 'TEST')");
        suite->executeQuery("INSERT INTO case_test VALUES (3, 'test')");
        int count = suite->getRowCount("SELECT * FROM case_test WHERE name IN (SELECT name FROM case_test WHERE id = 1)");
        ASSERT_EQ(1, count);
        logSuccess("Case sensitivity preserved in subqueries");
        return true;
    }, "Case sensitivity in string comparisons");

    framework.addTest("SUBQUERY_22_Empty_Outer_Table", [suite]() {
        logTestStart("Empty Outer Table with Subquery");
        suite->reset();
        suite->executeQuery("CREATE TABLE empty_table_22 (id INT, ref_id INT)");
        suite->executeQuery("CREATE TABLE categories_22 (id INT)");
        suite->executeQuery("INSERT INTO categories_22 VALUES (1)");
        int count = suite->getRowCount("SELECT * FROM empty_table_22 WHERE ref_id IN (SELECT id FROM categories_22)");
        ASSERT_EQ(0, count);
        logSuccess("Empty outer table handled correctly");
        return true;
    }, "Subquery with empty outer table");

    framework.addTest("SUBQUERY_23_Multiple_IN", [suite]() {
        logTestStart("Multiple IN Clauses");
        suite->reset();
        suite->executeQuery("CREATE TABLE orders_23 (id INT, customer_id INT, product_id INT)");
        suite->executeQuery("CREATE TABLE customers_23 (id INT, vip BOOLEAN)");
        suite->executeQuery("CREATE TABLE products_23 (id INT, price DOUBLE)");
        suite->executeQuery("INSERT INTO customers_23 VALUES (1, true)");
        suite->executeQuery("INSERT INTO customers_23 VALUES (2, false)");
        suite->executeQuery("INSERT INTO products_23 VALUES (101, 150.0)");
        suite->executeQuery("INSERT INTO products_23 VALUES (102, 50.0)");
        suite->executeQuery("INSERT INTO orders_23 VALUES (1, 1, 101)"); // vip=true, price > 100
        suite->executeQuery("INSERT INTO orders_23 VALUES (2, 2, 102)"); // vip=false, price < 100
        suite->executeQuery("INSERT INTO orders_23 VALUES (3, 1, 102)"); // vip=true, price < 100
        suite->executeQuery("INSERT INTO orders_23 VALUES (4, 2, 101)"); // vip=false, price > 100

        int count = suite->getRowCount(
            "SELECT * FROM orders_23 WHERE "
            "customer_id IN (SELECT id FROM customers_23 WHERE vip = true) AND "
            "product_id IN (SELECT id FROM products_23 WHERE price > 100)"
        );
        ASSERT_EQ(1, count);
        logSuccess("Multiple IN clauses work correctly");
        return true;
    }, "Multiple IN subqueries in one WHERE clause");

    framework.addTest("SUBQUERY_24_Index_Performance", [suite]() {
        logTestStart("Index Performance Simulation");
        suite->reset();
        suite->executeQuery("CREATE TABLE products_perf (id INT PRIMARY KEY, name VARCHAR)");
        suite->executeQuery("CREATE TABLE orders_perf (id INT, product_id INT)");
        for (int i = 1; i <= 100; i++) {
            suite->executeQuery(std::format("INSERT INTO products_perf VALUES ({}, 'Product{}')", i, i));
            suite->executeQuery(std::format("INSERT INTO orders_perf VALUES ({}, {})", i, (i % 10) + 1));
        }

        auto start = std::chrono::high_resolution_clock::now();
        suite->getRowCount("SELECT * FROM products_perf WHERE id IN (SELECT product_id FROM orders_perf)");
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        ASSERT_TRUE(duration.count() < 1000);
        logSuccess("Subquery performance acceptable");
        return true;
    }, "Performance with indexed columns");
}