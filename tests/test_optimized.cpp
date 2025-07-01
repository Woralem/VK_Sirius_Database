#include "test_framework.h"
#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include "storage/optimized_in_memory_storage.h"
#include <nlohmann/json.hpp>
#include <chrono>
#include <iostream>

using namespace tests;
using namespace query_engine;
using json = nlohmann::json;

struct OptimizedExecutorTestCase {
    std::string name;
    std::vector<std::string> setup_queries;
    std::string test_query;
    std::function<bool(const json&)> validate_func;
};

bool run_optimized_executor_tests() {
    const std::vector<OptimizedExecutorTestCase> test_cases = {
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

    std::vector<bool> results;
    results.reserve(test_cases.size());

    auto start = std::chrono::high_resolution_clock::now();
    
    for (const auto& tc : test_cases) {
        auto storage = std::make_shared<OptimizedInMemoryStorage>();
        OptimizedQueryExecutor executor(storage);
        executor.setLoggingEnabled(false);

        std::vector<std::pair<std::string, ASTNodePtr>> setupAsts;
        setupAsts.reserve(tc.setup_queries.size());
        
        bool setup_ok = true;
        for (const auto& query : tc.setup_queries) {
            Lexer lexer(query);
            auto tokens = lexer.tokenize();
            Parser parser(tokens);
            auto ast = parser.parse();
            
            if (!ast || parser.hasError()) {
                setup_ok = false;
                break;
            }
            setupAsts.emplace_back(query, std::move(ast));
        }
        
        if (!setup_ok) {
            results.push_back(false);
            continue;
        }

        for (const auto& [query, ast] : setupAsts) {
            executor.execute(ast);
        }

        Lexer lexer(tc.test_query);
        auto tokens = lexer.tokenize();
        Parser parser(tokens);
        auto ast = parser.parse();
        
        if (!ast || parser.hasError()) {
            results.push_back(false);
            continue;
        }
        
        json result = executor.execute(ast);
        results.push_back(tc.validate_func(result));
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    std::cout << "\nOptimized executor tests completed in " 
              << duration.count() << " microseconds" << std::endl;
    
    return std::all_of(results.begin(), results.end(), 
                      [](bool r) { return r; });
}

bool performance_comparison_test() {
    const std::string create_query = "CREATE TABLE perf_test (id INT, value INT)";
    const int num_rows = 1000;

    std::string insert_query = "INSERT INTO perf_test VALUES ";
    for (int i = 0; i < num_rows; ++i) {
        if (i > 0) insert_query += ", ";
        insert_query += "(" + std::to_string(i) + ", " + std::to_string(i * 10) + ")";
    }

    {
        auto start = std::chrono::high_resolution_clock::now();
        
        auto storage = std::make_shared<OptimizedInMemoryStorage>();
        OptimizedQueryExecutor executor(storage);
        executor.setLoggingEnabled(false);

        Lexer lexer1(create_query);
        auto tokens1 = lexer1.tokenize();
        Parser parser1(tokens1);
        executor.execute(parser1.parse());

        Lexer lexer2(insert_query);
        auto tokens2 = lexer2.tokenize();
        Parser parser2(tokens2);
        executor.execute(parser2.parse());

        std::string select_query = "SELECT * FROM perf_test WHERE value > 5000";
        Lexer lexer3(select_query);
        auto tokens3 = lexer3.tokenize();
        Parser parser3(tokens3);
        auto result = executor.execute(parser3.parse());
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        std::cout << "\nOptimized version processed " << num_rows 
                  << " rows in " << duration.count() << " ms" << std::endl;
        
        return result["status"] == "success";
    }
}

void addOptimizedExecutorTests(tests::TestFramework& framework) {
    framework.addTest("Optimized Executor: Functional Tests", run_optimized_executor_tests,
                     "Run optimized executor tests");
    framework.addTest("Optimized Executor: Performance Test", performance_comparison_test,
                     "Test performance with large dataset");
}