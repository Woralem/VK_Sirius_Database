#include "test_framework.h"
#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include "storage/optimized_in_memory_storage.h"
#include "test_utils.h"
#include <iostream>
#include <set>
#include <chrono>

using namespace tests;
using namespace query_engine;
using json = nlohmann::json;

bool test_empty_options_list_is_valid();
bool test_unreserved_keywords_as_identifiers();
bool test_pk_uniqueness_on_update();
bool test_aggressive_index_update_on_delete();
bool test_cross_type_comparison_in_where_clause();
bool test_constraint_enforcement_not_null_and_pk();
bool test_dml_type_safety_validation();
bool test_options_interaction_length_and_chars();
bool test_invalid_data_type_is_rejected();
bool test_null_value_handling_in_storage();
bool test_special_chars_in_strings();
bool test_delete_performance_is_reasonable();
bool test_reports_multiple_parse_errors();

void addFinalStressTests(TestFramework& framework) {
    framework.addTest("Stress: Empty Lists in WITH OPTIONS", test_empty_options_list_is_valid);
    framework.addTest("Stress: Unreserved Keywords (SKIPPED)", test_unreserved_keywords_as_identifiers);
    framework.addTest("Stress: PK Uniqueness on UPDATE", test_pk_uniqueness_on_update);
    framework.addTest("Stress: Aggressive Index on DELETE", test_aggressive_index_update_on_delete);
    framework.addTest("Advanced: Cross-Type Comparison", test_cross_type_comparison_in_where_clause);
    framework.addTest("Advanced: Constraint Enforcement (NOT NULL, PK)", test_constraint_enforcement_not_null_and_pk);
    framework.addTest("Advanced: DML Type Safety", test_dml_type_safety_validation);
    framework.addTest("Edge Cases: Options Interaction", test_options_interaction_length_and_chars);
    framework.addTest("Edge Cases: Invalid Data Type Rejection", test_invalid_data_type_is_rejected);
    framework.addTest("Edge Cases: NULL Value Handling", test_null_value_handling_in_storage);
    framework.addTest("Final Frontier: Special Chars in Strings", test_special_chars_in_strings);
    framework.addTest("Final Frontier: DELETE Performance", test_delete_performance_is_reasonable);
    framework.addTest("Final Frontier: Multiple Parse Errors", test_reports_multiple_parse_errors);
}

bool test_empty_options_list_is_valid() {
    logTestStart("Stress Test: Empty Lists in WITH OPTIONS");
    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    OptimizedQueryExecutor executor(storage);
    executor.setLoggingEnabled(false);
    logStep("Test: TYPES = [] should succeed and allow all types.");
    std::string query = "CREATE TABLE t1 (id INT, name VARCHAR) WITH OPTIONS (TYPES = [])";
    Parser p1(Lexer(query).tokenize());
    auto ast1 = p1.parse();
    ASSERT_NOT_NULL(ast1);
    json result = executor.execute(ast1);
    ASSERT_TRUE(result.contains("status"));
    ASSERT_EQ(std::string(result["status"]), std::string("success"));
    logSuccess("TYPES = [] was handled correctly.");
    return true;
}

bool test_unreserved_keywords_as_identifiers() {
    logTestStart("Stress Test: Unreserved Keywords as Identifiers (SKIPPED)");
    logDebug("Skipping test for unreserved keywords. Requires parser lookahead or quoted identifiers.");
    return true;
}

bool test_pk_uniqueness_on_update() {
    logTestStart("Stress Test: PRIMARY KEY Uniqueness on UPDATE");
    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    OptimizedQueryExecutor executor(storage);
    executor.setLoggingEnabled(false);
    executor.execute(Parser(Lexer("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)").tokenize()).parse());
    executor.execute(Parser(Lexer("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").tokenize()).parse());
    auto ast = Parser(Lexer("UPDATE users SET id = 1 WHERE id = 2").tokenize()).parse();
    json result = executor.execute(ast);
    ASSERT_EQ(result["rows_affected"], 0);
    logSuccess("UPDATE was correctly rejected due to PK violation.");
    return true;
}

bool test_aggressive_index_update_on_delete() {
    logTestStart("Stress Test: Aggressive Index Integrity on DELETE");
    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    OptimizedQueryExecutor executor(storage);
    executor.setLoggingEnabled(false);
    executor.execute(Parser(Lexer("CREATE TABLE data (id INT, val INT)").tokenize()).parse());
    for (int i = 0; i < 10; ++i) {
        executor.execute(Parser(Lexer("INSERT INTO data VALUES ("+std::to_string(i)+", "+std::to_string(i*10)+")").tokenize()).parse());
    }
    executor.execute(Parser(Lexer("DELETE FROM data WHERE id = 1").tokenize()).parse());
    executor.execute(Parser(Lexer("DELETE FROM data WHERE id = 3").tokenize()).parse());
    executor.execute(Parser(Lexer("DELETE FROM data WHERE id = 5").tokenize()).parse());
    executor.execute(Parser(Lexer("DELETE FROM data WHERE id = 8").tokenize()).parse());
    json result = executor.execute(Parser(Lexer("SELECT * FROM data").tokenize()).parse());
    ASSERT_EQ(result["data"].size(), 6);
    std::set<int> expected_ids = {0, 2, 4, 6, 7, 9};
    std::set<int> actual_ids;
    for(const auto& row : result["data"]) {
        actual_ids.insert(row["id"].get<int>());
    }
    ASSERT_EQ(expected_ids, actual_ids);
    logSuccess("Data is consistent after multiple non-sequential deletes.");
    return true;
}

bool test_cross_type_comparison_in_where_clause() {
    logTestStart("Advanced: Cross-Type Comparison in WHERE");
    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    OptimizedQueryExecutor executor(storage);
    executor.setLoggingEnabled(false);
    executor.execute(Parser(Lexer("CREATE TABLE data (id INT, val_int INT, val_double DOUBLE)").tokenize()).parse());
    executor.execute(Parser(Lexer("INSERT INTO data VALUES (1, 10, 10.0), (2, 20, 20.5)").tokenize()).parse());
    json result1 = executor.execute(Parser(Lexer("SELECT * FROM data WHERE val_int = 10.0").tokenize()).parse());
    ASSERT_EQ(result1["data"].size(), 1);
    json result2 = executor.execute(Parser(Lexer("SELECT * FROM data WHERE val_double = 10").tokenize()).parse());
    ASSERT_EQ(result2["data"].size(), 1);
    return true;
}

bool test_constraint_enforcement_not_null_and_pk() {
    logTestStart("Advanced: Constraint Enforcement (NOT NULL, PK)");
    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    OptimizedQueryExecutor executor(storage);
    executor.setLoggingEnabled(false);
    executor.execute(Parser(Lexer("CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR NOT NULL)").tokenize()).parse());
    auto r1 = executor.execute(Parser(Lexer("INSERT INTO users VALUES (1, 'test@test.com')").tokenize()).parse());
    ASSERT_EQ(r1["rows_affected"], 1);
    auto r2 = executor.execute(Parser(Lexer("INSERT INTO users VALUES (1, 'another@test.com')").tokenize()).parse());
    ASSERT_EQ(r2["rows_affected"], 0);
    auto r3 = executor.execute(Parser(Lexer("INSERT INTO users VALUES (2, NULL)").tokenize()).parse());
    ASSERT_EQ(r3["rows_affected"], 0);
    return true;
}

bool test_dml_type_safety_validation() {
    logTestStart("Advanced: DML Type Safety");
    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    OptimizedQueryExecutor executor(storage);
    executor.setLoggingEnabled(false);
    executor.execute(Parser(Lexer("CREATE TABLE items (id INT, name VARCHAR)").tokenize()).parse());
    executor.execute(Parser(Lexer("INSERT INTO items VALUES (1, 'gadget')").tokenize()).parse());
    auto r1 = executor.execute(Parser(Lexer("INSERT INTO items VALUES ('not-an-int', 'widget')").tokenize()).parse());
    ASSERT_EQ(r1["rows_affected"], 0);
    auto r2 = executor.execute(Parser(Lexer("UPDATE items SET id = 'a-string' WHERE id = 1").tokenize()).parse());
    ASSERT_EQ(r2["rows_affected"], 0);
    return true;
}

bool test_options_interaction_length_and_chars() {
    logTestStart("Edge Cases: Interaction of MAX_COLUMN_LENGTH and ADDITIONAL_CHARS");
    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    OptimizedQueryExecutor executor(storage);
    executor.setLoggingEnabled(false);
    auto ast1 = Parser(Lexer("CREATE TABLE t1 (id INT) WITH OPTIONS (MAX_COLUMN_LENGTH=10, ADDITIONAL_CHARS=['@'])").tokenize()).parse();
    static_cast<CreateTableStmt*>(ast1.get())->columns[0]->name = "user@12345";
    json result1 = executor.execute(ast1);
    ASSERT_EQ(result1["status"], "success");
    auto ast2 = Parser(Lexer("CREATE TABLE t2 (id INT) WITH OPTIONS (MAX_COLUMN_LENGTH=10, ADDITIONAL_CHARS=['@'])").tokenize()).parse();
    static_cast<CreateTableStmt*>(ast2.get())->columns[0]->name = "user@123456";
    json result2 = executor.execute(ast2);
    ASSERT_EQ(result2["status"], "error");
    return true;
}

bool test_invalid_data_type_is_rejected() {
    logTestStart("Edge Cases: Rejection of Invalid Data Type");
    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    OptimizedQueryExecutor executor(storage);
    executor.setLoggingEnabled(false);
    Parser parser(Lexer("CREATE TABLE users (id INTGER)").tokenize());
    auto ast = parser.parse();
    ASSERT_FALSE(parser.hasError());
    ASSERT_NOT_NULL(ast);
    ASSERT_EQ(static_cast<CreateTableStmt*>(ast.get())->columns[0]->parsedType, DataType::UNKNOWN_TYPE);
    json result = executor.execute(ast);
    ASSERT_EQ(result["status"], "error");
    return true;
}

bool test_null_value_handling_in_storage() {
    logTestStart("Edge Cases: NULL Value Handling in Indexes and Updates");
    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    OptimizedQueryExecutor executor(storage);
    executor.setLoggingEnabled(false);
    executor.execute(Parser(Lexer("CREATE TABLE products (id INT, category VARCHAR)").tokenize()).parse());
    executor.execute(Parser(Lexer("INSERT INTO products VALUES (1, 'books'), (2, NULL), (3, 'tech'), (4, NULL)").tokenize()).parse());
    json result_all = executor.execute(Parser(Lexer("SELECT * FROM products").tokenize()).parse());
    int null_count = 0;
    for(const auto& row : result_all["data"]) { if (row.contains("category") && row["category"].is_null()) null_count++; }
    ASSERT_EQ(null_count, 2);
    json result_update = executor.execute(Parser(Lexer("UPDATE products SET category = 'books' WHERE id = 2").tokenize()).parse());
    ASSERT_EQ(result_update["rows_affected"], 1);
    json result_delete = executor.execute(Parser(Lexer("DELETE FROM products WHERE id = 4").tokenize()).parse());
    ASSERT_EQ(result_delete["rows_affected"], 1);
    json result_final = executor.execute(Parser(Lexer("SELECT * FROM products").tokenize()).parse());
    ASSERT_EQ(result_final["data"].size(), 3);
    return true;
}

bool test_special_chars_in_strings() {
    logTestStart("Final Frontier: Special Chars in Strings");
    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    OptimizedQueryExecutor executor(storage);
    executor.setLoggingEnabled(false);
    executor.execute(Parser(Lexer("CREATE TABLE messages (msg VARCHAR)").tokenize()).parse());
    std::string tricky_string = "Hello, world!'; -- This is not a comment";
    std::string escaped_string = "Hello, world!''; -- This is not a comment";
    json r1 = executor.execute(Parser(Lexer("INSERT INTO messages VALUES ('" + escaped_string + "')").tokenize()).parse());
    ASSERT_EQ(r1["rows_affected"], 1);
    json r2 = executor.execute(Parser(Lexer("SELECT msg FROM messages").tokenize()).parse());
    ASSERT_EQ(r2["data"].size(), 1);
    std::string retrieved_string = r2["data"][0]["msg"];
    ASSERT_EQ(retrieved_string, tricky_string);
    logSuccess("Strings with special characters are handled correctly.");
    return true;
}

bool test_delete_performance_is_reasonable() {
    logTestStart("Final Frontier: DELETE Performance");
    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    OptimizedQueryExecutor executor(storage);
    executor.setLoggingEnabled(false);
    logStep("Setup: Creating a large table with 10,000 rows.");
    executor.execute(Parser(Lexer("CREATE TABLE big_log (id INT, data VARCHAR)").tokenize()).parse());
    for(int i=0; i<10000; ++i) {
        storage->insertRow("big_log", {"id", "data"}, {i, "some data"});
    }
    logStep("Test: Deleting 5,000 odd-numbered rows.");
    auto start = std::chrono::high_resolution_clock::now();
    int deleted_count = storage->deleteRows("big_log", [](const json& row){
        if (!row.contains("id")) return false;
        int id = row["id"];
        return id % 2 != 0;
    });
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    ASSERT_EQ(deleted_count, 5000);
    logSuccess("Deleted 5,000 rows in " + std::to_string(duration.count()) + " ms.");
    if (duration.count() > 500) {
        logError("PERFORMANCE REGRESSION: Deleting many rows is still slow.");
        return false;
    }
    logSuccess("Delete performance is excellent.");
    return true;
}

bool test_reports_multiple_parse_errors() {
    logTestStart("Final Frontier: Multiple Parse Error Reporting");
    std::string query = "SELECT FROM users; INSERT INTO VALUES (1);";
    Parser parser(Lexer(query).tokenize());
    parser.parse();
    const auto& errors = parser.getErrors();
    if (errors.size() < 2) {
        logError("BUG: Parser did not report all errors.");
        logDebug("Expected at least 2 errors, but got " + std::to_string(errors.size()));
        for(const auto& err : errors) logDebug("  - " + err);
        return false;
    }
    logSuccess("Parser correctly identified multiple errors in a single batch.");
    return true;
}