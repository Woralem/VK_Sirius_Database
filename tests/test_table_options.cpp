#include "test_framework.h"
#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include "storage/optimized_in_memory_storage.h"
#include "test_utils.h" // Подключаем общие утилиты
#include <iostream>

using namespace tests;
using namespace query_engine;
using json = nlohmann::json;

bool test_create_table_with_all_options() {
    logTestStart("Create Table With All Options (Happy Path)");

    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    OptimizedQueryExecutor executor(storage);
    executor.setLoggingEnabled(false);

    std::string query = R"(
        CREATE TABLE users (
            id INT,
            user_name VARCHAR,
            email VARCHAR
        ) WITH OPTIONS (
            TYPES = [INT, VARCHAR, BOOLEAN],
            MAX_COLUMN_LENGTH = 32,
            ADDITIONAL_CHARS = ['@', '.'],
            MAX_STRING_LENGTH = 100,
            GC_FREQUENCY = 30 DAYS
        )
    )";

    logStep("Parsing and executing CREATE TABLE with a full set of valid options.");
    Lexer lexer(query);
    Parser parser(lexer.tokenize());
    auto ast = parser.parse();

    ASSERT_FALSE(parser.hasError());
    ASSERT_NOT_NULL(ast);

    json result = executor.execute(ast);
    logDebug("Execution result: " + result.dump());
    ASSERT_EQ(result["status"], "success");
    logSuccess("Table created successfully with custom options.");

    logStep("Testing option enforcement: inserting a valid row.");
    std::string validInsert = "INSERT INTO users VALUES (1, 'JohnDoe', 'john@test.com')";
    Lexer lexer2(validInsert);
    Parser parser2(lexer2.tokenize());
    json validInsertResult = executor.execute(parser2.parse());
    ASSERT_EQ(validInsertResult["rows_affected"], 1);
    logSuccess("Valid data inserted successfully.");

    logStep("Testing option enforcement: inserting a string that exceeds MAX_STRING_LENGTH (100).");
    std::string longString(101, 'x');
    std::string invalidInsert = "INSERT INTO users VALUES (2, '" + longString + "', 'test@test.com')";
    Lexer lexer3(invalidInsert);
    Parser parser3(lexer3.tokenize());
    json invalidInsertResult = executor.execute(parser3.parse());
    ASSERT_EQ(invalidInsertResult["rows_affected"], 0);
    logSuccess("Row with oversized string was correctly rejected.");

    return true;
}

bool test_creation_time_validation() {
    logTestStart("Creation-Time Validation (Invalid Options)");

    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    OptimizedQueryExecutor executor(storage);
    executor.setLoggingEnabled(false);

    logStep("Test 1: Column name exceeds MAX_COLUMN_LENGTH.");
    std::string query1 = "CREATE TABLE test1 (this_is_a_very_long_column_name_that_should_fail INT) WITH OPTIONS (MAX_COLUMN_LENGTH = 16)";
    Parser p1((Lexer(query1)).tokenize());
    json result1 = executor.execute(p1.parse());
    ASSERT_EQ(result1["status"], "error");
    logSuccess("Column name length validation worked correctly.");

    logStep("Test 2: Column data type is not in the allowed TYPES list.");
    std::string query2 = "CREATE TABLE test2 (id INT, price DOUBLE) WITH OPTIONS (TYPES = [INT, VARCHAR])";
    Parser p2((Lexer(query2)).tokenize());
    json result2 = executor.execute(p2.parse());
    ASSERT_EQ(result2["status"], "error");
    logSuccess("Data type restriction worked correctly.");

    logStep("Test 3: Column name contains a character not allowed by default or in ADDITIONAL_CHARS.");
    std::string query3 = "CREATE TABLE test3 (user_at_domain VARCHAR) WITH OPTIONS (ADDITIONAL_CHARS = ['_'])";
    Parser p3((Lexer(query3)).tokenize());
    auto ast3 = p3.parse();
    auto* stmt3 = static_cast<CreateTableStmt*>(ast3.get());
    stmt3->columns[0]->name = "user@domain";

    json result3 = executor.execute(ast3);
    ASSERT_EQ(result3["status"], "error");
    logSuccess("Additional character validation worked correctly.");

    return true;
}

bool test_table_options_parser_syntax() {
    logTestStart("Parser Syntax for Table Options");

    struct TestCase { std::string name; std::string options; bool shouldParse; };
    std::vector<TestCase> testCases = {
        {"All options", "TYPES=[INT],MAX_COLUMN_LENGTH=32,ADDITIONAL_CHARS=['_'],MAX_STRING_LENGTH=1024,GC_FREQUENCY=7", true},
        {"Subset of options", "MAX_STRING_LENGTH = 256, GC_FREQUENCY = 90 DAYS", true},
        {"Only one option", "TYPES = [INT, DOUBLE, VARCHAR, BOOLEAN, DATE, TIMESTAMP]", true},
        {"Empty options", "", true},
        {"Missing comma", "MAX_COLUMN_LENGTH=10 MAX_STRING_LENGTH=10", false},
        {"Missing equals", "GC_FREQUENCY 10", false},
        {"Unknown keyword", "INVALID_OPTION = 123", false},
    };

    for (const auto& tc : testCases) {
        logStep("Testing parser with: " + tc.name);
        std::string fullQuery = "CREATE TABLE test (id INT) WITH OPTIONS (" + tc.options + ")";

        Lexer lexer(fullQuery);
        Parser parser(lexer.tokenize());
        parser.parse();
        bool parsedSuccessfully = !parser.hasError();

        if (parsedSuccessfully != tc.shouldParse) {
            logError("Expected parse result: " + std::string(tc.shouldParse ? "SUCCESS" : "FAIL") + ", but got " + (parsedSuccessfully ? "SUCCESS" : "FAIL"));
            if (!parsedSuccessfully) {
                for (const auto& error : parser.getErrors()) logDebug("  - Parser error: " + error);
            }
            return false;
        }
        logSuccess("Parse result was as expected.");
    }

    return true;
}

bool test_default_options_enforcement() {
    logTestStart("Default Table Options Enforcement");

    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    OptimizedQueryExecutor executor(storage);
    executor.setLoggingEnabled(false);

    logStep("Creating a table without any explicit options.");
    std::string query = "CREATE TABLE products (id INT, name VARCHAR)";
    Parser p1((Lexer(query)).tokenize());
    json result = executor.execute(p1.parse());
    ASSERT_EQ(result["status"], "success");
    logSuccess("Table created successfully using default options.");

    logStep("Testing default MAX_STRING_LENGTH (65536).");
    std::string longString(65537, 'x');
    std::string insertQuery = "INSERT INTO products VALUES (1, '" + longString + "')";
    Parser p2((Lexer(insertQuery)).tokenize());
    json insertResult = executor.execute(p2.parse());
    ASSERT_EQ(insertResult["rows_affected"], 0);
    logSuccess("Default string length limit was correctly enforced.");

    logStep("Testing default MAX_COLUMN_LENGTH (16).");
    std::string longColNameTable = "CREATE TABLE another_test (a_very_long_col_name INT)";
    Parser p3((Lexer(longColNameTable)).tokenize());
    json longColResult = executor.execute(p3.parse());
    ASSERT_EQ(longColResult["status"], "error");
    logSuccess("Default column name length was correctly enforced.");

    return true;
}

bool test_garbage_collection_parameter_validation() {
    logTestStart("Garbage Collection Parameter Validation");

    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    OptimizedQueryExecutor executor(storage);
    executor.setLoggingEnabled(false);

    logStep("Testing valid GC frequency (1-365 days).");
    Parser p1((Lexer("CREATE TABLE gc1 (id INT) WITH OPTIONS (GC_FREQUENCY = 1)")).tokenize());
    ASSERT_EQ(executor.execute(p1.parse())["status"], "success");
    logSuccess("Table created with GC_FREQUENCY = 1 day.");

    Parser p2((Lexer("CREATE TABLE gc2 (id INT) WITH OPTIONS (GC_FREQUENCY = 365 DAYS)")).tokenize());
    ASSERT_EQ(executor.execute(p2.parse())["status"], "success");
    logSuccess("Table created with GC_FREQUENCY = 365 days.");

    logStep("Testing invalid GC frequency (0 and 366).");
    Parser p3((Lexer("CREATE TABLE gc3 (id INT) WITH OPTIONS (GC_FREQUENCY = 0)")).tokenize());
    ASSERT_EQ(executor.execute(p3.parse())["status"], "error");
    logSuccess("GC_FREQUENCY = 0 was correctly rejected.");

    Parser p4((Lexer("CREATE TABLE gc4 (id INT) WITH OPTIONS (GC_FREQUENCY = 367)")).tokenize());
    ASSERT_EQ(executor.execute(p4.parse())["status"], "error");
    logSuccess("GC_FREQUENCY > 365 was correctly rejected.");

    return true;
}

void addTableOptionsTests(TestFramework& framework) {
    framework.addTest("Table Options: Create with all valid options", test_create_table_with_all_options);
    framework.addTest("Table Options: Creation-time validation", test_creation_time_validation);
    framework.addTest("Table Options: Parser syntax variations", test_table_options_parser_syntax);
    framework.addTest("Table Options: Default options enforcement", test_default_options_enforcement);
    framework.addTest("Table Options: GC parameter validation", test_garbage_collection_parameter_validation);
}