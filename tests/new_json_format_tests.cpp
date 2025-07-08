#include "test_framework.h"
#include "test_utils.h"
#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include "query_engine/executor.h"
#include "storage/optimized_in_memory_storage.h"
#include <nlohmann/json.hpp>

using namespace query_engine;
using json = nlohmann::json;

class JsonFormatTestSuite {
private:
    std::shared_ptr<OptimizedInMemoryStorage> storage;
    std::shared_ptr<OptimizedQueryExecutor> executor;

public:
    JsonFormatTestSuite() {
        storage = std::make_shared<OptimizedInMemoryStorage>();
        executor = std::make_shared<OptimizedQueryExecutor>(storage);
        executor->setLoggingEnabled(false);
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

    bool executeQuery(const std::string& query) {
        auto result = queryResult(query);
        return result.contains("status") && result["status"] == "success";
    }
};

void addNewJsonFormatTests(tests::TestFramework& framework) {
    auto suite = std::make_shared<JsonFormatTestSuite>();

    // Setup test data
    framework.addTest("JSON_FORMAT_00_Setup", [suite]() {
        logTestStart("Setup for JSON Format Tests");
        
        bool success = suite->executeQuery(R"(
            CREATE TABLE format_test (
                id INT PRIMARY KEY,
                name VARCHAR,
                score DOUBLE,
                active BOOLEAN
            )
        )");
        ASSERT_TRUE(success);
        
        success = suite->executeQuery("INSERT INTO format_test VALUES (1, 'Alice', 95.5, TRUE)");
        ASSERT_TRUE(success);
        
        success = suite->executeQuery("INSERT INTO format_test VALUES (2, 'Bob', 87.2, FALSE)");
        ASSERT_TRUE(success);
        
        logSuccess("Test data setup complete");
        return true;
    }, "Setup test data for JSON format tests");

    // Test 1: Header format
    framework.addTest("JSON_FORMAT_01_Header_Structure", [suite]() {
        logTestStart("Header Structure");
        
        auto result = suite->queryResult("SELECT * FROM format_test");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        // Check header exists and has correct structure
        ASSERT_TRUE(result.contains("header"));
        ASSERT_TRUE(result["header"].is_array());
        ASSERT_EQ(4, result["header"].size());
        
        // Check each header cell structure
        for (const auto& headerCell : result["header"]) {
            ASSERT_TRUE(headerCell.contains("content"));
            ASSERT_TRUE(headerCell.contains("id"));
            ASSERT_TRUE(headerCell.contains("type"));
            
            ASSERT_TRUE(headerCell["content"].is_string());
            ASSERT_TRUE(headerCell["id"].is_string());
            ASSERT_TRUE(headerCell["type"].is_string());
        }
        
        // Check specific header content
        ASSERT_EQ("id", result["header"][0]["content"]);
        ASSERT_EQ("col_0", result["header"][0]["id"]);
        ASSERT_EQ("INT", result["header"][0]["type"]);
        
        ASSERT_EQ("name", result["header"][1]["content"]);
        ASSERT_EQ("col_1", result["header"][1]["id"]);
        ASSERT_EQ("VARCHAR", result["header"][1]["type"]);
        
        ASSERT_EQ("score", result["header"][2]["content"]);
        ASSERT_EQ("col_2", result["header"][2]["id"]);
        ASSERT_EQ("DOUBLE", result["header"][2]["type"]);
        
        ASSERT_EQ("active", result["header"][3]["content"]);
        ASSERT_EQ("col_3", result["header"][3]["id"]);
        ASSERT_EQ("BOOLEAN", result["header"][3]["type"]);
        
        logSuccess("Header structure is correct");
        return true;
    }, "Test new JSON header format structure");

    // Test 2: Cells format
    framework.addTest("JSON_FORMAT_02_Cells_Structure", [suite]() {
        logTestStart("Cells Structure");
        
        auto result = suite->queryResult("SELECT * FROM format_test");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        // Check cells exists and has correct structure
        ASSERT_TRUE(result.contains("cells"));
        ASSERT_TRUE(result["cells"].is_array());
        ASSERT_EQ(2, result["cells"].size()); // 2 rows
        
        // Check each row structure
        for (const auto& row : result["cells"]) {
            ASSERT_TRUE(row.is_array());
            ASSERT_EQ(4, row.size()); // 4 columns
            
            // Check each cell structure
            for (const auto& cell : row) {
                ASSERT_TRUE(cell.contains("content"));
                ASSERT_TRUE(cell.contains("id"));
                ASSERT_TRUE(cell["id"].is_string());
                // content can be any type
            }
        }
        
        // Check specific cell IDs and content
        auto firstRow = result["cells"][0];
        ASSERT_EQ("cell_0_0", firstRow[0]["id"]);
        ASSERT_EQ(1, firstRow[0]["content"]);
        
        ASSERT_EQ("cell_0_1", firstRow[1]["id"]);
        ASSERT_EQ("Alice", firstRow[1]["content"]);
        
        ASSERT_EQ("cell_0_2", firstRow[2]["id"]);
        ASSERT_EQ(95.5, firstRow[2]["content"]);
        
        ASSERT_EQ("cell_0_3", firstRow[3]["id"]);
        ASSERT_EQ(true, firstRow[3]["content"]);
        
        logSuccess("Cells structure is correct");
        return true;
    }, "Test new JSON cells format structure");

    // Test 3: Specific column selection
    framework.addTest("JSON_FORMAT_03_Column_Selection", [suite]() {
        logTestStart("Column Selection Format");
        
        auto result = suite->queryResult("SELECT name, score FROM format_test");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        // Check header for selected columns
        ASSERT_EQ(2, result["header"].size());
        ASSERT_EQ("name", result["header"][0]["content"]);
        ASSERT_EQ("VARCHAR", result["header"][0]["type"]);
        ASSERT_EQ("score", result["header"][1]["content"]);
        ASSERT_EQ("DOUBLE", result["header"][1]["type"]);
        
        // Check cells for selected columns
        ASSERT_EQ(2, result["cells"].size()); // 2 rows
        for (const auto& row : result["cells"]) {
            ASSERT_EQ(2, row.size()); // 2 columns selected
        }
        
        // Check first row content
        auto firstRow = result["cells"][0];
        ASSERT_EQ("Alice", firstRow[0]["content"]);
        ASSERT_EQ(95.5, firstRow[1]["content"]);
        
        logSuccess("Column selection format is correct");
        return true;
    }, "Test JSON format with column selection");

    // Test 4: Empty result set
    framework.addTest("JSON_FORMAT_04_Empty_Result", [suite]() {
        logTestStart("Empty Result Format");
        
        auto result = suite->queryResult("SELECT * FROM format_test WHERE id > 1000");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        // Check that header still exists
        ASSERT_TRUE(result.contains("header"));
        ASSERT_EQ(4, result["header"].size());
        
        // Check that cells is empty array
        ASSERT_TRUE(result.contains("cells"));
        ASSERT_TRUE(result["cells"].is_array());
        ASSERT_EQ(0, result["cells"].size());
        
        logSuccess("Empty result format is correct");
        return true;
    }, "Test JSON format for empty result sets");

    // Test 5: Single row result
    framework.addTest("JSON_FORMAT_05_Single_Row", [suite]() {
        logTestStart("Single Row Format");
        
        auto result = suite->queryResult("SELECT * FROM format_test WHERE name = 'Alice'");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        // Check header
        ASSERT_EQ(4, result["header"].size());
        
        // Check single row
        ASSERT_EQ(1, result["cells"].size());
        auto row = result["cells"][0];
        ASSERT_EQ(4, row.size());
        
        // Verify content
        ASSERT_EQ(1, row[0]["content"]);
        ASSERT_EQ("Alice", row[1]["content"]);
        ASSERT_EQ(95.5, row[2]["content"]);
        ASSERT_EQ(true, row[3]["content"]);
        
        logSuccess("Single row format is correct");
        return true;
    }, "Test JSON format for single row result");

    // Test 6: NULL values in new format
    framework.addTest("JSON_FORMAT_06_NULL_Values", [suite]() {
        logTestStart("NULL Values Format");
        
        // Create table that allows NULLs
        suite->executeQuery(R"(
            CREATE TABLE null_format_test (
                id INT PRIMARY KEY,
                optional_name VARCHAR,
                optional_score DOUBLE
            )
        )");
        
        suite->executeQuery("INSERT INTO null_format_test (id) VALUES (1)");
        suite->executeQuery("INSERT INTO null_format_test VALUES (2, 'Bob', 85.5)");
        
        auto result = suite->queryResult("SELECT * FROM null_format_test");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        // Check first row with NULLs
        auto firstRow = result["cells"][0];
        ASSERT_EQ(1, firstRow[0]["content"]);
        ASSERT_TRUE(firstRow[1]["content"].is_null());
        ASSERT_TRUE(firstRow[2]["content"].is_null());
        
        // Check second row with values
        auto secondRow = result["cells"][1];
        ASSERT_EQ(2, secondRow[0]["content"]);
        ASSERT_EQ("Bob", secondRow[1]["content"]);
        ASSERT_EQ(85.5, secondRow[2]["content"]);
        
        logSuccess("NULL values handled correctly in new format");
        return true;
    }, "Test NULL value handling in new JSON format");

    // Test 7: Type consistency in format
    framework.addTest("JSON_FORMAT_07_Type_Consistency", [suite]() {
        logTestStart("Type Consistency in Format");
        
        // Create table with all data types
        suite->executeQuery(R"(
            CREATE TABLE all_types (
                int_col INT,
                double_col DOUBLE,
                varchar_col VARCHAR,
                boolean_col BOOLEAN
            )
        )");
        
        suite->executeQuery("INSERT INTO all_types VALUES (42, 3.14, 'test', TRUE)");
        
        auto result = suite->queryResult("SELECT * FROM all_types");
        ASSERT_TRUE(result.contains("status") && result["status"] == "success");
        
        // Verify header types
        ASSERT_EQ("INT", result["header"][0]["type"]);
        ASSERT_EQ("DOUBLE", result["header"][1]["type"]);
        ASSERT_EQ("VARCHAR", result["header"][2]["type"]);
        ASSERT_EQ("BOOLEAN", result["header"][3]["type"]);
        
        // Verify content types match JSON types
        auto row = result["cells"][0];
        ASSERT_TRUE(row[0]["content"].is_number_integer()); // INT
        ASSERT_TRUE(row[1]["content"].is_number_float());   // DOUBLE
        ASSERT_TRUE(row[2]["content"].is_string());         // VARCHAR
        ASSERT_TRUE(row[3]["content"].is_boolean());        // BOOLEAN
        
        logSuccess("Type consistency maintained in new format");
        return true;
    }, "Test type consistency in new JSON format");
}