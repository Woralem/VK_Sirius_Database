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

class LikeTestSuite {
private:
    std::shared_ptr<OptimizedInMemoryStorage> storage;
    std::shared_ptr<OptimizedQueryExecutor> executor;

public:
    LikeTestSuite() {
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

            // Для INSERT проверяем, что действительно что-то вставилось
            if (success && result.contains("rows_affected")) {
                int affected = result["rows_affected"];
                if (affected == 0 && query.find("INSERT") != std::string::npos) {
                    std::cout << "INSERT returned success but 0 rows affected\n";
                    return false;
                }
            }

            if (!success && result.contains("message")) {
                std::cout << "Execution error: " << result["message"] << "\n";
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
        if (result.contains("data") && result["data"].is_array()) {
            return result["data"].size();
        }
        return -1;
    }

    bool setupTestData() {
        // Создаем таблицу пользователей
        std::string createTable = R"(
            CREATE TABLE users (
                id INT PRIMARY KEY,
                name VARCHAR NOT NULL,
                email VARCHAR,
                phone VARCHAR,
                age INT,
                salary DOUBLE,
                active BOOLEAN
            )
        )";

        if (!executeQuery(createTable)) {
            return false;
        }

        // Вставляем тестовые данные
        std::vector<std::string> insertQueries = {
            "INSERT INTO users VALUES (1, 'John Smith', 'john.smith@gmail.com', '555-123-4567', 25, 50000.50, TRUE)",
            "INSERT INTO users VALUES (2, 'Jane Doe', 'jane.doe@yahoo.com', '555-987-6543', 30, 65000.75, TRUE)",
            "INSERT INTO users VALUES (3, 'Johnny Cash', 'johnny@gmail.com', '444-555-1234', 35, 45000.00, FALSE)",
            "INSERT INTO users VALUES (4, 'Bob Johnson', 'bob.johnson@hotmail.com', '555-111-2222', 28, 55000.25, TRUE)",
            "INSERT INTO users VALUES (5, 'Alice Johnson', 'alice@company.com', '333-444-5555', 32, 70000.00, TRUE)",
            "INSERT INTO users VALUES (6, 'Mike OConnor', 'mike@gmail.com', '666-777-8888', 29, 48000.75, FALSE)",
            "INSERT INTO users VALUES (7, 'Sarah Wilson', 'sarah.wilson@outlook.com', '222-333-4444', 26, 52000.00, TRUE)",
            "INSERT INTO users VALUES (8, 'David Brown', 'david@company.com', '777-888-9999', 40, 80000.50, TRUE)",
            "INSERT INTO users VALUES (9, 'Emily Davis', 'emily.davis@gmail.com', '111-222-3333', 27, 58000.25, FALSE)",
            "INSERT INTO users VALUES (10, 'Tom Anderson', 'tom@yahoo.com', '999-000-1111', 33, 62000.00, TRUE)"
        };

        for (const auto& query : insertQueries) {
            if (!executeQuery(query)) {
                return false;
            }
        }

        return getRowCount("SELECT * FROM users") == 10;
    }
};

void addLikeTests(tests::TestFramework& framework) {
    auto suite = std::make_shared<LikeTestSuite>();

    // Setup test data
    framework.addTest("LIKE_00_Setup", [suite]() {
        return suite->setupTestData();
    }, "Setup test data for LIKE operations");

    // Test 1: Basic % wildcard - prefix matching
    framework.addTest("LIKE_01_Prefix_Matching", [suite]() {
        // Names starting with 'John'
        int count = suite->getRowCount("SELECT * FROM users WHERE name LIKE 'John%'");
        ASSERT_EQ(2, count); // John Smith, Johnny Cash
        return true;
    }, "Test LIKE with % for prefix matching");

    // Test 2: Basic % wildcard - suffix matching
    framework.addTest("LIKE_02_Suffix_Matching", [suite]() {
        // Emails ending with '@gmail.com'
        int count = suite->getRowCount("SELECT * FROM users WHERE email LIKE '%@gmail.com'");
        ASSERT_EQ(4, count); // john.smith@gmail.com, johnny@gmail.com, mike@gmail.com, emily.davis@gmail.com
        return true;
    }, "Test LIKE with % for suffix matching");

    // Test 3: Basic % wildcard - contains matching
    framework.addTest("LIKE_03_Contains_Matching", [suite]() {
        // Names containing 'son'
        int count = suite->getRowCount("SELECT * FROM users WHERE name LIKE '%son%'");
        ASSERT_EQ(4, count); // Bob Johnson, Alice Johnson, Sarah Wilson, Tom Anderson
        return true;
    }, "Test LIKE with % for contains matching");

    // Test 4: Single character wildcard _
    framework.addTest("LIKE_04_Single_Char_Wildcard", [suite]() {
        // Names with pattern 'J_hn%' (John but not Jane)
        int count = suite->getRowCount("SELECT * FROM users WHERE name LIKE 'J_hn%'");
        ASSERT_EQ(2, count); // John Smith, Johnny Cash
        return true;
    }, "Test LIKE with _ for single character matching");

    // Test 5: Phone format matching
    framework.addTest("LIKE_05_Phone_Format", [suite]() {
        // Standard phone format xxx-xxx-xxxx
        int count = suite->getRowCount("SELECT * FROM users WHERE phone LIKE '___-___-____'");
        ASSERT_EQ(10, count); // All phones should match this format
        return true;
    }, "Test LIKE with _ for format validation");

    // Test 6: Prefix with area code
    framework.addTest("LIKE_06_Area_Code_555", [suite]() {
        // Phones starting with 555
        int count = suite->getRowCount("SELECT * FROM users WHERE phone LIKE '555%'");
        ASSERT_EQ(3, count); // 555-123-4567, 555-987-6543, 555-111-2222
        return true;
    }, "Test LIKE for specific area code matching");

    // Test 7: Complex pattern with multiple wildcards
    framework.addTest("LIKE_07_Complex_Pattern", [suite]() {
        // Emails with pattern %@%.com
        int count = suite->getRowCount("SELECT * FROM users WHERE email LIKE '%@%.com'");
        ASSERT_EQ(10, count); // All emails end with .com
        return true;
    }, "Test LIKE with complex patterns");

    // Test 8: Case sensitivity
    framework.addTest("LIKE_08_Case_Sensitivity", [suite]() {
        // Test lowercase vs uppercase
        int count1 = suite->getRowCount("SELECT * FROM users WHERE name LIKE 'john%'");
        int count2 = suite->getRowCount("SELECT * FROM users WHERE name LIKE 'John%'");
        ASSERT_EQ(0, count1); // Should be case sensitive
        ASSERT_EQ(2, count2); // Should match John Smith, Johnny Cash
        return true;
    }, "Test LIKE case sensitivity");

    // Test 9: LIKE with AND condition
    framework.addTest("LIKE_09_LIKE_AND_Condition", [suite]() {
        // Gmail users with age > 30
        int count = suite->getRowCount("SELECT * FROM users WHERE email LIKE '%@gmail.com' AND age > 30");
        ASSERT_EQ(1, count); // Johnny Cash (35)
        return true;
    }, "Test LIKE combined with AND condition");

    // Test 10: LIKE with OR condition
    framework.addTest("LIKE_10_LIKE_OR_Condition", [suite]() {
        // Gmail or Yahoo users
        int count = suite->getRowCount("SELECT * FROM users WHERE email LIKE '%@gmail.com' OR email LIKE '%@yahoo.com'");
        ASSERT_EQ(6, count); // 4 Gmail + 2 Yahoo
        return true;
    }, "Test LIKE combined with OR condition");

    // Test 11: Multiple LIKE conditions
    framework.addTest("LIKE_11_Multiple_LIKE", [suite]() {
        // Names starting with J and containing 'n'
        int count = suite->getRowCount("SELECT * FROM users WHERE name LIKE 'J%' AND name LIKE '%n%'");
        ASSERT_EQ(3, count); // John Smith, Johnny Cash, Jane Doe
        return true;
    }, "Test multiple LIKE conditions");

    // Test 12: LIKE with no matches
    framework.addTest("LIKE_12_No_Matches", [suite]() {
        // Pattern that shouldn't match anything
        int count = suite->getRowCount("SELECT * FROM users WHERE name LIKE 'XYZ%'");
        ASSERT_EQ(0, count);
        return true;
    }, "Test LIKE with pattern that matches nothing");

    // Test 13: LIKE with empty pattern behavior
    framework.addTest("LIKE_13_Match_All_Pattern", [suite]() {
        // % should match all non-null strings
        int count1 = suite->getRowCount("SELECT * FROM users WHERE name LIKE '%'");
        int count2 = suite->getRowCount("SELECT * FROM users");
        ASSERT_EQ(count2, count1); // Should match all records
        return true;
    }, "Test LIKE with % pattern (match all)");

    // Test 14: Specific email domains
    framework.addTest("LIKE_14_Domain_Matching", [suite]() {
        // Test different domains
        int gmailCount = suite->getRowCount("SELECT * FROM users WHERE email LIKE '%@gmail.com'");
        int yahooCount = suite->getRowCount("SELECT * FROM users WHERE email LIKE '%@yahoo.com'");
        int companyCount = suite->getRowCount("SELECT * FROM users WHERE email LIKE '%@company.com'");
        int hotmailCount = suite->getRowCount("SELECT * FROM users WHERE email LIKE '%@hotmail.com'");
        int outlookCount = suite->getRowCount("SELECT * FROM users WHERE email LIKE '%@outlook.com'");

        ASSERT_EQ(4, gmailCount);
        ASSERT_EQ(2, yahooCount);
        ASSERT_EQ(2, companyCount);
        ASSERT_EQ(1, hotmailCount);
        ASSERT_EQ(1, outlookCount);
        return true;
    }, "Test LIKE for different email domains");

    // Test 15: Name patterns with underscore
    framework.addTest("LIKE_15_Underscore_Patterns", [suite]() {
        // Names with exactly 3 characters starting with T
        int count = suite->getRowCount("SELECT * FROM users WHERE name LIKE 'T__'");
        ASSERT_EQ(0, count); // No 3-letter names starting with T

        // Names with pattern like 'M_ke%'
        int count2 = suite->getRowCount("SELECT * FROM users WHERE name LIKE 'M_ke%'");
        ASSERT_EQ(1, count2); // Mike OConnor
        return true;
    }, "Test LIKE with underscore for exact character matching");

    // Test 16: Complex phone patterns
    framework.addTest("LIKE_16_Complex_Phone_Patterns", [suite]() {
        // Phones with middle digits '777'
        int count1 = suite->getRowCount("SELECT * FROM users WHERE phone LIKE '%777%'");
        ASSERT_EQ(2, count1); // 666-777-8888, 777-888-9999

        // Phones ending in '4444'
        int count2 = suite->getRowCount("SELECT * FROM users WHERE phone LIKE '%4444'");
        ASSERT_EQ(1, count2); // 222-333-4444

        return true;
    }, "Test LIKE with complex phone number patterns");

    // Test 17: UPDATE with LIKE
    framework.addTest("LIKE_17_UPDATE_With_LIKE", [suite]() {
        // Update all Gmail users to inactive (FALSE)
        bool success = suite->executeQuery("UPDATE users SET active = FALSE WHERE email LIKE '%@gmail.com'");
        ASSERT_TRUE(success);

        // Check that Gmail users are now inactive
        int inactiveGmail = suite->getRowCount("SELECT * FROM users WHERE email LIKE '%@gmail.com' AND active = FALSE");
        ASSERT_EQ(4, inactiveGmail);

        return true;
    }, "Test UPDATE statement with LIKE condition");

    // Test 18: Complex combinations
    framework.addTest("LIKE_18_Complex_Combinations", [suite]() {
        // Users with names containing 'o' and Gmail addresses
        int count = suite->getRowCount("SELECT * FROM users WHERE name LIKE '%o%' AND email LIKE '%@gmail.com'");
        ASSERT_EQ(3, count); // John Smith, Mike OConnor, Johnny Cash
        return true;
    }, "Test complex LIKE combinations");

    // Test 19: Final validation
    framework.addTest("LIKE_19_Final_Validation", [suite]() {
        // Verify total record count hasn't changed (except for updates)
        int totalCount = suite->getRowCount("SELECT * FROM users");
        ASSERT_EQ(10, totalCount);

        // Test that all emails still contain @
        int emailCount = suite->getRowCount("SELECT * FROM users WHERE email LIKE '%@%'");
        ASSERT_EQ(10, emailCount);

        return true;
    }, "Final validation of LIKE functionality");
}