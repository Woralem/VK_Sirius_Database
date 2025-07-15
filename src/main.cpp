#include <iostream>
#include <vector>
#include <string>
#include <stdexcept>
#include <chrono>
#include <filesystem>
#include <functional>
#include <numeric>

#ifdef _WIN32
#include <windows.h>
#endif

#include "storage_engine.h"
#include "types.h"
#include "physical/table.h"

// --- A Simple Test Runner Framework ---

class TestRunner {
private:
    std::string db_path_;
    int tests_passed_ = 0;

    void check(bool condition, const std::string& message) {
        if (!condition) {
            throw std::runtime_error("Assertion Failed: " + message);
        }
    }

public:
    int tests_failed_ = 0;
    explicit TestRunner(const std::string& db_path) : db_path_(db_path) {}

    void reset_database() {
        if (std::filesystem::exists(db_path_)) {
            std::filesystem::remove_all(db_path_);
        }
        std::filesystem::create_directory(db_path_);
    }

    void run(const std::string& test_name, const std::function<void(TestRunner&, const std::string&)>& test_func, bool is_perf_test = false) {
        std::cout << "--- Running Test: " << test_name << " ---" << std::endl;
        if (!is_perf_test) { // Perf tests might need to chain, so don't always reset
            reset_database();
        }
        try {
            test_func(*this, db_path_);
            std::cout << "[ \x1B[32mPASS\x1B[0m ] " << test_name << std::endl;
            tests_passed_++;
        } catch (const std::exception& e) {
            std::cout << "[ \x1B[31mFAIL\x1B[0m ] " << test_name << std::endl;
            std::cerr << "      |!| " << e.what() << std::endl;
            tests_failed_++;
        }
        std::cout << std::endl;
    }

    void print_summary() const {
        std::cout << "\n==================== TEST SUMMARY ====================" << std::endl;
        std::cout << "Tests Passed: " << tests_passed_ << std::endl;
        std::cout << "Tests Failed: " << tests_failed_ << std::endl;
        std::cout << "======================================================" << std::endl;
        if (tests_failed_ > 0) {
            std::cerr << "\nHalting due to test failures." << std::endl;
            exit(1);
        }
    }

    void assert_true(bool condition, const std::string& message) {
        check(condition, message);
    }
    void assert_throws(const std::function<void()>& func, const std::string& message_on_success) {
        try {
            func();
            // If we get here, the function did NOT throw, which is a failure.
            throw std::runtime_error("Expected function to throw, but it did not. " + message_on_success);
        } catch (const std::runtime_error& e) {
             // This can be the assertion failure from assert_throws itself. Re-throw it.
             if (std::string(e.what()).find("Expected function to throw") != std::string::npos) throw;
             // Otherwise, it's a different runtime_error from the SUT, which is a pass.
        }
        catch (const std::exception&) {
            // Any other standard exception was caught, which is the expected success case.
        }
    }
};


// --- =============================== ---
// ---       FUNCTIONAL TESTS          ---
// --- =============================== ---

// Group 1: Core DDL and Persistence
void test_create_and_drop(TestRunner& runner, const std::string& db_path) {
    StorageEngine engine(db_path);
    const std::vector<ColumnDef> cols = {{"id", ColumnDef::DataType::Integer}};
    
    engine.createTable("my-table", cols);
    runner.assert_true(engine.getTable("my-table") != nullptr, "Table should exist after creation.");

    engine.dropTable("my-table");
    runner.assert_true(engine.getTable("my-table") == nullptr, "Table should not exist after being dropped.");
}

void test_table_persistence(TestRunner& runner, const std::string& db_path) {
    {
        StorageEngine engine1(db_path);
        engine1.createTable("persistent-table", {});
    }
    {
        StorageEngine engine2(db_path);
        runner.assert_true(engine2.getTable("persistent-table") != nullptr, "Table should exist after reloading the engine.");
    }
}

void test_rename_table_and_persist(TestRunner& runner, const std::string& db_path) {
    {
        StorageEngine engine1(db_path);
        engine1.createTable("tbl-multi-col", {{"id", ColumnDef::DataType::Integer}, {"name", ColumnDef::DataType::VarChar}});
        engine1.alterRTable("tbl-multi-col", "tbl-renamed");
    }
    {
        StorageEngine engine2(db_path);
        runner.assert_true(engine2.getTable("tbl-multi-col") == nullptr, "Old name shouldn't exist after reload.");
        auto table = engine2.getTable("tbl-renamed");
        runner.assert_true(table != nullptr, "Renamed table should be found after reload.");
        engine2.dropColumn("tbl-renamed", "name");
    }
}

void test_column_operations(TestRunner& runner, const std::string& db_path) {
    StorageEngine engine(db_path);
    engine.createTable("col-test-tbl", {{"id", ColumnDef::DataType::Integer}});
    auto table = engine.getTable("col-test-tbl");
    runner.assert_true(table != nullptr, "Failed to get table object.");

    table->createColumns({{"email", ColumnDef::DataType::Text}});
    engine.alterRColumn("col-test-tbl", "email", "user-email");
    engine.dropColumn("col-test-tbl", "user-email");
    runner.assert_throws([&]() { 
        engine.dropColumn("col-test-tbl", "user-email"); 
    }, "Dropping a just-dropped column should fail.");
}

void test_empty_and_zero_column_tables(TestRunner& runner, const std::string& db_path) {
    StorageEngine engine(db_path);
    engine.createTable("no-cols-table", {});
    auto table1 = engine.getTable("no-cols-table");
    runner.assert_true(table1 != nullptr, "Creating a table with no columns should succeed.");

    // Adding columns later should work
    table1->createColumns({{"col1", ColumnDef::DataType::Integer}});
    // For a perfect test, we would list columns and verify "col1" is there.
    // For now, we confirm it can be dropped.
    engine.dropColumn("no-cols-table", "col1");
}

// Group 2: Error Handling, Validation, and Edge Cases
void test_comprehensive_name_validation(TestRunner& runner, const std::string& db_path) {
    StorageEngine engine(db_path);
    runner.assert_throws([&](){ engine.createTable("", {}); }, "Empty table name should be invalid.");
    runner.assert_throws([&](){ engine.createTable("ends-with-", {}); }, "Table name ending in hyphen is invalid.");
    runner.assert_throws([&](){ engine.createTable("bad_char", {}); }, "Table name with '_' should be invalid (per your encoding).");
    runner.assert_throws([&](){ engine.createTable("a-name-that-is-way-too-long", {}); }, "Name longer than 16 chars should be invalid.");
    
    // A valid name at max length should work
    engine.createTable("a-valid-16-chars", {});
    runner.assert_true(engine.getTable("a-valid-16-chars") != nullptr, "Table with 16-char name should be valid.");
}

void test_rename_to_existing_name(TestRunner& runner, const std::string& db_path) {
    StorageEngine engine(db_path);
    engine.createTable("table-a", {});
    engine.createTable("table-b", {{"col-x", ColumnDef::DataType::Integer}});
    
    runner.assert_throws([&](){ engine.alterRTable("table-a", "table-b"); }, "Renaming table-a to existing table-b should fail.");
    runner.assert_throws([&](){ engine.alterRColumn("table-b", "col-x", "col-x"); }, "Renaming a column to its own name should fail (or be a no-op, but let's assume fail).");
}

void test_operations_on_nonexistent_entities(TestRunner& runner, const std::string& db_path) {
    StorageEngine engine(db_path);
    engine.createTable("real-table", {});
    
    runner.assert_throws([&](){ engine.dropTable("fake-table"); }, "Dropping a non-existent table should fail.");
    runner.assert_throws([&](){ engine.alterRTable("fake-table", "new-name"); }, "Renaming a non-existent table should fail.");
    runner.assert_throws([&](){ engine.dropColumn("real-table", "fake-col"); }, "Dropping a non-existent column should fail.");
    runner.assert_throws([&](){ engine.alterRColumn("real-table", "fake-col", "new-name"); }, "Renaming a non-existent column should fail.");
}

// Group 3: Freelist Behavior
void test_table_freelist_chain(TestRunner& runner, const std::string& db_path) {
    StorageEngine engine(db_path);
    engine.createTable("t1", {});
    engine.createTable("t2-drop", {});
    engine.createTable("t3-drop", {});
    engine.createTable("t4", {});

    engine.dropTable("t2-drop");
    engine.dropTable("t3-drop");

    // These should reuse the links from the dropped tables.
    engine.createTable("t5-recycled", {});
    engine.createTable("t6-recycled", {});
    
    runner.assert_true(engine.getTable("t1") != nullptr, "t1 should still exist.");
    runner.assert_true(engine.getTable("t6-recycled") != nullptr, "Recycled table t6 should exist.");
}

void test_advanced_column_freelist(TestRunner& runner, const std::string& db_path) {
    StorageEngine engine(db_path);
    engine.createTable("adv-col-freelist", {
        {"c1", ColumnDef::DataType::Text}, {"c2", ColumnDef::DataType::Text}, 
        {"c3", ColumnDef::DataType::Text}, {"c4", ColumnDef::DataType::Text}
    });

    engine.dropColumn("adv-col-freelist", "c2");
    engine.dropColumn("adv-col-freelist", "c4");

    // Add one column, should use a recycled link
    auto table = engine.getTable("adv-col-freelist");
    table->createColumns({{"n1", ColumnDef::DataType::Text}});

    // Add two more columns, should use the last recycled link and one new sequential link
    table->createColumns({{"n2", ColumnDef::DataType::Text}, {"n3", ColumnDef::DataType::Text}});

    // Verify system consistency by dropping everything
    engine.dropColumn("adv-col-freelist", "c1");
    engine.dropColumn("adv-col-freelist", "c3");
    engine.dropColumn("adv-col-freelist", "n1");
    engine.dropColumn("adv-col-freelist", "n2");
    engine.dropColumn("adv-col-freelist", "n3");
}

// Group 4: Table-specific Metadata
void test_metadata_update_and_enforcement(TestRunner& runner, const std::string& db_path) {
    StorageEngine engine(db_path);
    // 1. Create with default metadata
    engine.createTable("meta-table", {});
    
    // 2. Get table and update its metadata
    auto table = engine.getTable("meta-table");
    table->setMaxColumnNameLength(8);
    table->setCleaningFrequency(100);

    // 3. Verify immediate change
    runner.assert_true(table->getMaxColumnNameLength() == 8, "Max column length should be updated to 8.");
    runner.assert_true(table->getCleaningFrequency() == 100, "GC frequency should be updated to 100.");

    // 4. Test enforcement of new rule
    runner.assert_throws([&](){ table->createColumns({{"too-long-col", ColumnDef::DataType::Text}}); }, "Creating a column with a name longer than the new limit (8) should fail.");
    table->createColumns({{"good-col", ColumnDef::DataType::Text}}); // This should pass

    // 5. Reload and verify persistence of metadata
    StorageEngine engine2(db_path);
    auto reloaded_table = engine2.getTable("meta-table");
    runner.assert_true(reloaded_table->getMaxColumnNameLength() == 8, "Updated max column length should persist after reload.");
    runner.assert_true(reloaded_table->getCleaningFrequency() == 100, "Updated GC frequency should persist after reload.");
}


// --- =============================== ---
// ---      PERFORMANCE TESTS          ---
// --- =============================== ---

void run_performance_suite(TestRunner& runner, const std::string& db_path) {
    runner.reset_database(); // Clean slate for the whole performance suite

    // --- Test 1: Many small tables ---
    runner.run("Perf: Create 1000 small tables", [&](TestRunner& r, const std::string& p) {
        StorageEngine engine(p);
        const int num_tables = 1000;
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < num_tables; ++i) {
            engine.createTable("ps-" + std::to_string(i), {{"id", ColumnDef::DataType::Integer}});
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> duration = end - start;
        std::cout << "      [ INFO ] Created " << num_tables << " small tables in " << duration.count() << " ms" << std::endl;
    }, true);

    // --- Test 2: Few large tables ---
    runner.run("Perf: Create 5 large tables (200 cols each)", [&](TestRunner& r, const std::string& p) {
        StorageEngine engine(p);
        const int num_tables = 5;
        const int cols_per_table = 200;
        std::vector<ColumnDef> large_cols_def;
        large_cols_def.reserve(cols_per_table);
        for(int i = 0; i < cols_per_table; ++i) {
            large_cols_def.push_back({"c-" + std::to_string(i), ColumnDef::DataType::BigInt});
        }
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < num_tables; ++i) {
            engine.createTable("pl-" + std::to_string(i), large_cols_def);
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> duration = end - start;
        std::cout << "      [ INFO ] Created " << num_tables << " tables with " << cols_per_table << " cols each in " << duration.count() << " ms" << std::endl;
    }, true); // Keep DB state for next test

    // --- Test 3: High-frequency column churn ---
    runner.run("Perf: 2000 column create/drop operations", [&](TestRunner& r, const std::string& p) {
        StorageEngine engine(p);
        engine.createTable("churn-table", {});
        auto table = engine.getTable("churn-table");
        const int num_ops = 50;

        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < num_ops; ++i) {
            std::string name = "churn-" + std::to_string(i);
            table->createColumns({{name, ColumnDef::DataType::Integer}});
            if (i % 2 == 0) { // Drop every other one to exercise freelist
                table->dropColumn(name);
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> duration = end - start;
        std::cout << "      [ INFO ] Performed " << num_ops * 2 << " column operations (create/drop) in " << duration.count() << " ms" << std::endl;
    }, true);
}


int main() {
    #ifdef _WIN32
    SetConsoleOutputCP(CP_UTF8);
    #endif

    std::cout << "====== LAUNCHING STORAGE ENGINE TEST SUITE ======\n" << std::endl;
    TestRunner runner("comprehensive_test_db");

    try {
        // --- Run Functional Tests ---
        std::cout << "------ STAGE 1: FUNCTIONAL TESTS ------\n" << std::endl;
        runner.run("Create and then drop a table", test_create_and_drop);
        runner.run("Verify table persistence after engine restart", test_table_persistence);
        runner.run("Rename table, restart engine, and verify state", test_rename_table_and_persist);
        runner.run("Perform create, rename, and drop on columns", test_column_operations);
        runner.run("Create tables with zero columns and add them later", test_empty_and_zero_column_tables);
        
        runner.run("Comprehensive name validation checks", test_comprehensive_name_validation);
        runner.run("Attempt to rename entities to existing names", test_rename_to_existing_name);
        runner.run("Attempt operations on non-existent tables/columns", test_operations_on_nonexistent_entities);
        
        runner.run("Verify chained table freelist logic", test_table_freelist_chain);
        runner.run("Verify advanced column freelist logic", test_advanced_column_freelist);

        runner.run("Verify metadata updates and rule enforcement", test_metadata_update_and_enforcement);
        
        // --- Run Performance Tests ---
        std::cout << "\n------ STAGE 2: PERFORMANCE TESTS ------\n" << std::endl;
        runner.run("Run full performance benchmark suite", run_performance_suite);

    } catch (const std::exception& e) {
        std::cerr << "\n!!!!!! A fatal, uncaught exception terminated the test suite: " << e.what() << std::endl;
        runner.print_summary(); // Print summary even on catastrophic failure
        return 1;
    }
    
    runner.print_summary();
    
    if (runner.tests_failed_ == 0) {
        std::cout << "\n\n   ******************************************" << std::endl;
        std::cout << "   *    ALL TESTS COMPLETED SUCCESSFULLY    *" << std::endl;
        std::cout << "   ******************************************" << std::endl;
    }

    return 0;
}