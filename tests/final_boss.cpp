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

bool test_the_unbreakable_gauntlet() {
    logTestStart("FINAL BOSS: The Unbreakable Gauntlet");
    
    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    auto executor = std::make_shared<OptimizedQueryExecutor>(storage);
    executor->setLoggingEnabled(false);

    logStep("PHASE 1: The Architect - Setting up the battlefield...");
    
    // Таблица с большим количеством ограничений и опций
    std::string create_users_q = R"(
        CREATE TABLE users (
            user_id INT PRIMARY KEY,
            email VARCHAR NOT NULL,
            reputation INT,
            is_active BOOLEAN
        ) WITH OPTIONS (
            MAX_STRING_LENGTH = 50,
            TYPES = [INT, VARCHAR, BOOLEAN]
        )
    )";
    json r1 = executor->execute(Parser(Lexer(create_users_q).tokenize()).parse());
    ASSERT_EQ(std::string(r1["status"]), "success");

    std::string create_logs_q = "CREATE TABLE event_logs (log_id INT, message VARCHAR)";
    json r2 = executor->execute(Parser(Lexer(create_logs_q).tokenize()).parse());
    ASSERT_EQ(std::string(r2["status"]), "success");
    logSuccess("Phase 1 complete: Tables created.");

    logStep("PHASE 2: The Hoard - Populating with valid and invalid data...");

    executor->execute(Parser(Lexer("INSERT INTO users VALUES (1, 'alice@a.com', 100, true), (2, 'bob@b.com', 50, false)").tokenize()).parse());

    json r_pk_viol = executor->execute(Parser(Lexer("INSERT INTO users VALUES (1, 'eve@e.com', 0, true)").tokenize()).parse());
    ASSERT_EQ(r_pk_viol["rows_affected"], 0);

    json r_not_null_viol = executor->execute(Parser(Lexer("INSERT INTO users VALUES (3, NULL, 0, true)").tokenize()).parse());
    ASSERT_EQ(r_not_null_viol["rows_affected"], 0);

    std::string long_email(51, 'a');
    json r_strlen_viol = executor->execute(Parser(Lexer("INSERT INTO users VALUES (4, '"+long_email+"@a.com', 0, true)").tokenize()).parse());
    ASSERT_EQ(r_strlen_viol["rows_affected"], 0);

    json r_type_viol = executor->execute(Parser(Lexer("INSERT INTO users VALUES (5, 'carol@c.com', 'HIGH', true)").tokenize()).parse());
    ASSERT_EQ(r_type_viol["rows_affected"], 0);

    // Проверка состояния после всех вставок
    json r_select_users = executor->execute(Parser(Lexer("SELECT * FROM users").tokenize()).parse());
    ASSERT_EQ(r_select_users["data"].size(), 2);
    logSuccess("Phase 2 complete: Constraint validators are working.");

    logStep("PHASE 3: The Mutation - Testing data integrity under modification...");

    json r_upd_pk_viol = executor->execute(Parser(Lexer("UPDATE users SET user_id = 1 WHERE user_id = 2").tokenize()).parse());
    ASSERT_EQ(r_upd_pk_viol["rows_affected"], 0);

    json r_upd_ok = executor->execute(Parser(Lexer("UPDATE users SET reputation = 150, is_active = false WHERE user_id = 1").tokenize()).parse());
    ASSERT_EQ(r_upd_ok["rows_affected"], 1);

    for (int i=0; i < 2000; ++i) {
        storage->insertRow("event_logs", {"log_id", "message"}, {i, "log entry"});
    }

    auto start_del = std::chrono::high_resolution_clock::now();
    int deleted_count = storage->deleteRows("event_logs", [](const json& row){ return row["log_id"].get<int>() < 1000; });
    auto end_del = std::chrono::high_resolution_clock::now();
    auto duration_del = std::chrono::duration_cast<std::chrono::milliseconds>(end_del - start_del);

    ASSERT_EQ(deleted_count, 1000);
    if(duration_del.count() > 500) {
        logError("PERFORMANCE WEAKNESS: Mass delete is too slow (" + std::to_string(duration_del.count()) + " ms).");
        return false;
    }
    logSuccess("Phase 3 complete: Data integrity maintained, DELETE is performant.");

    logStep("PHASE 4: The Inquisition - Verifying final state with complex queries...");

    json r_select_updated = executor->execute(Parser(Lexer("SELECT reputation, is_active FROM users WHERE user_id = 1").tokenize()).parse());
    ASSERT_EQ(r_select_updated["data"][0]["reputation"], 150);
    ASSERT_EQ(r_select_updated["data"][0]["is_active"], false);

    json r_cross_type = executor->execute(Parser(Lexer("SELECT * FROM users WHERE reputation > 50.5").tokenize()).parse());
    ASSERT_EQ(r_cross_type["data"].size(), 1);

    json r_select_logs = executor->execute(Parser(Lexer("SELECT * FROM event_logs").tokenize()).parse());
    ASSERT_EQ(r_select_logs["data"].size(), 1000);
    logSuccess("Phase 4 complete: Database state is consistent and correct.");

    logStep("PHASE 5: The Cataclysm - Testing parser resilience to multiple errors...");

    std::string multi_error_query = "CREATE TABLE; SELECT name FROM users WHERE user_id =; DROP TABLE;";
    Parser parser(Lexer(multi_error_query).tokenize());
    parser.parse();
    const auto& errors = parser.getErrors();
    
    if (errors.size() < 2) {
        logError("WEAKNESS DETECTED: Parser did not recover to report multiple errors.");
        return false;
    }
    logSuccess("Phase 5 complete: Parser correctly reported multiple errors.");

    return true;
}

void addFinalBossTest(TestFramework& framework) {
    framework.addTest("THE UNBREAKABLE GAUNTLET", test_the_unbreakable_gauntlet);
}