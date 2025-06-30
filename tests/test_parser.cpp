#include "test_framework.h"
#include "query_engine/lexer.h"
#include "query_engine/parser.h"
#include <vector>

using namespace tests;
using namespace query_engine;

struct ParserTestCase {
    std::string name;
    std::string input;
    bool should_succeed;
};

bool run_parser_test_cases() {
    const std::vector<ParserTestCase> test_cases = {
    {"CREATE: Simple", "CREATE TABLE t (c1 INT)", true},
    {"CREATE: Two columns", "CREATE TABLE t (c1 INT, c2 VARCHAR)", true},
    {"CREATE: No table name", "CREATE TABLE (id INT)", false},
    {"General: Empty string", "", true},
    {"General: Whitespace only", " \n\t \r ", true},
    {"General: Just a semicolon", ";", true},
    {"General: Keyword as identifier (unquoted)", "SELECT select FROM from", false},
    {"General: Number at start of identifier", "CREATE TABLE 1t (id INT)", false},
};

    bool all_passed = true;
    int test_count = 0;
    for (const auto& tc : test_cases) {
        test_count++;
        Lexer lexer(tc.input);
        auto tokens = lexer.tokenize();
        Parser parser(tokens);
        parser.parse();

        bool success = !parser.hasError();
        if (success != tc.should_succeed) {
            std::cerr << "\nTC '" << tc.name << "' FAILED: Expected "
                      << (tc.should_succeed ? "SUCCESS" : "FAIL")
                      << ", but got " << (success ? "SUCCESS" : "FAIL") << std::endl;
            std::cerr << "  Input: \"" << tc.input << "\"" << std::endl;
            if(!success) {
                for(const auto& err : parser.getErrors()) {
                    std::cerr << "  - Parser error: " << err << std::endl;
                }
            }
            all_passed = false;
        }
    }
    std::cout << "\nParser data-driven test suite finished. Total cases: " << test_count << std::endl;
    return all_passed;
}

void addParserTests(TestFramework& framework) {
    framework.addTest("Parser: Data-Driven Tests", run_parser_test_cases,
                     "Run a suite of data-driven tests for the parser.");
}