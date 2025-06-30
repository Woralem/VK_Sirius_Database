#include "test_framework.h"
#include "query_engine/lexer.h"
#include <vector>

using namespace tests;
using namespace query_engine;

struct LexerTestCase {
    std::string name;
    std::string input;
    std::vector<TokenType> expected_types;
};

bool run_lexer_test_cases() {
    const std::vector<LexerTestCase> test_cases = {
        {"Keywords", "SELECT FROM WHERE", {TokenType::SELECT, TokenType::FROM, TokenType::WHERE, TokenType::END_OF_FILE}},
        {"Keywords Case-Insensitive", "select from wHeRe", {TokenType::SELECT, TokenType::FROM, TokenType::WHERE, TokenType::END_OF_FILE}},
        {"Identifiers", "a _b c123", {TokenType::IDENTIFIER, TokenType::IDENTIFIER, TokenType::IDENTIFIER, TokenType::END_OF_FILE}},
        {"Numbers", "123 45.67 0.9", {TokenType::NUMBER_LITERAL, TokenType::NUMBER_LITERAL, TokenType::NUMBER_LITERAL, TokenType::END_OF_FILE}},
        {"Strings", "'hello' 'world'", {TokenType::STRING_LITERAL, TokenType::STRING_LITERAL, TokenType::END_OF_FILE}},
        {"Escaped String", "'it''s great'", {TokenType::STRING_LITERAL, TokenType::END_OF_FILE}},
        {"Operators", "= != < > <= >=", {TokenType::EQUALS, TokenType::NOT_EQUALS, TokenType::LESS_THAN, TokenType::GREATER_THAN, TokenType::LESS_EQUALS, TokenType::GREATER_EQUALS, TokenType::END_OF_FILE}},
        {"Alternative Not Equals", "<>", {TokenType::NOT_EQUALS, TokenType::END_OF_FILE}},
        {"Delimiters", "() , ; *", {TokenType::LEFT_PAREN, TokenType::RIGHT_PAREN, TokenType::COMMA, TokenType::SEMICOLON, TokenType::ASTERISK, TokenType::END_OF_FILE}},
        {"Complex Query 1", "SELECT id, name FROM users;", {TokenType::SELECT, TokenType::IDENTIFIER, TokenType::COMMA, TokenType::IDENTIFIER, TokenType::FROM, TokenType::IDENTIFIER, TokenType::SEMICOLON, TokenType::END_OF_FILE}},
        {"Complex Query 2", "INSERT INTO t (c) VALUES (1);", {TokenType::INSERT, TokenType::INTO, TokenType::IDENTIFIER, TokenType::LEFT_PAREN, TokenType::IDENTIFIER, TokenType::RIGHT_PAREN, TokenType::VALUES, TokenType::LEFT_PAREN, TokenType::NUMBER_LITERAL, TokenType::RIGHT_PAREN, TokenType::SEMICOLON, TokenType::END_OF_FILE}},
        {"Comment Simple", "SELECT --comment\nid", {TokenType::SELECT, TokenType::IDENTIFIER, TokenType::END_OF_FILE}},
        {"Comment at end", "SELECT 1 --comment", {TokenType::SELECT, TokenType::NUMBER_LITERAL, TokenType::END_OF_FILE}},
        {"Empty Input", "", {TokenType::END_OF_FILE}},
        {"Whitespace Input", "  \t\n  ", {TokenType::END_OF_FILE}},
        {"Unterminated String", "'hello", {TokenType::UNKNOWN, TokenType::END_OF_FILE}},
        {"Invalid Character", "SELECT @", {TokenType::SELECT, TokenType::UNKNOWN, TokenType::END_OF_FILE}},
        {"Invalid Number", "1.2.3", {TokenType::NUMBER_LITERAL, TokenType::UNKNOWN, TokenType::NUMBER_LITERAL, TokenType::END_OF_FILE}},
    };

    bool all_passed = true;
    for (const auto& tc : test_cases) {
        Lexer lexer(tc.input);
        auto tokens = lexer.tokenize();

        bool test_passed = true;
        if (tokens.size() != tc.expected_types.size()) {
            std::cerr << "TC '" << tc.name << "' FAILED: Token count mismatch. Expected " << tc.expected_types.size() << ", got " << tokens.size() << std::endl;
            test_passed = false;
        } else {
            for (size_t i = 0; i < tokens.size(); ++i) {
                if (tokens[i].type != tc.expected_types[i]) {
                    std::cerr << "TC '" << tc.name << "' FAILED: Token type mismatch at index " << i << ". Expected " << tc.expected_types[i] << ", got " << tokens[i].type << std::endl;
                    test_passed = false;
                    break;
                }
            }
        }

        if (!test_passed) {
            all_passed = false;
        }
    }
    return all_passed;
}

void addLexerTests(TestFramework& framework) {
    framework.addTest("Lexer: Data-Driven Tests", run_lexer_test_cases,
                     "Run a suite of data-driven tests for the lexer.");
}