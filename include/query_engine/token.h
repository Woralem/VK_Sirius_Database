#pragma once
#include <string>
#include <variant>
#include <ostream>

namespace query_engine {

    enum class TokenType {
        // Keywords
        SELECT, FROM, WHERE, INSERT, INTO, VALUES,
        UPDATE_KEYWORD,
        SET,
        DELETE_KEYWORD,
        CREATE, TABLE,
        AND, OR, NOT, NULL_TOKEN,

        // Identifiers and Literals
        IDENTIFIER, STRING_LITERAL, NUMBER_LITERAL,

        // Operators
        EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_EQUALS, GREATER_EQUALS,

        // Delimiters
        LEFT_PAREN, RIGHT_PAREN, COMMA, SEMICOLON, ASTERISK,

        // Special
        END_OF_FILE, UNKNOWN
    };

    using Value = std::variant<std::monostate, int, double, std::string, bool>;

    struct Token {
        TokenType type;
        std::string lexeme;
        Value value;
        size_t line;
        size_t column;

        Token(TokenType type, const std::string& lexeme, size_t line, size_t column)
            : type(type), lexeme(lexeme), line(line), column(column) {}
    };

    std::string tokenTypeToString(TokenType type);

    inline std::ostream& operator<<(std::ostream& os, const query_engine::TokenType& type) {
        os << tokenTypeToString(type);
        return os;
    }

}