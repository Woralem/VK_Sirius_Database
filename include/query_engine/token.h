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

    inline std::ostream& operator<<(std::ostream& os, const query_engine::TokenType& type) {
        switch (type) {
            case TokenType::SELECT:           os << "SELECT"; break;
            case TokenType::FROM:             os << "FROM"; break;
            case TokenType::WHERE:            os << "WHERE"; break;
            case TokenType::INSERT:           os << "INSERT"; break;
            case TokenType::INTO:             os << "INTO"; break;
            case TokenType::VALUES:           os << "VALUES"; break;
            // ИЗМЕНЕНО
            case TokenType::UPDATE_KEYWORD:   os << "UPDATE"; break;
            case TokenType::SET:              os << "SET"; break;
            // ИЗМЕНЕНО
            case TokenType::DELETE_KEYWORD:   os << "DELETE"; break;
            case TokenType::CREATE:           os << "CREATE"; break;
            case TokenType::TABLE:            os << "TABLE"; break;
            case TokenType::AND:              os << "AND"; break;
            case TokenType::OR:               os << "OR"; break;
            case TokenType::NOT:              os << "NOT"; break;
            case TokenType::NULL_TOKEN:       os << "NULL_TOKEN"; break;
            case TokenType::IDENTIFIER:       os << "IDENTIFIER"; break;
            case TokenType::STRING_LITERAL:   os << "STRING_LITERAL"; break;
            case TokenType::NUMBER_LITERAL:   os << "NUMBER_LITERAL"; break;
            case TokenType::EQUALS:           os << "EQUALS"; break;
            case TokenType::NOT_EQUALS:       os << "NOT_EQUALS"; break;
            case TokenType::LESS_THAN:        os << "LESS_THAN"; break;
            case TokenType::GREATER_THAN:     os << "GREATER_THAN"; break;
            case TokenType::LESS_EQUALS:      os << "LESS_EQUALS"; break;
            case TokenType::GREATER_EQUALS:   os << "GREATER_EQUALS"; break;
            case TokenType::LEFT_PAREN:       os << "LEFT_PAREN"; break;
            case TokenType::RIGHT_PAREN:      os << "RIGHT_PAREN"; break;
            case TokenType::COMMA:            os << "COMMA"; break;
            case TokenType::SEMICOLON:        os << "SEMICOLON"; break;
            case TokenType::ASTERISK:         os << "ASTERISK"; break;
            case TokenType::END_OF_FILE:      os << "END_OF_FILE"; break;
            case TokenType::UNKNOWN:          os << "UNKNOWN"; break;
            default: os << "UNKNOWN_TOKEN(" << static_cast<int>(type) << ")"; break;
        }
        return os;
    }

}