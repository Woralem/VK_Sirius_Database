#include "query_engine/lexer.h"
#include <cctype>
#include <algorithm>

namespace query_engine {

thread_local char Lexer::upperBuffer[256];

std::string tokenTypeToString(TokenType type) {
    switch (type) {
        case TokenType::SELECT: return "SELECT";
        case TokenType::FROM: return "FROM";
        case TokenType::WHERE: return "WHERE";
        case TokenType::INSERT: return "INSERT";
        case TokenType::INTO: return "INTO";
        case TokenType::VALUES: return "VALUES";
        case TokenType::UPDATE_KEYWORD: return "UPDATE";
        case TokenType::SET: return "SET";
        case TokenType::DELETE_KEYWORD: return "DELETE";
        case TokenType::CREATE: return "CREATE";
        case TokenType::TABLE: return "TABLE";
        case TokenType::AND: return "AND";
        case TokenType::OR: return "OR";
        case TokenType::NOT: return "NOT";
        case TokenType::NULL_TOKEN: return "NULL";
        case TokenType::ALTER: return "ALTER";
        case TokenType::RENAME: return "RENAME";
        case TokenType::TO: return "TO";
        case TokenType::COLUMN: return "COLUMN";
        case TokenType::TYPE: return "TYPE";
        case TokenType::LIKE: return "LIKE";
        case TokenType::WITH: return "WITH";
        case TokenType::OPTIONS: return "OPTIONS";
        case TokenType::TYPES: return "TYPES";
        case TokenType::MAX_COLUMN_LENGTH: return "MAX_COLUMN_LENGTH";
        case TokenType::ADDITIONAL_CHARS: return "ADDITIONAL_CHARS";
        case TokenType::MAX_STRING_LENGTH: return "MAX_STRING_LENGTH";
        case TokenType::GC_FREQUENCY: return "GC_FREQUENCY";
        case TokenType::DAYS: return "DAYS";
        case TokenType::IDENTIFIER: return "IDENTIFIER";
        case TokenType::STRING_LITERAL: return "STRING_LITERAL";
        case TokenType::NUMBER_LITERAL: return "NUMBER_LITERAL";
        case TokenType::EQUALS: return "EQUALS";
        case TokenType::NOT_EQUALS: return "NOT_EQUALS";
        case TokenType::LESS_THAN: return "LESS_THAN";
        case TokenType::GREATER_THAN: return "GREATER_THAN";
        case TokenType::LESS_EQUALS: return "LESS_EQUALS";
        case TokenType::GREATER_EQUALS: return "GREATER_EQUALS";
        case TokenType::LEFT_PAREN: return "LEFT_PAREN";
        case TokenType::RIGHT_PAREN: return "RIGHT_PAREN";
        case TokenType::COMMA: return "COMMA";
        case TokenType::SEMICOLON: return "SEMICOLON";
        case TokenType::ASTERISK: return "ASTERISK";
        case TokenType::LEFT_BRACKET: return "LEFT_BRACKET";
        case TokenType::RIGHT_BRACKET: return "RIGHT_BRACKET";
        case TokenType::END_OF_FILE: return "EOF";
        case TokenType::UNKNOWN: return "UNKNOWN";
        default: return "?";
    }
}

const std::unordered_map<std::string_view, TokenType> Lexer::keywords = {
    {"SELECT", TokenType::SELECT},
    {"FROM", TokenType::FROM},
    {"WHERE", TokenType::WHERE},
    {"INSERT", TokenType::INSERT},
    {"INTO", TokenType::INTO},
    {"VALUES", TokenType::VALUES},
    {"UPDATE", TokenType::UPDATE_KEYWORD},
    {"SET", TokenType::SET},
    {"DELETE", TokenType::DELETE_KEYWORD},
    {"CREATE", TokenType::CREATE},
    {"TABLE", TokenType::TABLE},
    {"AND", TokenType::AND},
    {"OR", TokenType::OR},
    {"NOT", TokenType::NOT},
    {"NULL", TokenType::NULL_TOKEN},
    {"ALTER", TokenType::ALTER},
    {"RENAME", TokenType::RENAME},
    {"TO", TokenType::TO},
    {"COLUMN", TokenType::COLUMN},
    {"TYPE", TokenType::TYPE},
    {"LIKE", TokenType::LIKE},
    {"WITH", TokenType::WITH},
    {"OPTIONS", TokenType::OPTIONS},
    {"TYPES", TokenType::TYPES},
    {"MAX_COLUMN_LENGTH", TokenType::MAX_COLUMN_LENGTH},
    {"ADDITIONAL_CHARS", TokenType::ADDITIONAL_CHARS},
    {"MAX_STRING_LENGTH", TokenType::MAX_STRING_LENGTH},
    {"GC_FREQUENCY", TokenType::GC_FREQUENCY},
    {"DAYS", TokenType::DAYS}
};

Lexer::Lexer(const std::string& source)
    : source(source), sourceView(source) {}

std::vector<Token> Lexer::tokenize() {
    std::vector<Token> tokens;
    tokens.reserve(source.length() / 5);

    while (!isAtEnd()) {
        skipWhitespace();
        if (isAtEnd()) break;

        Token token = scanToken();
        tokens.push_back(std::move(token));
    }

    tokens.push_back(makeToken(TokenType::END_OF_FILE, ""));
    return tokens;
}

char Lexer::advance() {
    char c = source[current++];
    if (c == '\n') {
        line++;
        column = 1;
    } else {
        column++;
    }
    return c;
}

char Lexer::peek() const {
    return (current < source.length()) ? source[current] : '\0';
}

char Lexer::peekNext() const {
    return (current + 1 < source.length()) ? source[current + 1] : '\0';
}

bool Lexer::match(char expected) {
    if (current >= source.length() || source[current] != expected) {
        return false;
    }
    advance();
    return true;
}

void Lexer::skipWhitespace() {
    while (current < source.length()) {
        char c = source[current];
        if (c == ' ' || c == '\r' || c == '\t') {
            current++;
            column++;
        } else if (c == '\n') {
            current++;
            line++;
            column = 1;
        } else if (c == '-' && current + 1 < source.length() && source[current + 1] == '-') {
            current += 2;
            column += 2;
            while (current < source.length() && source[current] != '\n') {
                current++;
                column++;
            }
        } else {
            break;
        }
    }
}

Token Lexer::scanToken() {
    char c = advance();

    switch (c) {
        case '(': return makeToken(TokenType::LEFT_PAREN, "(");
        case ')': return makeToken(TokenType::RIGHT_PAREN, ")");
        case ',': return makeToken(TokenType::COMMA, ",");
        case ';': return makeToken(TokenType::SEMICOLON, ";");
        case '*': return makeToken(TokenType::ASTERISK, "*");
        case '[': return makeToken(TokenType::LEFT_BRACKET, "[");
        case ']': return makeToken(TokenType::RIGHT_BRACKET, "]");

        case '=': return makeToken(TokenType::EQUALS, "=");
        case '<':
            if (match('=')) return makeToken(TokenType::LESS_EQUALS, "<=");
            if (match('>')) return makeToken(TokenType::NOT_EQUALS, "<>");
            return makeToken(TokenType::LESS_THAN, "<");
        case '>':
            if (match('=')) return makeToken(TokenType::GREATER_EQUALS, ">=");
            return makeToken(TokenType::GREATER_THAN, ">");
        case '!':
            if (match('=')) return makeToken(TokenType::NOT_EQUALS, "!=");
            return errorToken("Unexpected character '!'");

        case '\'':
            return string();

        default:
            if (std::isdigit(c)) {
                return number();
            }
            if (std::isalpha(c) || c == '_') {
                return identifier();
            }
            return errorToken("Unexpected character");
    }
}

Token Lexer::identifier() {
    size_t start = current - 1;

    while (current < source.length() &&
           (std::isalnum(source[current]) || source[current] == '_')) {
        current++;
        column++;
    }

    std::string_view text(&source[start], current - start);

    // Оптимизированное uppercase преобразование
    size_t len = std::min(text.length(), sizeof(upperBuffer) - 1);
    for (size_t i = 0; i < len; ++i) {
        upperBuffer[i] = std::toupper(text[i]);
    }
    std::string_view upperText(upperBuffer, len);

    auto it = keywords.find(upperText);
    TokenType type = (it != keywords.end()) ? it->second : TokenType::IDENTIFIER;

    return makeToken(type, std::string(text));
}

Token Lexer::number() {
    size_t start = current - 1;
    bool isFloat = false;

    while (current < source.length() && std::isdigit(source[current])) {
        current++;
        column++;
    }

    if (current < source.length() && source[current] == '.' &&
        current + 1 < source.length() && std::isdigit(source[current + 1])) {
        isFloat = true;
        advance(); // consume '.'
        while (current < source.length() && std::isdigit(source[current])) {
            current++;
            column++;
        }
    }

    std::string text = source.substr(start, current - start);
    Token token = makeToken(TokenType::NUMBER_LITERAL, text);

    try {
        if (isFloat) {
            token.value = std::stod(text);
        } else {
            token.value = std::stoi(text);
        }
    } catch(const std::out_of_range&) {
        return errorToken("Number literal is out of range");
    }

    return token;
}

Token Lexer::string() {
    std::string value;
    value.reserve(32);
    size_t startLexeme = current - 1;

    while (current < source.length()) {
        if (source[current] == '\'') {
            if (current + 1 < source.length() && source[current + 1] == '\'') {
                advance();
                value += '\'';
            } else {
                break;
            }
        } else {
            value += source[current];
        }
        advance();
    }

    if (current >= source.length()) {
        return errorToken("Unterminated string");
    }

    advance();

    std::string lexeme = source.substr(startLexeme, current - startLexeme);
    Token token = makeToken(TokenType::STRING_LITERAL, lexeme);
    token.value = std::move(value);
    return token;
}

Token Lexer::makeToken(TokenType type, const std::string& lexeme) {
    return Token(type, lexeme, line, column - lexeme.length());
}

Token Lexer::errorToken(const std::string& message) {
    return Token(TokenType::UNKNOWN, std::string(1, source[current - 1]), line, column - 1);
}

}