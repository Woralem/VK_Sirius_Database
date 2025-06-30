#include "query_engine/parser.h"
#include "query_engine/lexer.h"
#include "utils/logger.h"
#include <stdexcept>
#include <sstream>
#include <algorithm>

namespace query_engine {

Parser::Parser(const std::vector<Token>& tokens) : tokens(tokens) {}

ASTNodePtr Parser::parse() {
    while(match({TokenType::SEMICOLON})) {}

    if (isAtEnd()) {
        return nullptr;
    }

    try {
        ASTNodePtr result = statement();
        if (hasError()) {
            return nullptr;
        }

        match({TokenType::SEMICOLON});

        if (!isAtEnd()) {
            error("Unexpected token '" + peek().lexeme + "' after end of statement.");
            return nullptr;
        }

        return result;
    } catch (const std::runtime_error&) {
        return nullptr;
    }
}


bool Parser::isAtEnd() const {
    return tokens[current].type == TokenType::END_OF_FILE;
}

Token Parser::peek() const {
    return tokens[current];
}

Token Parser::previous() const {
    return tokens[current - 1];
}

Token Parser::advance() {
    if (!isAtEnd()) {
        current++;
    }
    return previous();
}

bool Parser::check(TokenType type) const {
    if (isAtEnd()) return false;
    return peek().type == type;
}

bool Parser::match(std::initializer_list<TokenType> types) {
    for (auto type : types) {
        if (check(type)) {
            advance();
            return true;
        }
    }
    return false;
}

Token Parser::consume(TokenType type, const std::string& message) {
    if (check(type)) {
        return advance();
    }
    std::string error_msg = message + " (got " + tokenTypeToString(peek().type) + " at line " + std::to_string(peek().line) + ")";
    error(error_msg);
    throw std::runtime_error(message);
}

void Parser::error(const std::string& message) {
    if (errors.empty() || errors.back() != message) {
        errors.push_back(message);
    }
}

ASTNodePtr Parser::statement() {
    if (check(TokenType::SELECT)) return selectStatement();
    if (check(TokenType::INSERT)) return insertStatement();
    // --- ИЗМЕНЕНО ---
    if (check(TokenType::UPDATE_KEYWORD)) return updateStatement();
    if (check(TokenType::DELETE_KEYWORD)) return deleteStatement();
    // ---
    if (check(TokenType::CREATE)) return createTableStatement();

    if (!isAtEnd()) {
        error("Expected a statement (SELECT, INSERT, etc.) but got " + tokenTypeToString(peek().type));
    }
    return nullptr;
}


std::unique_ptr<SelectStmt> Parser::selectStatement() {
    consume(TokenType::SELECT, "Expected SELECT");
    auto stmt = std::make_unique<SelectStmt>();

    if (match({TokenType::ASTERISK})) {
        if (check(TokenType::COMMA)) {
            error("'*' cannot be used with other column names.");
        }
    } else if (check(TokenType::IDENTIFIER)) {
        stmt->columns = parseColumnList();
    } else {
        error("Expected '*' or column names after SELECT.");
    }

    consume(TokenType::FROM, "Expected FROM after column list");
    Token tableName = consume(TokenType::IDENTIFIER, "Expected table name");
    stmt->tableName = tableName.lexeme;

    if (match({TokenType::WHERE})) {
        stmt->whereClause = expression();
    }

    return stmt;
}

std::unique_ptr<InsertStmt> Parser::insertStatement() {
    consume(TokenType::INSERT, "Expected INSERT");
    auto stmt = std::make_unique<InsertStmt>();
    consume(TokenType::INTO, "Expected INTO after INSERT");
    Token tableName = consume(TokenType::IDENTIFIER, "Expected table name");
    stmt->tableName = tableName.lexeme;

    if (match({TokenType::LEFT_PAREN})) {
        stmt->columns = parseColumnList();
        consume(TokenType::RIGHT_PAREN, "Expected ')' after column list");
    }

    consume(TokenType::VALUES, "Expected VALUES");

    do {
        consume(TokenType::LEFT_PAREN, "Expected '(' before values");
        if (check(TokenType::RIGHT_PAREN)) {
            error("Value list cannot be empty");
        } else {
             stmt->values.push_back(parseValueList());
        }
        consume(TokenType::RIGHT_PAREN, "Expected ')' after values");
    } while (match({TokenType::COMMA}));

    return stmt;
}

std::unique_ptr<UpdateStmt> Parser::updateStatement() {
    // --- ИЗМЕНЕНО ---
    consume(TokenType::UPDATE_KEYWORD, "Expected UPDATE");
    auto stmt = std::make_unique<UpdateStmt>();
    Token tableName = consume(TokenType::IDENTIFIER, "Expected table name after UPDATE");
    stmt->tableName = tableName.lexeme;

    consume(TokenType::SET, "Expected SET after table name");

    do {
        Token column = consume(TokenType::IDENTIFIER, "Expected column name in SET clause");
        consume(TokenType::EQUALS, "Expected '=' after column name");

        if (match({TokenType::NUMBER_LITERAL, TokenType::STRING_LITERAL, TokenType::NULL_TOKEN})) {
            stmt->assignments.push_back({column.lexeme, previous().value});
        } else if (check(TokenType::IDENTIFIER)) {
            std::string id_lexeme = peek().lexeme;
            std::transform(id_lexeme.begin(), id_lexeme.end(), id_lexeme.begin(), ::toupper);
            if (id_lexeme == "TRUE") {
                advance();
                stmt->assignments.push_back({column.lexeme, true});
            } else if (id_lexeme == "FALSE") {
                advance();
                stmt->assignments.push_back({column.lexeme, false});
            } else {
                 error("Unexpected identifier '" + peek().lexeme + "' in SET clause, expected a value.");
            }
        }
        else {
            error("Expected a literal value after '='");
        }
    } while (match({TokenType::COMMA}));

    if (match({TokenType::WHERE})) {
        stmt->whereClause = expression();
    }

    return stmt;
}

std::unique_ptr<DeleteStmt> Parser::deleteStatement() {
    // --- ИЗМЕНЕНО ---
    consume(TokenType::DELETE_KEYWORD, "Expected DELETE");
    auto stmt = std::make_unique<DeleteStmt>();
    consume(TokenType::FROM, "Expected FROM after DELETE");
    Token tableName = consume(TokenType::IDENTIFIER, "Expected table name");
    stmt->tableName = tableName.lexeme;

    if (match({TokenType::WHERE})) {
        stmt->whereClause = expression();
    }
    return stmt;
}

std::unique_ptr<CreateTableStmt> Parser::createTableStatement() {
    consume(TokenType::CREATE, "Expected CREATE");
    auto stmt = std::make_unique<CreateTableStmt>();
    consume(TokenType::TABLE, "Expected TABLE after CREATE");
    Token tableName = consume(TokenType::IDENTIFIER, "Expected table name");
    stmt->tableName = tableName.lexeme;

    consume(TokenType::LEFT_PAREN, "Expected '(' after table name");

    if (check(TokenType::RIGHT_PAREN)) {
        error("Column definitions cannot be empty");
    } else {
        do {
            stmt->columns.push_back(parseColumnDef());
        } while (match({TokenType::COMMA}));
    }

    consume(TokenType::RIGHT_PAREN, "Expected ')' after column definitions");
    return stmt;
}

ASTNodePtr Parser::expression() { return logic_or(); }

ASTNodePtr Parser::logic_or() {
    auto expr = logic_and();
    while (match({TokenType::OR})) {
        auto right = logic_and();
        expr = std::make_unique<BinaryExpr>(BinaryExpr::Operator::OR, std::move(expr), std::move(right));
    }
    return expr;
}

ASTNodePtr Parser::logic_and() {
    auto expr = equality();
    while (match({TokenType::AND})) {
        auto right = equality();
        expr = std::make_unique<BinaryExpr>(BinaryExpr::Operator::AND, std::move(expr), std::move(right));
    }
    return expr;
}

ASTNodePtr Parser::equality() {
    auto expr = comparison();
    while (match({TokenType::EQUALS, TokenType::NOT_EQUALS})) {
        Token op = previous();
        auto right = comparison();
        auto opType = (op.type == TokenType::EQUALS) ? BinaryExpr::Operator::EQ : BinaryExpr::Operator::NE;
        expr = std::make_unique<BinaryExpr>(opType, std::move(expr), std::move(right));
    }
    return expr;
}

ASTNodePtr Parser::comparison() {
    auto expr = primary();
    while (match({TokenType::GREATER_THAN, TokenType::GREATER_EQUALS, TokenType::LESS_THAN, TokenType::LESS_EQUALS})) {
        Token op = previous();
        auto right = primary();
        BinaryExpr::Operator opType;
        if (op.type == TokenType::GREATER_THAN) opType = BinaryExpr::Operator::GT;
        else if (op.type == TokenType::GREATER_EQUALS) opType = BinaryExpr::Operator::GE;
        else if (op.type == TokenType::LESS_THAN) opType = BinaryExpr::Operator::LT;
        else opType = BinaryExpr::Operator::LE;
        expr = std::make_unique<BinaryExpr>(opType, std::move(expr), std::move(right));
    }
    return expr;
}

ASTNodePtr Parser::primary() {
    if (match({TokenType::NUMBER_LITERAL, TokenType::STRING_LITERAL, TokenType::NULL_TOKEN})) {
        return std::make_unique<LiteralExpr>(previous().value);
    }
    if (check(TokenType::IDENTIFIER)) {
        std::string upperLexeme = peek().lexeme;
        std::transform(upperLexeme.begin(), upperLexeme.end(), upperLexeme.begin(), ::toupper);
        if (upperLexeme == "TRUE") {
            advance(); return std::make_unique<LiteralExpr>(true);
        }
        if (upperLexeme == "FALSE") {
            advance(); return std::make_unique<LiteralExpr>(false);
        }
        return std::make_unique<IdentifierExpr>(advance().lexeme);
    }
    if (match({TokenType::LEFT_PAREN})) {
        auto expr = expression();
        consume(TokenType::RIGHT_PAREN, "Expected ')' after expression");
        return expr;
    }
    error("Expected expression, literal or identifier");
    return nullptr;
}
std::vector<std::string> Parser::parseColumnList() {
    std::vector<std::string> columns;
    if (!check(TokenType::RIGHT_PAREN)) {
        do {
            if (check(TokenType::ASTERISK)) error("'*' must be used alone in column list.");
            columns.push_back(consume(TokenType::IDENTIFIER, "Expected column name").lexeme);
        } while (match({TokenType::COMMA}));
    }
    return columns;
}
std::vector<Value> Parser::parseValueList() {
    std::vector<Value> values;
    if (!check(TokenType::RIGHT_PAREN)) {
        do {
            if (match({TokenType::NUMBER_LITERAL, TokenType::STRING_LITERAL, TokenType::NULL_TOKEN})) {
                values.push_back(previous().value);
            } else if (check(TokenType::IDENTIFIER)) {
                 std::string upperLexeme = peek().lexeme;
                 std::transform(upperLexeme.begin(), upperLexeme.end(), upperLexeme.begin(), ::toupper);
                 if (upperLexeme == "TRUE") {
                     advance(); values.push_back(true);
                 } else if (upperLexeme == "FALSE") {
                     advance(); values.push_back(false);
                 } else {
                    error("Unexpected identifier in value list, expected a literal");
                 }
            } else {
                error("Expected a literal value");
            }
        } while (match({TokenType::COMMA}));
    }
    return values;
}

std::unique_ptr<ColumnDef> Parser::parseColumnDef() {
    auto columnDef = std::make_unique<ColumnDef>();
    columnDef->name = consume(TokenType::IDENTIFIER, "Expected column name").lexeme;
    columnDef->dataType = consume(TokenType::IDENTIFIER, "Expected data type").lexeme;

    while(true) {
        if (match({TokenType::NOT})) {
            consume(TokenType::NULL_TOKEN, "Expected NULL after NOT");
            columnDef->notNull = true;
        } else if (check(TokenType::IDENTIFIER)) {
            std::string upperLexeme = peek().lexeme;
            std::transform(upperLexeme.begin(), upperLexeme.end(), upperLexeme.begin(), ::toupper);
            if (upperLexeme == "PRIMARY") {
                advance();
                Token keyToken = consume(TokenType::IDENTIFIER, "Expected KEY after PRIMARY");
                std::string keyUpper = keyToken.lexeme;
                std::transform(keyUpper.begin(), keyUpper.end(), keyUpper.begin(), ::toupper);
                if (keyUpper != "KEY") {
                    error("Expected 'KEY' after 'PRIMARY'");
                }
                columnDef->primaryKey = true;
            } else {
                break;
            }
        } else {
            break;
        }
    }
    return columnDef;
}

}