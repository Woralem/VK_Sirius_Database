#include "query_engine/parser.h"
#include "query_engine/lexer.h"
#include <stdexcept>
#include <algorithm>

namespace query_engine {

const std::unordered_map<TokenType, Parser::ParseRule> Parser::parseRules = {
    {TokenType::NUMBER_LITERAL, {primary, nullptr, Precedence::NONE}},
    {TokenType::STRING_LITERAL, {primary, nullptr, Precedence::NONE}},
    {TokenType::NULL_TOKEN, {primary, nullptr, Precedence::NONE}},
    {TokenType::IDENTIFIER, {primary, nullptr, Precedence::NONE}},
    {TokenType::LEFT_PAREN, {grouping, nullptr, Precedence::NONE}},

    {TokenType::EQUALS, {nullptr, binary, Precedence::EQUALITY}},
    {TokenType::NOT_EQUALS, {nullptr, binary, Precedence::EQUALITY}},
    {TokenType::LESS_THAN, {nullptr, binary, Precedence::COMPARISON}},
    {TokenType::GREATER_THAN, {nullptr, binary, Precedence::COMPARISON}},
    {TokenType::LESS_EQUALS, {nullptr, binary, Precedence::COMPARISON}},
    {TokenType::GREATER_EQUALS, {nullptr, binary, Precedence::COMPARISON}},
    {TokenType::AND, {nullptr, binary, Precedence::AND}},
    {TokenType::OR, {nullptr, binary, Precedence::OR}},
};

const std::unordered_map<TokenType, BinaryExpr::Operator> Parser::binaryOps = {
    {TokenType::EQUALS, BinaryExpr::Operator::EQ},
    {TokenType::NOT_EQUALS, BinaryExpr::Operator::NE},
    {TokenType::LESS_THAN, BinaryExpr::Operator::LT},
    {TokenType::GREATER_THAN, BinaryExpr::Operator::GT},
    {TokenType::LESS_EQUALS, BinaryExpr::Operator::LE},
    {TokenType::GREATER_EQUALS, BinaryExpr::Operator::GE},
    {TokenType::AND, BinaryExpr::Operator::AND},
    {TokenType::OR, BinaryExpr::Operator::OR},
};

Parser::Parser(const std::vector<Token>& tokens) : tokens(tokens) {
    errors.reserve(10);
}

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
    return current >= tokens.size() || tokens[current].type == TokenType::END_OF_FILE;
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

    std::string error_msg = message + " (got ";
    error_msg += tokenTypeToString(peek().type);
    error_msg += " at line ";
    error_msg += std::to_string(peek().line);
    error_msg += ")";

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
    if (check(TokenType::UPDATE_KEYWORD)) return updateStatement();
    if (check(TokenType::DELETE_KEYWORD)) return deleteStatement();
    if (check(TokenType::CREATE)) return createTableStatement();

    if (!isAtEnd()) {
        error("Expected a statement (SELECT, INSERT, etc.) but got " + tokenTypeToString(peek().type));
    }
    return nullptr;
}

ASTNodePtr Parser::expression() {
    return parsePrecedence(Precedence::OR);
}

ASTNodePtr Parser::parsePrecedence(Precedence precedence) {
    advance();

    auto prefixIt = parseRules.find(previous().type);
    if (prefixIt == parseRules.end() || !prefixIt->second.prefix) {
        error("Expected expression");
        return nullptr;
    }

    auto left = prefixIt->second.prefix(this);

    while (!isAtEnd()) {
        auto infixIt = parseRules.find(peek().type);
        if (infixIt == parseRules.end() ||
            infixIt->second.precedence <= precedence) {
            break;
        }

        advance();
        left = infixIt->second.infix(this, std::move(left));
    }

    return left;
}

ASTNodePtr Parser::primary(Parser* parser) {
    Token token = parser->previous();

    switch (token.type) {
        case TokenType::NUMBER_LITERAL:
        case TokenType::STRING_LITERAL:
        case TokenType::NULL_TOKEN:
            return std::make_unique<LiteralExpr>(token.value);

        case TokenType::IDENTIFIER: {
            std::string upperLexeme = token.lexeme;
            std::transform(upperLexeme.begin(), upperLexeme.end(), upperLexeme.begin(), ::toupper);

            if (upperLexeme == "TRUE") return std::make_unique<LiteralExpr>(true);
            if (upperLexeme == "FALSE") return std::make_unique<LiteralExpr>(false);

            return std::make_unique<IdentifierExpr>(token.lexeme);
        }

        default:
            parser->error("Unexpected token in expression");
            return nullptr;
    }
}

ASTNodePtr Parser::grouping(Parser* parser) {
    auto expr = parser->expression();
    parser->consume(TokenType::RIGHT_PAREN, "Expected ')' after expression");
    return expr;
}

ASTNodePtr Parser::binary(Parser* parser, ASTNodePtr left) {
    Token opToken = parser->previous();
    auto precedence = parseRules.at(opToken.type).precedence;

    auto right = parser->parsePrecedence(static_cast<Precedence>(static_cast<int>(precedence) + 1));

    auto opIt = binaryOps.find(opToken.type);
    if (opIt != binaryOps.end()) {
        return std::make_unique<BinaryExpr>(opIt->second, std::move(left), std::move(right));
    }

    parser->error("Unknown binary operator");
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
    stmt->tableName = std::move(tableName.lexeme);

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
    stmt->tableName = std::move(tableName.lexeme);

    if (match({TokenType::LEFT_PAREN})) {
        stmt->columns = parseColumnList();
        consume(TokenType::RIGHT_PAREN, "Expected ')' after column list");
    }

    consume(TokenType::VALUES, "Expected VALUES");

    stmt->values.reserve(10);

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
    consume(TokenType::UPDATE_KEYWORD, "Expected UPDATE");
    auto stmt = std::make_unique<UpdateStmt>();
    Token tableName = consume(TokenType::IDENTIFIER, "Expected table name after UPDATE");
    stmt->tableName = std::move(tableName.lexeme);

    consume(TokenType::SET, "Expected SET after table name");

    stmt->assignments.reserve(5);

    do {
        Token column = consume(TokenType::IDENTIFIER, "Expected column name in SET clause");
        consume(TokenType::EQUALS, "Expected '=' after column name");

        if (match({TokenType::NUMBER_LITERAL, TokenType::STRING_LITERAL, TokenType::NULL_TOKEN})) {
            stmt->assignments.emplace_back(std::move(column.lexeme), previous().value);
        } else if (check(TokenType::IDENTIFIER)) {
            std::string id_lexeme = peek().lexeme;
            std::transform(id_lexeme.begin(), id_lexeme.end(), id_lexeme.begin(), ::toupper);
            if (id_lexeme == "TRUE") {
                advance();
                stmt->assignments.emplace_back(std::move(column.lexeme), true);
            } else if (id_lexeme == "FALSE") {
                advance();
                stmt->assignments.emplace_back(std::move(column.lexeme), false);
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
    consume(TokenType::DELETE_KEYWORD, "Expected DELETE");
    auto stmt = std::make_unique<DeleteStmt>();
    consume(TokenType::FROM, "Expected FROM after DELETE");
    Token tableName = consume(TokenType::IDENTIFIER, "Expected table name");
    stmt->tableName = std::move(tableName.lexeme);

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
    stmt->tableName = std::move(tableName.lexeme);

    consume(TokenType::LEFT_PAREN, "Expected '(' after table name");

    stmt->columns.reserve(10);

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

std::vector<std::string> Parser::parseColumnList() {
    std::vector<std::string> columns;
    columns.reserve(10);

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
    values.reserve(10);

    if (!check(TokenType::RIGHT_PAREN)) {
        do {
            if (match({TokenType::NUMBER_LITERAL, TokenType::STRING_LITERAL, TokenType::NULL_TOKEN})) {
                values.push_back(previous().value);
            } else if (check(TokenType::IDENTIFIER)) {
                 std::string upperLexeme = peek().lexeme;
                 std::transform(upperLexeme.begin(), upperLexeme.end(), upperLexeme.begin(), ::toupper);
                 if (upperLexeme == "TRUE") {
                     advance();
                     values.push_back(true);
                 } else if (upperLexeme == "FALSE") {
                     advance();
                     values.push_back(false);
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