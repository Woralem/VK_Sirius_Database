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
    {TokenType::EQUALS, BinaryExpr::Operator::EQ}, {TokenType::NOT_EQUALS, BinaryExpr::Operator::NE},
    {TokenType::LESS_THAN, BinaryExpr::Operator::LT}, {TokenType::GREATER_THAN, BinaryExpr::Operator::GT},
    {TokenType::LESS_EQUALS, BinaryExpr::Operator::LE}, {TokenType::GREATER_EQUALS, BinaryExpr::Operator::GE},
    {TokenType::AND, BinaryExpr::Operator::AND}, {TokenType::OR, BinaryExpr::Operator::OR},
};

Parser::Parser(const std::vector<Token>& tokens) : tokens(tokens) {
    errors.reserve(10);
}

std::vector<ASTNodePtr> Parser::parse_all() {
    std::vector<ASTNodePtr> statements;
    while (!isAtEnd()) {
        try {
            while (match({TokenType::SEMICOLON})) {}
            if (isAtEnd()) break;

            statements.push_back(statement());

            if (!isAtEnd()) {
                consume(TokenType::SEMICOLON, "Expected ';' after statement.");
            }

        } catch (const std::runtime_error&) {
            synchronize();
        }
    }
    return statements;
}

ASTNodePtr Parser::parse() {
    auto stmts = parse_all();
    if (stmts.empty()) {
        return nullptr;
    }
    return std::move(stmts.front());
}

bool Parser::isAtEnd() const { return current >= tokens.size() || tokens[current].type == TokenType::END_OF_FILE; }
Token Parser::peek() const { return tokens[current]; }
Token Parser::previous() const { return tokens[current > 0 ? current - 1 : 0]; }
Token Parser::advance() { if (!isAtEnd()) current++; return previous(); }
bool Parser::check(TokenType type) const { if (isAtEnd()) return false; return peek().type == type; }

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
    if (check(type)) return advance();
    std::string error_msg = message + " (got " + tokenTypeToString(peek().type) + " at line " + std::to_string(peek().line) + ")";
    error(error_msg);
    throw std::runtime_error(message);
}

void Parser::error(const std::string& message) {
    if (errors.empty() || errors.back() != message) {
        errors.push_back(message);
    }
}

void Parser::synchronize() {
    if (!isAtEnd()) advance();
    while (!isAtEnd()) {
        if (previous().type == TokenType::SEMICOLON) return;
        switch (peek().type) {
            case TokenType::CREATE: case TokenType::SELECT: case TokenType::INSERT:
            case TokenType::UPDATE_KEYWORD: case TokenType::DELETE_KEYWORD: case TokenType::END_OF_FILE:
                return;
        }
        advance();
    }
}

ASTNodePtr Parser::statement() {
    if (check(TokenType::SELECT)) return selectStatement();
    if (check(TokenType::INSERT)) return insertStatement();
    if (check(TokenType::UPDATE_KEYWORD)) return updateStatement();
    if (check(TokenType::DELETE_KEYWORD)) return deleteStatement();
    if (check(TokenType::CREATE)) return createTableStatement();

    error("Expected a statement (SELECT, INSERT, etc.) but got '" + peek().lexeme + "'");
    throw std::runtime_error("Parsing error: Invalid statement start");
}

ASTNodePtr Parser::expression() { return parsePrecedence(Precedence::OR); }

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
        if (infixIt == parseRules.end() || infixIt->second.precedence < precedence) break;
        if (!infixIt->second.infix) break;
        advance();
        left = infixIt->second.infix(this, std::move(left));
    }
    return left;
}

ASTNodePtr Parser::primary(Parser* parser) {
    Token token = parser->previous();
    switch (token.type) {
        case TokenType::NUMBER_LITERAL: case TokenType::STRING_LITERAL: case TokenType::NULL_TOKEN:
            return std::make_unique<LiteralExpr>(token.value);
        case TokenType::IDENTIFIER: {
            std::string upper = token.lexeme;
            std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
            if (upper == "TRUE") return std::make_unique<LiteralExpr>(true);
            if (upper == "FALSE") return std::make_unique<LiteralExpr>(false);
            return std::make_unique<IdentifierExpr>(token.lexeme);
        }
        default: parser->error("Unexpected token in expression"); return nullptr;
    }
}

ASTNodePtr Parser::grouping(Parser* parser) {
    auto expr = parser->expression();
    parser->consume(TokenType::RIGHT_PAREN, "Expected ')' after expression");
    return expr;
}

ASTNodePtr Parser::binary(Parser* parser, ASTNodePtr left) {
    Token opToken = parser->previous();
    auto right = parser->parsePrecedence(static_cast<Precedence>(static_cast<int>(parseRules.at(opToken.type).precedence) + 1));
    return std::make_unique<BinaryExpr>(binaryOps.at(opToken.type), std::move(left), std::move(right));
}

std::unique_ptr<SelectStmt> Parser::selectStatement() {
    consume(TokenType::SELECT, "Expected SELECT");
    auto stmt = std::make_unique<SelectStmt>();
    if (match({TokenType::ASTERISK})) {
        if (check(TokenType::COMMA)) error("'*' cannot be used with other column names.");
    } else if (check(TokenType::IDENTIFIER)) {
        stmt->columns = parseColumnList();
    } else {
        consume(TokenType::IDENTIFIER, "Expected '*' or column names after SELECT.");
    }
    consume(TokenType::FROM, "Expected FROM after column list");
    stmt->tableName = consume(TokenType::IDENTIFIER, "Expected table name").lexeme;
    if (match({TokenType::WHERE})) stmt->whereClause = expression();
    return stmt;
}

std::unique_ptr<InsertStmt> Parser::insertStatement() {
    consume(TokenType::INSERT, "Expected INSERT");
    auto stmt = std::make_unique<InsertStmt>();
    consume(TokenType::INTO, "Expected INTO");
    stmt->tableName = consume(TokenType::IDENTIFIER, "Expected table name").lexeme;
    if (match({TokenType::LEFT_PAREN})) {
        stmt->columns = parseColumnList();
        consume(TokenType::RIGHT_PAREN, "Expected ')'");
    }
    consume(TokenType::VALUES, "Expected VALUES");
    do {
        consume(TokenType::LEFT_PAREN, "Expected '('");
        if (check(TokenType::RIGHT_PAREN)) error("Value list cannot be empty");
        else stmt->values.push_back(parseValueList());
        consume(TokenType::RIGHT_PAREN, "Expected ')'");
    } while (match({TokenType::COMMA}));
    return stmt;
}

std::unique_ptr<UpdateStmt> Parser::updateStatement() {
    consume(TokenType::UPDATE_KEYWORD, "Expected UPDATE");
    auto stmt = std::make_unique<UpdateStmt>();
    stmt->tableName = consume(TokenType::IDENTIFIER, "Expected table name").lexeme;
    consume(TokenType::SET, "Expected SET");
    do {
        Token column = consume(TokenType::IDENTIFIER, "Expected column name");
        consume(TokenType::EQUALS, "Expected '='");
        if (match({TokenType::NUMBER_LITERAL, TokenType::STRING_LITERAL, TokenType::NULL_TOKEN})) {
            stmt->assignments.emplace_back(std::move(column.lexeme), previous().value);
        } else if (check(TokenType::IDENTIFIER)) {
            std::string id = peek().lexeme; std::transform(id.begin(), id.end(), id.begin(), ::toupper);
            if (id == "TRUE") { advance(); stmt->assignments.emplace_back(std::move(column.lexeme), true); }
            else if (id == "FALSE") { advance(); stmt->assignments.emplace_back(std::move(column.lexeme), false); }
            else error("Unexpected identifier '" + peek().lexeme + "' in SET clause");
        } else { error("Expected a literal value after '='"); }
    } while (match({TokenType::COMMA}));
    if (match({TokenType::WHERE})) stmt->whereClause = expression();
    return stmt;
}

std::unique_ptr<DeleteStmt> Parser::deleteStatement() {
    consume(TokenType::DELETE_KEYWORD, "Expected DELETE");
    auto stmt = std::make_unique<DeleteStmt>();
    consume(TokenType::FROM, "Expected FROM");
    stmt->tableName = consume(TokenType::IDENTIFIER, "Expected table name").lexeme;
    if (match({TokenType::WHERE})) stmt->whereClause = expression();
    return stmt;
}

std::unique_ptr<CreateTableStmt> Parser::createTableStatement() {
    consume(TokenType::CREATE, "Expected CREATE");
    auto stmt = std::make_unique<CreateTableStmt>();
    consume(TokenType::TABLE, "Expected TABLE");
    stmt->tableName = consume(TokenType::IDENTIFIER, "Expected table name").lexeme;
    consume(TokenType::LEFT_PAREN, "Expected '('");
    if (check(TokenType::RIGHT_PAREN)) error("Column definitions cannot be empty");
    else do { stmt->columns.push_back(parseColumnDef()); } while (match({TokenType::COMMA}));
    consume(TokenType::RIGHT_PAREN, "Expected ')'");
    if (match({TokenType::WITH})) {
        consume(TokenType::OPTIONS, "Expected OPTIONS");
        consume(TokenType::LEFT_PAREN, "Expected '('");
        stmt->options = parseTableOptions();
        consume(TokenType::RIGHT_PAREN, "Expected ')'");
    }
    return stmt;
}

TableOptions Parser::parseTableOptions() {
    TableOptions options;
    if (check(TokenType::RIGHT_PAREN)) return options;
    do {
        if (match({TokenType::TYPES})) {
            consume(TokenType::EQUALS, "="); consume(TokenType::LEFT_BRACKET, "[");
            options.allowedTypes = parseDataTypeList(); consume(TokenType::RIGHT_BRACKET, "]");
        } else if (match({TokenType::MAX_COLUMN_LENGTH})) {
            consume(TokenType::EQUALS, "=");
            options.maxColumnNameLength = std::get<int>(consume(TokenType::NUMBER_LITERAL, "number").value);
        } else if (match({TokenType::ADDITIONAL_CHARS})) {
            consume(TokenType::EQUALS, "="); consume(TokenType::LEFT_BRACKET, "[");
            options.additionalNameChars = parseCharacterList(); consume(TokenType::RIGHT_BRACKET, "]");
        } else if (match({TokenType::MAX_STRING_LENGTH})) {
            consume(TokenType::EQUALS, "=");
            options.maxStringLength = std::get<int>(consume(TokenType::NUMBER_LITERAL, "number").value);
        } else if (match({TokenType::GC_FREQUENCY})) {
            consume(TokenType::EQUALS, "=");
            options.gcFrequencyDays = std::get<int>(consume(TokenType::NUMBER_LITERAL, "number").value);
            match({TokenType::DAYS});
        } else {
            error("Unknown option: " + peek().lexeme); advance();
        }
    } while (match({TokenType::COMMA}));
    return options;
}

std::set<DataType> Parser::parseDataTypeList() {
    std::set<DataType> types;
    if (!check(TokenType::RIGHT_BRACKET)) {
        do {
            types.insert(parseDataType(consume(TokenType::IDENTIFIER, "data type").lexeme));
        } while (match({TokenType::COMMA}));
    }
    return types;
}

std::set<char> Parser::parseCharacterList() {
    std::set<char> chars;
    if (!check(TokenType::RIGHT_BRACKET)) {
        do {
            if (check(TokenType::STRING_LITERAL)) {
                for (char c : std::get<std::string>(advance().value)) chars.insert(c);
            } else error("Expected character string");
        } while (match({TokenType::COMMA}));
    }
    return chars;
}

std::vector<std::string> Parser::parseColumnList() {
    std::vector<std::string> columns;
    if (!check(TokenType::RIGHT_PAREN)) {
        do {
            columns.push_back(consume(TokenType::IDENTIFIER, "column name").lexeme);
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
                std::string upper = peek().lexeme;
                std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
                if (upper == "TRUE") { advance(); values.push_back(true); }
                else if (upper == "FALSE") { advance(); values.push_back(false); }
                else error("Unexpected identifier in value list");
            } else error("Expected a literal value");
        } while (match({TokenType::COMMA}));
    }
    return values;
}

std::unique_ptr<ColumnDef> Parser::parseColumnDef() {
    auto columnDef = std::make_unique<ColumnDef>();
    columnDef->name = consume(TokenType::IDENTIFIER, "column name").lexeme;
    columnDef->dataType = consume(TokenType::IDENTIFIER, "data type").lexeme;
    columnDef->parsedType = parseDataType(columnDef->dataType);
    while (true) {
        if (match({TokenType::NOT})) {
            consume(TokenType::NULL_TOKEN, "NULL after NOT");
            columnDef->notNull = true;
        } else if (check(TokenType::IDENTIFIER)) {
            std::string upper = peek().lexeme;
            std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
            if (upper == "PRIMARY") {
                advance();
                Token keyToken = consume(TokenType::IDENTIFIER, "KEY after PRIMARY");
                std::string keyUpper = keyToken.lexeme;
                std::transform(keyUpper.begin(), keyUpper.end(), keyUpper.begin(), ::toupper);
                if (keyUpper != "KEY") error("Expected 'KEY' after 'PRIMARY'");
                columnDef->primaryKey = true;
            } else break;
        } else break;
    }
    return columnDef;
}

}