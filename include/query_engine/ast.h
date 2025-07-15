#pragma once
#include <memory>
#include <vector>
#include <string>
#include <variant>
#include <ostream>
#include <set>
#include <algorithm>
#include <compare>
#include <format>
#include <cstdint>

namespace query_engine {

// Forward declarations
struct ASTNode;
using ASTNodePtr = std::unique_ptr<ASTNode>;

using Value = std::variant<std::monostate, int64_t, double, std::string, bool>;

// Data types that can be stored in table
enum class DataType {
    INT,
    DOUBLE,
    VARCHAR,
    BOOLEAN,
    DATE,
    TIMESTAMP,
    UNKNOWN_TYPE
};

inline std::strong_ordering operator<=>(DataType a, DataType b) noexcept {
    return static_cast<int>(a) <=> static_cast<int>(b);
}

inline bool operator==(DataType a, DataType b) noexcept {
    return static_cast<int>(a) == static_cast<int>(b);
}

// Table creation options
struct TableOptions {
    // Allowed data types for this table
    std::set<DataType> allowedTypes;

    // Maximum column name length (default: 16, max: 64)
    size_t maxColumnNameLength = 16;

    // Additional allowed characters for column names
    std::set<char> additionalNameChars;

    // Maximum string length (default: 65536, max: 2^40)
    size_t maxStringLength = 65536;

    // Garbage collection frequency in days (default: 7, range: 1-365)
    int gcFrequencyDays = 7;

    [[nodiscard]] constexpr bool validate() const noexcept {
        return maxColumnNameLength > 0 && maxColumnNameLength <= 64 &&
               maxStringLength > 0 && maxStringLength <= (1ULL << 40) &&
               gcFrequencyDays >= 1 && gcFrequencyDays <= 365;
    }
};

// Base AST Node
struct ASTNode {
    enum class Type {
        // Statements
        SELECT_STMT, INSERT_STMT, UPDATE_STMT, DELETE_STMT, CREATE_TABLE_STMT, ALTER_TABLE_STMT, DROP_TABLE_STMT,

        // Expressions
        BINARY_EXPR, UNARY_EXPR, LITERAL_EXPR, IDENTIFIER_EXPR, SUBQUERY_EXPR,

        // Others
        COLUMN_DEF, TABLE_REF, COLUMN_REF, VALUE_LIST, TABLE_OPTIONS
    };

    Type type;
    virtual ~ASTNode() = default;

protected:
    ASTNode(Type type) : type(type) {}
};

// Column Definition for CREATE TABLE
struct ColumnDef : public ASTNode {
    std::string name;
    std::string dataType;
    DataType parsedType = DataType::VARCHAR; // Default type
    bool notNull = false;
    bool primaryKey = false;
    size_t maxLength = 0; // For VARCHAR types

    ColumnDef() : ASTNode(Type::COLUMN_DEF) {}
};

// Expressions
struct Expression : public ASTNode {
    Expression(Type type) : ASTNode(type) {}
};

struct LiteralExpr : public Expression {
    Value value;

    LiteralExpr(const Value& val) : Expression(Type::LITERAL_EXPR), value(val) {}
};

struct IdentifierExpr : public Expression {
    std::string name;

    IdentifierExpr(const std::string& name)
        : Expression(Type::IDENTIFIER_EXPR), name(name) {}
};

struct SelectStmt;

struct SubqueryExpr : public Expression {
    std::unique_ptr<SelectStmt> selectStmt;

    SubqueryExpr(std::unique_ptr<SelectStmt> stmt)
        : Expression(Type::SUBQUERY_EXPR), selectStmt(std::move(stmt)) {}
};

struct BinaryExpr : public Expression {
    enum class Operator {
        EQ, NE, LT, GT, LE, GE, AND, OR, LIKE, IN_OP, NOT_IN_OP
    };

    Operator op;
    ASTNodePtr left;
    ASTNodePtr right;

    BinaryExpr(Operator op, ASTNodePtr left, ASTNodePtr right)
        : Expression(Type::BINARY_EXPR), op(op),
          left(std::move(left)), right(std::move(right)) {}
};

// Statements
struct Statement : public ASTNode {
    Statement(Type type) : ASTNode(type) {}
};

struct CreateTableStmt : public Statement {
    std::string tableName;
    std::vector<std::unique_ptr<ColumnDef>> columns;
    TableOptions options;

    CreateTableStmt() : Statement(Type::CREATE_TABLE_STMT) {}
};

struct AlterTableStmt : public Statement {
    enum class AlterType {
        RENAME_TABLE,
        RENAME_COLUMN,
        ALTER_COLUMN_TYPE,
        DROP_COLUMN,
        ADD_COLUMN
    };
    AlterType alterType;
    std::string tableName;
    std::string newTableName;
    std::string columnName;
    std::string newColumnName;
    std::string newDataType;
    DataType newParsedType;
    std::unique_ptr<ColumnDef> newColumn;
    AlterTableStmt() : Statement(Type::ALTER_TABLE_STMT) {}
};

struct DropTableStmt : public Statement {
    std::string tableName;
    bool ifExists = false;  // Для поддержки DROP TABLE IF EXISTS

    DropTableStmt() : Statement(Type::DROP_TABLE_STMT) {}
};

struct SelectStmt : public Statement {
    std::vector<std::string> columns;
    std::string tableName;
    ASTNodePtr whereClause;

    SelectStmt() : Statement(Type::SELECT_STMT) {}
};

struct InsertStmt : public Statement {
    std::string tableName;
    std::vector<std::string> columns;
    std::vector<std::vector<Value>> values;

    InsertStmt() : Statement(Type::INSERT_STMT) {}
};

struct UpdateStmt : public Statement {
    std::string tableName;
    std::vector<std::pair<std::string, Value>> assignments;
    ASTNodePtr whereClause;

    UpdateStmt() : Statement(Type::UPDATE_STMT) {}
};

struct DeleteStmt : public Statement {
    std::string tableName;
    ASTNodePtr whereClause;

    DeleteStmt() : Statement(Type::DELETE_STMT) {}
};

// Helper functions
[[nodiscard]] inline constexpr std::string_view dataTypeToString(DataType type) noexcept {
    switch (type) {
        case DataType::INT: return "INT";
        case DataType::DOUBLE: return "DOUBLE";
        case DataType::VARCHAR: return "VARCHAR";
        case DataType::BOOLEAN: return "BOOLEAN";
        case DataType::DATE: return "DATE";
        case DataType::TIMESTAMP: return "TIMESTAMP";
        case DataType::UNKNOWN_TYPE: return "UNKNOWN_TYPE";
        default: return "UNKNOWN";
    }
}

inline std::ostream& operator<<(std::ostream& os, const query_engine::DataType& type) {
    os << dataTypeToString(type);
    return os;
}

[[nodiscard]] inline DataType parseDataType(std::string_view typeStr) {
    std::string upper(typeStr);
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);

    if (upper == "INT" || upper == "INTEGER") return DataType::INT;
    if (upper == "DOUBLE" || upper == "FLOAT" || upper == "REAL") return DataType::DOUBLE;
    if (upper == "VARCHAR" || upper == "STRING" || upper == "TEXT") return DataType::VARCHAR;
    if (upper == "BOOLEAN" || upper == "BOOL") return DataType::BOOLEAN;
    if (upper == "DATE") return DataType::DATE;
    if (upper == "TIMESTAMP" || upper == "DATETIME") return DataType::TIMESTAMP;

    return DataType::UNKNOWN_TYPE;
}

inline std::ostream& operator<<(std::ostream& os, const ASTNode::Type& type) {
    switch (type) {
        case ASTNode::Type::SELECT_STMT:        os << "SELECT_STMT"; break;
        case ASTNode::Type::INSERT_STMT:        os << "INSERT_STMT"; break;
        case ASTNode::Type::UPDATE_STMT:        os << "UPDATE_STMT"; break;
        case ASTNode::Type::DELETE_STMT:        os << "DELETE_STMT"; break;
        case ASTNode::Type::CREATE_TABLE_STMT:  os << "CREATE_TABLE_STMT"; break;
        case ASTNode::Type::ALTER_TABLE_STMT:   os << "ALTER_TABLE_STMT"; break;
        case ASTNode::Type::DROP_TABLE_STMT:    os << "DROP_TABLE_STMT"; break;
        case ASTNode::Type::BINARY_EXPR:        os << "BINARY_EXPR"; break;
        case ASTNode::Type::UNARY_EXPR:         os << "UNARY_EXPR"; break;
        case ASTNode::Type::LITERAL_EXPR:       os << "LITERAL_EXPR"; break;
        case ASTNode::Type::IDENTIFIER_EXPR:    os << "IDENTIFIER_EXPR"; break;
        case ASTNode::Type::SUBQUERY_EXPR:      os << "SUBQUERY_EXPR"; break;
        case ASTNode::Type::COLUMN_DEF:         os << "COLUMN_DEF"; break;
        case ASTNode::Type::TABLE_REF:          os << "TABLE_REF"; break;
        case ASTNode::Type::COLUMN_REF:         os << "COLUMN_REF"; break;
        case ASTNode::Type::VALUE_LIST:         os << "VALUE_LIST"; break;
        case ASTNode::Type::TABLE_OPTIONS:      os << "TABLE_OPTIONS"; break;
        default:
            os << "UNKNOWN_AST_NODE(" << static_cast<int>(type) << ")";
            break;
    }
    return os;
}

}  // namespace query_engine