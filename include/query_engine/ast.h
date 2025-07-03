#pragma once
#include <memory>
#include <vector>
#include <string>
#include <variant>
#include <ostream>
#include <set>
#include <algorithm>

namespace query_engine {

// Forward declarations
struct ASTNode;
using ASTNodePtr = std::unique_ptr<ASTNode>;

// Value types
using Value = std::variant<std::monostate, int, double, std::string, bool>;

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

    bool validate() const {
        return maxColumnNameLength > 0 && maxColumnNameLength <= 64 &&
               maxStringLength > 0 && maxStringLength <= (1ULL << 40) &&
               gcFrequencyDays >= 1 && gcFrequencyDays <= 365;
    }
};

// Base AST Node
struct ASTNode {
    enum class Type {
        // Statements
        SELECT_STMT, INSERT_STMT, UPDATE_STMT, DELETE_STMT, CREATE_TABLE_STMT, ALTER_TABLE_STMT,

        // Expressions
        BINARY_EXPR, UNARY_EXPR, LITERAL_EXPR, IDENTIFIER_EXPR,

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

struct BinaryExpr : public Expression {
    enum class Operator {
        EQ, NE, LT, GT, LE, GE, AND, OR, LIKE
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
        ALTER_COLUMN_TYPE
    };
    AlterType alterType;
    std::string tableName;
    std::string newTableName;
    std::string columnName;
    std::string newColumnName;
    std::string newDataType;
    DataType newParsedType;
    AlterTableStmt() : Statement(Type::ALTER_TABLE_STMT) {}
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
inline std::string dataTypeToString(DataType type) {
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

inline DataType parseDataType(const std::string& typeStr) {
    std::string upper = typeStr;
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
        case ASTNode::Type::BINARY_EXPR:       os << "BINARY_EXPR"; break;
        case ASTNode::Type::UNARY_EXPR:        os << "UNARY_EXPR"; break;
        case ASTNode::Type::LITERAL_EXPR:     os << "LITERAL_EXPR"; break;
        case ASTNode::Type::IDENTIFIER_EXPR:  os << "IDENTIFIER_EXPR"; break;
        case ASTNode::Type::COLUMN_DEF:         os << "COLUMN_DEF"; break;
        case ASTNode::Type::TABLE_REF:          os << "TABLE_REF"; break;
        case ASTNode::Type::COLUMN_REF:         os << "COLUMN_REF"; break;
        case ASTNode::Type::VALUE_LIST:         os << "VALUE_LIST"; break;
        case ASTNode::Type::TABLE_OPTIONS:      os << "TABLE_OPTIONS"; break;
        default: os << "UNKNOWN_AST_NODE(" << static_cast<int>(type) << ")"; break;
    }
    return os;
}

}