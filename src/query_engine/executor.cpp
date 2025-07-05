#include "query_engine/executor.h"
#include "utils/logger.h"
#include <regex>

namespace query_engine {

    bool matchLikePattern(const std::string& text, const std::string& pattern) {
        size_t tLen = text.length();
        size_t pLen = pattern.length();
        size_t tIdx = 0;
        size_t pIdx = 0;
        size_t tStar = std::string::npos;
        size_t pStar = std::string::npos;

        while (tIdx < tLen) {
            if (pIdx < pLen && (pattern[pIdx] == text[tIdx] || pattern[pIdx] == '_')) {
                tIdx++;
                pIdx++;
            }
            else if (pIdx < pLen && pattern[pIdx] == '%') {
                tStar = tIdx;
                pStar = pIdx;
                pIdx++;
            }
            else if (pStar != std::string::npos) {
                pIdx = pStar + 1;
                tStar++;
                tIdx = tStar;
            }
            else {
                return false;
            }
        }

        while (pIdx < pLen && pattern[pIdx] == '%') {
            pIdx++;
        }

        return pIdx == pLen;
    }
std::string astNodeTypeToString(ASTNode::Type type) {
    switch (type) {
        case ASTNode::Type::SELECT_STMT: return "SELECT";
        case ASTNode::Type::INSERT_STMT: return "INSERT";
        case ASTNode::Type::UPDATE_STMT: return "UPDATE";
        case ASTNode::Type::DELETE_STMT: return "DELETE";
        case ASTNode::Type::CREATE_TABLE_STMT: return "CREATE TABLE";
        case ASTNode::Type::ALTER_TABLE_STMT: return "ALTER TABLE";
        case ASTNode::Type::DROP_TABLE_STMT: return "DROP TABLE";
        default: return "UNKNOWN";
    }
}

QueryExecutor::QueryExecutor(std::shared_ptr<StorageInterface> storage)
    : storage(storage), enableLogging(true) {
    if (enableLogging) {
        LOG_INFO("Executor", "Initialized with storage interface");
    }
}

nlohmann::json QueryExecutor::execute(const ASTNodePtr& ast) {
    if (!ast) {
        if (enableLogging) LOG_ERROR("Executor", "Received null AST");
        return {{"error", "Invalid AST"}};
    }

    if (enableLogging) {
        LOG_INFO("Executor", "Executing " + astNodeTypeToString(ast->type) + " statement");
    }

    try {
        nlohmann::json result;

        switch (ast->type) {
            case ASTNode::Type::SELECT_STMT:
                result = executeSelect(static_cast<SelectStmt*>(ast.get()));
                break;
            case ASTNode::Type::INSERT_STMT:
                result = executeInsert(static_cast<InsertStmt*>(ast.get()));
                break;
            case ASTNode::Type::UPDATE_STMT:
                result = executeUpdate(static_cast<UpdateStmt*>(ast.get()));
                break;
            case ASTNode::Type::DELETE_STMT:
                result = executeDelete(static_cast<DeleteStmt*>(ast.get()));
                break;
            case ASTNode::Type::CREATE_TABLE_STMT:
                result = executeCreateTable(static_cast<CreateTableStmt*>(ast.get()));
                break;
            case ASTNode::Type::ALTER_TABLE_STMT:
                result = executeAlterTable(static_cast<AlterTableStmt*>(ast.get()));
                break;
            case ASTNode::Type::DROP_TABLE_STMT:
                result = executeDropTable(static_cast<DropTableStmt*>(ast.get()));
                break;
            default:
                if (enableLogging) LOG_ERROR("Executor", "Unknown statement type");
                return {{"error", "Unknown statement type"}};
        }

        if (enableLogging) LOG_SUCCESS("Executor", "Execution completed successfully");
        return result;

    } catch (const std::exception& e) {
        if (enableLogging) LOG_ERROR("Executor", "Execution failed: " + std::string(e.what()));
        return {{"error", e.what()}};
    }
}

nlohmann::json QueryExecutor::executeSelect(const SelectStmt* stmt) {
    if (enableLogging) {
        LOG_INFO("Executor", "Executing SELECT from table: " + stmt->tableName);

        if (stmt->columns.empty()) {
            LOG_DEBUG("Executor", "Selecting all columns (*)");
        } else {
            LOG_DEBUG("Executor", "Selecting " + std::to_string(stmt->columns.size()) + " columns");
        }
    }

    auto predicate = createPredicate(stmt->whereClause.get());

    auto result = storage->selectRows(stmt->tableName, stmt->columns, predicate);

    if (enableLogging && result.contains("data") && result["data"].is_array()) {
        LOG_SUCCESS("Executor", "Selected " + std::to_string(result["data"].size()) + " rows");
    }

    return result;
}

nlohmann::json QueryExecutor::executeInsert(const InsertStmt* stmt) {
    if (enableLogging) {
        LOG_INFO("Executor", "Executing INSERT into table: " + stmt->tableName);
        LOG_DEBUG("Executor", "Inserting " + std::to_string(stmt->values.size()) + " row(s)");
    }

    nlohmann::json result;
    int rowsInserted = 0;

    for (const auto& valueList : stmt->values) {
        if (storage->insertRow(stmt->tableName, stmt->columns, valueList)) {
            rowsInserted++;
        }
    }

    if (enableLogging) {
        LOG_SUCCESS("Executor", "Inserted " + std::to_string(rowsInserted) + " row(s)");
    }

    result["status"] = "success";
    result["rows_affected"] = rowsInserted;
    return result;
}

nlohmann::json QueryExecutor::executeUpdate(const UpdateStmt* stmt) {
    if (enableLogging) {
        LOG_INFO("Executor", "Executing UPDATE on table: " + stmt->tableName);
        LOG_DEBUG("Executor", "Setting " + std::to_string(stmt->assignments.size()) + " column(s)");

        if (!stmt->whereClause) {
            LOG_WARNING("Executor", "UPDATE without WHERE will affect all rows!");
        }
    }

    auto predicate = createPredicate(stmt->whereClause.get());
    int rowsUpdated = storage->updateRows(stmt->tableName, stmt->assignments, predicate);

    if (enableLogging) {
        LOG_SUCCESS("Executor", "Updated " + std::to_string(rowsUpdated) + " row(s)");
    }

    nlohmann::json result;
    result["status"] = "success";
    result["rows_affected"] = rowsUpdated;
    return result;
}

nlohmann::json QueryExecutor::executeDelete(const DeleteStmt* stmt) {
    if (enableLogging) {
        LOG_INFO("Executor", "Executing DELETE from table: " + stmt->tableName);

        if (!stmt->whereClause) {
            LOG_WARNING("Executor", "DELETE without WHERE will delete ALL rows!");
        }
    }

    auto predicate = createPredicate(stmt->whereClause.get());
    int rowsDeleted = storage->deleteRows(stmt->tableName, predicate);

    if (enableLogging) {
        LOG_SUCCESS("Executor", "Deleted " + std::to_string(rowsDeleted) + " row(s)");
    }

    nlohmann::json result;
    result["status"] = "success";
    result["rows_affected"] = rowsDeleted;
    return result;
}

nlohmann::json QueryExecutor::executeCreateTable(const CreateTableStmt* stmt) {
    if (enableLogging) {
        LOG_INFO("Executor", "Executing CREATE TABLE: " + stmt->tableName);
        LOG_DEBUG("Executor", "Creating table with " + std::to_string(stmt->columns.size()) + " column(s)");

        // Log table options
        if (!stmt->options.allowedTypes.empty()) {
            std::string typesStr = "Allowed types: ";
            for (auto type : stmt->options.allowedTypes) {
                typesStr += dataTypeToString(type) + " ";
            }
            LOG_DEBUG("Executor", typesStr);
        }
        LOG_DEBUG("Executor", "Max column name length: " + std::to_string(stmt->options.maxColumnNameLength));
        LOG_DEBUG("Executor", "Max string length: " + std::to_string(stmt->options.maxStringLength));
        LOG_DEBUG("Executor", "GC frequency: " + std::to_string(stmt->options.gcFrequencyDays) + " days");
    }

    // Validate options
    if (!stmt->options.validate()) {
        LOG_ERROR("Executor", "Invalid table options");
        nlohmann::json result;
        result["status"] = "error";
        result["message"] = "Invalid table options";
        return result;
    }

    std::vector<ColumnDef*> columnPtrs;
    columnPtrs.reserve(stmt->columns.size());

    for (const auto& col : stmt->columns) {
        columnPtrs.push_back(col.get());
    }

    bool success = storage->createTable(stmt->tableName, columnPtrs, stmt->options);

    nlohmann::json result;
    result["status"] = success ? "success" : "error";
    result["message"] = success ? "Table created successfully" : "Failed to create table";

    if (enableLogging) {
        if (success) {
            LOG_SUCCESS("Executor", "Table '" + stmt->tableName + "' created successfully");
        } else {
            LOG_ERROR("Executor", "Failed to create table '" + stmt->tableName + "'");
        }
    }

    return result;
}

nlohmann::json QueryExecutor::executeAlterTable(const AlterTableStmt* stmt) {
    if (enableLogging) {
        LOG_INFO("Executor", "Executing ALTER TABLE on: " + stmt->tableName);
    }

    nlohmann::json result;
    bool success = false;
    std::string message;

    switch (stmt->alterType) {
        case AlterTableStmt::AlterType::RENAME_TABLE:
            if (enableLogging) {
                LOG_DEBUG("Executor", "Renaming table to: " + stmt->newTableName);
            }
            success = storage->renameTable(stmt->tableName, stmt->newTableName);
            message = success ? "Table renamed successfully" : "Failed to rename table";
            break;

        case AlterTableStmt::AlterType::RENAME_COLUMN:
            if (enableLogging) {
                LOG_DEBUG("Executor", "Renaming column '" + stmt->columnName + "' to '" + stmt->newColumnName + "'");
            }
            success = storage->renameColumn(stmt->tableName, stmt->columnName, stmt->newColumnName);
            message = success ? "Column renamed successfully" : "Failed to rename column";
            break;

        case AlterTableStmt::AlterType::ALTER_COLUMN_TYPE:
            if (enableLogging) {
                LOG_DEBUG("Executor", "Changing column '" + stmt->columnName + "' type to: " + stmt->newDataType);
            }
            success = storage->alterColumnType(stmt->tableName, stmt->columnName, stmt->newParsedType);
            message = success ? "Column type changed successfully" : "Failed to change column type";
            break;

        case AlterTableStmt::AlterType::DROP_COLUMN:
            if (enableLogging) {
                LOG_DEBUG("Executor", "Dropping column '" + stmt->columnName + "'");
            }
            success = storage->dropColumn(stmt->tableName, stmt->columnName);
            message = success ? "Column dropped successfully" : "Failed to drop column";
            break;
    }

    result["status"] = success ? "success" : "error";
    result["message"] = message;

    if (enableLogging) {
        if (success) {
            LOG_SUCCESS("Executor", message);
        } else {
            LOG_ERROR("Executor", message);
        }
    }

    return result;
}

nlohmann::json QueryExecutor::executeDropTable(const DropTableStmt* stmt) {
    if (enableLogging) {
        LOG_INFO("Executor", "Executing DROP TABLE: " + stmt->tableName);
    }

    bool success = storage->dropTable(stmt->tableName);

    nlohmann::json result;

    if (success) {
        result["status"] = "success";
        result["message"] = "Table '" + stmt->tableName + "' dropped successfully";
        if (enableLogging) {
            LOG_SUCCESS("Executor", "Table '" + stmt->tableName + "' dropped successfully");
        }
    } else {
        if (stmt->ifExists) {
            // Если используется IF EXISTS, не считаем отсутствие таблицы ошибкой
            result["status"] = "success";
            result["message"] = "Table '" + stmt->tableName + "' does not exist (IF EXISTS specified)";
            if (enableLogging) {
                LOG_INFO("Executor", "Table '" + stmt->tableName + "' does not exist (IF EXISTS specified)");
            }
        } else {
            result["status"] = "error";
            result["message"] = "Table '" + stmt->tableName + "' does not exist";
            if (enableLogging) {
                LOG_ERROR("Executor", "Table '" + stmt->tableName + "' does not exist");
            }
        }
    }

    return result;
}

Value QueryExecutor::evaluateExpression(const ASTNode* expr, const nlohmann::json& row) {
    if (!expr) {
        return std::monostate{};
    }

    switch (expr->type) {
        case ASTNode::Type::LITERAL_EXPR: {
            auto* lit = static_cast<const LiteralExpr*>(expr);
            return lit->value;
        }

        case ASTNode::Type::IDENTIFIER_EXPR: {
            auto* id = static_cast<const IdentifierExpr*>(expr);

            if (row.contains(id->name)) {
                const auto& val = row[id->name];
                if (val.is_number_integer()) {
                    return val.get<int>();
                }
                if (val.is_number_float()) {
                    return val.get<double>();
                }
                if (val.is_string()) {
                    return val.get<std::string>();
                }
                if (val.is_boolean()) {
                    return val.get<bool>();
                }
                if (val.is_null()) {
                    return std::monostate{};
                }
            }
            return std::monostate{};
        }

        default:
            return std::monostate{};
    }
}

// Helper function for type-aware value comparison
bool compareValues(const Value& left, const Value& right, BinaryExpr::Operator op) {
    if (op == BinaryExpr::Operator::LIKE) {
        if (!std::holds_alternative<std::string>(left) || !std::holds_alternative<std::string>(right)) {
            return false;
        }

        const std::string& text = std::get<std::string>(left);
        const std::string& pattern = std::get<std::string>(right);

        return matchLikePattern(text, pattern);
    }

    bool leftIsNumeric = std::holds_alternative<int>(left) || std::holds_alternative<double>(left);
    bool rightIsNumeric = std::holds_alternative<int>(right) || std::holds_alternative<double>(right);

    if (leftIsNumeric && rightIsNumeric) {
        double leftDouble = std::holds_alternative<int>(left) ? static_cast<double>(std::get<int>(left)) : std::get<double>(left);
        double rightDouble = std::holds_alternative<int>(right) ? static_cast<double>(std::get<int>(right)) : std::get<double>(right);

        switch (op) {
            case BinaryExpr::Operator::EQ: return leftDouble == rightDouble;
            case BinaryExpr::Operator::NE: return leftDouble != rightDouble;
            case BinaryExpr::Operator::LT: return leftDouble < rightDouble;
            case BinaryExpr::Operator::GT: return leftDouble > rightDouble;
            case BinaryExpr::Operator::LE: return leftDouble <= rightDouble;
            case BinaryExpr::Operator::GE: return leftDouble >= rightDouble;
            default: return false;
        }
    }

    // Default strict comparison for non-numeric types (string, bool, etc.)
    switch (op) {
        case BinaryExpr::Operator::EQ: return left == right;
        case BinaryExpr::Operator::NE: return left != right;
        case BinaryExpr::Operator::LT: return left < right;
        case BinaryExpr::Operator::GT: return left > right;
        case BinaryExpr::Operator::LE: return left <= right;
        case BinaryExpr::Operator::GE: return left >= right;
        default: return false;
    }
}

bool QueryExecutor::evaluatePredicate(const ASTNode* expr, const nlohmann::json& row) {
    if (!expr) {
        return true;
    }

    if (expr->type == ASTNode::Type::BINARY_EXPR) {
        auto* binExpr = static_cast<const BinaryExpr*>(expr);

        if (binExpr->op == BinaryExpr::Operator::AND) {
            bool left = evaluatePredicate(binExpr->left.get(), row);
            if (!left) return false; // Short-circuit
            return evaluatePredicate(binExpr->right.get(), row);
        }

        if (binExpr->op == BinaryExpr::Operator::OR) {
            bool left = evaluatePredicate(binExpr->left.get(), row);
            if (left) return true; // Short-circuit
            return evaluatePredicate(binExpr->right.get(), row);
        }

        // Comparison operators (including LIKE)
        Value leftVal = evaluateExpression(binExpr->left.get(), row);
        Value rightVal = evaluateExpression(binExpr->right.get(), row);

        return compareValues(leftVal, rightVal, binExpr->op);
    }

    return true;
}

std::function<bool(const nlohmann::json&)> QueryExecutor::createPredicate(const ASTNode* whereClause) {
    if (!whereClause) {
        return [](const nlohmann::json&) { return true; };
    }

    // Check cache first
    auto it = predicateCache.find(whereClause);
    if (it != predicateCache.end()) {
        return it->second;
    }

    auto predicate = [this, whereClause](const nlohmann::json& row) {
        return evaluatePredicate(whereClause, row);
    };

    // Cache the predicate
    predicateCache[whereClause] = predicate;

    return predicate;
}

void QueryExecutor::appendValueToString(std::string& result, const Value& value) {
    if (std::holds_alternative<int>(value)) {
        result += std::to_string(std::get<int>(value));
    } else if (std::holds_alternative<double>(value)) {
        result += std::to_string(std::get<double>(value));
    } else if (std::holds_alternative<std::string>(value)) {
        result += '\'';
        result += std::get<std::string>(value);
        result += '\'';
    } else if (std::holds_alternative<bool>(value)) {
        result += std::get<bool>(value) ? "true" : "false";
    } else {
        result += "NULL";
    }
}

std::string QueryExecutor::valueToString(const Value& value) {
    std::string result;
    result.reserve(32); // Pre-allocate
    appendValueToString(result, value);
    return result;
}

}