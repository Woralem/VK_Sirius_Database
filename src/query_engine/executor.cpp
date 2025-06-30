#include "query_engine/executor.h"
#include "utils/logger.h"
#include <sstream>

namespace query_engine {

std::string astNodeTypeToString(ASTNode::Type type) {
    switch (type) {
        case ASTNode::Type::SELECT_STMT: return "SELECT";
        case ASTNode::Type::INSERT_STMT: return "INSERT";
        case ASTNode::Type::UPDATE_STMT: return "UPDATE";
        case ASTNode::Type::DELETE_STMT: return "DELETE";
        case ASTNode::Type::CREATE_TABLE_STMT: return "CREATE TABLE";
        default: return "UNKNOWN";
    }
}

QueryExecutor::QueryExecutor(std::shared_ptr<StorageInterface> storage)
    : storage(storage) {
    LOG_INFO("Executor", "Initialized with storage interface");
}

nlohmann::json QueryExecutor::execute(const ASTNodePtr& ast) {
    if (!ast) {
        LOG_ERROR("Executor", "Received null AST");
        return {{"error", "Invalid AST"}};
    }

    LOG_INFO("Executor", "Executing " + astNodeTypeToString(ast->type) + " statement");

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
            default:
                LOG_ERROR("Executor", "Unknown statement type");
                return {{"error", "Unknown statement type"}};
        }

        LOG_SUCCESS("Executor", "Execution completed successfully");
        return result;

    } catch (const std::exception& e) {
        LOG_ERROR("Executor", "Execution failed: " + std::string(e.what()));
        return {{"error", e.what()}};
    }
}

nlohmann::json QueryExecutor::executeSelect(const SelectStmt* stmt) {
    LOG_INFO("Executor", "Executing SELECT from table: " + stmt->tableName);

    if (stmt->columns.empty()) {
        LOG_DEBUG("Executor", "Selecting all columns (*)");
    } else {
        LOG_DEBUG("Executor", "Selecting " + std::to_string(stmt->columns.size()) + " columns");
        for (const auto& col : stmt->columns) {
            LOG_DEBUG("Executor", "  - " + col);
        }
    }

    auto predicate = createPredicate(stmt->whereClause.get());

    if (stmt->whereClause) {
        LOG_DEBUG("Executor", "Applying WHERE clause filter");
    }

    auto result = storage->selectRows(stmt->tableName, stmt->columns, predicate);

    if (result.contains("data") && result["data"].is_array()) {
        LOG_SUCCESS("Executor", "Selected " + std::to_string(result["data"].size()) + " rows");
    }

    return result;
}

nlohmann::json QueryExecutor::executeInsert(const InsertStmt* stmt) {
    LOG_INFO("Executor", "Executing INSERT into table: " + stmt->tableName);

    nlohmann::json result;
    int rowsInserted = 0;

    LOG_DEBUG("Executor", "Inserting " + std::to_string(stmt->values.size()) + " row(s)");

    for (size_t i = 0; i < stmt->values.size(); i++) {
        const auto& valueList = stmt->values[i];

        std::stringstream ss;
        ss << "Row " << (i + 1) << ": ";
        for (size_t j = 0; j < valueList.size(); j++) {
            if (j > 0) ss << ", ";
            ss << valueToString(valueList[j]);
        }
        LOG_DEBUG("Executor", ss.str());

        if (storage->insertRow(stmt->tableName, stmt->columns, valueList)) {
            rowsInserted++;
        }
    }

    LOG_SUCCESS("Executor", "Inserted " + std::to_string(rowsInserted) + " row(s)");

    result["status"] = "success";
    result["rows_affected"] = rowsInserted;
    return result;
}

nlohmann::json QueryExecutor::executeUpdate(const UpdateStmt* stmt) {
    LOG_INFO("Executor", "Executing UPDATE on table: " + stmt->tableName);

    LOG_DEBUG("Executor", "Setting " + std::to_string(stmt->assignments.size()) + " column(s)");
    for (const auto& [col, val] : stmt->assignments) {
        LOG_DEBUG("Executor", "  - " + col + " = " + valueToString(val));
    }

    if (stmt->whereClause) {
        LOG_DEBUG("Executor", "Applying WHERE clause filter");
    } else {
        LOG_WARNING("Executor", "UPDATE without WHERE will affect all rows!");
    }

    auto predicate = createPredicate(stmt->whereClause.get());
    int rowsUpdated = storage->updateRows(stmt->tableName, stmt->assignments, predicate);

    LOG_SUCCESS("Executor", "Updated " + std::to_string(rowsUpdated) + " row(s)");

    nlohmann::json result;
    result["status"] = "success";
    result["rows_affected"] = rowsUpdated;
    return result;
}

nlohmann::json QueryExecutor::executeDelete(const DeleteStmt* stmt) {
    LOG_INFO("Executor", "Executing DELETE from table: " + stmt->tableName);

    if (stmt->whereClause) {
        LOG_DEBUG("Executor", "Applying WHERE clause filter");
    } else {
        LOG_WARNING("Executor", "DELETE without WHERE will delete ALL rows!");
    }

    auto predicate = createPredicate(stmt->whereClause.get());
    int rowsDeleted = storage->deleteRows(stmt->tableName, predicate);

    LOG_SUCCESS("Executor", "Deleted " + std::to_string(rowsDeleted) + " row(s)");

    nlohmann::json result;
    result["status"] = "success";
    result["rows_affected"] = rowsDeleted;
    return result;
}

nlohmann::json QueryExecutor::executeCreateTable(const CreateTableStmt* stmt) {
    LOG_INFO("Executor", "Executing CREATE TABLE: " + stmt->tableName);

    LOG_DEBUG("Executor", "Creating table with " + std::to_string(stmt->columns.size()) + " column(s)");

    std::vector<ColumnDef*> columnPtrs;
    for (const auto& col : stmt->columns) {
        std::stringstream ss;
        ss << "  - " << col->name << " " << col->dataType;
        if (col->notNull) ss << " NOT NULL";
        if (col->primaryKey) ss << " PRIMARY KEY";
        LOG_DEBUG("Executor", ss.str());

        columnPtrs.push_back(col.get());
    }

    bool success = storage->createTable(stmt->tableName, columnPtrs);

    nlohmann::json result;
    result["status"] = success ? "success" : "error";
    result["message"] = success ? "Table created successfully" : "Failed to create table";

    if (success) {
        LOG_SUCCESS("Executor", "Table '" + stmt->tableName + "' created successfully");
    } else {
        LOG_ERROR("Executor", "Failed to create table '" + stmt->tableName + "'");
    }

    return result;
}

Value QueryExecutor::evaluateExpression(const ASTNode* expr, const nlohmann::json& row) {
    if (!expr) {
        LOG_DEBUG("Executor", "Evaluating null expression");
        return std::monostate{};
    }

    switch (expr->type) {
        case ASTNode::Type::LITERAL_EXPR: {
            auto* lit = static_cast<const LiteralExpr*>(expr);
            LOG_DEBUG("Executor", "Evaluated literal: " + valueToString(lit->value));
            return lit->value;
        }

        case ASTNode::Type::IDENTIFIER_EXPR: {
            auto* id = static_cast<const IdentifierExpr*>(expr);
            LOG_DEBUG("Executor", "Evaluating identifier: " + id->name);

            if (row.contains(id->name)) {
                auto& val = row[id->name];
                if (val.is_number_integer()) {
                    LOG_DEBUG("Executor", "  Value: " + std::to_string(val.get<int>()));
                    return val.get<int>();
                }
                if (val.is_number_float()) {
                    LOG_DEBUG("Executor", "  Value: " + std::to_string(val.get<double>()));
                    return val.get<double>();
                }
                if (val.is_string()) {
                    LOG_DEBUG("Executor", "  Value: \"" + val.get<std::string>() + "\"");
                    return val.get<std::string>();
                }
                if (val.is_null()) {
                    LOG_DEBUG("Executor", "  Value: NULL");
                    return std::monostate{};
                }
            } else {
                LOG_WARNING("Executor", "Column '" + id->name + "' not found in row");
            }
            return std::monostate{};
        }

        default:
            LOG_WARNING("Executor", "Unknown expression type");
            return std::monostate{};
    }
}

bool QueryExecutor::evaluatePredicate(const ASTNode* expr, const nlohmann::json& row) {
    if (!expr) {
        LOG_DEBUG("Executor", "No predicate, returning true");
        return true;
    }

    if (expr->type == ASTNode::Type::BINARY_EXPR) {
        auto* binExpr = static_cast<const BinaryExpr*>(expr);

        if (binExpr->op == BinaryExpr::Operator::AND) {
            LOG_DEBUG("Executor", "Evaluating AND expression");
            bool left = evaluatePredicate(binExpr->left.get(), row);
            if (!left) return false; // Short-circuit
            bool right = evaluatePredicate(binExpr->right.get(), row);
            bool result = left && right;
            LOG_DEBUG("Executor", "  Result: " + std::string(result ? "true" : "false"));
            return result;
        }

        if (binExpr->op == BinaryExpr::Operator::OR) {
            LOG_DEBUG("Executor", "Evaluating OR expression");
            bool left = evaluatePredicate(binExpr->left.get(), row);
            if (left) return true; // Short-circuit
            bool right = evaluatePredicate(binExpr->right.get(), row);
            bool result = left || right;
            LOG_DEBUG("Executor", "  Result: " + std::string(result ? "true" : "false"));
            return result;
        }

        // Comparison operators
        Value leftVal = evaluateExpression(binExpr->left.get(), row);
        Value rightVal = evaluateExpression(binExpr->right.get(), row);

        LOG_DEBUG("Executor", "Comparing: " + valueToString(leftVal) +
                             " " + (binExpr->op == BinaryExpr::Operator::EQ ? "=" :
                                             binExpr->op == BinaryExpr::Operator::NE ? "!=" :
                                             binExpr->op == BinaryExpr::Operator::LT ? "<" :
                                             binExpr->op == BinaryExpr::Operator::GT ? ">" :
                                             binExpr->op == BinaryExpr::Operator::LE ? "<=" :
                                             binExpr->op == BinaryExpr::Operator::GE ? ">=" : "?") +
                             " " + valueToString(rightVal));

        bool result = false;
        switch (binExpr->op) {
            case BinaryExpr::Operator::EQ: result = leftVal == rightVal; break;
            case BinaryExpr::Operator::NE: result = leftVal != rightVal; break;
            case BinaryExpr::Operator::LT: result = leftVal < rightVal; break;
            case BinaryExpr::Operator::GT: result = leftVal > rightVal; break;
            case BinaryExpr::Operator::LE: result = leftVal <= rightVal; break;
            case BinaryExpr::Operator::GE: result = leftVal >= rightVal; break;
            default: result = false;
        }

        LOG_DEBUG("Executor", "  Result: " + std::string(result ? "true" : "false"));
        return result;
    }

    return true;
}

std::function<bool(const nlohmann::json&)> QueryExecutor::createPredicate(const ASTNode* whereClause) {
    if (!whereClause) {
        return [](const nlohmann::json&) { return true; };
    }

    return [this, whereClause](const nlohmann::json& row) {
        return evaluatePredicate(whereClause, row);
    };
}

std::string QueryExecutor::valueToString(const Value& value) {
    if (std::holds_alternative<int>(value)) {
        return std::to_string(std::get<int>(value));
    } else if (std::holds_alternative<double>(value)) {
        return std::to_string(std::get<double>(value));
    } else if (std::holds_alternative<std::string>(value)) {
        return "'" + std::get<std::string>(value) + "'";
    } else if (std::holds_alternative<bool>(value)) {
        return std::get<bool>(value) ? "true" : "false";
    }
    return "NULL";
}

}