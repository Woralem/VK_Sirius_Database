#include "utils/activity_logger.h"
#include <algorithm>
#include <format>
#include <regex>

ActivityLogger::ActivityLogger() : persistToFile(false) {}

ActivityLogger& ActivityLogger::getInstance() {
    static ActivityLogger instance;
    return instance;
}

std::string ActivityLogger::actionTypeToString(ActionType type) const {
    switch (type) {
        case ActionType::QUERY_EXECUTED: return "QUERY_EXECUTED";
        case ActionType::DATABASE_CREATED: return "DATABASE_CREATED";
        case ActionType::DATABASE_RENAMED: return "DATABASE_RENAMED";
        case ActionType::DATABASE_DELETED: return "DATABASE_DELETED";
        case ActionType::DATABASE_SWITCHED: return "DATABASE_SWITCHED";
        case ActionType::TABLE_CREATED: return "TABLE_CREATED";
        case ActionType::TABLE_DROPPED: return "TABLE_DROPPED";
        case ActionType::TABLE_ALTERED: return "TABLE_ALTERED";
        case ActionType::DATA_SELECTED: return "DATA_SELECTED";
        case ActionType::DATA_INSERTED: return "DATA_INSERTED";
        case ActionType::DATA_UPDATED: return "DATA_UPDATED";
        case ActionType::DATA_DELETED: return "DATA_DELETED";
        case ActionType::ERROR_OCCURRED: return "ERROR_OCCURRED";
        case ActionType::LOG_VIEWED: return "LOG_VIEWED";
        case ActionType::LOG_DOWNLOADED: return "LOG_DOWNLOADED";
        default: return "UNKNOWN";
    }
}

std::string ActivityLogger::formatTimestamp(const std::chrono::system_clock::time_point& tp) const {
    auto time_t = std::chrono::system_clock::to_time_t(tp);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
    return ss.str();
}

std::string ActivityLogger::truncateResult(const nlohmann::json& result) const {
    std::string resultStr = result.dump();
    if (resultStr.length() > MAX_RESULT_SIZE) {
        return resultStr.substr(0, MAX_RESULT_SIZE - 3) + "...";
    }
    return resultStr;
}

void ActivityLogger::rotateLogsIfNeeded() {
    while (entries.size() > MAX_LOG_ENTRIES) {
        entries.pop_front();
    }
}

void ActivityLogger::writeToFile(const LogEntry& entry) {
    if (!persistToFile) return;
    
    std::ofstream file(logFilePath, std::ios::app);
    if (file.is_open()) {
        nlohmann::json jsonEntry = {
            {"timestamp", formatTimestamp(entry.timestamp)},
            {"action", actionTypeToString(entry.action)},
            {"database", entry.database},
            {"details", entry.details},
            {"query", entry.query},
            {"success", entry.success},
            {"error", entry.error}
        };
        
        if (!entry.result.is_null()) {
            jsonEntry["result_preview"] = truncateResult(entry.result);
        }
        
        file << jsonEntry.dump() << std::endl;
    }
}

void ActivityLogger::logQuery(const std::string& database, const std::string& query,
                             const nlohmann::json& parsedAST, const nlohmann::json& result,
                             bool success, const std::string& error) {
    std::lock_guard<std::mutex> lock(mutex);
    
    LogEntry entry;
    entry.timestamp = std::chrono::system_clock::now();
    entry.database = database;
    entry.query = query;
    entry.success = success;
    entry.error = error;
    
    // Determine action type based on parsed AST
    if (!parsedAST.is_null() && parsedAST.contains("type")) {
        std::string astType = parsedAST["type"];
        if (astType == "SELECT_STMT" || astType == "SELECT") {
            entry.action = ActionType::DATA_SELECTED;
        } else if (astType == "INSERT_STMT" || astType == "INSERT") {
            entry.action = ActionType::DATA_INSERTED;
        } else if (astType == "UPDATE_STMT" || astType == "UPDATE") {
            entry.action = ActionType::DATA_UPDATED;
        } else if (astType == "DELETE_STMT" || astType == "DELETE") {
            entry.action = ActionType::DATA_DELETED;
        } else if (astType == "CREATE_TABLE_STMT" || astType == "CREATE TABLE") {
            entry.action = ActionType::TABLE_CREATED;
        } else if (astType == "DROP_TABLE_STMT" || astType == "DROP TABLE") {
            entry.action = ActionType::TABLE_DROPPED;
        } else if (astType == "ALTER_TABLE_STMT" || astType == "ALTER TABLE") {
            entry.action = ActionType::TABLE_ALTERED;
        } else {
            entry.action = ActionType::QUERY_EXECUTED;
        }
    } else {
        entry.action = success ? ActionType::QUERY_EXECUTED : ActionType::ERROR_OCCURRED;
    }
    
    // Store limited result to avoid memory issues
    if (!result.is_null()) {
        if (result.contains("data") && result["data"].is_array()) {
            auto dataArray = result["data"];
            if (dataArray.size() > 10) {
                // Only store first 10 rows
                nlohmann::json limitedResult = result;
                limitedResult["data"] = nlohmann::json::array();
                for (size_t i = 0; i < 10 && i < dataArray.size(); ++i) {
                    limitedResult["data"].push_back(dataArray[i]);
                }
                limitedResult["truncated"] = true;
                limitedResult["total_rows"] = dataArray.size();
                entry.result = limitedResult;
            } else {
                entry.result = result;
            }
        } else {
            entry.result = result;
        }
    }
    
    entry.details = std::format("Query type: {}", actionTypeToString(entry.action));
    
    entries.push_back(entry);
    rotateLogsIfNeeded();
    writeToFile(entry);
}

void ActivityLogger::logDatabaseAction(ActionType action, const std::string& database,
                                      const std::string& details, bool success,
                                      const std::string& error) {
    std::lock_guard<std::mutex> lock(mutex);
    
    LogEntry entry;
    entry.timestamp = std::chrono::system_clock::now();
    entry.action = action;
    entry.database = database;
    entry.details = details;
    entry.success = success;
    entry.error = error;
    
    entries.push_back(entry);
    rotateLogsIfNeeded();
    writeToFile(entry);
}

void ActivityLogger::logDatabaseSwitch(const std::string& fromDb, const std::string& toDb) {
    logDatabaseAction(ActionType::DATABASE_SWITCHED, toDb,
                     std::format("Switched from '{}' to '{}'", fromDb, toDb));
}

nlohmann::json ActivityLogger::getLogsAsJson(size_t limit, size_t offset) const {
    std::lock_guard<std::mutex> lock(mutex);
    
    nlohmann::json result = nlohmann::json::array();
    
    size_t start = std::min(offset, entries.size());
    size_t end = std::min(start + limit, entries.size());
    
    for (size_t i = start; i < end; ++i) {
        const auto& entry = entries[entries.size() - 1 - i]; // Newest first
        
        nlohmann::json jsonEntry = {
            {"timestamp", formatTimestamp(entry.timestamp)},
            {"action", actionTypeToString(entry.action)},
            {"database", entry.database},
            {"details", entry.details},
            {"query", entry.query},
            {"success", entry.success}
        };
        
        if (!entry.error.empty()) {
            jsonEntry["error"] = entry.error;
        }
        
        if (!entry.result.is_null()) {
            jsonEntry["result"] = entry.result;
        }
        
        result.push_back(jsonEntry);
    }
    
    return {
        {"logs", result},
        {"total", entries.size()},
        {"offset", offset},
        {"limit", limit}
    };
}

std::string ActivityLogger::getLogsAsText(size_t limit, size_t offset) const {
    std::lock_guard<std::mutex> lock(mutex);
    
    std::stringstream ss;
    ss << "=== ACTIVITY LOG ===\n\n";
    
    size_t start = std::min(offset, entries.size());
    size_t end = std::min(start + limit, entries.size());
    
    for (size_t i = start; i < end; ++i) {
        const auto& entry = entries[entries.size() - 1 - i];
        
        ss << "[" << formatTimestamp(entry.timestamp) << "] ";
        ss << actionTypeToString(entry.action) << " | ";
        ss << "DB: " << entry.database << " | ";
        ss << (entry.success ? "SUCCESS" : "FAILED");
        
        if (!entry.query.empty()) {
            ss << "\nQuery: " << entry.query;
        }
        
        if (!entry.details.empty()) {
            ss << "\nDetails: " << entry.details;
        }
        
        if (!entry.error.empty()) {
            ss << "\nError: " << entry.error;
        }
        
        ss << "\n" << std::string(80, '-') << "\n";
    }
    
    return ss.str();
}

std::string ActivityLogger::getLogsAsCsv(size_t limit, size_t offset) const {
    std::lock_guard<std::mutex> lock(mutex);
    
    std::stringstream ss;
    ss << "Timestamp,Action,Database,Success,Query,Details,Error\n";
    
    size_t start = std::min(offset, entries.size());
    size_t end = std::min(start + limit, entries.size());
    
    for (size_t i = start; i < end; ++i) {
        const auto& entry = entries[entries.size() - 1 - i];
        
        ss << "\"" << formatTimestamp(entry.timestamp) << "\",";
        ss << "\"" << actionTypeToString(entry.action) << "\",";
        ss << "\"" << entry.database << "\",";
        ss << "\"" << (entry.success ? "YES" : "NO") << "\",";
        
        // Escape quotes in query
        std::string escapedQuery = entry.query;
        std::replace(escapedQuery.begin(), escapedQuery.end(), '"', '\'');
        ss << "\"" << escapedQuery << "\",";
        
        ss << "\"" << entry.details << "\",";
        ss << "\"" << entry.error << "\"\n";
    }
    
    return ss.str();
}

void ActivityLogger::clearLogs() {
    std::lock_guard<std::mutex> lock(mutex);
    entries.clear();
    
    if (persistToFile) {
        std::ofstream file(logFilePath, std::ios::trunc);
        file.close();
    }
}

size_t ActivityLogger::getLogCount() const {
    std::lock_guard<std::mutex> lock(mutex);
    return entries.size();
}

void ActivityLogger::setPersistToFile(bool persist, const std::string& filePath) {
    std::lock_guard<std::mutex> lock(mutex);
    persistToFile = persist;
    logFilePath = filePath;
}