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

std::string ActivityLogger::formatTimestampShort(const std::chrono::system_clock::time_point& tp) const {
    auto time_t = std::chrono::system_clock::to_time_t(tp);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%H:%M");
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
            {"id", entry.id},
            {"timestamp", formatTimestamp(entry.timestamp)},
            {"action", actionTypeToString(entry.action)},
            {"database", entry.database},
            {"details", entry.details},
            {"query", entry.query},
            {"success", entry.success},
            {"error", entry.error},
            {"isSelect", entry.isSelect}
        };

        if (!entry.result.is_null()) {
            jsonEntry["result_preview"] = truncateResult(entry.result);
        }

        file << jsonEntry.dump() << std::endl;
    }
}

void ActivityLogger::rewriteLogFile() {
    if (!persistToFile) return;

    std::ofstream file(logFilePath, std::ios::trunc);
    if (file.is_open()) {
        for (const auto& entry : entries) {
            nlohmann::json jsonEntry = {
                {"id", entry.id},
                {"timestamp", formatTimestamp(entry.timestamp)},
                {"action", actionTypeToString(entry.action)},
                {"database", entry.database},
                {"details", entry.details},
                {"query", entry.query},
                {"success", entry.success},
                {"error", entry.error},
                {"isSelect", entry.isSelect}
            };

            if (!entry.result.is_null()) {
                jsonEntry["result_preview"] = truncateResult(entry.result);
            }

            file << jsonEntry.dump() << std::endl;
        }
        file.close();
    }
}

void ActivityLogger::logQuery(const std::string& database, const std::string& query,
                             const nlohmann::json& parsedAST, const nlohmann::json& result,
                             bool success, const std::string& error) {
    std::lock_guard<std::mutex> lock(mutex);

    LogEntry entry;
    entry.id = nextId++;
    entry.timestamp = std::chrono::system_clock::now();
    entry.database = database;
    entry.query = query;
    entry.success = success;
    entry.error = error;

    bool isSelect = false;
    if (!parsedAST.is_null() && parsedAST.contains("type")) {
        std::string astType = parsedAST["type"];
        isSelect = (astType == "SELECT_STMT" || astType == "SELECT");
    } else {
        std::string upperQuery = query;
        std::transform(upperQuery.begin(), upperQuery.end(), upperQuery.begin(), ::toupper);
        isSelect = upperQuery.find("SELECT") == 0 || upperQuery == "SHOW LOGS" || upperQuery == "SELECT * FROM LOGS";
    }
    entry.isSelect = isSelect;

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

    if (!result.is_null()) {
        if (result.contains("cells") && result["cells"].is_array()) {
            auto dataArray = result["cells"];
            if (dataArray.size() > 10) {
                nlohmann::json limitedResult = result;
                limitedResult["cells"] = nlohmann::json::array();
                for (size_t i = 0; i < 10 && i < dataArray.size(); ++i) {
                    limitedResult["cells"].push_back(dataArray[i]);
                }
                limitedResult["truncated"] = true;
                limitedResult["total_rows"] = dataArray.size();
                entry.result = limitedResult;
            } else {
                entry.result = result;
            }
        } else if (result.contains("data") && result["data"].is_array()) {
            auto dataArray = result["data"];
            if (dataArray.size() > 10) {
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
    entry.id = nextId++;
    entry.timestamp = std::chrono::system_clock::now();
    entry.action = action;
    entry.database = database;
    entry.details = details;
    entry.success = success;
    entry.error = error;
    entry.isSelect = false;

    entries.push_back(entry);
    rotateLogsIfNeeded();
    writeToFile(entry);
}

void ActivityLogger::logDatabaseSwitch(const std::string& fromDb, const std::string& toDb) {
    logDatabaseAction(ActionType::DATABASE_SWITCHED, toDb,
                     std::format("Switched from '{}' to '{}'", fromDb, toDb));
}

nlohmann::json ActivityLogger::getHistoryLogs(size_t limit, size_t offset) const {
    std::lock_guard<std::mutex> lock(mutex);

    std::vector<const LogEntry*> filteredEntries;
    for (const auto& entry : entries) {
        if (!entry.query.empty()) {
            filteredEntries.push_back(&entry);
        }
    }

    nlohmann::json result = nlohmann::json::array();

    size_t start = std::min(offset, filteredEntries.size());
    size_t end = std::min(start + limit, filteredEntries.size());

    for (size_t i = start; i < end; ++i) {
        const auto& entry = *filteredEntries[filteredEntries.size() - 1 - i];

        nlohmann::json jsonEntry = {
            {"id", entry.id},
            {"timestamp", formatTimestampShort(entry.timestamp)}, // Only HH:MM
            {"query", entry.query},
            {"success", entry.success},
            {"isSelect", entry.isSelect}
        };

        result.push_back(jsonEntry);
    }

    return {
        {"history", result},
        {"total", filteredEntries.size()},
        {"offset", offset},
        {"limit", limit}
    };
}

bool ActivityLogger::deleteLogById(size_t id) {
    std::lock_guard<std::mutex> lock(mutex);

    auto it = std::find_if(entries.begin(), entries.end(),
        [id](const LogEntry& entry) { return entry.id == id; });

    if (it != entries.end()) {
        entries.erase(it);
        rewriteLogFile();
        return true;
    }
    return false;
}

nlohmann::json ActivityLogger::getLogById(size_t id) const {
    std::lock_guard<std::mutex> lock(mutex);

    auto it = std::find_if(entries.begin(), entries.end(),
        [id](const LogEntry& entry) { return entry.id == id; });

    if (it != entries.end()) {
        nlohmann::json jsonEntry = {
            {"id", it->id},
            {"timestamp", formatTimestampShort(it->timestamp)},
            {"action", actionTypeToString(it->action)},
            {"database", it->database},
            {"details", it->details},
            {"query", it->query},
            {"success", it->success},
            {"isSelect", it->isSelect}
        };

        if (!it->error.empty()) {
            jsonEntry["error"] = it->error;
        }

        if (!it->result.is_null()) {
            jsonEntry["result"] = it->result;
        }

        return jsonEntry;
    }

    return nlohmann::json{{"error", "Log not found"}};
}

size_t ActivityLogger::deleteLogsBySuccess(std::optional<bool> successFilter) {
    std::lock_guard<std::mutex> lock(mutex);

    size_t deletedCount = 0;

    if (successFilter.has_value()) {
        auto it = std::remove_if(entries.begin(), entries.end(),
            [successFilter, &deletedCount](const LogEntry& entry) {
                if (entry.success == successFilter.value()) {
                    deletedCount++;
                    return true;
                }
                return false;
            });
        entries.erase(it, entries.end());
    } else {
        deletedCount = entries.size();
        entries.clear();
    }

    if (deletedCount > 0) {
        rewriteLogFile();
    }

    return deletedCount;
}

size_t ActivityLogger::deleteLogsByDatabase(const std::string& database, std::optional<bool> successFilter) {
    std::lock_guard<std::mutex> lock(mutex);

    size_t deletedCount = 0;

    auto it = std::remove_if(entries.begin(), entries.end(),
        [&database, successFilter, &deletedCount](const LogEntry& entry) {
            if (entry.database == database) {
                if (!successFilter.has_value() || entry.success == successFilter.value()) {
                    deletedCount++;
                    return true;
                }
            }
            return false;
        });

    entries.erase(it, entries.end());

    if (deletedCount > 0) {
        rewriteLogFile();
    }

    return deletedCount;
}

nlohmann::json ActivityLogger::getLogsByDatabase(const std::string& database, size_t limit,
                                                 size_t offset, std::optional<bool> successFilter) const {
    std::lock_guard<std::mutex> lock(mutex);

    // Filter logs by database and success
    std::vector<const LogEntry*> filteredEntries;
    for (const auto& entry : entries) {
        if (entry.database == database) {
            if (!successFilter.has_value() || entry.success == successFilter.value()) {
                filteredEntries.push_back(&entry);
            }
        }
    }

    nlohmann::json result = nlohmann::json::array();

    size_t start = std::min(offset, filteredEntries.size());
    size_t end = std::min(start + limit, filteredEntries.size());

    for (size_t i = start; i < end; ++i) {
        const auto& entry = *filteredEntries[filteredEntries.size() - 1 - i]; // Newest first

        nlohmann::json jsonEntry = {
            {"id", entry.id},
            {"timestamp", formatTimestampShort(entry.timestamp)},
            {"action", actionTypeToString(entry.action)},
            {"database", entry.database},
            {"details", entry.details},
            {"query", entry.query},
            {"success", entry.success},
            {"isSelect", entry.isSelect}
        };

        if (!entry.error.empty()) {
            jsonEntry["error"] = entry.error;
        }

        if (!entry.result.is_null()) {
            jsonEntry["result"] = entry.result;
        }

        result.push_back(jsonEntry);
    }

    nlohmann::json responseJson = {
        {"logs", result},
        {"total", filteredEntries.size()},
        {"offset", offset},
        {"limit", limit},
        {"database", database}
    };

    if (successFilter.has_value()) {
        responseJson["success_filter"] = successFilter.value();
    } else {
        responseJson["success_filter"] = nlohmann::json(); // null value
    }

    return responseJson;
}

nlohmann::json ActivityLogger::getLogsAsJson(size_t limit, size_t offset, std::optional<bool> successFilter) const {
    std::lock_guard<std::mutex> lock(mutex);

    // Filter logs by success if specified
    std::vector<const LogEntry*> filteredEntries;
    for (const auto& entry : entries) {
        if (!successFilter.has_value() || entry.success == successFilter.value()) {
            filteredEntries.push_back(&entry);
        }
    }

    nlohmann::json result = nlohmann::json::array();

    size_t start = std::min(offset, filteredEntries.size());
    size_t end = std::min(start + limit, filteredEntries.size());

    for (size_t i = start; i < end; ++i) {
        const auto& entry = *filteredEntries[filteredEntries.size() - 1 - i]; // Newest first

        nlohmann::json jsonEntry = {
            {"id", entry.id},
            {"timestamp", formatTimestampShort(entry.timestamp)},
            {"action", actionTypeToString(entry.action)},
            {"database", entry.database},
            {"details", entry.details},
            {"query", entry.query},
            {"success", entry.success},
            {"isSelect", entry.isSelect}
        };

        if (!entry.error.empty()) {
            jsonEntry["error"] = entry.error;
        }

        if (!entry.result.is_null()) {
            jsonEntry["result"] = entry.result;
        }

        result.push_back(jsonEntry);
    }

    nlohmann::json responseJson = {
        {"logs", result},
        {"total", filteredEntries.size()},
        {"offset", offset},
        {"limit", limit}
    };

    if (successFilter.has_value()) {
        responseJson["success_filter"] = successFilter.value();
    } else {
        responseJson["success_filter"] = nlohmann::json(); // null value
    }

    return responseJson;
}

std::string ActivityLogger::getLogsAsText(size_t limit, size_t offset, std::optional<bool> successFilter) const {
    std::lock_guard<std::mutex> lock(mutex);

    std::stringstream ss;
    ss << "=== ACTIVITY LOG ===\n";
    if (successFilter.has_value()) {
        ss << "Filter: " << (successFilter.value() ? "SUCCESS ONLY" : "ERRORS ONLY") << "\n";
    }
    ss << "\n";

    std::vector<const LogEntry*> filteredEntries;
    for (const auto& entry : entries) {
        if (!successFilter.has_value() || entry.success == successFilter.value()) {
            filteredEntries.push_back(&entry);
        }
    }

    size_t start = std::min(offset, filteredEntries.size());
    size_t end = std::min(start + limit, filteredEntries.size());

    for (size_t i = start; i < end; ++i) {
        const auto& entry = *filteredEntries[filteredEntries.size() - 1 - i];

        ss << "[" << formatTimestamp(entry.timestamp) << "] ";
        ss << "ID: " << entry.id << " | ";
        ss << actionTypeToString(entry.action) << " | ";
        ss << "DB: " << entry.database << " | ";
        ss << (entry.success ? "SUCCESS" : "FAILED");
        if (entry.isSelect) ss << " | SELECT";

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

std::string ActivityLogger::getLogsAsCsv(size_t limit, size_t offset, std::optional<bool> successFilter) const {
    std::lock_guard<std::mutex> lock(mutex);

    std::stringstream ss;
    ss << "ID,Timestamp,Action,Database,Success,IsSelect,Query,Details,Error\n";

    std::vector<const LogEntry*> filteredEntries;
    for (const auto& entry : entries) {
        if (!successFilter.has_value() || entry.success == successFilter.value()) {
            filteredEntries.push_back(&entry);
        }
    }

    size_t start = std::min(offset, filteredEntries.size());
    size_t end = std::min(start + limit, filteredEntries.size());

    for (size_t i = start; i < end; ++i) {
        const auto& entry = *filteredEntries[filteredEntries.size() - 1 - i];

        ss << entry.id << ",";
        ss << "\"" << formatTimestamp(entry.timestamp) << "\",";
        ss << "\"" << actionTypeToString(entry.action) << "\",";
        ss << "\"" << entry.database << "\",";
        ss << "\"" << (entry.success ? "YES" : "NO") << "\",";
        ss << "\"" << (entry.isSelect ? "YES" : "NO") << "\",";

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
