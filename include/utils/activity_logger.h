#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <nlohmann/json.hpp>
#include <deque>
#include <memory>
#include <atomic>
#include <optional>

class ActivityLogger {
public:
    enum class ActionType {
        QUERY_EXECUTED,
        DATABASE_CREATED,
        DATABASE_RENAMED,
        DATABASE_DELETED,
        DATABASE_SWITCHED,
        TABLE_CREATED,
        TABLE_DROPPED,
        TABLE_ALTERED,
        DATA_SELECTED,
        DATA_INSERTED,
        DATA_UPDATED,
        DATA_DELETED,
        ERROR_OCCURRED,
        LOG_VIEWED,
        LOG_DOWNLOADED
    };

    struct LogEntry {
        size_t id;
        std::chrono::system_clock::time_point timestamp;
        ActionType action;
        std::string database;
        std::string details;
        std::string query;
        nlohmann::json result;
        bool success;
        std::string error;
        bool isSelect = false;
    };

private:
    static constexpr size_t MAX_LOG_ENTRIES = 10000;
    static constexpr size_t MAX_RESULT_SIZE = 1000; // Max chars for result preview

    std::deque<LogEntry> entries;
    mutable std::mutex mutex;
    std::string logFilePath;
    bool persistToFile;
    std::atomic<size_t> nextId{1};

    ActivityLogger();

    std::string actionTypeToString(ActionType type) const;
    std::string formatTimestamp(const std::chrono::system_clock::time_point& tp) const;
    std::string formatTimestampShort(const std::chrono::system_clock::time_point& tp) const; // New method
    std::string truncateResult(const nlohmann::json& result) const;
    void writeToFile(const LogEntry& entry);
    void rewriteLogFile();
    void rotateLogsIfNeeded();

public:
    static ActivityLogger& getInstance();

    // Delete copy constructor and assignment
    ActivityLogger(const ActivityLogger&) = delete;
    ActivityLogger& operator=(const ActivityLogger&) = delete;

    void logQuery(const std::string& database, const std::string& query,
                  const nlohmann::json& parsedAST, const nlohmann::json& result,
                  bool success, const std::string& error = "");

    void logDatabaseAction(ActionType action, const std::string& database,
                          const std::string& details = "", bool success = true,
                          const std::string& error = "");

    void logDatabaseSwitch(const std::string& fromDb, const std::string& toDb);
    nlohmann::json getLogsAsJson(size_t limit = 100, size_t offset = 0,
                                std::optional<bool> successFilter = std::nullopt) const;
    std::string getLogsAsText(size_t limit = 100, size_t offset = 0,
                             std::optional<bool> successFilter = std::nullopt) const;
    std::string getLogsAsCsv(size_t limit = 100, size_t offset = 0,
                            std::optional<bool> successFilter = std::nullopt) const;
    bool deleteLogById(size_t id);
    nlohmann::json getLogById(size_t id) const;

    nlohmann::json getLogsByDatabase(const std::string& database, size_t limit = 100,
                                    size_t offset = 0, std::optional<bool> successFilter = std::nullopt) const;

    nlohmann::json getHistoryLogs(size_t limit = 100, size_t offset = 0) const;

    size_t deleteLogsBySuccess(std::optional<bool> successFilter);
    size_t deleteLogsByDatabase(const std::string& database, std::optional<bool> successFilter = std::nullopt);
    void clearLogs();
    size_t getLogCount() const;
    void setPersistToFile(bool persist, const std::string& filePath = "activity.log");
};
