#include "api/database_manager.h"
#include "storage/optimized_in_memory_storage.h"
#include "query_engine/executor.h"
#include "utils/activity_logger.h"

DatabaseManager::DatabaseManager() {
    (void)createDatabase("default");
}

bool DatabaseManager::createDatabase(const std::string& name) {
    std::lock_guard<std::mutex> lock(db_mutex);

    if (databases.find(name) != databases.end()) {
        return false;
    }

    auto storage = std::make_shared<OptimizedInMemoryStorage>();
    auto executor = std::make_shared<query_engine::OptimizedQueryExecutor>(storage);
    executor->setLoggingEnabled(false);

    databases[name] = {storage, executor};

    // Log database creation
    auto& logger = ActivityLogger::getInstance();
    logger.logDatabaseAction(ActivityLogger::ActionType::DATABASE_CREATED, name,
                           "Database created successfully");

    return true;
}

bool DatabaseManager::renameDatabase(const std::string& oldName, const std::string& newName) {
    std::lock_guard<std::mutex> lock(db_mutex);

    if (oldName == "default") {
        auto& logger = ActivityLogger::getInstance();
        logger.logDatabaseAction(ActivityLogger::ActionType::DATABASE_RENAMED, oldName,
                               "Cannot rename default database", false);
        return false;
    }

    auto it = databases.find(oldName);
    if (it == databases.end()) {
        auto& logger = ActivityLogger::getInstance();
        logger.logDatabaseAction(ActivityLogger::ActionType::DATABASE_RENAMED, oldName,
                               "Database not found", false);
        return false;
    }

    if (databases.find(newName) != databases.end()) {
        auto& logger = ActivityLogger::getInstance();
        logger.logDatabaseAction(ActivityLogger::ActionType::DATABASE_RENAMED, oldName,
                               "Target database name already exists", false);
        return false;
    }

    auto dbPair = std::move(it->second);
    databases.erase(it);
    databases[newName] = std::move(dbPair);

    // Log successful rename
    auto& logger = ActivityLogger::getInstance();
    logger.logDatabaseAction(ActivityLogger::ActionType::DATABASE_RENAMED, newName,
                           std::format("Renamed from '{}' to '{}'", oldName, newName));

    return true;
}

bool DatabaseManager::deleteDatabase(const std::string& name) {
    std::lock_guard<std::mutex> lock(db_mutex);

    if (name == "default") {
        auto& logger = ActivityLogger::getInstance();
        logger.logDatabaseAction(ActivityLogger::ActionType::DATABASE_DELETED, name,
                               "Cannot delete default database", false);
        return false;
    }

    bool deleted = databases.erase(name) > 0;

    // Log deletion attempt
    auto& logger = ActivityLogger::getInstance();
    if (deleted) {
        logger.logDatabaseAction(ActivityLogger::ActionType::DATABASE_DELETED, name,
                               "Database deleted successfully");
    } else {
        logger.logDatabaseAction(ActivityLogger::ActionType::DATABASE_DELETED, name,
                               "Database not found", false);
    }

    return deleted;
}

std::vector<std::string> DatabaseManager::listDatabases() const {
    std::lock_guard<std::mutex> lock(db_mutex);
    std::vector<std::string> names;
    names.reserve(databases.size());

    for (const auto& pair : databases) {
        names.push_back(pair.first);
    }

    return names;
}

std::shared_ptr<query_engine::OptimizedQueryExecutor> DatabaseManager::getExecutor(const std::string& name) {
    std::lock_guard<std::mutex> lock(db_mutex);

    auto it = databases.find(name);
    if (it == databases.end()) {
        return nullptr;
    }

    return it->second.second;
}