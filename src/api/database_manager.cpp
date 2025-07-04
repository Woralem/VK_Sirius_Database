#include "api/database_manager.h"
#include "storage/optimized_in_memory_storage.h"
#include "query_engine/executor.h"

DatabaseManager::DatabaseManager() {
    createDatabase("default");
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
    return true;
}

bool DatabaseManager::renameDatabase(const std::string& oldName, const std::string& newName) {
    std::lock_guard<std::mutex> lock(db_mutex);

    if (oldName == "default") {
        return false;
    }

    auto it = databases.find(oldName);
    if (it == databases.end()) {
        return false;
    }

    if (databases.find(newName) != databases.end()) {
        return false;
    }

    auto dbPair = std::move(it->second);
    databases.erase(it);
    databases[newName] = std::move(dbPair);

    return true;
}

bool DatabaseManager::deleteDatabase(const std::string& name) {
    std::lock_guard<std::mutex> lock(db_mutex);

    if (name == "default") {
        return false;
    }

    return databases.erase(name) > 0;
}

std::vector<std::string> DatabaseManager::listDatabases() {
    std::lock_guard<std::mutex> lock(db_mutex);
    std::vector<std::string> names;

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