#pragma once

#include <memory>
#include <unordered_map>
#include <mutex>
#include <vector>
#include <string>

class OptimizedInMemoryStorage;
namespace query_engine {
    class OptimizedQueryExecutor;
}

class DatabaseManager {
private:
    std::unordered_map<std::string, std::pair<
        std::shared_ptr<OptimizedInMemoryStorage>,
        std::shared_ptr<query_engine::OptimizedQueryExecutor>
    >> databases;
    std::mutex db_mutex;

public:
    DatabaseManager();
    
    bool createDatabase(const std::string& name);
    bool renameDatabase(const std::string& oldName, const std::string& newName);
    bool deleteDatabase(const std::string& name);
    std::vector<std::string> listDatabases();
    std::shared_ptr<query_engine::OptimizedQueryExecutor> getExecutor(const std::string& name);
};