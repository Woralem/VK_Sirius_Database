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
    mutable std::mutex db_mutex;

public:
    DatabaseManager();

    [[nodiscard]] bool createDatabase(const std::string& name);
    [[nodiscard]] bool renameDatabase(const std::string& oldName, const std::string& newName);
    [[nodiscard]] bool deleteDatabase(const std::string& name);
    [[nodiscard]] std::vector<std::string> listDatabases() const;
    [[nodiscard]] std::shared_ptr<query_engine::OptimizedQueryExecutor> getExecutor(const std::string& name);
};