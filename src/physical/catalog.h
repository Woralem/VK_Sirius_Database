#pragma once

#include "physical/file_manager.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint> 

// Forward declaration to avoid including the full storage_engine.h header.
// This reduces compile times and prevents circular dependencies.
struct ColumnDef;

// --- Helper Structures for the Hash Map Key ---

// 1. The key structure for the table name.
// Represents the 12-byte (96-bit) name as two integer types.
// This is the fastest approach for comparison and hashing.
struct TableNameKey {
    uint64_t part1; // First 8 bytes (64 bits)
    uint32_t part2; // Remaining 4 bytes (32 bits)

    // The equality operator (==) is required for a key in std::unordered_map.
    // It determines if two keys are identical.
    bool operator==(const TableNameKey& other) const {
        return part1 == other.part1 && part2 == other.part2;
    }
};

// 2. A custom hasher struct for our TableNameKey.
// std::unordered_map does not know how to hash our custom key structure,
// so we must provide the logic for it.
struct KeyHasher {
    std::size_t operator()(const TableNameKey& k) const {
        // Combine the hashes of the two parts. This is a standard and effective method.
        // A simple addition or XOR could lead to more collisions.
        // Shifting the second hash makes the final hash more unique.
        const std::size_t h1 = std::hash<uint64_t>{}(k.part1);
        const std::size_t h2 = std::hash<uint32_t>{}(k.part2);
        return h1 ^ (h2 << 1);
    }
};

// The Catalog is responsible for managing database-level metadata.
// Its primary role is to manage the list of all tables.
class Catalog {
public:
    // The constructor loads the existing catalog from manager.db into memory.
    explicit Catalog(const std::string& db_path);
    ~Catalog();

    // Core table management operations
    void createTable(const std::string& table_name, const std::vector<ColumnDef>& columns);
    void dropTable(const std::string& table_name);

    // Method to retrieve information about a table (e.g., its file link).
    // Returns true if the table is found.
    bool getTableLink(const std::string& table_name, uint16_t& link_out) const;


private:
    // --- Internal (Private) Methods ---

    // Loads all records from manager.db into the table_links_ hash map.
    void load();

    void setLink(const TableNameKey& key);
    
    // Helper function to convert a std::string into our internal key format.
    // This hides the implementation details from the public API.
    TableNameKey stringToKey(const std::string& s) const;


    // --- Class Members ---

    std::string db_path_;         // Path to the database directory (e.g., "./my_db")
    std::string manager_db_path_; // Full path to the master catalog file
    std::string meta_db_path_;
    FileManager file_manager_;    // Instance of the file manager for all disk operations

    std::fstream manager_file_;
    std::fstream meta_file_;
    // The primary data structure: an in-memory cache of the manager.db file.
    // Key: The 12-byte table name. Value: Offset.
    // We use our custom KeyHasher to enable hashing of TableNameKey.
    std::unordered_map<TableNameKey, uint16_t, KeyHasher> table_links_;
};