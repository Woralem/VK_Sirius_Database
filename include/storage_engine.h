#pragma once

#include <string>
#include <vector>
#include <memory>
#include <utility>
#include "types.h" // Provides `Value`, `ColumnDef`, `Options`

// Forward declarations to avoid including full headers here.
class Catalog;
class Table;

//================================================================================
// API Usage Guide for the Query Layer Developer
//================================================================================
/**
 * @class StorageEngine
 * @brief The primary public API for interacting with the database storage layer.
 *
 * --- 1. The Core Principle: Separation of Concerns ---
 *
 *   - The Storage Layer (this API) is a "dumb but powerful" engine. It knows
 *     how to physically read, write, and manage data on disk. It operates
 *     on physical row indices.
 *
 *   - The Query Layer (your code) is the "brains." You are responsible for
 *     parsing SQL, planning the query, filtering, sorting, and aggregating data.
 *
 * --- 2. Getting Started ---
 *
 *   Create an instance of the StorageEngine, providing the path to the database
 *   directory. All operations are performed through this object.
 *
 *   @code
 *   #include "storage_engine.h"
 *   StorageEngine engine("./my_database_dir");
 *   @endcode
 *
 * --- 3. DDL (Data Definition Language) Operations ---
 *
 *   These operations are straightforward. You simply call the corresponding
 *   methods to manage the database schema.
 *
 *   @code
 *   engine.createTable(...);
 *   engine.dropTable(...);
 *   engine.alterRTable(...); // Rename Table
 *   engine.alterRColumn(...); // Rename Column
 *   @endcode
 *
 * --- 4. DML (Data Manipulation Language) Operations ---
 *
 *   This section explains how to insert, update, delete, and select data.
 *
 *   --- 4.1. INSERT ---
 *
 *   This is a direct operation. Build the required data structures from your
 *   parsed SQL and call the `insert` method.
 *
 *   @code
 *   // INSERT INTO users (id, name) VALUES (1, 'Alice');
 *   std::vector<std::string> cols = {"id", "name"};
 *   Value val1, val2;
 *   val1.data = (int64_t)1;
 *   val2.data = std::string("Alice");
 *   std::vector<std::vector<Value>> rows = {{val1, val2}};
 *   engine.insert("users", cols, rows);
 *   @endcode
 *
 *   --- 4.2. UPDATE and DELETE (The "Find-Then-Act" Pattern) ---
 *
 *   This engine provides high-level `update` and `deleteRows` methods that
 *   encapsulate a simple "find-then-act" logic.
 *
 *   1. Your Query Layer parses the SQL to extract the `set_clause` and `where_clause`.
 *   2. You build these clauses into the `std::vector<std::pair<std::string, Value>>` format.
 *   3. You call `engine.update(...)` or `engine.deleteRows(...)`.
 *
 *   Inside, the engine will perform a **simple full table scan** to find matching
 *   row indices and then perform the action.
 *
 *   @code
 *   // DELETE FROM users WHERE id = 1;
 *   Value delete_val;
 *   delete_val.data = (int64_t)1;
 *   std::vector<std::pair<std::string, Value>> where = {{"id", delete_val}};
 *   engine.deleteRows("users", where);
 *   @endcode
 *
 *   @warning This internal scan is **inefficient by design**. It is a placeholder
 *   for a future query optimization where your Query Layer will use an Index to
 *   find row indices much faster. For high performance, see section 4.3.
 *
 *   --- 4.3. SELECT (and High-Performance DML) ---
 *
 *   The `SELECT` operation is **entirely the responsibility of the Query Layer**.
 *   The standard workflow for your Query Layer is:
 *
 *   1. **Get the Table Object**: This is your primary tool for direct data access.
 *      @code
 *      std::unique_ptr<Table> table = engine.getTable("users");
 *      if (!table) { // handle error }
 *      @endcode
 *
 *   2. **Get Row Count**: Find out how many rows to iterate over.
 *      @code
 *      uint64_t row_count = table->getRowCount();
 *      @endcode
 *
 *   3. **Iterate and Read Data**: Loop from `0` to `row_count - 1` and read the
 *      data for each column you need using the table's low-level primitives.
 *      @code
 *      for (uint64_t i = 0; i < row_count; ++i) {
 *          Value id = table->readValue(i, "id");
 *          Value name = table->readValue(i, "name");
 *          // ... store the retrieved values ...
 *      }
 *      @endcode
 *
 *   4. **Process Data**: With the raw data in memory, your Query Layer can now
 *      perform its filtering, sorting, aggregation, and projection.
 *
 *   This same pattern (`getTable` -> find indices -> act on indices with `table->...`)
 *   should be used for high-performance `UPDATE` and `DELETE` operations when
 *   you have an index and want to bypass the Storage Engine's slow full scan.
 */
class StorageEngine {
public:
    explicit StorageEngine(const std::string& data_path);
    ~StorageEngine();

    // --- DDL API ---
    void createTable(const std::string& name, const std::vector<ColumnDef>& columns, const Options& options = {});
    void dropTable(const std::string& table_name);
    void alterRTable(const std::string& old_table_name, const std::string& new_table_name);
    void alterRColumn(const std::string& table_name, const std::string& old_column_name, const std::string& new_column_name);
    void alterTColumn(const std::string& table_name, const std::string& column_name, const std::string& new_column_type); 
    void dropColumn(const std::string& table_name, const std::string& column_name);

    // --- DML API ---
    void insert(const std::string& table_name, const std::vector<std::string>& columns, const std::vector<std::vector<Value>>& values);
    void update(const std::string& table_name, const std::vector<std::pair<std::string, Value>>& set_clause, const std::vector<std::pair<std::string, Value>>& where_clause);
    void deleteRows(const std::string& table_name, const std::vector<std::pair<std::string, Value>>& where_clause);            
    
    /**
     * @brief Gets a pointer to the low-level Table object.
     * @details This is the main entry point for the Query Layer to perform efficient
     * data reads (for SELECT) and indexed DML operations.
     * @param table_name The name of the table to access.
     * @return A unique_ptr to the Table object, or nullptr if not found.
     */
    std::unique_ptr<Table> getTable(const std::string& table_name);

private:
    std::string data_path_;
    std::unique_ptr<Catalog> db_catalog_;
};