#pragma once

#include "physical/block_data_io.h"
#include "physical/heap_io.h"
#include "physical/space_manager_io.h"
#include "types.h" // For ColumnDef::DataType and Value
#include <filesystem>
#include <memory>
#include <vector>
#include <optional>

/**
 * @file column_manager.h
 * @brief Defines the ColumnManager class, which orchestrates storage for a single column.
 */

/**
 * @class ColumnManager
 * @brief Coordinates all storage operations for a single column of data.
 *
 * This class acts as a high-level facade over the low-level I/O workers
 * (`BlockDataIO`, `HeapIO`, `SpaceManager`). It understands the column's data type
 * and handles the complex logic for:
 *  - Storing fixed-size vs. variable-size data.
 *  - Serializing and deserializing `Value` objects to/from their raw byte representations.
 *  - Managing space reclamation for variable-sized data via its own in-memory freelist cache.
 */
class ColumnManager {
public:
    /**
     * @brief Constructs a manager for a single column.
     * @param table_data_dir The path to the directory containing the column's files (e.g., "db/0A/01/").
     * @param column_link A unique 16-bit identifier for this column, used to name its files.
     * @param type The data type of the column, which determines the storage strategy.
     */
    ColumnManager(const std::filesystem::path& table_data_dir, uint16_t column_link, ColumnDef::DataType type);
    ~ColumnManager();

    // Prevent copying and moving to ensure clear ownership of file resources and caches.
    ColumnManager(const ColumnManager&) = delete;
    ColumnManager& operator=(const ColumnManager&) = delete;
    ColumnManager(ColumnManager&&) = delete;
    ColumnManager& operator=(ColumnManager&&) = delete;

    /** @brief Appends a new value to the end of the column. */
    void appendValue(const Value& value);

    /**
     * @brief Reads a value from a specific row index in the column.
     * @param row_index The zero-based index of the row to read.
     * @return The deserialized `Value` object.
     */
    Value readValue(uint64_t row_index);

    /**
     * @brief Updates the value at a specific row index. For variable-length types,
     * this will free the old data in the heap.
     * @param row_index The index of the row to update.
     * @param new_value The new value to write.
     */
    void updateValue(uint64_t row_index, const Value& new_value);

    /**
     * @brief Deletes a value using the swap-and-pop method. It swaps the target row
     * with the last row and then truncates the column files.
     * @param row_index The index of the row to delete.
     */
    void swapAndPop(uint64_t row_index);
    
    /** @brief Returns the total number of rows (values) in this column. */
    uint64_t getRowCount() const;

    /**
     * @brief Static utility to delete all files associated with a column.
     * @param table_data_dir The path to the table's data directory.
     * @param column_link The link of the column whose files should be deleted.
     */
    static void dropFiles(const std::filesystem::path& table_data_dir, uint16_t column_link);

    /** @brief Gets the path to the primary block data (.dt) file for this column. */
    const std::filesystem::path& getBlockDataPath() const { return block_io_->getPath(); }

    /**
     * @brief Serializes a `Value` object into an 8-byte array suitable for writing to the block file.
     * @param value The `Value` to serialize.
     * @return An 8-byte array. For fixed-size types, this is the value itself. For
     *         variable-size types, this is the offset into the heap file.
     */
    std::array<char, 8> serializeForBlock(const Value& value);

private:
    /// Lazy-loads the freelist from the .sp file into the in-memory cache if it's not already loaded.
    void ensureFreelistIsLoaded();
    /// If the freelist cache is "dirty" (has been modified), persists it back to the .sp file.
    void persistFreelistIfDirty();
    
    /// Helper to check if the column's data type is variable-sized.
    bool isVariableLength() const;

    /// Deserializes an 8-byte array from the block file back into a `Value` object.
    Value deserializeFromBlock(const std::array<char, 8>& block_data);
    
    // --- Member Variables ---
    ColumnDef::DataType data_type_;
    /// Path to the space manager file (.sp), held for use with the stateless SpaceManager.
    std::filesystem::path sp_path_; 

    // Low-level I/O workers
    std::unique_ptr<BlockDataIO> block_io_; // Manages the .dt file (always present)
    std::unique_ptr<HeapIO> heap_io_;       // Manages the .bg file (null if column is fixed-size)

    // In-memory cache for this column's heap freelist.
    // This improves performance by avoiding disk I/O for every space management operation.
    // std::optional is used to represent the "not yet loaded" state.
    std::optional<std::vector<FreeSpaceRecord>> freelist_cache_;
    /// A flag to track if the freelist cache has been modified and needs to be persisted to disk.
    bool freelist_is_dirty_;
};