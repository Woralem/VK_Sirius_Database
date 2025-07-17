#pragma once

#include <filesystem>
#include <cstdint>
#include <vector>
#include <optional>

/**
 * @file space_manager_io.h
 * @brief Defines structures and stateless functions for managing free space in heap files.
 */

/**
 * @struct FreeSpaceRecord
 * @brief Represents a single contiguous block of free space in a heap file.
 *
 * This simple struct is used to track available chunks of space that have been
 * freed by UPDATE or DELETE operations.
 */
struct FreeSpaceRecord {
    uint64_t offset; /// The starting byte offset of the free block.
    uint16_t length; /// The length of the free block in bytes.

    /**
     * @struct Less
     * @brief A "transparent" comparator for sorting and searching FreeSpaceRecords.
     *
     * This allows comparing a `FreeSpaceRecord` with another record or with a raw `uint16_t` length,
     * enabling efficient searches in a sorted vector using `std::lower_bound`.
     */
    struct Less {
        using is_transparent = void;
        /// Compares two records by length.
        bool operator()(const FreeSpaceRecord& a, const FreeSpaceRecord& b) const {
            return a.length < b.length;
        }
        /// Compares a record's length to a raw length value.
        bool operator()(const FreeSpaceRecord& a, uint16_t b_len) const {
            return a.length < b_len;
        }
        /// Compares a raw length value to a record's length.
        bool operator()(uint16_t a_len, const FreeSpaceRecord& b) const {
            return a_len < b.length;
        }
    };
};

/**
 * @namespace SpaceManager
 * @brief A collection of stateless utility functions to manage a free space inventory.
 *
 * This service operates on an in-memory vector of `FreeSpaceRecord`s (the "cache").
 * It is the responsibility of the caller (i.e., `ColumnManager`) to own this cache
 * and decide when to load it from or persist it to disk. This stateless design avoids
 * the overhead of each `ColumnManager` having its own persistent file stream and
 * complex state for the `.sp` file.
 */
namespace SpaceManager {
    
    /// The size of a single `FreeSpaceRecord` on disk (8-byte offset + 2-byte length).
    constexpr size_t RECORD_SIZE = 10;

    /**
     * @brief Loads all free space records from a .sp file into an in-memory cache.
     * @param sp_path The path to the source `.sp` file.
     * @param inventory_cache The vector to populate. It will be cleared and then filled.
     */
    void load(const std::filesystem::path& sp_path, std::vector<FreeSpaceRecord>& inventory_cache);

    /**
     * @brief Writes an in-memory cache of free space records to a .sp file, overwriting it completely.
     * @param sp_path The path to the destination `.sp` file.
     * @param inventory_cache The vector of records to write to disk.
     */
    void persist(const std::filesystem::path& sp_path, const std::vector<FreeSpaceRecord>& inventory_cache);

    /**
     * @brief Adds a new free chunk record to an in-memory inventory cache.
     * @param inventory_cache The vector of records, which MUST be sorted by length.
     * @param offset The starting offset of the newly freed space.
     * @param length The length of the newly freed space.
     */
    void add(std::vector<FreeSpaceRecord>& inventory_cache, uint64_t offset, uint16_t length);

    /**
     * @brief Finds and claims a suitable free chunk from an in-memory inventory.
     * Implements a "best-fit" search strategy.
     * @param inventory_cache The vector of records, which MUST be sorted by length. The found record will be removed.
     * @param required_length The minimum size of the chunk needed.
     * @return An optional containing the record of the claimed chunk if one is found.
     */
    std::optional<FreeSpaceRecord> claim(std::vector<FreeSpaceRecord>& inventory_cache, uint16_t required_length);

} // namespace SpaceManager