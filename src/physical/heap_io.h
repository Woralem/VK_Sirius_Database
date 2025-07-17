#pragma once

#include <filesystem>
#include <cstdint>
#include <vector>
#include <memory>
#include <fstream>

/**
 * @file heap_io.h
 * @brief Defines the HeapIO class for managing variable-length data files.
 */

/**
 * @class HeapIO
 * @brief Manages read/write operations for a variable-length data heap file (e.g., a `.bg` file).
 *
 * This class treats a file as a simple append-only heap. Data is written to the end,
 * and its starting offset is returned. Data can be read from any valid offset. This
 * class is the backing storage for variable-length types like VARCHAR and TEXT, where
 * the main data file (.dt) stores the offset into this heap file.
 */
class HeapIO {
public:
    /**
     * @brief Constructs a HeapIO manager for a given heap file path.
     * @param bg_path The full path to the heap file (e.g., ".../0A/01.bg").
     */
    explicit HeapIO(std::filesystem::path bg_path);
    ~HeapIO();

    // Prevent copying and moving to ensure a single owner for the file stream resource.
    HeapIO(const HeapIO&) = delete;
    HeapIO& operator=(const HeapIO&) = delete;
    HeapIO(HeapIO&&) = delete;
    HeapIO& operator=(HeapIO&&) = delete;

    /**
     * @brief Appends a block of data to the end of the heap file.
     * @param data The vector of bytes to write.
     * @return The 64-bit starting offset where the data was written in the file.
     */
    uint64_t append(const std::vector<char>& data);

    /**
     * @brief Reads a block of data from a specific offset in the heap.
     * @param offset The starting byte offset to read from.
     * @param length The number of bytes to read.
     * @return A vector of bytes containing the read data.
     */
    std::vector<char> read(uint64_t offset, size_t length);

     /**
     * @brief Writes a block of data to a specific offset, overwriting existing data.
     * This is typically used for reclaiming space from the freelist.
     * @param offset The starting byte offset to write to.
     * @param data The vector of bytes to write.
     */
    void writeAt(uint64_t offset, const std::vector<char>& data);

private:
    /// Ensures the file stream is open and ready for I/O, creating it if necessary.
    void ensureStreamOpen();

    std::filesystem::path bg_path_;
    std::unique_ptr<std::fstream> file_stream_;
};