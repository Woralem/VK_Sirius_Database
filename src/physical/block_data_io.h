#pragma once

#include <filesystem>
#include <cstdint>
#include <array>
#include <vector>
#include <fstream>
#include <memory> // For std::unique_ptr

/**
 * @file block_data_io.h
 * @brief Defines the BlockDataIO class for managing fixed-size record files.
 */

/**
 * @class BlockDataIO
 * @brief Manages I/O for a file consisting of fixed-size records (a ".dt" file).
 *
 * This class provides an abstraction over a file, treating it as an array of
 * fixed-size records (`RECORD_SIZE`). It includes a simple block-based read buffer
 * to reduce the number of physical disk reads for sequential access patterns.
 * All write operations bypass the buffer and write directly to disk to ensure durability,
 * invalidating the buffer in the process.
 */
class BlockDataIO {
public:
    /// The fixed size of each record in the file, in bytes.
    static constexpr size_t RECORD_SIZE = 8;
    /// The number of records to buffer in memory for read operations.
    static constexpr size_t BUFFERED_RECORDS = 512;
    /// The total size of the in-memory read buffer (512 records * 8 bytes/record = 4KB).
    static constexpr size_t READ_BUFFER_SIZE = RECORD_SIZE * BUFFERED_RECORDS;

    /**
     * @brief Constructs a BlockDataIO manager for a given file path.
     * @param dt_path The full path to the data file (e.g., ".../0A/01.dt").
     */
    explicit BlockDataIO(std::filesystem::path dt_path);
    ~BlockDataIO();

    // Prevent copying and moving to ensure a single owner for the file stream resource.
    BlockDataIO(const BlockDataIO&) = delete;
    BlockDataIO& operator=(const BlockDataIO&) = delete;
    BlockDataIO(BlockDataIO&&) = delete;
    BlockDataIO& operator=(BlockDataIO&&) = delete;

    /** @brief Appends a new record to the end of the file. */
    void append(const std::array<char, RECORD_SIZE>& data);

    /** @brief Reads a single record from a specific row index. */
    std::array<char, RECORD_SIZE> readAt(uint64_t row_index);

    /** @brief Overwrites the record at a specific row index. */
    void writeAt(uint64_t row_index, const std::array<char, RECORD_SIZE>& data);

    /** @brief Reads the very last record from the file. */
    std::array<char, RECORD_SIZE> readLast();

    /** @brief Removes the last record from the file by truncating it. */
    void truncate();

    /** @brief Gets the total number of records in the file based on its size. */
    uint64_t getRowCount() const;

    /** @brief Gets the path of the file being managed. */
    const std::filesystem::path& getPath() const { return dt_path_; }

private:
    /// Ensures the file stream is open and ready for I/O, creating it if necessary.
    void ensureStreamOpen();
    /// Loads the correct block of data into the read buffer for a given row index.
    bool loadBufferForIndex(uint64_t row_index);

    std::filesystem::path dt_path_;
    std::unique_ptr<std::fstream> file_stream_;

    // --- Read Buffer Members ---
    // NOTE on Scalability: This per-instance buffer is simple and efficient for a small
    // number of concurrently accessed columns. However, it does not scale to millions of
    // columns, as each instance would allocate its own 4KB buffer.
    // PLANNED REFACTOR: In a future step, this will be replaced by a system-wide,
    // global BufferPoolManager that manages a fixed-size pool of shared buffers.
    // This will provide better memory usage and a global cache.
    std::vector<char> read_buffer_;
    /// The starting row index of the block currently loaded in `read_buffer_`. -1 if invalid.
    int64_t buffered_block_start_index_;
};