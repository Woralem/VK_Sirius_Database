#include "physical/block_data_io.h"
#include <fstream>
#include <stdexcept>
#include <algorithm>

BlockDataIO::BlockDataIO(std::filesystem::path dt_path)
    : dt_path_(std::move(dt_path)),
      read_buffer_(READ_BUFFER_SIZE),
      buffered_block_start_index_(-1) {} // Initialize buffer as invalid.

BlockDataIO::~BlockDataIO() {}

// Ensures the file stream is open and ready for an I/O operation.
void BlockDataIO::ensureStreamOpen() {
    // If the stream exists and is open, we MUST clear its state bits (like eofbit, failbit).
    // A previous read that hit the end-of-file would set eofbit, causing all
    // subsequent I/O operations (even writes) to fail until the state is cleared.
    // This is a common and subtle bug with fstream.
    if (file_stream_ && file_stream_->is_open()) {
        file_stream_->clear();
        return;
    }

    // If the stream is not open, open it in binary mode for both input and output.
    file_stream_ = std::make_unique<std::fstream>(dt_path_, std::ios::in | std::ios::out | std::ios::binary);
    
    // If opening failed (likely because the file doesn't exist yet), we must create it.
    if (!file_stream_->is_open()) {
        std::ofstream creator(dt_path_, std::ios::binary);
        creator.close();
        if (!creator) {
            throw std::runtime_error("FATAL: Could not create block data file at: " + dt_path_.string());
        }

        // After creating the file, try opening the stream again.
        file_stream_ = std::make_unique<std::fstream>(dt_path_, std::ios::in | std::ios::out | std::ios::binary);
        if (!file_stream_->is_open()) {
            throw std::runtime_error("FATAL: Could not open newly created block data file: " + dt_path_.string());
        }
    }
}

// Appends a single record to the end of the file.
void BlockDataIO::append(const std::array<char, RECORD_SIZE>& data) {
    ensureStreamOpen();
    file_stream_->seekp(0, std::ios::end); // Move write pointer to the end.
    if (!file_stream_->write(data.data(), data.size())) {
        throw std::runtime_error("Failed to write on append for file: " + dt_path_.string());
    }
    file_stream_->flush(); // Ensure data is persisted to disk.
    buffered_block_start_index_ = -1; // Invalidate the read buffer as its content is now stale.
}

// Reads a single record from a given row index, using the buffer if possible.
std::array<char, BlockDataIO::RECORD_SIZE> BlockDataIO::readAt(uint64_t row_index) {
    // Check if the requested row is outside the currently buffered block.
    if (buffered_block_start_index_ < 0 ||
        row_index < static_cast<uint64_t>(buffered_block_start_index_) ||
        row_index >= static_cast<uint64_t>(buffered_block_start_index_) + BUFFERED_RECORDS) {
        
        // If it is, we need to load the correct block from disk.
        if (!loadBufferForIndex(row_index)) {
            throw std::out_of_range("Read failed: row index " + std::to_string(row_index) + " is out of bounds.");
        }
    }

    // Calculate the offset of the requested record within the in-memory buffer.
    const size_t offset_in_buffer = (row_index - buffered_block_start_index_) * RECORD_SIZE;
    std::array<char, RECORD_SIZE> result;
    // Copy the record from the buffer into the result array.
    std::copy_n(read_buffer_.data() + offset_in_buffer, RECORD_SIZE, result.begin());
    return result;
}

// Loads a block of records from disk into the read buffer.
bool BlockDataIO::loadBufferForIndex(uint64_t row_index) {
    ensureStreamOpen();
    
    // Get the total file size to determine the number of rows.
    uintmax_t file_size_bytes;
    try {
        file_size_bytes = std::filesystem::file_size(dt_path_);
    } catch(const std::filesystem::filesystem_error&) {
        file_size_bytes = 0; // Treat a non-existent file as size 0.
    }
    
    const uint64_t total_rows = file_size_bytes / RECORD_SIZE;

    // Check if the requested index is valid.
    if (row_index >= total_rows) {
        return false;
    }

    // Calculate the starting row index of the block that contains the target row_index.
    const uint64_t block_start_index = (row_index / BUFFERED_RECORDS) * BUFFERED_RECORDS;

    // Seek to the calculated position in the file.
    file_stream_->seekg(block_start_index * RECORD_SIZE);
    if (file_stream_->fail()) {
        throw std::runtime_error("Seekg failed for file: " + dt_path_.string());
    }
    
    // Read the entire block into our buffer.
    file_stream_->read(read_buffer_.data(), READ_BUFFER_SIZE);
    
    // A read of 0 bytes when not at EOF indicates a physical disk error.
    if (file_stream_->gcount() == 0 && !file_stream_->eof()) {
        throw std::runtime_error("Disk read error (read 0 bytes) for file: " + dt_path_.string());
    }

    // Update the buffer's state to reflect the new block it holds.
    buffered_block_start_index_ = block_start_index;
    return true;
}

// Writes a single record at a specific row index, overwriting existing data.
void BlockDataIO::writeAt(uint64_t row_index, const std::array<char, RECORD_SIZE>& data) {
    ensureStreamOpen();
    const std::streampos pos = static_cast<std::streampos>(row_index) * RECORD_SIZE;
    file_stream_->seekp(pos); // Move write pointer to the correct position.
    if (!file_stream_->write(data.data(), data.size())) {
        throw std::runtime_error("Failed to write at index " + std::to_string(row_index) + " for file: " + dt_path_.string());
    }
    file_stream_->flush(); // Ensure data is persisted to disk.
    buffered_block_start_index_ = -1; // Invalidate the read buffer.
}

// Reads the last record in the file.
std::array<char, BlockDataIO::RECORD_SIZE> BlockDataIO::readLast() {
    ensureStreamOpen();
    
    // Get the file size to ensure it's large enough to contain at least one record.
    file_stream_->seekg(0, std::ios::end);
    std::streampos file_size = file_stream_->tellg();
    
    if (file_size < static_cast<std::streampos>(RECORD_SIZE)) {
        throw std::runtime_error("Cannot read last record: file is too small. Path: " + dt_path_.string());
    }

    // Seek relative to the end of the file.
    file_stream_->seekg(-static_cast<std::streamoff>(RECORD_SIZE), std::ios::end);
    if (file_stream_->fail()) {
        throw std::runtime_error("Cannot read last record (seek failed). Path: " + dt_path_.string());
    }
    
    std::array<char, RECORD_SIZE> result;
    file_stream_->read(result.data(), result.size());
    // Check for a physical read error.
    if (!*file_stream_ && !file_stream_->eof()) {
        throw std::runtime_error("Failed to read last record from file: " + dt_path_.string());
    }
    return result;
}

// Removes the last record by resizing the file.
void BlockDataIO::truncate() {
    // We must close the stream before resizing the file with std::filesystem.
    if (file_stream_ && file_stream_->is_open()) {
        file_stream_->close();
    }

    std::error_code ec;
    const uintmax_t current_size = std::filesystem::file_size(dt_path_, ec);
    if (ec || current_size < RECORD_SIZE) {
        return; // Nothing to truncate.
    }

    // Resize the file to be one record smaller.
    std::filesystem::resize_file(dt_path_, current_size - RECORD_SIZE, ec);
    if (ec) {
        throw std::runtime_error("Failed to truncate file: " + dt_path_.string());
    }
    // The stream is now closed, and the buffer is implicitly invalid.
    buffered_block_start_index_ = -1;
}

// Calculates the number of rows based on the file size.
uint64_t BlockDataIO::getRowCount() const {
    std::error_code ec;
    const uintmax_t file_size = std::filesystem::file_size(dt_path_, ec);
    if (ec) return 0; // If the file doesn't exist or can't be accessed, it has 0 rows.
    return file_size / RECORD_SIZE;
}