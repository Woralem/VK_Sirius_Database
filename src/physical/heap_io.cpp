#include "physical/heap_io.h"
#include <fstream>
#include <stdexcept>

HeapIO::HeapIO(std::filesystem::path bg_path)
    : bg_path_(std::move(bg_path)) {}

HeapIO::~HeapIO() {}

// Ensures the file stream is open and ready for an I/O operation.
void HeapIO::ensureStreamOpen() {
    // If the stream exists and is open, simply clear its state flags (like eofbit).
    // This is crucial because a previous read might have set the eofbit, which
    // would cause subsequent operations to fail until the state is cleared.
    if (file_stream_ && file_stream_->is_open()) {
        file_stream_->clear();
        return;
    }
    
    // If the stream isn't open, attempt to open the file.
    file_stream_ = std::make_unique<std::fstream>(bg_path_, std::ios::in | std::ios::out | std::ios::binary);

    // If opening failed (likely because the file doesn't exist yet), create it.
    if (!file_stream_->is_open()) {
        std::ofstream creator(bg_path_, std::ios::binary);
        creator.close();
        if (!creator) {
            throw std::runtime_error("FATAL: Could not create heap file at: " + bg_path_.string());
        }
        
        // After creating, try to open the stream again for I/O.
        file_stream_ = std::make_unique<std::fstream>(bg_path_, std::ios::in | std::ios::out | std::ios::binary);
        if (!file_stream_->is_open()) {
            throw std::runtime_error("FATAL: Could not open newly created heap file: " + bg_path_.string());
        }
    }
}

// Appends data to the end of the file and returns the starting offset.
uint64_t HeapIO::append(const std::vector<char>& data) {
    ensureStreamOpen();
    
    // Seek to the end of the file to get the current size, which will be the new offset.
    file_stream_->seekp(0, std::ios::end);
    const uint64_t offset = file_stream_->tellp();

    if (!data.empty()) {
        if (!file_stream_->write(data.data(), data.size())) {
            throw std::runtime_error("Failed to write on append for heap file: " + bg_path_.string());
        }
        // Ensure data is written to disk immediately.
        file_stream_->flush();
    }
    return offset;
}

// Reads a specified number of bytes from a given offset.
std::vector<char> HeapIO::read(uint64_t offset, size_t length) {
    if (length == 0) return {};
    ensureStreamOpen();
    
    // Position the read cursor at the desired offset.
    file_stream_->seekg(offset);
    if (file_stream_->fail()) {
        // A seek can fail if the offset is invalid or past the end of the file.
        throw std::runtime_error("Seekg failed for heap file: " + bg_path_.string());
    }
    
    std::vector<char> buffer(length);
    file_stream_->read(buffer.data(), length);
    
    // Check for read errors. A read can fail if we try to read past the end of the
    // file or if a physical disk error occurs. The eofbit is expected if we read
    // exactly to the end, so we don't treat it as an error.
    if (!*file_stream_ && !file_stream_->eof()) {
         throw std::runtime_error("Heap read error: requested " + std::to_string(length) + 
            " bytes from offset " + std::to_string(offset) + ", but read failed. File may be corrupt.");
    }
    
    return buffer;
}

// Overwrites data at a specific offset in the file.
void HeapIO::writeAt(uint64_t offset, const std::vector<char>& data) {
    if (data.empty()) return;
    ensureStreamOpen();

    // Position the write cursor at the desired offset.
    file_stream_->seekp(offset);
    if (!file_stream_->write(data.data(), data.size())) {
        throw std::runtime_error("Failed to write at offset " + std::to_string(offset) + " for heap file: " + bg_path_.string());
    }
    // Ensure data is written to disk immediately.
    file_stream_->flush();
}