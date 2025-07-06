#include "physical/file_manager.h"
#include <iostream>
#include <stdexcept>

// Checks if a file or directory exists at the specified path.
// Marked noexcept as filesystem errors are not expected in normal operation
// and returning false is a safe default.
bool FileManager::fileExists(const std::filesystem::path& path) const noexcept {
    return std::filesystem::exists(path);
}

// Creates an empty file at the specified path if one does not already exist.
void FileManager::createFile(const std::filesystem::path& path) {
    if (!std::filesystem::exists(path)) {
        // Using std::ofstream is a simple and effective way to create an empty file.
        std::ofstream file(path, std::ios::binary);
        if (!file.is_open()) {
            // Failure to create a file is considered a critical, unrecoverable error.
            throw std::runtime_error("Could not create required file: " + path.string());
        }
    }
}

// Creates a directory at the specified path if one does not already exist.
void FileManager::createDirectory(const std::filesystem::path& path) {
    if (!fileExists(path)) {
        std::filesystem::create_directory(path);
    }
}