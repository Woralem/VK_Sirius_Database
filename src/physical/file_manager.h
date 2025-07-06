#pragma once

#include <string>
#include <filesystem>
#include <fstream>

class FileManager {
public:
    // Checks if a file or directory exists at the given path.
    // 'const' means the method doesn't change the FileManager object.
    // 'noexcept' means this function promises not to throw exceptions.
    bool fileExists(const std::filesystem::path& path) const noexcept;

    // Creates a new, empty file at the given path.
    // If the file already exists, it will be truncated (emptied).
    void createFile(const std::filesystem::path& path);

    // Creates a new directory at the given path.
    void createDirectory(const std::filesystem::path& path);
};