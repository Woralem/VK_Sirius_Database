#pragma once

#include <string>
#include <filesystem>
#include <fstream>

/**
 * @file file_manager.h
 * @brief Defines a simple utility class for basic file and directory operations.
 */

/**
 * @class FileManager
 * @brief A stateless helper class for common filesystem tasks.
 *
 * This class provides a thin, error-handling wrapper around `std::filesystem`
 * to ensure that required files and directories exist.
 */
class FileManager {
public:
    /**
     * @brief Checks if a file or directory exists at the given path.
     * @param path The path to check.
     * @return `true` if the path exists, `false` otherwise.
     */
    bool fileExists(const std::filesystem::path& path) const noexcept;

    /**
     * @brief Creates a new, empty file at the given path if it doesn't already exist.
     * @param path The path where the file should be created.
     * @throws std::runtime_error If the file cannot be created.
     */
    void createFile(const std::filesystem::path& path);

    /**
     * @brief Creates a new directory (and any necessary parent directories) at the given path.
     * @param path The path where the directory should be created.
     */
    void createDirectory(const std::filesystem::path& path);
};