# Sets the minimum version of CMake required to build this project.
cmake_minimum_required(VERSION 3.15)

# Defines the project name and specifies its primary programming language.
project(StorageEngineProject CXX)

# Enforces the C++20 standard for the entire project.
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# --- 1. Create the storage engine library ---

# Compiles all specified source files into a single static library (.a/.lib)
# named "storage_engine". A static library is a collection of object files
# that is linked into the final executable.
add_library(storage_engine STATIC
    # API Layer
    src/api/storage_engine.cpp

    # Logical Layer
    src/logical/table.cpp
    src/logical/column.cpp

    # Physical Layer
    src/physical/file_manager.cpp
    src/physical/catalog.cpp
    src/physical/metadata_io.cpp
    src/physical/block_data_io.cpp
    src/physical/heap_io.cpp
    src/physical/space_manager.cpp
)

# Specifies the include directories required to compile the "storage_engine" library.
# PUBLIC: Directories needed by targets that link against this library (e.g., main.exe).
#         This is for the public API headers.
# PRIVATE: Directories needed only for compiling this library itself.
#          This is for internal implementation headers.
target_include_directories(storage_engine
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include # Public headers for the API
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src     # Internal headers for implementation
)

# --- 2. Create an executable for testing ---

# Creates the main executable program from the specified source file.
add_executable(main src/main.cpp)

# Explicitly tell the 'main' target where to find the project's public headers.
# This ensures that `#include "storage_engine.h"` and other public headers are found.
# Note: This is automatically handled by `target_link_libraries` if the library's
# public include directories are set correctly, but being explicit can improve clarity.
target_include_directories(main PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# --- 3. Link the executable against the library ---

# Specifies that the 'main' executable depends on and must be linked with the
# 'storage_engine' library. The 'PRIVATE' keyword indicates that 'main' uses
# 'storage_engine', but this dependency is not propagated to other targets that
# might link against 'main'.
target_link_libraries(main PRIVATE storage_engine)