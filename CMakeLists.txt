# Sets the minimum version of CMake required to build this project.
cmake_minimum_required(VERSION 3.15)

# Defines the project name and specifies its primary programming language.
project(StorageEngineProject CXX)

# Enforces the C++20 standard for the entire project.
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# --- 1. Create the storage engine library ---
# Compiles all specified source files into a single static library.
add_library(storage_engine STATIC
    src/api/storage_engine.cpp
    src/physical/file_manager.cpp
    src/physical/catalog.cpp
    src/physical/table.cpp # Make sure table.cpp is here
    # Add other .cpp files as needed
    # src/physical/metadata_io.cpp
    # ...
)

# Specifies the include directories for the "storage_engine" library.
target_include_directories(storage_engine
    PUBLIC
        # Anyone linking to this library needs access to the public API headers.
        ${CMAKE_CURRENT_SOURCE_DIR}/include 
    PRIVATE
        # For compiling this library, it also needs to see its own internal headers.
        ${CMAKE_CURRENT_SOURCE_DIR}/src     
)

# --- 2. Create an executable for testing ---
# Creates the main executable program from the specified source file.
add_executable(main src/main.cpp)

# Specifies the include directories for the 'main' executable.
# Because main.cpp is a test file that needs to access internal library details
# (like the Table and Catalog classes directly), we must explicitly give it
# access to both the public AND private headers of the library.
target_include_directories(main PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include # For storage_engine.h
    ${CMAKE_CURRENT_SOURCE_DIR}/src     # For physical/table.h, physical/catalog.h
)

# --- 3. Link the executable against the library ---
# Links `main` with `storage_engine`. This also automatically adds the PUBLIC
# include directories from storage_engine to main's search paths.
target_link_libraries(main PRIVATE storage_engine)