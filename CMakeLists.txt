cmake_minimum_required(VERSION 3.15)
project(DatabaseEngine CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

FetchContent_Declare(
        asio
        GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
        GIT_TAG asio-1-28-0
)

FetchContent_Declare(
        nlohmann_json
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        GIT_TAG v3.11.2
)

FetchContent_MakeAvailable(asio nlohmann_json)

set(ASIO_INCLUDE_DIR ${asio_SOURCE_DIR}/asio/include CACHE INTERNAL "")
set(QUERY_ENGINE_SOURCES
        src/query_engine/lexer.cpp
        src/query_engine/parser.cpp
        src/query_engine/executor.cpp
)
set (API_SOURCES
        src/api/json_handler.cpp
        src/api/http_server.cpp
        src/api/database_manager.cpp
)

add_executable(database_terminal
        src/terminal_app.cpp
        ${QUERY_ENGINE_SOURCES}
)

target_include_directories(database_terminal
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(database_terminal
        PRIVATE
        nlohmann_json::nlohmann_json
)

if(WIN32)
    target_compile_definitions(database_terminal PRIVATE _WIN32_WINNT=0x0601)
endif()

FetchContent_Declare(
        crow
        GIT_REPOSITORY https://github.com/CrowCpp/Crow.git
        GIT_TAG master
)

set(CROW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(CROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(CROW_BUILD_DOCS OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(crow)

file(GLOB_RECURSE SOURCES "src/*.cpp")

add_executable(database_server
        src/main.cpp
        ${QUERY_ENGINE_SOURCES}
        ${API_SOURCES}
        include/storage/optimized_in_memory_storage.h

)

target_include_directories(database_server
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${ASIO_INCLUDE_DIR}
)

target_link_libraries(database_server
        PRIVATE
        Crow::Crow
        nlohmann_json::nlohmann_json
)

if(WIN32)
    target_link_libraries(database_server PRIVATE ws2_32 wsock32)
    target_compile_definitions(database_server PRIVATE _WIN32_WINNT=0x0601)
endif()

if(CMAKE_EXPORT_COMPILE_COMMANDS)
    set(CMAKE_CXX_STANDARD_INCLUDE_DIRECTORIES
            ${CMAKE_CXX_IMPLICIT_INCLUDE_DIRECTORIES})
endif()

target_include_directories(database_server
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${crow_SOURCE_DIR}/include
        ${nlohmann_json_SOURCE_DIR}/include
        ${ASIO_INCLUDE_DIR}
)