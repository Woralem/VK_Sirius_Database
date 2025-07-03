cmake_minimum_required(VERSION 3.15)
project(DatabaseEngine CXX)

add_definitions(-D_WIN32_WINNT=0x0A00)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

FetchContent_Declare(
        nlohmann_json
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        GIT_TAG v3.11.2
)
FetchContent_MakeAvailable(nlohmann_json)

set(QUERY_ENGINE_SOURCES
        src/query_engine/lexer.cpp
        src/query_engine/parser.cpp
        src/query_engine/executor.cpp
)

add_executable(database_terminal
        src/terminal_app.cpp
        ${QUERY_ENGINE_SOURCES}
)
target_include_directories(database_terminal PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(database_terminal PRIVATE nlohmann_json::nlohmann_json)

add_executable(run_tests
        tests/test_framework.h
        tests/run_tests.cpp
        tests/test_utils.h
        tests/final_boss.cpp
        ${QUERY_ENGINE_SOURCES}
)
target_include_directories(run_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(run_tests PRIVATE nlohmann_json::nlohmann_json)

option(BUILD_WEB_SERVER "Build web server with Crow" OFF)

if(BUILD_WEB_SERVER)
    message(STATUS "Web server is enabled. Building 'database_server' executable.")

    FetchContent_Declare(
            asio
            GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
            GIT_TAG asio-1-28-0
    )
    FetchContent_MakeAvailable(asio)

    set(ASIO_INCLUDE_DIR ${asio_SOURCE_DIR}/asio/include)
    message(STATUS "ASIO include directory set to: ${ASIO_INCLUDE_DIR}")

    FetchContent_Declare(
            crow
            GIT_REPOSITORY https://github.com/CrowCpp/Crow.git
            GIT_TAG master
    )
    set(CROW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(CROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(CROW_BUILD_DOCS OFF CACHE BOOL "" FORCE)

    FetchContent_MakeAvailable(crow)

    add_executable(database_server
            src/main.cpp
            ${QUERY_ENGINE_SOURCES}
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
    endif()

endif()