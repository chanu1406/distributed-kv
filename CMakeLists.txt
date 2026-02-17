cmake_minimum_required(VERSION 3.16)
project(distributedKV
    VERSION 0.1.0
    LANGUAGES CXX
)

# ── C++ Standard ─────────────────────────────────────────────────────────────
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ── Compiler Warnings ────────────────────────────────────────────────────────
add_compile_options(
    -Wall -Wextra -Wpedantic -Werror
    -Wno-unused-parameter       # Allow unused params during early dev
)

# ── Platform Detection ────────────────────────────────────────────────────────
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_compile_definitions(PLATFORM_LINUX)
    message(STATUS "Platform: Linux (using epoll)")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    add_compile_definitions(PLATFORM_MACOS)
    message(STATUS "Platform: macOS (using kqueue)")
else()
    message(WARNING "Unsupported platform: ${CMAKE_SYSTEM_NAME}. Build may fail.")
endif()

# ── Output Directories ───────────────────────────────────────────────────────
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# ── Google Test via FetchContent ──────────────────────────────────────────────
include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0
)
# Prevent GoogleTest from overriding our compiler/linker options
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

# ── Source Library (all modules, linked by both the binary and tests) ─────────
add_library(dkv_core STATIC
    src/utils/murmurhash3.cpp
    src/utils/crc32.cpp
    src/config/config.cpp
)

target_include_directories(dkv_core PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(dkv_core PUBLIC
    pthread
)

# ── Main Executable ──────────────────────────────────────────────────────────
add_executable(dkv_node
    src/main.cpp
)

target_link_libraries(dkv_node PRIVATE dkv_core)

# ── Unit Tests ───────────────────────────────────────────────────────────────
add_executable(dkv_tests
    tests/unit/test_murmurhash3.cpp
    tests/unit/test_crc32.cpp
    tests/unit/test_config.cpp
)

target_link_libraries(dkv_tests PRIVATE
    dkv_core
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(dkv_tests)
