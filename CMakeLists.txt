cmake_minimum_required(VERSION 2.8)

project(fmdisk)
include_directories(include)
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

#set(CMAKE_C_FLAGS "-D_FILE_OFFSET_BITS=64 -Wall -ggdb -O0 -fsanitize=address -fsanitize=undefined")
#set(CMAKE_CXX_FLAGS "-D_FILE_OFFSET_BITS=64 -std=c++11 -Wall -ggdb -O0 -fsanitize=address -fsanitize=undefined")

set(CMAKE_C_FLAGS "-D_FILE_OFFSET_BITS=64 -Wall -ggdb -O0")
set(CMAKE_CXX_FLAGS "-D_FILE_OFFSET_BITS=64 -std=c++11 -Wall -ggdb -O0")

find_package(JSON-C REQUIRED)
include_directories(${JSON-C_INCLUDE_DIRS})

find_package(fuse REQUIRED)
include_directories(${FUSE_INCLUDE_DIRS})

find_package(sqlite3 REQUIRED)
include_directories(${SQLITE3_INCLUDE_DIRS})

#set(CMAKE_C_FLAGS "-D_FILE_OFFSET_BITS=64 -Wall -O2 -DNDEBUG -ggdb")
#set(CMAKE_CXX_FLAGS "-D_FILE_OFFSET_BITS=64 -std=c++11 -Wall -O2 -DNDEBUG -ggdb")

add_subdirectory(src)
