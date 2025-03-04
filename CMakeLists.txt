cmake_minimum_required(VERSION 3.8)

project(fmdisk)
include_directories(include)
#set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

find_package(PkgConfig REQUIRED)

#set(CMAKE_C_FLAGS "-D_FILE_OFFSET_BITS=64 -Wall -ggdb -O0 -fsanitize=address -fsanitize=undefined")
#set(CMAKE_CXX_FLAGS "-D_FILE_OFFSET_BITS=64 -std=c++11 -Wall -ggdb -O0 -fsanitize=address -fsanitize=undefined")

set(CMAKE_C_FLAGS "-D_FILE_OFFSET_BITS=64 -Wall -ggdb -O0")
set(CMAKE_CXX_FLAGS "-D_FILE_OFFSET_BITS=64 -std=c++11 -Wall -ggdb -O0")

pkg_check_modules(JSON-C REQUIRED json-c)
link_directories(${JSON-C_LIBRARY_DIRS})
include_directories(${JSON-C_INCLUDE_DIRS})

pkg_check_modules(FUSE REQUIRED fuse)
link_directories(${FUSE_LIBRARY_DIRS})
include_directories(${FUSE_INCLUDE_DIRS})

pkg_check_modules(SQLITE3 REQUIRED sqlite3)
link_directories(${SQLITE3_LIBRARY_DIRS})
include_directories(${SQLITE3_INCLUDE_DIRS})

#set(CMAKE_C_FLAGS "-D_FILE_OFFSET_BITS=64 -Wall -O2 -DNDEBUG -ggdb")
#set(CMAKE_CXX_FLAGS "-D_FILE_OFFSET_BITS=64 -std=c++11 -Wall -O2 -DNDEBUG -ggdb")

add_subdirectory(src)
