cmake_minimum_required(VERSION 3.10)

project(fmdisk)
include_directories(include)
#set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

find_package(PkgConfig REQUIRED)

#set(CMAKE_C_FLAGS "-D_FILE_OFFSET_BITS=64 -Wall -ggdb -O0 -fsanitize=address -fsanitize=undefined")
#set(CMAKE_CXX_FLAGS "-D_FILE_OFFSET_BITS=64 -std=c++11 -Wall -ggdb -O0 -fsanitize=address -fsanitize=undefined")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_FILE_OFFSET_BITS=64 -fsigned-char")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64 -fsigned-char")

include(CheckCXXCompilerFlag)

# Function to check if AVX512 is supported
function(check_avx512_support result)
    set(CMAKE_REQUIRED_FLAGS "-mavx512f -mavx512bw")
    check_cxx_compiler_flag("" ${result})
    set(CMAKE_REQUIRED_FLAGS "")
endfunction()

# Function to check if AVX2 is supported
function(check_avx2_support result)
    set(CMAKE_REQUIRED_FLAGS "-mavx2")
    check_cxx_compiler_flag("" ${result})
    set(CMAKE_REQUIRED_FLAGS "")
endfunction()

# Check if AVX512 is supported
check_avx512_support(AVX512_SUPPORTED)

# Check if AVX2 is supported
check_avx2_support(AVX2_SUPPORTED)


if(AVX512_SUPPORTED)
    message(STATUS "AVX512 is supported, enabling it")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mavx512f -mavx512bw")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx512f -mavx512bw")
else()
    message(STATUS "AVX512 is not supported")
endif()

if(AVX2_SUPPORTED)
    message(STATUS "AVX2 is supported, enabling it")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mavx2")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2")
else()
    message(STATUS "AVX2 is not supported")
endif()



pkg_check_modules(JSON-C REQUIRED IMPORTED_TARGET json-c)
pkg_check_modules(FUSE REQUIRED "fuse3>=3.11" IMPORTED_TARGET fuse3)
pkg_check_modules(SQLITE3 REQUIRED IMPORTED_TARGET sqlite3)

#set(CMAKE_C_FLAGS "-D_FILE_OFFSET_BITS=64 -Wall -O2 -DNDEBUG -ggdb")
#set(CMAKE_CXX_FLAGS "-D_FILE_OFFSET_BITS=64 -std=c++11 -Wall -O2 -DNDEBUG -ggdb")

add_subdirectory(src)
