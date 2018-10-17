
add_library(fmdisk SHARED STATIC cache.cpp net.c utils.cpp fuse.cpp threadpool.cpp main.c dir.cpp file.cpp)
set_target_properties(fmdisk PROPERTIES OUTPUT_NAME "fmdisk")
target_link_libraries(fmdisk json-c fuse ssl crypto pthread curl)

add_executable(fmcheck  check.cpp stub_api.cpp)
target_link_libraries(fmcheck fmdisk)

add_executable(locker_test locker_test.cpp)
target_link_libraries(locker_test pthread)
