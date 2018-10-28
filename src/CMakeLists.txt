add_library(fmdisk STATIC fm_main.c cache.cpp utils.cpp fuse.cpp threadpool.cpp dir.cpp file.cpp)
set_target_properties(fmdisk PROPERTIES OUTPUT_NAME "fmdisk")
target_link_libraries(fmdisk json-c fuse pthread)

add_library(fmcheck STATIC  check.cpp threadpool.cpp utils.cpp)
set_target_properties(fmcheck PROPERTIES OUTPUT_NAME "fmcheck")
target_link_libraries(fmcheck json-c pthread)

add_executable(fmcheck_test stub_api.cpp)
target_link_libraries(fmcheck_test fmcheck)

add_executable(locker_test locker_test.cpp)
target_link_libraries(locker_test pthread)
