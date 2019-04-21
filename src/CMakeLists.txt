add_library(fmdisk STATIC fm_main.c cache.cpp utils.cpp fuse.cpp threadpool.cpp dir.cpp file.cpp sqlite.cpp)
set_target_properties(fmdisk PROPERTIES OUTPUT_NAME "fmdisk")
target_link_libraries(fmdisk ${JSON-C_LIBRARIES} ${FUSE_LIBRARIES} ${SQLITE3_LIBRARIES} pthread)

add_library(fmcheck STATIC  check.cpp threadpool.cpp utils.cpp sqlite.cpp)
set_target_properties(fmcheck PROPERTIES OUTPUT_NAME "fmcheck")
target_link_libraries(fmcheck ${JSON-C_LIBRARIES} ${SQLITE3_LIBRARIES} pthread)

add_executable(fmcheck_test stub_api.cpp)
target_link_libraries(fmcheck_test fmcheck)

add_executable(locker_test locker_test.cpp)
target_link_libraries(locker_test pthread)
