add_library(fmdisk STATIC fm_main.c entry.cpp utils.cpp fuse.cpp threadpool.cpp dir.cpp file.cpp block.cpp sqlite.cpp)
set_target_properties(fmdisk PROPERTIES OUTPUT_NAME "fmdisk")
target_link_libraries(fmdisk ${JSON-C_LIBRARIES} ${FUSE_LIBRARIES} ${SQLITE3_LIBRARIES} pthread)

add_library(fmfsck STATIC  fsck.cpp threadpool.cpp utils.cpp sqlite.cpp)
set_target_properties(fmfsck PROPERTIES OUTPUT_NAME "fmfsck")
target_link_libraries(fmfsck ${JSON-C_LIBRARIES} ${SQLITE3_LIBRARIES} pthread)

add_executable(fmfsck_test stub_api.cpp)
target_link_libraries(fmfsck_test fmfsck)

add_executable(locker_test locker_test.cpp)
target_link_libraries(locker_test pthread)
