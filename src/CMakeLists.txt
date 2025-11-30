add_library(fmdisk STATIC fm_main.c entry.cpp utils.cpp fuse.cpp threadpool.cpp dir.cpp file.cpp symlink.cpp block.cpp sqlite.cpp log.c transfer_helper.cpp)
set_target_properties(fmdisk PROPERTIES OUTPUT_NAME "fmdisk")
target_link_libraries(fmdisk PUBLIC PkgConfig::JSON-C PkgConfig::FUSE PkgConfig::SQLITE3 pthread)

add_library(fmfsck STATIC  fsck.cpp threadpool.cpp utils.cpp sqlite.cpp transfer_helper.cpp)
set_target_properties(fmfsck PROPERTIES OUTPUT_NAME "fmfsck")
target_link_libraries(fmfsck PUBLIC PkgConfig::JSON-C PkgConfig::FUSE PkgConfig::SQLITE3 pthread)

add_library(fmcli STATIC fmcli.cpp utils.cpp log.c transfer_helper.cpp)
set_target_properties(fmcli PROPERTIES OUTPUT_NAME "fmcli")
target_link_libraries(fmcli PUBLIC PkgConfig::JSON-C PkgConfig::FUSE pthread)
