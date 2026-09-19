add_library(fmdisk_objs OBJECT entry.cpp utils.cpp fuse.cpp threadpool.cpp
            dir.cpp file.cpp symlink.cpp block.cpp sqlite.cpp log.c transfer_helper.cpp fmbed.cpp stats.cpp)

# fuse3 头文件单独成 INTERFACE: 非 libfuse3 消费方只拿头文件; libfuse3 只有 fmdisk需要
add_library(fuse3_headers INTERFACE)
target_include_directories(fuse3_headers SYSTEM INTERFACE ${FUSE_INCLUDE_DIRS})
target_link_libraries(fmdisk_objs PUBLIC fuse3_headers PkgConfig::JSON-C PkgConfig::SQLITE3 pthread)

# fuse 符号垫片, 单独成 target: stub libfuse3 的这些符号,
add_library(fmbed_shim OBJECT fmbed-shim.c)
target_link_libraries(fmbed_shim PUBLIC fuse3_headers)

add_library(fmdisk_embed INTERFACE)
target_link_libraries(fmdisk_embed INTERFACE fmdisk_objs fmbed_shim)

add_library(fmdisk STATIC fm_main.c $<TARGET_OBJECTS:fmdisk_objs>)
set_target_properties(fmdisk PROPERTIES OUTPUT_NAME "fmdisk")
target_link_libraries(fmdisk PUBLIC fmdisk_objs PkgConfig::FUSE)

add_library(fmfsck STATIC  fsck.cpp threadpool.cpp utils.cpp sqlite.cpp transfer_helper.cpp stats.cpp)
set_target_properties(fmfsck PROPERTIES OUTPUT_NAME "fmfsck")
target_link_libraries(fmfsck PUBLIC PkgConfig::JSON-C fmbed_shim fuse3_headers PkgConfig::SQLITE3 pthread)

add_library(fmcli STATIC fmcli.cpp)
set_target_properties(fmcli PROPERTIES OUTPUT_NAME "fmcli")
target_link_libraries(fmcli PUBLIC fmdisk_objs fmbed_shim)
