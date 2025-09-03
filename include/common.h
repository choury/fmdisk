#ifndef COMMON_H__
#define COMMON_H__

#define INLINE_DLEN    (uint64_t)0x10000           //inline data limit (64K)
#define UPLOADTHREADS      10ll
#define DOWNLOADTHREADS    10ll
#define CHECKTHREADS       20ll

#define METANAME      "meta.json"

#define MAXFILE       100000
#define PATHLEN       1024

#ifdef __cplusplus
extern "C" {
#endif

#define FM_DELETE_NEED_PURGE   1
#define FM_DONOT_REQUIRE_MKDIR 2
#define FM_RENAME_NOTSUPPRTED  4
#define FM_HAS_STORAGE_CLASS   8

struct fmoption{
    const char* cache_dir;
    const char* secret;
    unsigned int block_len;
    unsigned int flags;
    int  no_cache;         // 禁用本地磁盘缓存，直接访问远程数据，不支持write操作
    long long cache_size;  // 缓存大小限制（字节），<0不限制，=0立即回收，>0按大小限制
    const char* log_path;  // 日志文件路径，NULL使用stderr
    void (*clean)();
};

extern struct fmoption opt;

int fm_main(int argc, char *argv[]);

#ifdef __cplusplus
}
#endif

#endif
