#ifndef COMMON_H__
#define COMMON_H__

#define INLINE_DLEN    (uint64_t)0x1000            //inline data limit (4K)
#define UPLOADTHREADS  10ll
#define DOWNLOADTHREADS    20
#define CHECKTHREADS   20

#define METANAME      "meta.json"
#define METAPATH      "/" METANAME

#define MAXFILE       100000
#define PATHLEN       1024

#ifdef __cplusplus
extern "C" {
#endif

#define FM_DELETE_NEED_PURGE   1
#define FM_DONOT_REQUIRE_MKDIR 2
#define FM_RENAME_NOTSUPPRTED  4

struct fmoption{
    const char* cache_dir;
    const char* secret;
    unsigned int block_len;
    unsigned int flags;
    long long cache_size;  // 缓存大小限制（字节），<0不限制，=0立即回收，>0按大小限制
};

extern struct fmoption opt;

int fm_main(int argc, char *argv[]);

#ifdef __cplusplus
}
#endif

#endif
