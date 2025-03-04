#ifndef COMMON_H__
#define COMMON_H__

#define BLOCKLEN       (uint64_t)0x100000          //1M,缓存分块大小 必须为4K的倍数
#define INLINE_DLEN    (uint64_t)0x1000            //inline data limit (4K)
#define UPLOADTHREADS  10
#define DOWNLOADTHREADS    20
#define CHECKTHREADS   20

#define METANAME      "meta.json"
#define METAPATH      "/" METANAME

#define MAXFILE       100000
#define PATHLEN       1024

#ifdef __cplusplus
extern "C" {
#endif

int fm_main(int argc, char *argv[]);

#ifdef __cplusplus
}
#endif

#endif
