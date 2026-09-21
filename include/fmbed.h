#ifndef FMBED_H__
#define FMBED_H__
/*
 * fmbed: fmdisk 的进程内嵌接口(不经过内核 FUSE)。
 *
 * 面向宿主程序的纯 C API:
 *   - 数据面句柄式: fmbed_open 拿句柄, 持句柄多次 read/write, fmbed_close 释放;
 *     不暴露任何 fuse 类型
 *   - 错误约定: fmbed_open 失败返回 NULL 并设置 errno(POSIX 风格);
 *     其余函数成功返回 0(读写返回>=0字节数), 失败返回 -errno
 *   - 进程内单实例: fmbed_init/fmbed_destroy 之间完成全部生命周期,
 *     fm_fuse_init 内部会做 journal 恢复并启动后台线程(GC/回写)
 */
#include <sys/stat.h>
#include <sys/statvfs.h>

#include <stddef.h>
#include <stdint.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/* fmbed_opts.mask: 哪些字段覆盖后端默认值*/
#define FMBED_OPT_NO_CACHE           (1u << 0)
#define FMBED_OPT_CACHE_SIZE         (1u << 1)
#define FMBED_OPT_ENTRY_CACHE_SECOND (1u << 2)
#define FMBED_OPT_CACHE_DIR          (1u << 3) //仅在测试中使用

struct fmbed_opts {
    unsigned int mask;
    int no_cache;              /* 非0禁用本地缓存: 只读, 句柄写路径返回-EROFS(fmbed_upload 例外) */
    long long cache_size;      /* 本地缓存上限(字节), <0不限, 0立即回收 */
    const char* cache_dir;     /* 本地缓存目录, 指针需在 fmbed_destroy 前保持有效 */
    int entry_cache_second;    /* 目录内存缓存秒数, <=0 禁用自动回收 */
};

/* 调用后端 fm_prepare()(鉴权/默认opt) -> 应用覆盖项 -> fm_fuse_init()。
 * 返回 0 成功; 失败返回负值(后端 fm_prepare 错误或 -errno)。
 * 注意: journal 恢复失败时 fmdisk 会直接 exit(),与挂载行为一致。 */
int fmbed_init(const struct fmbed_opts* opts);

/* fm_fuse_destroy(): 停 GC、清缓存、刷日志。重复调用安全。
 * 线程契约: 调用前宿主须保证所有在飞的 fmbed_* 调用已返回、
 * 所有句柄已 fmbed_close(与 fclose 等生命周期由调用方保证的约定一致)。 */
void fmbed_destroy(void);

/* ---- 数据面: 句柄式 ---- */

/* 不透明文件句柄 */
typedef struct fmbed_file fmbed_file;

/* 打开文件返回句柄。
 * flags 接受 O_RDONLY/O_WRONLY/O_RDWR/O_CREAT/O_EXCL/O_DIRECTORY:
 *   - O_CREAT: 不存在则创建(父目录必须存在; no_cache 模式 EROFS)
 *   - O_EXCL: 与 O_CREAT 同用时, 路径已存在则 EEXIST
 *   - O_DIRECTORY: 打开目录(不拉取目录项, 仅供 FMBED_WILL_SCAN)
 * 其余标志(如 O_APPEND/O_TRUNC)未实现, 静默忽略。
 * 打开目录(不带 O_DIRECTORY)返回 EISDIR, 打开符号链接返回 ELOOP。
 * 成功返回句柄; 失败返回 NULL 并设置 errno。
 * 句柄可被多线程并发使用(内部读写锁保护); fmbed_close 须等该句柄
 * 全部在飞调用返回后才能调用。 */
fmbed_file* fmbed_open(const char* path, int flags);

/* 读 [off, off+len): 返回读取字节数(0=EOF), <0 为 -errno。
 * O_WRONLY 句柄返回 -EBADF, 目录句柄返回 -EISDIR。 */
int64_t fmbed_read(fmbed_file* f, void* buf, size_t len, uint64_t off);

/* 写 [off, off+len): 超出末尾自动扩展。返回写入字节数, <0 为 -errno。
 * O_RDONLY 句柄返回 -EBADF, 目录句柄返回 -EISDIR。 */
int64_t fmbed_write(fmbed_file* f, const void* buf, size_t len, uint64_t off);

/* 截断/扩展到 size(对象存储天然稀疏, 可用于预分配); 目录句柄返回 -EISDIR */
int fmbed_truncate(fmbed_file* f, uint64_t size);

#define FMBED_CLOSE_SYNC 1  /* close 时同步推脏块并上传 meta 后再返回 */

/* 释放句柄; 无论成功失败句柄都被消费。
 * flags 传 FMBED_CLOSE_SYNC 则同步推送脏块并上传 meta, 传 0 走异步回写。
 * 返回 0 成功, <0 为 -errno。 */
int fmbed_close(fmbed_file* f, int flags);

/* ---- 元数据面: 按路径 ---- */

int fmbed_stat(const char* path, struct stat* st);
int fmbed_mkdir(const char* path, mode_t mode);          /* 单级, 父目录必须存在 */
int fmbed_unlink(const char* path);
int fmbed_rmdir(const char* path);                       /* 仅空目录 */
int fmbed_rename(const char* oldpath, const char* newpath);
int fmbed_statfs(const char* path, struct statvfs* sf);
int fmbed_utimens(const char* path, const struct timespec tv[2]);

/* 枚举目录: 对每个条目名(不含 ./..)调用 cb, 返回非0则提前停止。
 * 返回 0 完成, 1 被 cb 提前停止, <0 为 -errno */
typedef int (*fmbed_list_cb)(void* ud, const char* name);
int fmbed_listdir(const char* path, fmbed_list_cb cb, void* ud);

/* ---- 扩展属性: 与 FUSE 挂载的 xattr 同名同义 ---- */

int fmbed_setxattr(const char* path, const char* name, const void* value, size_t size, int flags);
int fmbed_getxattr(const char* path, const char* name, void* value, size_t size);

/* ---- 缓存策略 ---- */

#define FMBED_WILL_READ 1   /* 异步预取覆盖块(recheck 等顺序读前调用) */
#define FMBED_WILL_PUSH 2   /* 异步加速写回覆盖的脏块(writeback_thread 下一轮即推); 不上传 meta, meta 仍走 close/unmount 的常规路径 */
#define FMBED_WILL_SCAN 4   /* 目录句柄专用: 异步递归预取整棵子树的元数据; off 为下钻深度 (-1 无限, 0 只拉本层, 默认 0); len 忽略 */

/* 返回 0 成功, <0 为 -errno */
int fmbed_advise(fmbed_file* f, uint64_t off, uint64_t len, int advice);

/* 免缓存直传新文件; no_cache 可用; 0 成功, <0 -errno */
int fmbed_upload(const char* path, int fd);

#ifdef __cplusplus
}
#endif

#endif
