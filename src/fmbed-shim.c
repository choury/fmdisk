/*
 * libfuse3 四个符号(fuse_log/fuse_set_log_func/fuse_set/unset_feature_flag)的
 * 内置实现, 供不做 FUSE 内核挂载的消费方链接, 免去对 libfuse3 的链接依赖;
 */
#include "fuse.h"

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

static void fmbed_default_log_func(enum fuse_log_level level, const char* fmt, va_list ap)
{
    (void)level;
    vfprintf(stderr, fmt, ap);
}

static fuse_log_func_t fmbed_log_func = fmbed_default_log_func;

void fuse_set_log_func(fuse_log_func_t func)
{
    fmbed_log_func = func != NULL ? func : fmbed_default_log_func;
}

void fuse_log(enum fuse_log_level level, const char* fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    fmbed_log_func(level, fmt, ap);
    va_end(ap);
}

// 能力位协商只对内核挂载有意义; embed 的 conn 是零初始化哑结构, 空操作即可
bool fuse_set_feature_flag(struct fuse_conn_info* conn, uint64_t flag)
{
    (void)conn;
    (void)flag;
    return false; // 未实际置位; fmdisk 不检查返回值
}

void fuse_unset_feature_flag(struct fuse_conn_info* conn, uint64_t flag)
{
    (void)conn;
    (void)flag;
}
