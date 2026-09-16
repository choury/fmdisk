#include "fmbed.h"

#include "common.h"
#include "fmdisk.h"
#include "fuse.h"
#include "entry.h"
#include "dir.h"
#include "file.h"
#include "symlink.h"
#include "log.h"
#include "utils.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>

#include <atomic>
#include <memory>

// fuse.cpp 的 statvfs 缓存全局, 建文件后需置空使其失效
extern std::unique_ptr<struct statvfs> fs;

static void* fmbed_userdata = nullptr;
static std::atomic<bool> fmbed_inited{false};

int fmbed_init(const struct fmbed_opts* opts) {
    if(fmbed_inited) {
        return -EBUSY;
    }
    //cache_prepare 会在根目录 getattr/.objs 创建/sqlinit 失败时 throw(entry.cpp),
    //extern "C" 边界必须接住, 否则 C 宿主直接 std::terminate
    try {
        int ret = fm_prepare();
        if(ret) {
            return ret < 0 ? ret : -EIO;
        }
        static std::string fmbed_cache_dir;
        if(opts != nullptr) {
            if((opts->mask & FMBED_OPT_CACHE_DIR) && opts->cache_dir != nullptr) {
                fmbed_cache_dir = opts->cache_dir;
                opt.cache_dir = fmbed_cache_dir.c_str();
            }
            if(opts->mask & FMBED_OPT_NO_CACHE) {
                opt.no_cache = opts->no_cache;
            }
            if(opts->mask & FMBED_OPT_CACHE_SIZE) {
                opt.cache_size = opts->cache_size;
            }
            if(opts->mask & FMBED_OPT_ENTRY_CACHE_SECOND) {
                opt.entry_cache_second = opts->entry_cache_second;
            }
        }
        opt.fmbed_mode = 1;
        log_init(opt.log_path);
        struct fuse_conn_info conn;
        struct fuse_config cfg;
        memset(&conn, 0, sizeof(conn));
        memset(&cfg, 0, sizeof(cfg));
        fmbed_userdata = fm_fuse_init(&conn, &cfg);
        if(fmbed_userdata == nullptr) {
            errorlog("fmbed_init failed: fm_fuse_init returned null\n");
            return -EIO;
        }
    } catch(const std::exception& e) {
        errorlog("fmbed_init failed: %s\n", e.what());
        return -EIO;
    } catch(...) {
        errorlog("fmbed_init failed: unknown exception\n");
        return -EIO;
    }
    fmbed_inited = true;
    return 0;
}

void fmbed_destroy(void) {
    if(!fmbed_inited) {
        return;
    }
    fm_fuse_destroy(fmbed_userdata);
    fmbed_userdata = nullptr;
    fmbed_inited = false;
}

// 句柄即核心对象引用: 不包 fuse_file_info, 数据面直连 entry/file 层
struct fmbed_file {
    std::shared_ptr<file_t> file;
    int accmode;   // O_ACCMODE, read/write 的 EBADF 检查用
};

fmbed_file* fmbed_open(const char* path, int flags) {
    if(!fmbed_inited) {
        errno = ENODEV;
        return nullptr;
    }
    if(path == nullptr) {
        errno = EINVAL;
        return nullptr;
    }
    std::shared_ptr<entry_t> entry = find_entry(path);
    std::shared_ptr<file_t> file;
    if(entry != nullptr) {
        if(std::dynamic_pointer_cast<symlink_t>(entry) != nullptr) {
            errno = ELOOP;
            return nullptr;
        }
        file = std::dynamic_pointer_cast<file_t>(entry);
        if(file == nullptr) {
            errno = EISDIR;
            return nullptr;
        }
        if((flags & O_CREAT) && (flags & O_EXCL)) {
            errno = EEXIST;
            return nullptr;
        }
    } else {
        if((flags & O_CREAT) == 0) {
            errno = ENOENT;
            return nullptr;
        }
        if(opt.no_cache) {
            errno = EROFS;
            return nullptr;
        }
        auto pentry = find_entry(dirname(path));
        if(pentry == nullptr) {
            errno = ENOENT;
            return nullptr;
        }
        auto parent = std::dynamic_pointer_cast<dir_t>(pentry);
        if(parent == nullptr) {
            errno = ENOTDIR;
            return nullptr;
        }
        file = parent->create(basename(path), 0666);
        if(file == nullptr) {
            return nullptr; // errno 已由 dir_t::create 设置
        }
        fs = nullptr; // statvfs 缓存失效, 对齐 fm_fuse_create
    }
    int ret = file->open(); // 幂等: 已初始化时只做 opened++
    if(ret != 0) {
        errno = -ret;
        return nullptr;
    }
    return new fmbed_file{std::move(file), flags & O_ACCMODE};
}

int64_t fmbed_read(fmbed_file* f, void* buf, size_t len, uint64_t off) {
    if(!fmbed_inited) {
        return -ENODEV;
    }
    if(f == nullptr) {
        return -EBADF;
    }
    if(f->accmode == O_WRONLY) {
        return -EBADF;
    }
    int ret = f->file->read(buf, (off_t)off, len);
    // file_t::read 对 off>size 返回 EFAULT, 对齐 POSIX: 视为 EOF
    return ret == -EFAULT ? 0 : ret;
}

int64_t fmbed_write(fmbed_file* f, const void* buf, size_t len, uint64_t off) {
    if(!fmbed_inited) {
        return -ENODEV;
    }
    if(f == nullptr) {
        return -EBADF;
    }
    if(f->accmode == O_RDONLY) {
        return -EBADF;
    }
    return f->file->write(buf, (off_t)off, len);
}

int fmbed_truncate(fmbed_file* f, uint64_t size) {
    if(!fmbed_inited) {
        return -ENODEV;
    }
    if(f == nullptr) {
        return -EBADF;
    }
    // 必须是打开状态: 未打开条目的 fi.fd<0, truncate_wlocked 对其有断言
    return f->file->truncate((off_t)size);
}

int fmbed_close(fmbed_file* f, int flags) {
    if(!fmbed_inited) {
        return -ENODEV;
    }
    if(f == nullptr) {
        return -EBADF;
    }
    // FMBED_CLOSE_SYNC: 对齐 fm_fuse_release 的 waitsync 语义
    bool waitsync = (flags & FMBED_CLOSE_SYNC) != 0;
    int ret = f->file->release(waitsync);
    delete f;
    return ret;
}

int fmbed_stat(const char* path, struct stat* st) {
    if(!fmbed_inited) {
        return -ENODEV;
    }
    return fm_fuse_getattr(path, st, nullptr);
}

// 内核挂载下变更请求发出前 LOOKUP 已把父目录子项拉齐, 嵌入式直连没有这一步;
// children() 顺带完成 pull, 满足 dir_t 变更方法对 DIR_PULLED_F 的前置
static std::shared_ptr<dir_t> checked_parent(const char* path) {
    auto entry = find_entry(dirname(path));
    if(entry == nullptr){
        errno = ENOENT;
        return nullptr;
    }
    auto parent = std::dynamic_pointer_cast<dir_t>(entry);
    if(parent == nullptr){
        errno = ENOTDIR;
        return nullptr;
    }
    if(parent->children() < 0){
        return nullptr; // errno 已由 children() 设置
    }
    return parent;
}

int fmbed_mkdir(const char* path, mode_t mode) {
    if(!fmbed_inited) {
        return -ENODEV;
    }
    auto parent = checked_parent(path);
    if(parent == nullptr) {
        return -errno;
    }
    if(parent->mkdir(basename(path), mode) == nullptr) {
        return -errno;
    }
    fs = nullptr;
    return 0;
}

int fmbed_unlink(const char* path) {
    if(!fmbed_inited) {
        return -ENODEV;
    }
    auto parent = checked_parent(path);
    if(parent == nullptr) {
        return -errno;
    }
    fs = nullptr;
    return parent->unlink(basename(path));
}

int fmbed_rmdir(const char* path) {
    if(!fmbed_inited) {
        return -ENODEV;
    }
    auto parent = checked_parent(path);
    if(parent == nullptr) {
        return -errno;
    }
    fs = nullptr;
    return parent->rmdir(basename(path));
}

int fmbed_rename(const char* oldpath, const char* newpath) {
    if(!fmbed_inited) {
        return -ENODEV;
    }
    if(opt.no_cache) {
        return -EROFS; // rename 需要db来保证一致性，禁用本地缓存时禁止rename
    }
    auto parent = checked_parent(oldpath);
    if(parent == nullptr) {
        return -errno;
    }
    auto newparent = checked_parent(newpath);
    if(newparent == nullptr) {
        return -errno;
    }
    fs = nullptr;
    return parent->moveto(newparent, basename(oldpath), basename(newpath), 0);
}

int fmbed_statfs(const char* path, struct statvfs* sf) {
    if(!fmbed_inited) {
        return -ENODEV;
    }
    return fm_fuse_statfs(path, sf);
}

struct fmbed_list_ctx {
    fmbed_list_cb cb;
    void* ud;
    int stopped;
};

static int fmbed_listdir_filler(void* buf, const char* name, const struct stat* /*st*/, off_t /*off*/,
                                enum fuse_fill_dir_flags /*flags*/) {
    auto* ctx = (fmbed_list_ctx*)buf;
    if(strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
        return 0;
    }
    if(ctx->cb(ctx->ud, name) != 0) {
        ctx->stopped = 1;
        return 1;
    }
    return 0;
}

int fmbed_listdir(const char* path, fmbed_list_cb cb, void* ud) {
    if(!fmbed_inited) {
        return -ENODEV;
    }
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    int ret = fm_fuse_opendir(path, &fi);
    if(ret < 0) {
        return ret;
    }
    fmbed_list_ctx ctx = {cb, ud, 0};
    ret = fm_fuse_readdir(path, &ctx, fmbed_listdir_filler, 0, &fi, (enum fuse_readdir_flags)0);
    int rel = fm_fuse_releasedir(path, &fi);
    if(ret < 0) {
        return ret;
    }
    if(rel < 0) {
        return rel;
    }
    return ctx.stopped ? 1 : 0;
}

int fmbed_advise(fmbed_file* f, uint64_t off, uint64_t len, int advice) {
    if(!fmbed_inited) {
        return -ENODEV;
    }
    if(f == nullptr) {
        return -EBADF;
    }
    switch(advice) {
    case FMBED_WILL_READ:
        return f->file->prefetch_range((off_t)off, (size_t)len);
    case FMBED_WILL_PUSH:
        return f->file->writeback_range((off_t)off, (size_t)len);
    default:
        return -EINVAL;
    }
}

int fmbed_upload(const char* path, int fd) {
    if(!fmbed_inited) {
        return -ENODEV;
    }
    if(path == nullptr) {
        return -EINVAL;
    }
    auto parent = checked_parent(path);
    if(parent == nullptr) {
        return -errno;
    }
    auto file = parent->upload(basename(path), fd);
    if(file == nullptr) {
        return -errno;
    }
    fs = nullptr; // statvfs 缓存失效, 对齐 fm_fuse_create
    return 0;
}
