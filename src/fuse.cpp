#include "common.h"
#include "fuse.h"
#include "dir.h"
#include "file.h"
#include "symlink.h"
#include "utils.h"
#include "fmdisk.h"
#include "log.h"

#include <errno.h>
#include <string.h>
#include <memory>
#include <sstream>

std::unique_ptr<struct statvfs> fs = nullptr;
struct fmoption opt{};

void *fm_fuse_init(struct fuse_conn_info *conn, struct fuse_config *cfg){
    cache_prepare();
    conn->max_background = 20;
    //conn->max_read = opt.block_len;
    //conn->max_write = opt.block_len;
    conn->max_readahead = 10*1024*1024;
#if FUSE_VERSION >= 317 && FUSE_HOTFIX_VERSION >= 1
    conn->no_interrupt = 1;
    //we should make fuse framwork to lookup . and .., so disable FUSE_CAP_EXPORT_SUPPORT
    //fuse_set_feature_flag(conn, FUSE_CAP_EXPORT_SUPPORT);
    fuse_set_feature_flag(conn, FUSE_CAP_PARALLEL_DIROPS);
    fuse_set_feature_flag(conn, FUSE_CAP_WRITEBACK_CACHE);
    fuse_unset_feature_flag(conn, FUSE_CAP_ATOMIC_O_TRUNC);
#else
    conn->want |= conn->capable & (FUSE_CAP_WRITEBACK_CACHE | FUSE_CAP_PARALLEL_DIROPS);
    conn->want &= ~FUSE_CAP_ATOMIC_O_TRUNC;
#endif
    cfg->hard_remove = 1; // 不隐藏删除的文件
    if(!opt.no_cache) {
        cfg->direct_io = 1;
    }
    cfg->nullpath_ok = 1;
    cfg->no_rofd_flush = 1;
    cfg->attr_timeout = 3;
    cfg->entry_timeout = 10;
    cfg->negative_timeout = 30;
    cfg->use_ino = 0;
    cfg->readdir_ino = 0;
#if FUSE_VERSION > 314
    //这个理论上需要3.14.1，但是FUSE_HOTFIX_VERSION还没有引入，所以只能判断 > 3.14
    cfg->parallel_direct_writes = 1;
#endif
    // 恢复dirty数据并重新上传
    if(!opt.no_cache) {
        int journal_ret = recover_journals();
        if(journal_ret != 0) {
            errorlog("recover journals failed: %d\n", journal_ret);
            exit(EXIT_FAILURE);
        }
        recover_dirty_data();
    }
    return cache_root().get();
}

void fm_fuse_destroy(void*){
    fs = nullptr;
    cache_destroy();
    log_cleanup();
    if(opt.clean) {
        opt.clean();
    }
}

int fm_fuse_statfs(const char *path, struct statvfs *sf){
    if(fs){
        memcpy(sf, fs.get(), sizeof(struct statvfs));
        return 0;
    }
    auto ret = dir_t::statfs(path, sf);
    if(ret >= 0) {
        fs = std::make_unique<struct statvfs>();
        memcpy(fs.get(), sf, sizeof(struct statvfs));
    }
    return ret;
}

int fm_fuse_opendir(const char *path, struct fuse_file_info *fi){
    return fm_fuse_open(path, fi);
}

void metatostat(const filemeta& meta, struct stat* st) {
    memset(st, 0, sizeof(struct stat));
    assert((meta.flags & META_KEY_ONLY_F) == 0);
    st->st_ino = 1;
    st->st_mode = meta.mode;
    st->st_size = meta.size;
    st->st_blksize = meta.blksize;
    st->st_nlink = 1;
    st->st_blocks = meta.blocks;
    st->st_ctime = meta.ctime;
    st->st_mtime = meta.mtime;
    st->st_atime = meta.mtime;
    st->st_uid = getuid();
    st->st_gid = getgid();
}

int fm_fuse_readdir(const char* path, void *buf, fuse_fill_dir_t filler,
                    off_t offset, struct fuse_file_info *fi, enum fuse_readdir_flags flags){
    auto entry_ptr = (std::shared_ptr<entry_t>*)fi->fh;
    auto dir = std::dynamic_pointer_cast<dir_t>(*entry_ptr);
    if(dir == nullptr){
        return -ENOTDIR;
    }
    int ret = dir->foreach_entrys([&](const string& name, std::shared_ptr<filemeta> meta) {
        if(meta && (flags & FUSE_READDIR_PLUS)){
            struct stat st;
            metatostat(*meta, &st);
            return filler(buf, name.c_str(), &st, 0, FUSE_FILL_DIR_PLUS);
        }else{
            return filler(buf, name.c_str(), nullptr, 0, (enum fuse_fill_dir_flags)0);
        }
    });
    if(ret > 0){
        return 0;
    }
    return ret;
}

int fm_fuse_fsyncdir (const char *, int dataonly, struct fuse_file_info *fi) {
    auto entry_ptr = (std::shared_ptr<entry_t>*)fi->fh;
    auto entry = std::dynamic_pointer_cast<dir_t>(*entry_ptr);
    return entry->sync(dataonly);
}

int fm_fuse_releasedir(const char*, struct fuse_file_info *fi){
    return fm_fuse_release(nullptr, fi);
}

int fm_fuse_access(const char* path, int mask){
    auto entry = find_entry(path);
    if(entry == nullptr){
        return -ENOENT;
    }
    return 0;
}

int fm_fuse_getattr(const char *path, struct stat *st, struct fuse_file_info *fi){
    std::shared_ptr<entry_t> entry;
    if(fi){
        entry = *(std::shared_ptr<entry_t>*)fi->fh;
    }else{
        entry = find_entry(path);
    }
    if(entry == nullptr){
        return -ENOENT;
    }
    filemeta meta;
    int ret = entry->getmeta(meta);
    if(ret < 0) {
        return ret;
    }
    metatostat(meta, st);
    return 0;
}


int fm_fuse_mkdir(const char *path, mode_t mode){
    auto parent = std::dynamic_pointer_cast<dir_t>(find_entry(dirname(path)));
    if(parent == nullptr){
        return -ENOENT;
    }
    if(parent->mkdir(basename(path), mode) == nullptr){
        return -errno;
    }
    fs = nullptr;
    return 0;
}

int fm_fuse_unlink(const char *path){
    auto parent = std::dynamic_pointer_cast<dir_t>(find_entry(dirname(path)));
    if(parent == nullptr){
        return -ENOENT;
    }
    fs = nullptr;
    return parent->unlink(basename(path));
}

int fm_fuse_rmdir(const char *path){
    auto parent = std::dynamic_pointer_cast<dir_t>(find_entry(dirname(path)));
    if(parent == nullptr){
        return -ENOENT;
    }
    fs = nullptr;
    return parent->rmdir(basename(path));
}

int fm_fuse_rename(const char *oldname, const char *newname, unsigned int flags){
    if(opt.no_cache) {
        return -EROFS; // rename 需要db来保证一致性，禁用本地缓存时禁止rename
    }
    auto parent = std::dynamic_pointer_cast<dir_t>(find_entry(dirname(oldname)));
    if(parent == nullptr){
        return -ENOENT;
    }
    auto newparent = std::dynamic_pointer_cast<dir_t>(find_entry(dirname(newname)));
    if(newparent == nullptr){
        return -ENOENT;
    }
    fs = nullptr;
    return parent->moveto(newparent, basename(oldname), basename(newname), flags);
}

int fm_fuse_create(const char *path, mode_t mode, struct fuse_file_info *fi){
    if(opt.no_cache) {
        return -EROFS; // 禁止创建文件
    }
    auto parent = std::dynamic_pointer_cast<dir_t>(find_entry(dirname(path)));
    if(parent == nullptr){
        return -ENOENT;
    }
    auto entry = parent->create(basename(path), mode);
    if(entry == nullptr){
        return -errno;
    }
    // Store shared_ptr on heap to maintain reference count
    fi->fh = (uint64_t)(new std::shared_ptr<entry_t>(entry));
    fi->direct_io = 1;
    fs = nullptr;
    return entry->open();
}

int fm_fuse_open(const char *path, struct fuse_file_info *fi){
    auto entry = find_entry(path);
    if(entry == nullptr){
        return -ENOENT;
    }
    // Store shared_ptr on heap to maintain reference count
    fi->fh = (uint64_t)(new std::shared_ptr<entry_t>(entry));
    if(opt.no_cache) {
        fi->keep_cache = 1;
        fi->noflush = 1;
    } else {
        fi->direct_io = 1;
    }
#if FUSE_VERSION > 314
    fi->parallel_direct_writes = 1;
#endif
    return entry->open();
}

int fm_fuse_truncate(const char* path, off_t offset, struct fuse_file_info *fi){
    if(opt.no_cache) {
        return -EROFS;
    }
    std::shared_ptr<file_t> entry;
    if(fi){
        auto entry_ptr = (std::shared_ptr<entry_t>*)fi->fh;
        entry = std::dynamic_pointer_cast<file_t>(*entry_ptr);
    }else{
        entry = std::dynamic_pointer_cast<file_t>(find_entry(path));
    }
    if(entry == nullptr){
        return -ENOENT;
    }
    fs = nullptr;
    return entry->truncate(offset);
}

int fm_fuse_read(const char *, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    auto entry_ptr = (std::shared_ptr<entry_t>*)fi->fh;
    auto entry = std::dynamic_pointer_cast<file_t>(*entry_ptr);
    return entry->read(buf, offset, size);
}

int fm_fuse_write(const char *, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    if(opt.no_cache) {
        return -EROFS;
    }
    auto entry_ptr = (std::shared_ptr<entry_t>*)fi->fh;
    auto entry = std::dynamic_pointer_cast<file_t>(*entry_ptr);
    fs = nullptr;
    return entry->write(buf, offset, size);
}

int fm_fuse_fsync(const char *, int dataonly, struct fuse_file_info *fi){
    if(opt.no_cache) {
        return -EROFS;
    }
    auto entry_ptr = (std::shared_ptr<entry_t>*)fi->fh;
    auto entry = std::dynamic_pointer_cast<file_t>(*entry_ptr);
    return entry->sync(dataonly);
}

int fm_fuse_flush(const char*, struct fuse_file_info *fi){
    auto entry_ptr = (std::shared_ptr<entry_t>*)fi->fh;
    auto entry = std::dynamic_pointer_cast<file_t>(*entry_ptr);
    return entry->sync(1);
}

int fm_fuse_release(const char *, struct fuse_file_info *fi){
    auto entry_ptr = (std::shared_ptr<entry_t>*)fi->fh;
    int ret = (*entry_ptr)->release(fi->flags & (O_SYNC | O_DSYNC));
    delete entry_ptr;  // Release the shared_ptr, decrement reference count
    return ret;
}

int fm_fuse_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi){
    if(opt.no_cache) {
        return -EROFS;
    }
    std::shared_ptr<entry_t> entry;
    if(fi){
        entry = *(std::shared_ptr<entry_t>*)fi->fh;
    }else{
        entry = find_entry(path);
    }
    if(entry == nullptr){
        return -ENOENT;
    }
    return entry->utime(tv);
}


int fm_fuse_chmod(const char *path, mode_t mode, struct fuse_file_info *fi){
    if(opt.no_cache) {
        return -EROFS;
    }
    std::shared_ptr<entry_t> entry;
    if(fi){
        entry = *(std::shared_ptr<entry_t>*)fi->fh;
    }else{
        entry = find_entry(path);
    }
    if(entry == nullptr){
        return -ENOENT;
    }
    return entry->chmod(mode);
}

int fm_fuse_chown(const char* path, uid_t, gid_t, struct fuse_file_info *fi) {
    if(opt.no_cache) {
        return -EROFS;
    }
    std::shared_ptr<entry_t> entry;
    if(fi){
        entry = *(std::shared_ptr<entry_t>*)fi->fh;
    }else{
        entry = find_entry(path);
    }
    if(entry == nullptr){
        return -ENOENT;
    }
    return 0;
}

int fm_fuse_symlink(const char *target, const char *linkpath){
    auto parent = std::dynamic_pointer_cast<dir_t>(find_entry(dirname(linkpath)));
    if(parent == nullptr){
        return -ENOENT;
    }
    auto entry = parent->symlink(basename(linkpath), target);
    if(entry == nullptr){
        return -errno;
    }
    fs = nullptr;
    return 0;
}

int fm_fuse_readlink(const char *path, char *buf, size_t bufsiz){
    auto entry = std::dynamic_pointer_cast<symlink_t>(find_entry(path));
    if(entry == nullptr){
        return -ENOENT;
    }

    // 检查是否是符号链接
    filemeta meta;
    int ret = entry->getmeta(meta);
    if(ret < 0) {
        return ret;
    }
    if(!S_ISLNK(meta.mode)) {
        return -EINVAL;
    }

    // 从meta中读取符号链接目标
    if(meta.inline_data.empty()) {
        return -EIO;
    }

    size_t len = std::min(bufsiz - 1, meta.size);
    strncpy(buf, meta.inline_data.c_str(), len);
    buf[len] = '\0';
    return 0;
}

#ifdef __APPLE__
int fm_fuse_setxattr(const char *path, const char *name, const char *value, size_t size, int flags, uint32_t){
#else
int fm_fuse_setxattr(const char *path, const char *name, const char *value, size_t size, int flags){
#endif
    auto entry = find_entry(path);
    if(entry == nullptr){
        return -ENOENT;
    }
    if(strcmp(name, "user.drop_cache") == 0){
        if(value == nullptr || size == 0) {
            return -ERANGE;
        }
        std::string_view value_str(value, size);
        if(value_str == "1") {
            return entry->drop_cache(false);
        }
        if(value_str == "2"){
            return entry->drop_cache(true);
        }
        return -ENODATA;
    }
    if(strcmp(name, "user.storage_class") == 0) {
        if(value == nullptr || size == 0) {
            return -ERANGE;
        }
        std::string_view storage_str(value, size);
        enum storage_class storage = STORAGE_UNKNOWN;
        if(storage_str == "S") {
            storage = STORAGE_STANDARD;
        } else if(storage_str == "IA") {
            storage = STORAGE_IA;
        } else if(storage_str == "AR") {
            storage = STORAGE_ARCHIVE;
        } else if(storage_str == "DR") {
            storage = STORAGE_DEEP_ARCHIVE;
        } else {
            return -EINVAL;
        }
        return entry->set_storage_class(storage);
    }
    return -ENODATA;
}

#ifdef __APPLE__
int fm_fuse_getxattr(const char *path, const char *name, char *value, size_t len, uint32_t) {
#else
int fm_fuse_getxattr(const char *path, const char *name, char *value, size_t len){
#endif
    auto entry = find_entry(path);
    if(entry == nullptr){
        return -ENOENT;
    }
    if(strcmp(name, "user.underlay_path") == 0){
        string underlay_path = entry->getkey().path;
        if(len == 0){
            return underlay_path.length();
        }
        if(underlay_path.length() >= len){
            return -ERANGE;
        }
        strcpy(value, underlay_path.c_str());
        return underlay_path.length();
    }
    if(strcmp(name, "user.storage_class") == 0) {
        storage_class_info info;
        int ret = entry->get_storage_classes(info);
        if(ret < 0) {
            return ret;
        }
        std::stringstream ss;
        ss << "S: " << bytes2human(info.size_store[1])
           << " IA: " << bytes2human(info.size_store[2])
           << " AR: " << bytes2human(info.size_store[3]);
        if(info.size_archive_restored + info.size_archive_restoring) {
            size_t size = info.size_store[3];
            ss << " (" << bytes2human(size - info.size_archive_restored - info.size_archive_restoring) << "/"
            << bytes2human(size - info.size_archive_restored) << ")";
        }
        ss << " DR: " << bytes2human(info.size_store[4]);
        if(info.size_deep_archive_restored + info.size_deep_archive_restoring) {
            size_t size = info.size_store[4];
            ss << " (" << bytes2human(size - info.size_deep_archive_restored - info.size_deep_archive_restoring) << "/"
            << bytes2human(size - info.size_deep_archive_restored) << ")";
        }
        ss << " U: " << bytes2human(info.size_store[0]);
        std::string result = ss.str();
        if(len == 0) {
            return result.length();
        }
        if(result.length() >= len) {
            return -ERANGE;
        }
        strcpy(value, result.c_str());
        return result.length();
    }
    if(strcmp(name, "user.etag") == 0) {
        std::string etag;
        int ret = entry->get_etag(etag);
        if(ret < 0) {
            return ret;
        }
        if(len == 0) {
            return etag.length();
        }
        if(etag.length() >= len) {
            return -ERANGE;
        }
        strcpy(value, etag.c_str());
        return etag.length();
    }
    return -ENODATA;
}

int __attribute__((weak)) fm_statfs(struct statvfs* sf) {
    return 0;
}

int __attribute__((weak)) fm_mkdir(const filekey& fileat, struct filekey& file) {
    errno = EACCES;
    return -errno;
}

int __attribute__((weak)) fm_rename(const filekey& oldat, const filekey& file, const filekey& newat, filekey& newfile) {
    errno = EACCES;
    return -errno;
}

int __attribute__((weak)) fm_utime(const filekey& file, const time_t tv[2]) {
    errno = EACCES;
    return -errno;
}

int __attribute__((weak)) fm_copy(const filekey& file, const filekey& newat, filekey& newfile) {
    errno = EPERM;
    return -errno;
}

int __attribute__((weak)) fm_change_storage_class(const filekey& file, enum storage_class storage) {
    errno = EACCES;
    return -errno;
}

int __attribute__((weak)) fm_restore_archive(const filekey& file, int days, unsigned int mode) {
    errno = EACCES;
    return -errno;
}
