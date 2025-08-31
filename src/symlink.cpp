#include "common.h"
#include "symlink.h"
#include "dir.h"
//#include "fmdisk.h"
//#include "utils.h"
#include "sqlite.h"
#include <errno.h>
#include <string.h>

symlink_t::symlink_t(dir_t* parent, const filemeta& meta):
    entry_t(parent, meta),
    target_path(meta.inline_data)
{
    mode = (meta.mode & ~S_IFMT) | S_IFLNK;
    if(flags & ENTRY_CHUNCED_F){
        fk = std::make_shared<filekey>(decodepath(*fk.load(), symlink_encode_suffix));
    } else {
        abort(); //符号链接只支持chunced的
    }
    if(flags & ENTRY_INITED_F) {
        assert(length == target_path.size());
    }
}

symlink_t::~symlink_t() {
}

std::string symlink_t::getrealname() {
    if(flags & ENTRY_CHUNCED_F) {
        return encodepath(fk.load()->path, symlink_encode_suffix);
    }
    return fk.load()->path;
}

int symlink_t::getmeta(filemeta& meta) {
    auto_rlock(this);
    if(flags & ENTRY_DELETED_F) {
        return -ENOENT;
    }

    // 如果没有初始化，需要先拉取远程数据
    if((flags & ENTRY_INITED_F) == 0){
        __r.upgrade();
        int ret = pull_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    meta = initfilemeta(getkey());
    meta.flags = flags;
    meta.mode = mode;
    meta.ctime = ctime;
    meta.mtime = mtime;
    meta.blksize = opt.block_len;
    meta.inline_data = target_path;
    meta.size = length;
    meta.blocks = 1;
    assert(meta.size == target_path.size());
    return 0;
}

int symlink_t::pull_wlocked() {
    const filekey& key = getkey();
    filemeta meta = initfilemeta(key);
    meta.mode = this->mode;
    assert((flags & ENTRY_INITED_F) == 0);
    std::vector<filekey> fblocks;
    load_file_from_db(key.path, meta, fblocks);
    assert(fblocks.empty());
    assert(flags & ENTRY_CHUNCED_F);
    if(meta.blksize == 0){
        int ret = download_meta(key, meta, fblocks);
        if(ret < 0){
            return ret;
        }
        save_file_to_db(key.path, meta, fblocks);
    }
    mode = (meta.mode & ~S_IFMT) | S_IFLNK;
    length = meta.size;
    ctime = meta.ctime;
    mtime = meta.mtime;
    flags |= meta.flags;
    target_path = std::move(meta.inline_data);
    flags |= ENTRY_INITED_F;
    flags &= ~META_KEY_ONLY_F;
    return 0;
}

int symlink_t::utime(const struct timespec tv[2]) {
    //no atime in filemeta
    if(tv[1].tv_nsec == UTIME_OMIT){
        return 0;
    }
    auto_wlock(this);
    if(flags & ENTRY_DELETED_F) {
        return -ENOENT;
    }

    if((flags & ENTRY_INITED_F) == 0){
        int ret = pull_wlocked();
        if(ret < 0) {
            return ret;
        }
    }


    struct filemeta meta = initfilemeta({getrealname(), 0});
    meta.flags = ENTRY_CHUNCED_F | ENTRY_CREATE_F;
    meta.mode = S_IFLNK | 0777;  // 符号链接权限
    meta.blksize = opt.block_len;
    meta.size = target_path.length();
    meta.inline_data = target_path;
    meta.ctime = ctime;
    if(tv[1].tv_nsec == UTIME_NOW){
        meta.mtime = time(nullptr);
    } else {
        meta.mtime = tv[1].tv_sec;
    }
    int ret = HANDLE_EAGAIN(upload_meta(parent->getkey(), meta, {}));
    if(ret){
        return ret;
    }
    fk = std::make_shared<filekey>(decodepath(basename(meta.key), symlink_encode_suffix));
    mtime = meta.mtime;
    save_file_to_db(getkey().path, meta, {});
    return 0;
}

int symlink_t::drop_cache_wlocked() {
    if(flags & FILE_DIRTY_F){
        return -EAGAIN;
    }
    if((flags & ENTRY_INITED_F) == 0){
        return 0;
    }
    return delete_file_from_db(getkey().path);
}

int symlink_t::remove_wlocked() {
    auto key = getkey();
    int ret = HANDLE_EAGAIN(fm_delete(key));
    if(ret && errno != ENOENT){
        return ret;
    }

    flags |= ENTRY_DELETED_F;
    parent = nullptr;
    delete_entry_from_db(key.path);
    return delete_file_from_db(key.path);
}

void symlink_t::dump_to_db(const std::string& path, const std::string& name) {
    auto_rlock(this);
    if(flags & ENTRY_DELETED_F) {
        return;
    }
    if((flags & ENTRY_INITED_F) == 0){
        return;
    }
    filemeta meta;
    if(getmeta(meta) < 0) {
        return;
    }
    meta.key.path = encodepath(name, symlink_encode_suffix);
    save_entry_to_db(path, meta);
    save_file_to_db(pathjoin(path, meta.key.path), meta, {});
}