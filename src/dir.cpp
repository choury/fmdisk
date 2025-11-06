#include "common.h"
#include "fmdisk.h"
#include "dir.h"
#include "file.h"
#include "symlink.h"
#include "sqlite.h"
#include "utils.h"
#include "trdpool.h"
#include "defer.h"
#include "log.h"

#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/xattr.h>

static std::string_view childname(std::string_view path) {
    size_t pos = path.find_first_of("/");
    if(pos == string::npos) {
        return path;
    }
    if(pos == 0 ) {
        std::string_view path_truncate = path.substr(1, path.length());
        return childname(path_truncate);
    }
    return path.substr(0, pos);
}

static std::string_view subname(std::string_view path) {
    size_t pos = path.find_first_of("/");
    if(pos == string::npos || pos == path.length()-1) {
        return ".";
    }
    if(pos == 0 ) {
        std::string_view path_truncate = path.substr(1, path.length());
        return subname(path_truncate);
    }
    return path.substr(pos+1, path.length());
}


dir_t::dir_t(std::shared_ptr<dir_t> parent, const filemeta& meta): entry_t(parent, meta) {
    mode = (meta.mode & ~S_IFMT) | S_IFDIR;
}

dir_t::~dir_t(){
}

std::set<std::string> dir_t::insert_meta_wlocked(const std::vector<filemeta>& flist, bool save) {
    std::set<std::string> existnames;
    for(auto i: flist){
        string bname = basename(i.key.path);
        if(parent.lock() == nullptr && (opt.flags & FM_RENAME_NOTSUPPRTED) && bname == ".objs"){
            continue;
        }
        bool is_dir = S_ISDIR(i.mode), is_link = false;
        if(endwith(bname, file_encode_suffix) && is_dir){
            bname = decodepath(bname, file_encode_suffix);
            i.flags |= ENTRY_CHUNCED_F | META_KEY_ONLY_F;
            is_dir = false;
        }else if(endwith(bname, symlink_encode_suffix) && !is_dir){
            bname = decodepath(bname, symlink_encode_suffix);
            i.flags |= ENTRY_CHUNCED_F | META_KEY_ONLY_F;
            is_link = true;
        }
        existnames.insert(bname);
        if(entrys.contains(bname)){
            continue;
        }
        if(is_dir){
            entrys.emplace(bname, std::make_shared<dir_t>(shared_dir_from_this(), i));
        }else if(is_link){
            entrys.emplace(bname, std::make_shared<symlink_t>(shared_dir_from_this(), i));
        }else{
            entrys.emplace(bname, std::make_shared<file_t>(shared_dir_from_this(), i));
        }
        if(save){
            save_entry_to_db(getkey().path, i);
            if((i.flags & META_KEY_ONLY_F) == 0){
                save_file_to_db(entrys[bname]->getkey().path, i, {});
            }
        }
    }
    return existnames;
}

int dir_t::pull_wlocked() {
    const filekey& key = getkey();
    filemeta meta = initfilemeta(key);
    meta.mode = this->mode;
    assert((flags & ENTRY_INITED_F) == 0);
    std::vector<filekey> fblocks;
    load_file_from_db(key.path, meta, fblocks);
    assert(fblocks.empty());
    if(meta.blksize == 0){
        int ret = HANDLE_EAGAIN(fm_getattr(key, meta));
        if(ret < 0) {
            return ret;
        }
        save_file_to_db(key.path, meta, {});
    }
    assert(meta.inline_data.empty());
    mode = (meta.mode & ~S_IFMT) | S_IFDIR;
    ctime = meta.ctime;
    mtime = meta.mtime;
    flags |= ENTRY_INITED_F;
    flags &= ~META_KEY_ONLY_F;
    return 0;
}

// Must wlock before call this function
int dir_t::pull_entrys_wlocked() {
    if(flags & DIR_PULLED_F) {
        return 0;
    }

    assert(entrys.empty());

    bool cached = false;
    std::vector<filemeta> flist;
    auto key = getkey();
    if(load_entry_from_db(key.path, flist) == 0){
        debuglog("Miss from localcache: %s\n", key.path.c_str());
        int ret = HANDLE_EAGAIN(fm_list(key, flist));
        if(ret < 0) {
            return ret;
        }
    }else{
        cached = true;
    }
    insert_meta_wlocked(flist, !cached);
    flags |= DIR_PULLED_F;
    return 0;
}

std::shared_ptr<entry_t> dir_t::find(std::shared_ptr<dir_t> current, std::string_view path) {
    if(!current){
        return nullptr;
    }
    std::string_view remaining = path;
    while(true){
        if(remaining.empty() || remaining == "."){
            return current;
        }
        if(remaining.front() == '/'){
            remaining.remove_prefix(1);
            continue;
        }

        std::string_view cname_view = childname(remaining);
        if(cname_view.empty()){
            return current;
        }
        std::string cname(cname_view);
        std::shared_ptr<entry_t> child = nullptr;
        {
            auto_rlock(current.get());
            if((current->flags & DIR_PULLED_F) == 0){
                __r.upgrade();
                if(current->pull_entrys_wlocked() < 0){
                    return nullptr;
                }
            }
            auto iter = current->entrys.find(cname);
            if(iter == current->entrys.end()){
                return nullptr;
            }
            child = iter->second;
        }


        std::string_view rest = subname(remaining);
        if(rest.empty() || rest == "."){
            return child;
        }

        current = std::dynamic_pointer_cast<dir_t>(child);
        if(!current){
            return nullptr;
        }
        remaining = rest;
    }
}

int dir_t::open() {
    atime = time(nullptr);
    auto_wlock(this);
    if((flags & ENTRY_INITED_F) == 0){
        int ret = pull_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    if((flags & DIR_PULLED_F) == 0){
        int ret = pull_entrys_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    for(auto [name, entry]: entrys){
        if(name == "." || name == ".." || entry == nullptr){
            continue;
        }
        auto_rlock(entry);
        if((entry->flags & ENTRY_INITED_F) || (entry->flags & ENTRY_PULLING_F)){
            continue;
        }
        __r.upgrade();
        entry->flags |= ENTRY_PULLING_F;
        dpool->submit_fire_and_forget([entry = std::weak_ptr<entry_t>(entry)]{
            pull(entry);
        });
    }
    opened++;
    return 0;
}

int dir_t::foreach_entrys(const std::function<int(const string&, const std::shared_ptr<entry_t>&)>& visitor) {
    auto_rlock(this);
    if((flags & DIR_PULLED_F) == 0){
        __r.upgrade();
        int ret = pull_entrys_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    for(const auto& [name, entry] : entrys){
        int cb_ret = visitor(name, entry);
        if(cb_ret != 0){
            return cb_ret;
        }
    }
    return 0;
}

int dir_t::getmeta(filemeta& meta) {
    atime = time(nullptr);
    auto_rlock(this);
    if((flags & ENTRY_INITED_F) == 0){
        __r.upgrade();
        int ret = pull_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    meta = initfilemeta(getkey());
    meta.mode = mode;
    meta.flags = flags;
    meta.size = length;
    meta.blksize = opt.block_len;
    meta.blocks = 1;
    meta.ctime = ctime;
    meta.mtime = mtime;
    return 0;
}

std::shared_ptr<entry_t> dir_t::insert_child_wlocked(const string& name, std::shared_ptr<entry_t> entry){
    assert(!entrys.contains(name));
    assert(entrys.size() < MAXFILE);
    entry->dump_to_db(getcwd(), name);
    return entrys[name] = entry;
}

int dir_t::remove_wlocked(bool skip_entry) {
    if((flags & DIR_PULLED_F) == 0){
        int ret = pull_entrys_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    if(!entrys.empty()){
        return -ENOTEMPTY;
    }
    auto key = getkey();
    if(!skip_entry) {
        int ret = HANDLE_EAGAIN(fm_delete(key));
        if(ret && errno != ENOENT){
            return ret;
        }
    }

    delete_entry_from_db(key.path);
    delete_file_from_db(key.path);
    ::rmdir(get_cache_path(getcwd()).c_str());
    flags |= ENTRY_DELETED_F;
    parent.reset();
    if(opened || (flags & ENTRY_REASEWAIT_F)|| (flags & ENTRY_PULLING_F)) {
        //delete this in release or pull
        return 0;
    }
    return 1;
}

size_t dir_t::children() {
    auto_rlock(this);
    return entrys.size();
}

std::shared_ptr<dir_t> dir_t::mkdir(const string& name, mode_t mode) {
    atime = time(nullptr);
    if(endwith(name, file_encode_suffix)){
        errno = EINVAL;
        return nullptr;
    }
    auto_wlock(this);
    if(parent.lock() == nullptr && (opt.flags & FM_RENAME_NOTSUPPRTED) && name == ".objs"){
        errno = EINVAL;
        return nullptr;
    }
    assert(flags & DIR_PULLED_F);
    if(children() >= MAXFILE){
        errno = ENOSPC;
        return nullptr;
    }
    struct filemeta meta = initfilemeta(filekey{name, 0});
    if(HANDLE_EAGAIN(fm_mkdir(getkey(), meta.key))){
        return nullptr;
    }
    flags |= DIR_DIRTY_F;
    ctime = mtime = meta.ctime = meta.mtime = time(nullptr);
    meta.flags = ENTRY_CREATE_F | DIR_PULLED_F;
    meta.mode = S_IFDIR | (mode & ~S_IFMT);
    return std::dynamic_pointer_cast<dir_t>(insert_child_wlocked(name, std::make_shared<dir_t>(shared_dir_from_this(), meta)));
}

std::shared_ptr<file_t> dir_t::create(const string& name, mode_t mode){
    atime = time(nullptr);
    if(endwith(name, symlink_encode_suffix)){
        errno = EINVAL;
        return nullptr;
    }
    auto_wlock(this);
    if(parent.lock() == nullptr && (opt.flags & FM_RENAME_NOTSUPPRTED) && name == ".objs"){
        errno = EINVAL;
        return nullptr;
    }
    assert(flags & DIR_PULLED_F);
    if(children() >= MAXFILE){
        errno = ENOSPC;
        return nullptr;
    }
    struct filemeta meta = initfilemeta(filekey{encodepath(name, file_encode_suffix), 0});
    if((opt.flags & FM_DONOT_REQUIRE_MKDIR) == 0) {
        if(HANDLE_EAGAIN(fm_mkdir(getkey(), meta.key))){
            return nullptr;
        }
    } else {
        fm_getattrat(getkey(), meta.key);
    }
    flags |= DIR_DIRTY_F;
    mtime = ctime = meta.ctime = meta.mtime = time(nullptr);
    meta.flags =  ENTRY_CHUNCED_F | ENTRY_CREATE_F | FILE_ENCODE_F | FILE_DIRTY_F ;
    meta.blksize = opt.block_len;
    meta.mode = S_IFREG | (mode & ~S_IFMT);
    return std::dynamic_pointer_cast<file_t>(insert_child_wlocked(name, std::make_shared<file_t>(shared_dir_from_this(), meta)));
}

std::shared_ptr<symlink_t> dir_t::symlink(const string& name, const string& target) {
    atime = time(nullptr);
    auto_wlock(this);
    if(parent.lock() == nullptr && (opt.flags & FM_RENAME_NOTSUPPRTED) && name == ".objs"){
        errno = EINVAL;
        return nullptr;
    }
    assert(flags & DIR_PULLED_F);
    if(children() >= MAXFILE){
        errno = ENOSPC;
        return nullptr;
    }

    struct filemeta meta = initfilemeta(filekey{encodepath(name, symlink_encode_suffix), 0});
    meta.flags = ENTRY_CHUNCED_F | ENTRY_CREATE_F;
    meta.mode = S_IFLNK | 0777;  // 符号链接权限
    meta.blksize = opt.block_len;
    meta.size = target.length();
    meta.inline_data = target;
    meta.ctime = meta.mtime = time(nullptr);
    if (HANDLE_EAGAIN(upload_meta(getkey(), meta, {}))){
        return nullptr;
    }

    flags |= DIR_DIRTY_F;
    mtime = ctime = time(nullptr);
    return std::dynamic_pointer_cast<symlink_t>(insert_child_wlocked(name, std::make_shared<symlink_t>(shared_dir_from_this(), meta)));
}

int dir_t::unlink(const string& name) {
    atime = time(nullptr);
    auto_wlock(this);
    assert(flags & DIR_PULLED_F);
    if(!entrys.contains(name)){
        return -ENOENT;
    }
    auto entry = entrys[name];
    auto_wlocker __w2(entry);

    if(std::dynamic_pointer_cast<dir_t>(entry)){
        return -EISDIR;
    }
    int ret = entry->remove_wlocked(false);
    if(ret < 0) {
        return ret;
    }
    __w2.unlock();
    entrys.erase(name);
    mtime = time(nullptr);
    flags |= DIR_DIRTY_F;
    return 0;
}

int dir_t::rmdir(const string& name) {
    atime = time(nullptr);
    auto_wlock(this);
    assert(flags & DIR_PULLED_F);
    if(!entrys.contains(name)){
        return -ENOENT;
    }
    auto entry = entrys[name];
    auto_wlocker __w2(entry);
    if(std::dynamic_pointer_cast<file_t>(entry)){
        return -ENOTDIR;
    }
    int ret = entry->remove_wlocked(false);
    if (ret < 0) {
        return ret;
    }
    __w2.unlock();
    entrys.erase(name);
    mtime = time(nullptr);
    flags |= DIR_DIRTY_F;
    return 0;
}

int dir_t::moveto(std::shared_ptr<dir_t> newparent, const string& oldname, const string& newname, unsigned int mv_flags) {
    atime = time(nullptr);
#ifdef RENAME_EXCHANGE
    if(mv_flags & RENAME_EXCHANGE) {
        return -EINVAL;  // Not supported in this implementation
    }
#endif
    auto_wlocker __1(this);
    assert(flags & DIR_PULLED_F);
    if(!entrys.contains(oldname)){
        return -ENOENT;
    }

    if(this != newparent.get() && newparent->trywlock()) {
        return -EDEADLK;
    }
    defer([this, newparent] {
        if(this != newparent.get()) newparent->unwlock();
    });
    if(newparent->children() >= MAXFILE){
        return -ENOSPC;
    }
    auto entry = entrys[oldname];
    auto_wlocker __3(entry);
    std::shared_ptr<entry_t> existing;
    defer([&existing] {
        if(existing) existing->unwlock();
    });

    if(newparent->entrys.contains(newname)) {
#ifdef RENAME_NOREPLACE
        if(mv_flags & RENAME_NOREPLACE){
            return -EEXIST;
        }
#endif
        existing = newparent->entrys[newname];
        existing->wlock();
        // dir -> empty dir, allow replace
        // dir -> non-empty dir, return ENOTEMPTY
        // dir -> no-dir, return ENOTDIR
        // no-dir -> dir, return EISDIR
        // no-dir -> no-dir, allow replace
        if(S_ISDIR(entry->getmode())) {
            if(!S_ISDIR(existing->getmode())) {
                return -ENOTDIR;
            }
            auto dir_existing = std::dynamic_pointer_cast<dir_t>(existing);
            dir_existing->pull_entrys_wlocked();
            if(dir_existing->children()){
                return -ENOTEMPTY;
            }
        } else if(S_ISDIR(existing->getmode())) {
            return -EISDIR;
        }
    }

    std::shared_ptr<void> new_private_key;
    int ret = 0;
    if(opt.flags & FM_RENAME_NOTSUPPRTED) {
        //copy and delete
        if(S_ISREG(entry->getmode())) {
            filekey newdir = {encodepath(newname, file_encode_suffix), 0};
            fm_getattrat(newparent->getkey(), newdir);
            if((opt.flags & FM_DONOT_REQUIRE_MKDIR) == 0 && newdir.private_key == nullptr) {
                HANDLE_EAGAIN(fm_mkdir(newparent->getkey(), newdir));
            }
            if(newdir.private_key == nullptr) {
                return -EINVAL;
            }
            new_private_key = newdir.private_key;
            auto file = std::dynamic_pointer_cast<file_t>(entry);
            if(fm_private_key_tostring(file->private_key)[0] == '\0') {
                // 这个文件还没来得及上传到远程
                assert(file->flags & FILE_DIRTY_F);
                assert(file->flags & ENTRY_CHUNCED_F);
                goto skip_remote;
            }
            filekey newfile = filekey{entry->flags & ENTRY_CHUNCED_F ?
                pathjoin(encodepath(newname, file_encode_suffix), METANAME) : newname, 0};
            ret = HANDLE_EAGAIN(fm_copy(file->getmetakey(), newparent->getkey(), newfile));
            if(ret == 0){
                HANDLE_EAGAIN(fm_delete(file->getmetakey()));
                file->private_key = newfile.private_key;
                if((file->flags & ENTRY_CHUNCED_F) == 0) {
                    new_private_key = newfile.private_key;
                }
            }
        } else if(S_ISDIR(entry->getmode())){
            auto dir = std::dynamic_pointer_cast<dir_t>(entry);
            ret = dir->pull_entrys_wlocked();
            if(ret < 0) {
                return ret;
            }
            if(!dir->entrys.empty()) {
                return -ENOTEMPTY;
            }
            filekey newfile{newname, 0};
            ret = HANDLE_EAGAIN(fm_copy(dir->getkey(), newparent->getkey(), newfile));
            if(ret == 0){
                ret = HANDLE_EAGAIN(fm_delete(dir->getkey()));
                new_private_key = newfile.private_key;
            }
        } else if(S_ISLNK(entry->getmode())) {
            auto symlink = std::dynamic_pointer_cast<symlink_t>(entry);
            assert(symlink->flags & ENTRY_CHUNCED_F);
            filekey newfile{encodepath(newname, symlink_encode_suffix), 0};
            ret = HANDLE_EAGAIN(fm_copy(symlink->getkey(), newparent->getkey(), newfile));
            if(ret == 0){
                ret = HANDLE_EAGAIN(fm_delete(symlink->getkey()));
                new_private_key = newfile.private_key;
            }
        }
    } else {
        filekey newfile{(entry->flags & ENTRY_CHUNCED_F)?encodepath(newname, file_encode_suffix):newname, 0};
        ret = HANDLE_EAGAIN(fm_rename(this->getkey(), entry->getkey(), newparent->getkey(), newfile));
        new_private_key = newfile.private_key;
    }
    if(ret){
        return ret;
    }

skip_remote:
    // 如果是文件，需要移动持久化缓存文件
    if(S_ISREG(entry->getmode()) && !opt.no_cache) {
        string new_remote_path = pathjoin(newparent->getcwd(), newname);
        string old_cache_path = pathjoin(opt.cache_dir, "cache", this->getcwd(), oldname);
        string new_cache_path = pathjoin(opt.cache_dir, "cache", new_remote_path);

        // 确保新缓存目录存在
        string new_cache_dir = dirname(new_cache_path);
        if(create_dirs_recursive(new_cache_dir) != 0) {
            errorlog("failed to create cache dir %s: %s\n", new_cache_dir.c_str(), strerror(errno));
        }

        // 移动缓存文件
        if(rename(old_cache_path.c_str(), new_cache_path.c_str()) == 0) {
            setxattr(new_cache_path.c_str(), FM_REMOTE_PATH_ATTR, new_remote_path.c_str(), new_remote_path.size(), 0);
        }else if(errno != ENOENT) {
            errorlog("failed to rename cache file %s to %s: %s\n",
                    old_cache_path.c_str(), new_cache_path.c_str(), strerror(errno));
        }
    }

    flags |= DIR_DIRTY_F;
    mtime = time(nullptr);
    //这个地方不能直接unlink,因为如果之前文件也存在，copy会覆盖它，调用unlink可能删除的是新文件(比如以文件名作为key的s3后端)
    if(existing) {
        existing->remove_wlocked(true);
        newparent->entrys.erase(newname);
    }
    entry->fk.store(std::make_shared<filekey>(entry->fk.load()->path, new_private_key));
    //再此之前不能修改fk.path, 因为insert需要从files表读取原始文件信息
    //但是又需要保存新文件的private_key，不想再加一个参数了，就直接把private_key 先改了
    newparent->insert_child_wlocked(newname, entry);
    newparent->mtime = newparent->atime = time(nullptr);
    newparent->flags |= DIR_DIRTY_F;

    std::string oldpath = entry->getkey().path;
    delete_entry_from_db(oldpath);
    if(S_ISDIR(entry->getmode())) {
        delete_entry_prefix_from_db(oldpath);
    } else {
        delete_file_from_db(oldpath);
    }
    entrys.erase(oldname);
    entry->fk.store(std::make_shared<filekey>(newname, new_private_key));
    entry->parent = newparent;
    return 0;
}

int dir_t::sync(int dataonly) {
    atime = time(nullptr);
    auto_rlock(this);
    assert(flags & ENTRY_INITED_F);
    if(flags & ENTRY_DELETED_F){
        return 0;
    }
    if(dataonly) {
        __r.upgrade();
        std::vector<filemeta> flist;
        int ret = HANDLE_EAGAIN(fm_list(getkey(), flist));
        if(ret < 0) {
            return ret;
        }
        auto names = insert_meta_wlocked(flist, true);
        //drop_cache for noexist entrys
        for (auto it = entrys.begin(); it != entrys.end(); ) {
            if(it->first == "." || it->first == ".." || names.contains(it->first)){
                ++it;
                continue;
            }
            if(it->second->drop_cache(false, time(nullptr)) != 0){
                ++it;
                continue;
            }
            delete_entry_from_db(it->second->getkey().path);
            it = entrys.erase(it);
        }
        return 0;
    }
    if((flags & DIR_DIRTY_F) == 0){
        return 0;
    }
    time_t tv[2];
    tv[0] = ctime;
    tv[1] = mtime;
    HANDLE_EAGAIN(fm_utime(getkey(), tv));  //ignore error
    flags &= ~DIR_DIRTY_F;
    return 0;
}

int dir_t::utime(const struct timespec tv[2]) {
    atime = time(nullptr);
    //no atime in filemeta
    if(tv[1].tv_nsec == UTIME_OMIT){
        return 0;
    }
    auto_wlock(this);
    if(flags & ENTRY_DELETED_F){
        return 0;
    }
    if((flags & ENTRY_INITED_F) == 0){
        int ret = pull_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    ctime = time(nullptr);
    time_t ttv[2];
    ttv[0] = ctime;
    if(tv[1].tv_nsec == UTIME_NOW){
        ttv[1] = time(nullptr);
    } else {
        ttv[1] = tv[1].tv_sec;
    }
    int ret = 0;
    ret = HANDLE_EAGAIN(fm_utime(getkey(), ttv));
    if(ret){
        return ret;
    }
    mtime = ttv[1];
    return 0;
}


void dir_t::dump_to_db(const std::string& path, const std::string& name) {
    auto_rlock(this);
    if(flags & ENTRY_DELETED_F){
        return;
    }
    if((flags & ENTRY_INITED_F) == 0){
        return;
    }
    filemeta meta;
    if(getmeta(meta) < 0) {
        return;
    }
    meta.key.path = name;
    save_entry_to_db(path, meta);
    save_file_to_db(pathjoin(path, name), meta, {});
    if((flags & DIR_PULLED_F) == 0){
        return;
    }
    for(auto i: entrys){
        if(i.first == "." || i.first == ".."){
            continue;
        }
        i.second->dump_to_db(pathjoin(path, name), i.first);
    }
}

int dir_t::drop_cache_wlocked(bool mem_only, time_t before){
    if(opened){
        return -EBUSY;
    }
    if((flags & ENTRY_REASEWAIT_F) || (flags & ENTRY_PULLING_F)){
        return -EAGAIN;
    }
    if((flags & ENTRY_INITED_F) == 0){
        return 0;
    }
    int ret = 0;
    for(auto i : entrys){
        ret |= i.second->drop_cache(mem_only, before);
    }
    if(ret != 0) {
        return ret;
    }
    if(before && atime > before) {
        return -EAGAIN;
    }
    entrys.clear();
    flags &= ~DIR_PULLED_F;
    if(opt.no_cache || mem_only) {
        return 0;
    }
    return delete_entry_prefix_from_db(parent.lock() ? getkey().path: "");
}

int dir_t::collect_storage_classes(TrdPool* pool, std::vector<std::future<std::pair<int, storage_class_info>>>& futures) {
    assert(pool != nullptr);
    auto_rlock(this);
    if(flags & ENTRY_DELETED_F) {
        return -ENOENT;
    }
    if((flags & ENTRY_INITED_F) == 0) {
        __r.upgrade();
        int ret = pull_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    if((flags & DIR_PULLED_F) == 0) {
        __r.upgrade();
        int ret = pull_entrys_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    bool failed = false;
    for(auto& entry : entrys) {
        int ret = entry.second->collect_storage_classes(pool, futures);
        if(ret < 0) {
            failed = true;
        }
    }
    return failed ? -EIO : 0;
}

int dir_t::set_storage_class(enum storage_class storage, TrdPool* pool, std::vector<std::future<int>>& futures) {
    auto_rlock(this);
    if(flags & ENTRY_DELETED_F){
        return -ENOENT;
    }
    if((flags & ENTRY_INITED_F) == 0){
        __r.upgrade();
        int ret = pull_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    if((flags & DIR_PULLED_F) == 0){
        __r.upgrade();
        int ret = pull_entrys_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    int ret = 0;
    for(auto i : entrys){
        ret |= i.second->set_storage_class(storage, pool, futures);
    }
    if(ret == 0) {
        return 0;
    }
    return -EIO;
}
