#include "common.h"
#include "fmdisk.h"
#include "dir.h"
#include "file.h"
#include "sqlite.h"
#include "utils.h"
#include "trdpool.h"

#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/xattr.h>

static string childname(const string& path) {
    size_t pos = path.find_first_of("/");
    if(pos == string::npos) {
        return path;
    }
    if(pos == 0 ) {
        string path_truncate = path.substr(1, path.length());
        return childname(path_truncate);
    }
    return path.substr(0, pos);
}

static string subname(const string& path) {
    size_t pos = path.find_first_of("/");
    if(pos == string::npos || pos == path.length()-1) {
        return ".";
    }
    if(pos == 0 ) {
        string path_truncate = path.substr(1, path.length());
        return subname(path_truncate);
    }
    return path.substr(pos+1, path.length());
}

filekey basename(const filekey& file) {
    return filekey{basename(file.path), file.private_key};
}

filekey decodepath(const filekey& file) {
    return filekey{decodepath(file.path), file.private_key};
}



dir_t* cache_root() {
    struct filemeta meta = initfilemeta(filekey{"/", 0});
    if(HANDLE_EAGAIN(fm_getattr(filekey{"/", 0}, meta))){
        throw "getattr of root failed";
    }
    return new dir_t(nullptr, meta);
}


dir_t::dir_t(dir_t* parent, const filemeta& meta): entry_t(parent, meta) {
    if(flags & ENTRY_CREATE_F) {
        entrys.emplace(".", this);
        entrys.emplace("..",  parent);
    }
}

dir_t::~dir_t(){
    locker::wlock();
    for(const auto& i: entrys){
        if(i.first != "." && i.first != ".."){
            delete i.second;
        }
    }
}

std::set<std::string> dir_t::insert_meta_wlocked(const std::vector<filemeta>& flist, bool save) {
    std::set<std::string> existnames;
    for(auto i: flist){
        string bname = basename(i.key.path);
        if(parent == nullptr && (opt.flags & FM_RENAME_NOTSUPPRTED) && bname == ".objs"){
            continue;
        }
        bool is_dir = S_ISDIR(i.mode);
        if(endwith(bname, ".def") && is_dir){
            bname = decodepath(bname);
            i.flags |= ENTRY_CHUNCED_F | META_KEY_ONLY_F;
            is_dir = false;
        }
        existnames.insert(bname);
        if(entrys.contains(bname)){
            continue;
        }
        if(is_dir){
            entrys.emplace(bname, new dir_t(this, i));
        }else{
            entrys.emplace(bname, new file_t(this, i));
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

// Must wlock before call this function
int dir_t::pull_entrys_wlocked() {
    if(flags & DIR_PULLED_F) {
        return 0;
    }

    assert(entrys.empty());
    entrys.emplace(".", this);
    entrys.emplace("..", parent);

    bool cached = false;
    std::vector<filemeta> flist;
    if(load_entry_from_db(getkey().path, flist) == 0){
        printf("Miss from localcache\n");
        int ret = HANDLE_EAGAIN(fm_list(getkey(), flist));
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

entry_t* dir_t::find(std::string path) {
    if(path == "." || path == "/"){
        return this;
    }
    if(path[0] == '/'){
        path = path.substr(1);
    }
    auto_rlock(this);
    if((flags & DIR_PULLED_F) == 0){
        __r.upgrade();
        pull_entrys_wlocked();
    }
    string cname = childname(path);
    if(!entrys.contains(cname)) {
        return nullptr;
    }
    if(cname == path) {
        return entrys[cname];
    }
    if(dynamic_cast<dir_t*>(entrys[cname])){
        return dynamic_cast<dir_t*>(entrys[cname])->find(subname(path));
    }
    return nullptr;
}

int dir_t::open() {
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
        dpool->submit_fire_and_forget([entry]{ pull(entry); });
    }
    opened++;
    return 0;
}

const std::map<string, entry_t*>& dir_t::get_entrys(){
    auto_rlock(this);
    if((flags & DIR_PULLED_F) == 0){
        __r.upgrade();
        pull_entrys_wlocked();
    }
    //at least '.' and '..'
    assert(entrys.size() >= 2);
    return entrys;
}


int dir_t::getmeta(filemeta& meta) {
    auto_rlock(this);
    if((flags & ENTRY_INITED_F) == 0){
        __r.upgrade();
        int ret = pull_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    meta = initfilemeta(getkey());
    meta.mode = S_IFDIR | (mode & 0777);
    meta.flags = flags;
    meta.size = length;
    meta.inline_data = nullptr;
    meta.blksize = opt.block_len;
    meta.blocks = 1;
    meta.ctime = ctime;
    meta.mtime = mtime;
    return 0;
}

entry_t* dir_t::insert_child_wlocked(const string& name, entry_t* entry){
    assert(!entrys.contains(name));
    assert(entrys.size() < MAXFILE);
    entry->dump_to_db(getcwd(), name);
    return entrys[name] = entry;
}

int dir_t::remove_wlocked() {
    if((flags & DIR_PULLED_F) == 0){
        int ret = pull_entrys_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    if(entrys.size() != 2){
        return -ENOTEMPTY;
    }
    auto key = getkey();
    int ret = HANDLE_EAGAIN(fm_delete(key));
    if(ret && errno != ENOENT){
        return ret;
    }

    delete_entry_from_db(key.path);
    delete_file_from_db(key.path);
    ::rmdir(get_cache_path(getcwd()).c_str());
    flags |= ENTRY_DELETED_F;
    parent = nullptr;
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

dir_t* dir_t::mkdir(const string& name) {
    if(endwith(name, ".def")){
        errno = EINVAL;
        return nullptr;
    }
    auto_wlock(this);
    if(parent == nullptr && (opt.flags & FM_RENAME_NOTSUPPRTED) && name == ".objs"){
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
    meta.mode = S_IFDIR | 0755;
    return dynamic_cast<dir_t*>(insert_child_wlocked(name, new dir_t(this, meta)));
}

file_t* dir_t::create(const string& name){
    auto_wlock(this);
    if(parent == nullptr && (opt.flags & FM_RENAME_NOTSUPPRTED) && name == ".objs"){
        errno = EINVAL;
        return nullptr;
    }
    assert(flags & DIR_PULLED_F);
    if(children() >= MAXFILE){
        errno = ENOSPC;
        return nullptr;
    }
    struct filemeta meta = initfilemeta(filekey{encodepath(name), 0});
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
    meta.mode = S_IFREG | 0644;
    return dynamic_cast<file_t*>(insert_child_wlocked(name, new file_t(this, meta)));
}

int dir_t::unlink(const string& name) {
    auto_wlock(this);
    assert(flags & DIR_PULLED_F);
    if(!entrys.contains(name)){
        return -ENOENT;
    }
    entry_t* entry = entrys[name];
    auto_wlocker __w2(entry);

    if(dynamic_cast<dir_t*>(entry)){
        return -EISDIR;
    }
    int ret = entry->remove_wlocked();
    if(ret < 0) {
        return ret;
    }
    entrys.erase(name);
    if(ret > 0) {
        __w2.unlock();
        delete entry;
    }
    mtime = time(nullptr);
    flags |= DIR_DIRTY_F;
    return 0;
}

int dir_t::rmdir(const string& name) {
    auto_wlock(this);
    assert(flags & DIR_PULLED_F);
    if(!entrys.contains(name)){
        return -ENOENT;
    }
    entry_t* entry = entrys[name];
    auto_wlocker __w2(entry);
    if(dynamic_cast<file_t*>(entry)){
        return -ENOTDIR;
    }
    int ret = entry->remove_wlocked();
    if (ret < 0) {
        return ret;
    }
    entrys.erase(name);
    if(ret > 0) {
        __w2.unlock();
        delete entry;
    }
    mtime = time(nullptr);
    flags |= DIR_DIRTY_F;
    return 0;
}

int dir_t::moveto(dir_t* newparent, const string& oldname, const string& newname, unsigned int mv_flags) {
    auto_wlocker __1(this);
    assert(flags & DIR_PULLED_F);
    if(!entrys.contains(oldname)){
        return -ENOENT;
    }

    auto_wlocker __2(newparent);
    if(newparent->children() >= MAXFILE){
        return -ENOSPC;
    }

 #ifdef RENAME_NOREPLACE
    if(newparent->entrys.contains(newname) && (mv_flags & RENAME_NOREPLACE)){
        return -EEXIST;
    }
#endif

#ifdef RENAME_EXCHANGE
    if(mv_flags & RENAME_EXCHANGE) {
        return -EINVAL;  // Not supported in this implementation
    }
#endif

    entry_t* entry = entrys[oldname];
    auto_wlocker __3(entry);
    std::shared_ptr<void> new_private_key;
    int ret = 0;
    if(opt.flags & FM_RENAME_NOTSUPPRTED) {
        //copy and delete
        if(entry->isDir()){
            dir_t* dir = dynamic_cast<dir_t*>(entry);
            ret = dir->pull_entrys_wlocked();
            if(ret < 0) {
                return ret;
            }
            if(dir->entrys.size() > 2) {
                return -ENOTEMPTY;
            }
            filekey newfile{newname, 0};
            ret = HANDLE_EAGAIN(fm_copy(this->getkey(), dir->getkey(), newparent->getkey(), newfile));
            if(ret == 0){
                ret = HANDLE_EAGAIN(fm_delete(dir->getkey()));
                new_private_key = newfile.private_key;
            }
        }else {
            file_t* file = dynamic_cast<file_t*>(entry);
            if(fm_private_key_tostring(file->private_key)[0] == '\0') {
                // 这个文件还没来得及上传到远程
                assert(file->flags & FILE_DIRTY_F);
                assert(file->flags & ENTRY_CHUNCED_F);
                new_private_key = fk.load()->private_key;
                goto skip_remote;
            }
            filekey newfile = filekey{entry->flags & ENTRY_CHUNCED_F ?
                pathjoin(encodepath(newname), METANAME) : newname, 0};
            ret = HANDLE_EAGAIN(fm_copy(this->getkey(), file->getmetakey(), newparent->getkey(), newfile));
            if(ret == 0){
                ret = HANDLE_EAGAIN(fm_delete(file->getmetakey()));
                file->private_key = newfile.private_key;
                if((file->flags & ENTRY_CHUNCED_F) == 0) {
                    new_private_key = newfile.private_key;
                } else {
                    newfile = filekey{encodepath(newname), 0};
                    fm_getattrat(newparent->getkey(), newfile);
                    new_private_key = newfile.private_key;
                }
            }
        }
    } else {
        filekey newfile{(entry->flags & ENTRY_CHUNCED_F)?encodepath(newname):newname, 0};
        ret = HANDLE_EAGAIN(fm_rename(this->getkey(), entry->getkey(), newparent->getkey(), newfile));
        new_private_key = newfile.private_key;
    }
    if(ret){
        return ret;
    }

skip_remote:
    // 如果是文件，需要移动持久化缓存文件
    if(!entry->isDir() && !opt.no_cache) {
        string new_remote_path = pathjoin(newparent->getcwd(), newname);
        string old_cache_path = pathjoin(opt.cache_dir, "cache", this->getcwd(), oldname);
        string new_cache_path = pathjoin(opt.cache_dir, "cache", new_remote_path);

        // 确保新缓存目录存在
        string new_cache_dir = dirname(new_cache_path);
        if(create_dirs_recursive(new_cache_dir) != 0) {
            fprintf(stderr, "failed to create cache dir %s: %s\n", new_cache_dir.c_str(), strerror(errno));
        }

        // 移动缓存文件
        if(rename(old_cache_path.c_str(), new_cache_path.c_str()) == 0) {
            setxattr(new_cache_path.c_str(), FM_REMOTE_PATH_ATTR, new_remote_path.c_str(), new_remote_path.size(), 0);
        }else if(errno != ENOENT) {
            fprintf(stderr, "failed to rename cache file %s to %s: %s\n",
                    old_cache_path.c_str(), new_cache_path.c_str(), strerror(errno));
        }
    }

    flags |= DIR_DIRTY_F;
    mtime = time(nullptr);
    newparent->unlink(newname);
    entry->fk.store(std::make_shared<filekey>(entry->fk.load()->path, new_private_key));
    //再此之前不能修改fk.path, 因为insert需要从files表读取原始文件信息
    //但是又需要保存新文件的private_key，不想再加一个参数了，就直接把private_key 先改了
    newparent->insert_child_wlocked(newname, entry);
    newparent->mtime = time(nullptr);
    newparent->flags |= DIR_DIRTY_F;

    std::string oldpath = entry->getkey().path;
    delete_entry_from_db(oldpath);
    if(entry->isDir()) {
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
            if(it->second->drop_cache() != 0){
                ++it;
                continue;
            }
            delete_entry_from_db(it->second->getkey().path);
            delete it->second;
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
        i.second->dump_to_db(path, i.first);
    }
}

int dir_t::drop_cache_wlocked(){
    if(opened){
        return -EBUSY;
    }
    if((flags & ENTRY_REASEWAIT_F) || (flags & ENTRY_PULLING_F) || (flags & DIR_DIRTY_F)){
        return -EAGAIN;
    }
    if((flags & ENTRY_INITED_F) == 0){
        return 0;
    }
    int ret = 0;
    for(auto i : entrys){
        if(i.first == "." || i.first == ".."){
            continue;
        }
        ret |= i.second->drop_cache();
    }
    if(ret != 0) {
        return ret;
    }
    for(auto i: entrys) {
        if(i.first == "." || i.first == ".."){
            continue;
        }
        delete i.second;
    }
    entrys.clear();
    flags &= ~DIR_PULLED_F;
    if(opt.no_cache) {
        return 0;
    }
    return delete_entry_prefix_from_db(parent ? getkey().path: "");
}
