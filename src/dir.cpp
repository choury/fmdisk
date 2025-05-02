#include "common.h"
#include "fmdisk.h"
#include "dir.h"
#include "file.h"
#include "sqlite.h"
#include "utils.h"

#include <string.h>
#include <assert.h>

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
    this->wlock();
    for(auto i: entrys){
        if(i.first != "." && i.first != ".."){
            delete i.second;
        }
    }
}

// Must wlock before call this function
void dir_t::pull_entrys_wlocked() {
    if(flags & DIR_PULLED_F) {
        return;
    }
    bool cached = false;
    std::vector<filemeta> flist;
    if(load_entry_from_db(getkey().path, flist) == 0){
        printf("Miss from localcache\n");
        if(HANDLE_EAGAIN(fm_list(getkey(), flist))){
            throw "fm_list IO Error";
        }
    }else{
        cached = true;
    }
    assert(entrys.size() == 0);
    entrys.emplace(".", this);
    entrys.emplace("..", parent);
    for(auto i: flist){
        string bname = basename(i.key.path);
        if(parent == nullptr && (opt.flags & FM_DONOT_REQUIRE_MKDIR) && bname == ".objs"){
            continue;
        }
        bool is_dir = S_ISDIR(i.mode);
        if(endwith(bname, ".def") && is_dir){
            bname = decodepath(bname);
            i.flags |= ENTRY_CHUNCED_F | META_KEY_ONLY_F;
            is_dir = false;
        }
        if(entrys.count(bname) == 0){
            if(is_dir){
                entrys.emplace(bname, new dir_t(this, i));
            }else{
                entrys.emplace(bname, new file_t(this, i));
            }
        }
        if(!cached){
            save_entry_to_db(getkey(), i);
            if((i.flags & META_KEY_ONLY_F) == 0){
                save_file_to_db(entrys[bname]->getkey().path, i, std::vector<filekey>{});
            }
        }
    }
    flags |= DIR_PULLED_F;
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
    if(!entrys.count(cname)) {
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


filemeta dir_t::getmeta() {
    auto_rlock(this);
    if((flags & ENTRY_INITED_F) == 0){
        __r.upgrade();
        pull_wlocked();
    }
    filemeta meta = initfilemeta(getkey());
    meta.mode = S_IFDIR | (mode & 0777);
    meta.flags = flags;
    meta.size = length;
    meta.inline_data = 0;
    meta.blksize = opt.block_len;
    meta.blocks = 1;
    meta.ctime = ctime;
    meta.mtime = mtime;
    return meta;
}

entry_t* dir_t::insert_child_wlocked(string name, entry_t* entry){
    assert(entrys.count(name) == 0);
    assert(entrys.size() < MAXFILE);
    entry->dump_to_disk_cache();
    return entrys[name] = entry;
}

void dir_t::erase_child_wlocked(string name, entry_t* child) {
    assert(entrys.count(name));
    auto path = child->getkey().path;
    delete_entry_from_db(path);
    delete_entry_prefix_from_db(path);
    entrys.erase(name);
}


size_t dir_t::children() {
    auto_rlock(this);
    return entrys.size();
}

dir_t* dir_t::mkdir(string name) {
    if(endwith(name, ".def")){
        errno = EINVAL;
        return nullptr;
    }
    auto_wlock(this);
    if(parent == nullptr && (opt.flags & FM_DONOT_REQUIRE_MKDIR) && name == ".objs"){
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
    ctime = mtime = meta.ctime = meta.mtime = time(NULL);
    meta.flags = ENTRY_CREATE_F | DIR_PULLED_F;
    meta.mode = S_IFDIR | 0755;
    return dynamic_cast<dir_t*>(insert_child_wlocked(name, new dir_t(this, meta)));
}

file_t* dir_t::create(string name){
    auto_wlock(this);
    if(parent == nullptr && (opt.flags & FM_DONOT_REQUIRE_MKDIR) && name == ".objs"){
        errno = EINVAL;
        return nullptr;
    }
    assert(flags & DIR_PULLED_F);
    if(children() >= MAXFILE){
        errno = ENOSPC;
        return nullptr;
    }
    struct filemeta meta = initfilemeta(filekey{encodepath(name), 0});
    if((opt.flags & FM_DONOT_REQUIRE_MKDIR) == 0 && HANDLE_EAGAIN(fm_mkdir(getkey(), meta.key))){
        return nullptr;
    }
    flags |= DIR_DIRTY_F;
    mtime = ctime = meta.ctime = meta.mtime = time(NULL);
    meta.flags =  ENTRY_CHUNCED_F | ENTRY_CREATE_F | FILE_ENCODE_F | FILE_DIRTY_F ;
    meta.blksize = opt.block_len;
    meta.mode = S_IFREG | 0644;
    return dynamic_cast<file_t*>(insert_child_wlocked(name, new file_t(this, meta)));
}

int dir_t::unlink(const string& name) {
    auto_wlock(this);
    assert(flags & DIR_PULLED_F);
    if(entrys.count(name) == 0){
        return -ENOENT;
    }
    entry_t* entry = entrys[name];
    entry->wlock();

    if(dynamic_cast<dir_t*>(entry)){
        entry->unwlock();
        return -EISDIR;
    }
    erase_child_wlocked(name, entry);
    dynamic_cast<file_t*>(entry)->remove_and_release_wlock();
    mtime = time(NULL);
    flags |= DIR_DIRTY_F;
    return 0;
}

int dir_t::rmdir(const string& name) {
    auto_wlock(this);
    assert(flags & DIR_PULLED_F);
    if(entrys.count(name) == 0){
        return -ENOENT;
    }
    entry_t* entry = entrys[name];
    entry->wlock();
    if(dynamic_cast<file_t*>(entry)){
        entry->unwlock();
        return -ENOTDIR;
    }
    dir_t* child = dynamic_cast<dir_t*>(entry);
    if((child->flags & DIR_PULLED_F) == 0){
        child->pull_entrys_wlocked();
    }
    if(child->entrys.size() != 2){
        entry->unwlock();
        return -ENOTEMPTY;
    }
    int ret = HANDLE_EAGAIN(fm_delete(entry->getkey()));
    if(ret && errno != ENOENT){
        entry->unwlock();
        return ret;
    }
    erase_child_wlocked(name, entry);
    mtime = time(NULL);
    flags |= DIR_DIRTY_F;

    //entry->parent = nullptr;
    entry->flags |= ENTRY_DELETED_F;
    if(entry->opened ||
      (entry->flags & ENTRY_REASEWAIT_F)||
      (entry->flags & ENTRY_PULLING_F))
    {
        entry->unwlock();
        //delete this in release or pull
        return 0;
    }
    delete child;
    return 0;
}

int dir_t::moveto(dir_t* newparent, string oldname, string newname) {
    auto_wlocker __1(this);
    assert(flags & DIR_PULLED_F);
    if(entrys.count(oldname) == 0){
        return -ENOENT;
    }

    auto_wlocker __2(newparent);
    if(newparent->children() >= MAXFILE){
        return -ENOSPC;
    }

    entry_t* entry = entrys[oldname];
    auto_wlocker __3(entry);
    filekey newfile{(entry->flags & ENTRY_CHUNCED_F)?encodepath(newname):newname, 0};
    int ret = 0;
    if(opt.flags & FM_RENAME_NOTSUPPRTED) {
        //copy and delete
        if(entry->isDir()){
            dir_t* dir = dynamic_cast<dir_t*>(entry);
            dir->pull_entrys_wlocked();
            if(dir->entrys.size() > 2) {
                return -ENOTEMPTY;
            }
            ret = HANDLE_EAGAIN(fm_copy(this->getkey(), dir->getkey(), newparent->getkey(), newfile));
            if(ret == 0){
                ret = HANDLE_EAGAIN(fm_delete(dir->getkey()));
            }
        }else {
            file_t* file = dynamic_cast<file_t*>(entry);
            filekey newmeta = filekey{entry->flags & ENTRY_CHUNCED_F
                ? pathjoin(newfile.path, METANAME)
                : newfile.path, 0};
            ret = HANDLE_EAGAIN(fm_copy(this->getkey(), file->getmetakey(), newparent->getkey(), newmeta));
            if(ret == 0){
                ret = HANDLE_EAGAIN(fm_delete(file->getmetakey()));
                file->private_key = newmeta.private_key;
            }
        }
    } else {
        ret = HANDLE_EAGAIN(fm_rename(this->getkey(), entry->getkey(), newparent->getkey(), newfile));
    }
    if(ret){
        return ret;
    }
    flags |= DIR_DIRTY_F;
    mtime = time(NULL);
    erase_child_wlocked(oldname, entry);
    entry->parent = newparent;
    entry->fk = filekey{newname, newfile.private_key};
    newparent->unlink(newname);
    newparent->insert_child_wlocked(newname, entry);
    newparent->mtime = time(NULL);
    newparent->flags |= DIR_DIRTY_F;
    return 0;
}

int dir_t::sync(int dataonly) {
    auto_rlock(this);
    assert(flags & ENTRY_INITED_F);
    if(flags & ENTRY_DELETED_F){
        return 0;
    }
    if((flags & DIR_DIRTY_F) == 0 || dataonly){
        return 0;
    }
    struct timespec tv[2];
    tv[0].tv_sec = mtime;
    tv[1].tv_sec = ctime;
    HANDLE_EAGAIN(fm_utime(getkey(), tv));  //ignore error
    flags &= ~DIR_DIRTY_F;
    return 0;
}

int dir_t::utime(const struct timespec tv[2]) {
    auto_rlock(this);
    if(flags & ENTRY_DELETED_F){
        return 0;
    }
    if((flags & ENTRY_INITED_F) == 0){
        __r.upgrade();
        pull_wlocked();
    }
    int ret = 0;
    ret = HANDLE_EAGAIN(fm_utime(getkey(), tv));
    if(ret){
        return ret;
    }
    mtime = tv[0].tv_sec;
    ctime = tv[1].tv_sec;
    return 0;
}


void dir_t::dump_to_disk_cache(){
    auto_rlock(this);
    if(flags & ENTRY_DELETED_F){
        return;
    }
    if((flags & ENTRY_INITED_F) == 0){
        return;
    }
    filemeta meta = getmeta();
    save_entry_to_db(parent->getkey(), meta);
    save_file_to_db(meta.key.path, meta, std::vector<filekey>{});
    if((flags & DIR_PULLED_F) == 0){
        return;
    }
    for(auto i: entrys){
        if(i.first == "." || i.first == ".."){
            continue;
        }
        i.second->dump_to_disk_cache();
    }
}

int dir_t::drop_mem_cache(){
    int ret = 0;
    auto_rlock(this);
    if(opened){
        return -EBUSY;
    }
    if((flags & ENTRY_REASEWAIT_F) || (flags & ENTRY_PULLING_F)){
        return -EAGAIN;
    }
    if((flags & ENTRY_INITED_F) == 0){
        return 0;
    }
    __r.upgrade();
    for(auto i : entrys){
        if(i.first == "." || i.first == ".."){
            continue;
        }
        ret |= i.second->drop_mem_cache();
    }
    entrys.clear();
    flags &= ~DIR_PULLED_F;
    return ret;
}
