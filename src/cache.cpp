#include "cache.h"
#include "dir.h"
#include "file.h"
#include "threadpool.h"
#include "defer.h"
#include "sqlite.h"

#include <string.h>
#include <assert.h>
#include <thread>

thrdpool* upool;
thrdpool* dpool;

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

int cache_prepare() {
    int ret = fm_prepare();
    if(ret){
        return ret;
    }
    upool = creatpool(UPLOADTHREADS + 1); //for writeback_thread;
    dpool = creatpool(DOWNLOADTHREADS);
    addtask(upool, (taskfunc)writeback_thread, 0, 0);
    start_delay_thread();
    return sqlinit();
}

void cache_destroy(entry_t* root){
    sqldeinit();
    stop_delay_thread();
    delete root;
}

entry_t* cache_root() {
    struct filemeta meta = initfilemeta(filekey{"/", 0});
    if(HANDLE_EAGAIN(fm_getattr(filekey{"/", 0}, meta))){
        throw "getattr of root failed";
    }
    return new entry_t(nullptr, meta);
}

int entry_t::statfs(const char*, struct statvfs* sf) {
    return HANDLE_EAGAIN(fm_statfs(sf));
}

entry_t::entry_t(entry_t* parent, filemeta meta):
    parent(parent),
    fk(basename(meta.key)),
    mode(meta.mode),
    flags(meta.flags)
{
    assert((flags & ENTRY_INITED_F) == 0);
    if(flags & ENTRY_CHUNCED_F){
        fk = decodepath(fk);
        mode = S_IFREG | 0666;
    }
    if(meta.flags & META_KEY_ONLY_F){
        flags |= ENTRY_PULLING_F;
        addtask(dpool, (taskfunc)pull, this, 0);
        return;
    }
    if(S_ISDIR(mode)){
        dir = new dir_t(this, parent, meta);
    }else{
        file = new file_t(this, meta);
    }
    flags |= ENTRY_INITED_F;
}

entry_t::~entry_t() {
    assert(opened == 0);
    //pthread_mutex_destroy(&init_lock);
    //pthread_cond_destroy(&init_cond);
    if(S_ISDIR(mode)){
        delete dir;
    }else{
        delete file;
    }
}

#if 0
void entry_t::init_wait() {
    pthread_mutex_lock(&init_lock);
    while((flags & ENTRY_INITED_F) == 0){
        pthread_cond_wait(&init_cond, &init_lock);
    }
    pthread_mutex_unlock(&init_lock);
}
#endif

void entry_t::clean(entry_t* entry) {
    auto_wlock(entry);
    entry->flags &= ~ENTRY_REASEWAIT_F;
    if(entry->opened > 0){
        return;
    }
    assert((entry->file->getmeta().flags & FILE_DIRTY_F) == 0);
    assert(S_ISREG(entry->mode));
    if(entry->flags & ENTRY_DELETED_F){
        __w.unlock();
        delete entry;
    }else{
        entry->file->release();
    }
}


filekey entry_t::getkey() {
    auto_rlock(this);
    string path;
    if(flags & ENTRY_CHUNCED_F){
        path = encodepath(getcwd());
    }else{
        path = getcwd();
    }
    return filekey{path, fk.private_key};
}

string entry_t::getcwd() {
    auto_rlock(this);
    if(parent == nullptr){
        return "/";
    }
    return pathjoin(parent->getcwd(), fk.path);
}

void entry_t::pull_wlocked(){
    const filekey& key = getkey();
    filemeta meta = initfilemeta(key);
    assert(file == nullptr);
    std::vector<filekey> fblocks;
    load_file_from_db(key.path, meta, fblocks);
    if(flags & ENTRY_CHUNCED_F){
        if(meta.blksize == 0){
            if(download_meta(key, meta, fblocks)){
                throw "download_meta IO Error";
            }
            save_file_to_db(key.path, meta, fblocks);
        }
        file = new file_t(this, meta, fblocks);
    }else{
        if(meta.blksize == 0){
            if(HANDLE_EAGAIN(fm_getattr(key, meta))){
                throw "fm_getattr IO Error";
            }
            save_file_to_db(key.path, meta, {});
        }
        assert(meta.inline_data == nullptr);
        if(S_ISDIR(meta.mode)){
            dir = new dir_t(this, parent, meta);
        }else{
            file = new file_t(this, meta);
        }
    }
    flags |= ENTRY_INITED_F;
    flags &= ~META_KEY_ONLY_F;
}

void entry_t::pull(entry_t* entry){
    auto_wlock(entry);
    if(entry->flags & ENTRY_DELETED_F){
        __w.unlock();
        delete entry;
        return;
    }
    if((entry->flags & ENTRY_INITED_F) == 0){
        entry->pull_wlocked();
    }
    entry->flags &= ~ENTRY_PULLING_F;
    return;
}

filemeta entry_t::getmeta() {
    auto_rlock(this);
    if((flags & ENTRY_INITED_F) == 0){
        __r.upgrade();
        pull_wlocked();
    }
    filemeta meta = initfilemeta(getkey());
    meta.mode = mode;
    meta.flags = flags;
    if(!S_ISDIR(mode)){
        filemeta fmeta = file->getmeta();
        meta.size = fmeta.size;
        meta.mode = S_IFREG | (mode & 0777);
        meta.flags = fmeta.flags;
        meta.inline_data = fmeta.inline_data;
        meta.blksize = fmeta.blksize;
        meta.blocks = fmeta.blocks;
        meta.ctime = fmeta.ctime;
        meta.mtime = fmeta.mtime;
    }else{
        meta.size = 0;
        meta.mode = S_IFDIR | (mode & 0777);
        meta.inline_data = 0;
        meta.blksize =  0;
        meta.blocks = 0;
        time_t utime[2];
        dir->getutime(utime);
        meta.ctime = utime[0];
        meta.mtime = utime[1];
    }
    return meta;
}

entry_t* entry_t::find(string path){
    auto_rlock(this);
    if(path == "." || path == "/"){
        return this;
    }
    if((flags & ENTRY_INITED_F) == 0){
        __r.upgrade();
        pull_wlocked();
    }
    assert(S_ISDIR(mode));
    string cname = childname(path);
    entry_t* entry = dir->find(cname);
    if(entry){
        return entry->find(subname(path));
    }
    return nullptr;
}

entry_t* entry_t::create(string name){
    auto_rlock(this);
    if(dir->size() >= MAXFILE){
        errno = ENOSPC;
        return nullptr;
    }
    assert(S_ISDIR(mode));
    struct filemeta meta = initfilemeta(filekey{encodepath(name), 0});
    if(HANDLE_EAGAIN(fm_mkdir(getkey(), meta.key))){
        return nullptr;
    }

    meta.ctime = time(NULL);
    meta.mtime = time(NULL);
    meta.flags =  ENTRY_CHUNCED_F | ENTRY_CREATE_F | FILE_ENCODE_F | FILE_DIRTY_F ;
    meta.blksize = BLOCKLEN;
    meta.mode = S_IFREG | 0644;
    return dir->insert(name, new entry_t(this, meta));
}

entry_t* entry_t::mkdir(string name) {
    auto_rlock(this);
    if(endwith(name, ".def")){
        errno = EINVAL;
        return nullptr;
    }
    if(dir->size() >= MAXFILE){
        errno = ENOSPC;
        return nullptr;
    }
    assert(S_ISDIR(mode));
    struct filemeta meta = initfilemeta(filekey{name, 0});
    if(HANDLE_EAGAIN(fm_mkdir(getkey(), meta.key))){
        return nullptr;
    }

    meta.ctime = time(NULL);
    meta.mtime = time(NULL);
    meta.flags = ENTRY_CREATE_F;
    meta.mode = S_IFDIR | 0755;
    return dir->insert(name, new entry_t(this, meta));
}

int entry_t::open() {
    auto_wlock(this);
    if((flags & ENTRY_INITED_F) == 0){
        pull_wlocked();
    }
    if((mode & 0777) == 0){
        return -EACCES;
    }
    if(S_ISREG(mode) && file->open() < 0){
        return -errno;
    }
    opened++;
    return 0;
}


const std::map<string, entry_t*>& entry_t::entrys(){
    auto_rlock(this);
    return dir->get_entrys();
}


int entry_t::read(void* buff, off_t offset, size_t size) {
    auto_rlock(this);
    assert(opened);
    return file->read(buff, offset, size);
}

int entry_t::truncate(off_t offset){
    auto_rlock(this);
    assert(opened);
    if((flags & ENTRY_CHUNCED_F) == 0){
        return -EPERM;
    }
    int ret = file->truncate(offset);
    if(ret < 0){
        return -errno;
    }
    return ret;
}


int entry_t::write(const void* buff, off_t offset, size_t size) {
    auto_rlock(this);
    assert(opened);
    if((flags & ENTRY_CHUNCED_F) == 0){
        return -EPERM;
    }
    int ret = file->write(buff, offset, size);
    if(ret < 0){
        return -errno;
    }
    return ret;
}


int entry_t::sync(int datasync){
    auto_rlock(this);
    if((flags & ENTRY_CHUNCED_F) == 0){
        return 0;
    }
    assert(S_ISREG(mode));
    file->sync();
    filemeta meta = getmeta();
    if((!datasync && (meta.flags & FILE_DIRTY_F))){
        const filekey& key = getkey();
        std::vector<filekey> fblocks = file->getfblocks();
        if(upload_meta(key, meta, fblocks)){
            throw "upload_meta IO Error";
        }
        save_file_to_db(key.path, meta, fblocks);
        file->post_sync(meta.key);
    }
    return 0;
}

int entry_t::flush(){
    sync(0);
    return 0;
}

int entry_t::release() {
    {
        auto_wlock(this);
        assert(opened);
        opened--;
        if(opened || !S_ISREG(mode)){
            return 0;
        }
        flags |= ENTRY_REASEWAIT_F;
    }
    add_delay_job((taskfunc)clean, this, 60);
    return 0;
}

int entry_t::utime(const struct timespec  tv[2]) {
    auto_rlock(this);
    if((flags & ENTRY_INITED_F) == 0){
        __r.upgrade();
        pull_wlocked();
    }
    int ret = 0;
    if((flags & ENTRY_CHUNCED_F) == 0){
        ret = HANDLE_EAGAIN(fm_utime(getkey(), tv));
    }
    if(ret == 0){
        time_t utime[2];
        utime[0] = tv[0].tv_sec;
        utime[1] = tv[1].tv_sec;
        if(S_ISDIR(mode)){
            dir->setutime(utime);
        }else{
            file->setutime(utime);
        }
        sync(0);
    }
    return ret;
}

int entry_t::moveto(entry_t* newparent, string oldname, string newname) {
    auto_rlocker __1(this);
    assert(S_ISDIR(this->mode));
    entry_t* entry = this->dir->find(oldname);
    if(entry == nullptr){
        return -ENOENT;
    }
    auto_rlocker __2(newparent);
    assert(S_ISDIR(newparent->mode));
    newparent->unlink(newname);

    if(newparent->dir->size() >= MAXFILE){
        return -ENOSPC;
    }

    auto_wlocker __3(entry);
    filekey newfile{(entry->flags & ENTRY_CHUNCED_F)?encodepath(newname):newname, 0};
    int ret = HANDLE_EAGAIN(fm_rename(this->getkey(), entry->getkey(), newparent->getkey(), newfile));
    if(ret){
        return ret;
    }

    this->dir->erase(oldname);
    entry->parent = newparent;
    entry->fk = filekey{newname, newfile.private_key};
    newparent->dir->insert(newname, entry);
    return 0;
}

void entry_t::erase_child_rlocked(entry_t* child) {
    assert(S_ISDIR(mode));
    dir->erase(child->fk.path);
    child->upgrade();
    child->parent = nullptr;
    child->flags |= ENTRY_DELETED_F;
    if((child->flags & ENTRY_REASEWAIT_F)||
       (child->flags & ENTRY_PULLING_F))
    {
        child->unwlock();
        //delete this in clean or pull
        return;
    }
    delete child;
}

int entry_t::unlink(const string& name) {
    auto_rlock(this);
    assert(S_ISDIR(mode));
    entry_t* entry = this->dir->find(name);
    if(entry == nullptr){
        return -ENOENT;
    }
    entry->rlock();
    assert(entry->opened == 0);
    if(!S_ISREG(entry->mode)){
        entry->unrlock();
        return -EISDIR;
    }
    int ret = HANDLE_EAGAIN(fm_delete(entry->getkey()));
    if(ret){
        entry->unrlock();
        return ret;
    }
    this->erase_child_rlocked(entry);
    return 0;
}

int entry_t::rmdir(const string& name) {
    auto_rlock(this);
    assert(S_ISDIR(mode));
    entry_t* entry = this->dir->find(name);
    if(entry == nullptr){
        return -ENOENT;
    }
    entry->rlock();
    if(!S_ISDIR(entry->mode)){
        entry->unrlock();
        return -ENOTDIR;
    }
    if(entry->opened){
        entry->unrlock();
        return -EBUSY;
    }
    if(entry->entrys().size() != 2){
        entry->unrlock();
        return -ENOTEMPTY;
    }
    int ret = HANDLE_EAGAIN(fm_delete(entry->getkey()));
    if(ret){
        entry->unrlock();
        return ret;
    }
    this->erase_child_rlocked(entry);
    return 0;
}

void entry_t::dump_to_disk_cache(){
    auto_rlock(this);
    if((flags & ENTRY_INITED_F) == 0){
        return;
    }
    filemeta meta = getmeta();
    if(flags & ENTRY_CHUNCED_F){
        auto savemeta = meta;
        savemeta.mode = S_IFDIR | 0755;
        save_entry_to_db(parent->getkey(), savemeta);
    }else{
        save_entry_to_db(parent->getkey(), meta);
    }
    if(S_ISDIR(mode)){
        save_file_to_db(meta.key.path, meta, std::vector<filekey>{});
        dir->dump_to_disk_cache();
    }else{
        save_file_to_db(meta.key.path, meta, file->getfblocks());
    }
}

int entry_t::drop_mem_cache(){
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
    if(S_ISREG(mode)){
        delete file;
        flags &= ~ENTRY_INITED_F;
        file = nullptr;
        return 0;
    }else if(dir->drop_mem_cache() == 0){
        delete dir;
        flags &= ~ENTRY_INITED_F;
        dir = nullptr;
        return 0;
    }
    return -EAGAIN;
}

int entry_t::drop_disk_cache(){
    auto_rlock();
    string path = getkey().path;
    if(S_ISREG(mode)){
        return delete_file_from_db(path);
    }else{
        if(parent == nullptr){
            return delete_entry_prefix_from_db("");
        }
        return delete_entry_prefix_from_db(path);
    }
}
