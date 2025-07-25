#include "common.h"
#include "entry.h"
#include "dir.h"
#include "file.h"
#include "threadpool.h"
#include "sqlite.h"

#include <thread>
#include <assert.h>

thrdpool* upool;
thrdpool* dpool;
std::thread gc;

int cache_prepare() {
    int ret = fm_prepare();
    if(ret){
        return ret;
    }
    assert(opt.block_len > INLINE_DLEN);
    upool = creatpool(UPLOADTHREADS + 1); //for writeback_thread;
    dpool = creatpool(DOWNLOADTHREADS);
    addtask(upool, (taskfunc)writeback_thread, 0, 0);
    gc = std::thread(start_gc);
    start_delay_thread();
    return sqlinit();
}

void cache_destroy(dir_t* root){
    sqldeinit();
    stop_delay_thread();
    stop_gc();
    if(gc.joinable()){
        gc.join();
    }
    delete root;
}

int create_dirs_recursive(const string& path) {
    if (path.empty() || path == "/") {
        return 0;
    }

    struct stat st;
    if (stat(path.c_str(), &st) == 0) {
        return S_ISDIR(st.st_mode) ? 0 : -1;
    }

    string parent = dirname(path);
    if (create_dirs_recursive(parent) != 0) {
        return -1;
    }

    if (mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
        return -1;
    }

    return 0;
}

string get_cache_path(const string& remote_path) {
    string cache_path = pathjoin(opt.cache_dir, "cache");
    cache_path = pathjoin(cache_path, remote_path);
    return cache_path;
}




int entry_t::statfs(const char*, struct statvfs* sf) {
    return HANDLE_EAGAIN(fm_statfs(sf));
}

entry_t::entry_t(dir_t* parent, filemeta meta):
    parent(parent),
    fk(basename(meta.key)),
    mode(meta.mode),
    length(meta.size),
    mtime(meta.mtime),
    ctime(meta.ctime),
    flags(meta.flags)
{
    assert((flags & ENTRY_INITED_F) == 0);
    if(meta.flags & META_KEY_ONLY_F){
        //flags |= ENTRY_PULLING_F;
        //addtask(dpool, (taskfunc)pull, this, 0);
        return;
    }
    flags |= ENTRY_INITED_F;
}

entry_t::~entry_t() {
    assert(opened == 0);
    //pthread_mutex_destroy(&init_lock);
    //pthread_cond_destroy(&init_cond);
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

void entry_t::pull_wlocked(filemeta& meta, std::vector<filekey>& fblocks){
    const filekey& key = getkey();
    meta = initfilemeta(key);
    meta.mode = this->mode;
    assert((flags & ENTRY_INITED_F) == 0);
    load_file_from_db(key.path, meta, fblocks);
    if(flags & ENTRY_CHUNCED_F){
        if(meta.blksize == 0){
            if(download_meta(key, meta, fblocks)){
                throw "download_meta IO Error";
            }
            save_file_to_db(key.path, meta, fblocks);
        }
    }else{
        if(meta.blksize == 0){
            if(HANDLE_EAGAIN(fm_getattr(key, meta))){
                throw "fm_getattr IO Error";
            }
            save_file_to_db(key.path, meta, {});
        }
        assert(meta.inline_data == nullptr);
    }
    mode = meta.mode;
    length = meta.size;
    ctime = meta.ctime;
    mtime = meta.mtime;
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

int entry_t::drop_disk_cache(){
    auto_rlock(this);
    if(!isDir()) {
        delete_blocks_from_db(dynamic_cast<file_t*>(this)->getfblocks());
    }
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
