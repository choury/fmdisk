#include "common.h"
#include "entry.h"
#include "dir.h"
#include "file.h"
#include "trdpool.h"
#include "sqlite.h"

#include <thread>
#include <assert.h>

TrdPool* upool;
TrdPool* dpool;
std::thread gc;
bool writeback_done = false;

int cache_prepare() {
    int ret = fm_prepare();
    if(ret){
        return ret;
    }
    assert(opt.block_len > INLINE_DLEN);
    gc = std::thread(start_gc);
    dpool = new TrdPool(DOWNLOADTHREADS);
    if(opt.no_cache) {
        return 0;
    }
    upool = new TrdPool(UPLOADTHREADS + 1); //for writeback_thread;
    upool->submit_fire_and_forget([&]() { writeback_thread(&writeback_done); });
    start_delay_thread();
    return sqlinit();
}

void cache_destroy(dir_t* root){
    stop_gc();
    if(!opt.no_cache) {
        writeback_done = true;
        delete upool;
    }
    delete dpool;
    if(gc.joinable()){
        gc.join();
    }
    if(!opt.no_cache) stop_delay_thread();
    delete root;
    if(!opt.no_cache) sqldeinit();
    if(opt.clean) {
        opt.clean();
    }
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

int entry_t::statfs(const char*, struct statvfs* sf) {
    return HANDLE_EAGAIN(fm_statfs(sf));
}

entry_t::entry_t(dir_t* parent, const filemeta& meta):
    parent(parent),
    fk(std::make_shared<filekey>(basename(meta.key))),
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
    if(parent == nullptr){
        return filekey{pathjoin("/", getrealname()), fk.load()->private_key};
    }else{
        return filekey{pathjoin(parent->getcwd(), getrealname()), fk.load()->private_key};
    }
}

string entry_t::getcwd() {
    if(parent == nullptr){
        return "/";
    }
    return pathjoin(parent->getcwd(), fk.load()->path);
}

int entry_t::pull_wlocked(filemeta& meta, std::vector<filekey>& fblocks) {
    const filekey& key = getkey();
    meta = initfilemeta(key);
    meta.mode = this->mode;
    assert((flags & ENTRY_INITED_F) == 0);
    //std::vector<filekey> fblocks;
    load_file_from_db(key.path, meta, fblocks);
    if(flags & ENTRY_CHUNCED_F){
        if(meta.blksize == 0){
            int ret = download_meta(key, meta, fblocks);
            if(ret < 0){
                return ret;
            }
            save_file_to_db(key.path, meta, fblocks);
        }
    }else{
        if(meta.blksize == 0){
            int ret = HANDLE_EAGAIN(fm_getattr(key, meta));
            if(ret < 0) {
                return ret;
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
    return 0;
}

void entry_t::pull(entry_t* entry){
    auto_wlock(entry);
    if(entry->flags & ENTRY_DELETED_F){
        __w.unlock();
        delete entry;
        return;
    }
    try{
        if((entry->flags & ENTRY_INITED_F) == 0){
            entry->pull_wlocked();
        }
    }catch(...){}
    entry->flags &= ~ENTRY_PULLING_F;
}
