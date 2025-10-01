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

std::shared_ptr<dir_t> cache_root() {
    struct filemeta meta = initfilemeta(filekey{"/", 0});
    if(HANDLE_EAGAIN(fm_getattr(filekey{"/", 0}, meta))){
        throw "getattr of root failed";
    }
    return std::make_shared<dir_t>(nullptr, meta);
}

void cache_destroy(std::shared_ptr<dir_t> root){
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
    if(!opt.no_cache) sqldeinit();
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

entry_t::entry_t(std::shared_ptr<dir_t> parent, const filemeta& meta):
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
}

filekey entry_t::getkey() {
    auto p = parent.lock();
    if(p == nullptr){
        return filekey{pathjoin("/", getrealname()), fk.load()->private_key};
    }else{
        return filekey{pathjoin(p->getcwd(), getrealname()), fk.load()->private_key};
    }
}

string entry_t::getcwd() {
    auto p = parent.lock();
    if(p == nullptr){
        return "/";
    }
    return pathjoin(p->getcwd(), fk.load()->path);
}

void entry_t::pull(std::weak_ptr<entry_t> entry_){
    auto entry = entry_.lock();
    if(entry == nullptr) {
        return;
    }
    auto_wlock(entry);
    if(entry->flags & ENTRY_DELETED_F){
        return;
    }
    try{
        if((entry->flags & ENTRY_INITED_F) == 0){
            entry->pull_wlocked();
        }
    }catch(...){}
    entry->flags &= ~ENTRY_PULLING_F;
}

int entry_t::set_storage_class(enum storage_class storage) {
    if((opt.flags & FM_HAS_STORAGE_CLASS) == 0) {
        return -ENODATA; // 不支持分块存储类设置
    }
    std::vector<std::future<int>> futures;
    TrdPool pool(DOWNLOADTHREADS * 10);
    int ret = set_storage_class(storage, &pool, futures);
    if(ret < 0) {
        return ret;
    }
    pool.wait_all();
    bool failed = false;
    for(auto& f: futures) {
        ret = f.get();
        if(ret < 0) {
            failed = true;
        }
    }
    return failed ? -EIO : 0;
}