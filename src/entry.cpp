#include "common.h"
#include "entry.h"
#include "dir.h"
#include "file.h"
#include "trdpool.h"
#include "sqlite.h"

#include <string.h>
#include <thread>
#include <assert.h>

TrdPool* upool;
TrdPool* dpool;
bool writeback_done = false;

std::shared_ptr<dir_t> root = nullptr;

int cache_prepare() {
    assert(opt.block_len > INLINE_DLEN);
    dpool = new TrdPool(DOWNLOADTHREADS);
    struct filemeta meta = initfilemeta(filekey{"/", 0});
    if(HANDLE_EAGAIN(fm_getattr(filekey{"/", 0}, meta))){
        throw std::runtime_error("getattr of root failed");
    }
    root = std::make_shared<dir_t>(nullptr, meta);
    start_gc();
    if(opt.no_cache) {
        printf("--- EXIT cache_prepare (no_cache) ---\n");
        return 0;
    }
    upool = new TrdPool(UPLOADTHREADS + 1); //for writeback_thread;
    upool->submit_fire_and_forget([&]() { writeback_thread(&writeback_done); });
    start_delay_thread();
    if((opt.flags & FM_RENAME_NOTSUPPRTED) && (opt.flags & FM_DONOT_REQUIRE_MKDIR) == 0) {
        filekey objs = {".objs"};
        int ret = HANDLE_EAGAIN(fm_mkdir({"/"}, objs));
        if(ret < 0 && errno != EEXIST) {
            throw std::runtime_error("create .objs dir failed");
        }
    }
    int ret = sqlinit();
    if (ret != 0) {
        throw std::runtime_error("sqlinit failed");
    }
    root->open();
    root->release(false);
    return 0;
}

std::shared_ptr<dir_t> cache_root() {
    return root;
}

void cache_destroy(){
    stop_gc();
    if(!opt.no_cache) {
        writeback_done = true;
        delete upool;
    }
    delete dpool;
    if(!opt.no_cache) stop_delay_thread();
    if(!opt.no_cache) sqldeinit();
    root = nullptr;
}

void clean_entry_cache() {
    cache_root()->drop_cache(true, time(nullptr) - opt.entry_cache_second);
}

std::shared_ptr<entry_t> find_entry(const string& path) {
    return dir_t::find(root, path);
}

int entry_t::statfs(const char*, struct statvfs* sf) {
    return HANDLE_EAGAIN(fm_statfs(sf));
}

entry_t::entry_t(std::shared_ptr<dir_t> parent, const filemeta& meta):
    parent(parent),
    fk(std::make_shared<filekey>(basename(meta.key))),
    mode(meta.mode),
    atime(time(nullptr)),
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
        return -ENODATA; // 不支持
    }
    std::vector<std::future<int>> futures;
    TrdPool pool(UPLOADTHREADS * 2);
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

static void merge_storage_info(storage_class_info& dst, const storage_class_info& src) {
    for(size_t i = 0; i < sizeof(dst.size_store) / sizeof(dst.size_store[0]); ++i) {
        dst.size_store[i] += src.size_store[i];
    }
    dst.size_archive_restored += src.size_archive_restored;
    dst.size_archive_restoring += src.size_archive_restoring;
    dst.size_deep_archive_restored += src.size_deep_archive_restored;
    dst.size_deep_archive_restoring += src.size_deep_archive_restoring;
}

int entry_t::get_storage_classes(storage_class_info& info) {
    if((opt.flags & FM_HAS_STORAGE_CLASS) == 0) {
        return -ENODATA; // 不支持
    }
    memset(&info, 0, sizeof(info));
    std::vector<std::future<std::pair<int, storage_class_info>>> futures;
    TrdPool pool(DOWNLOADTHREADS * 10);
    int ret = collect_storage_classes(&pool, futures);
    if(ret < 0) {
        return ret;
    }
    pool.wait_all();
    bool failed = false;
    for(auto& future : futures) {
        auto result = future.get();
        if(result.first < 0) {
            failed = true;
            continue;
        }
        merge_storage_info(info, result.second);
    }
    return failed ? -EIO : 0;
}

int entry_t::chmod(mode_t mode) {
    atime = time(nullptr);
    auto_wlock(this);
    if(flags & ENTRY_DELETED_F){
        this->mode = (mode & ~S_IFMT) | (this->mode & S_IFMT);
        this->ctime = time(nullptr);
        return 0;
    }
    if((flags & ENTRY_INITED_F) == 0){
        int ret = pull_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    this->mode = (mode & ~S_IFMT) | (this->mode & S_IFMT);
    this->ctime = time(nullptr);
    return 0;
}
