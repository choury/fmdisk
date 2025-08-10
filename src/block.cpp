#include "common.h"
#include "trdpool.h"
#include "block.h"
#include "file.h"
#include "sqlite.h"

#include <string.h>

#include <semaphore.h>
#include <set>
#include <random>
#include <algorithm>

static std::string getPathFromFd(int fd) {
    if (fd < 0) {
        throw std::invalid_argument("File descriptor cannot be negative.");
    }

    std::string procPath = "/proc/self/fd/" + std::to_string(fd);
    std::vector<char> buffer(PATH_MAX);
    ssize_t len = readlink(procPath.c_str(), buffer.data(), buffer.size() - 1);

    if (len == -1) {
        throw std::runtime_error("Failed to readlink for " + procPath + ": " + strerror(errno));
    }
    return {buffer.data(), (size_t)len};
}

std::set<std::weak_ptr<block_t>, std::owner_less<std::weak_ptr<block_t>>> dblocks; // dirty blocks
pthread_mutex_t dblocks_lock = PTHREAD_MUTEX_INITIALIZER;

//static_assert(BLOCKLEN > INLINE_DLEN, "blocklen should not bigger than inline date length");

void writeback_thread(bool* done){
    while(!*done){
        usleep(100000);
        pthread_mutex_lock(&dblocks_lock);
        if(dblocks.empty()){
            pthread_mutex_unlock(&dblocks_lock);
            continue;
        }
        int staled_threshold = 30; // seconds
        if(upool->tasks_in_queue() == 0 && dblocks.size() >= UPLOADTHREADS){
            staled_threshold = 5;
        }
        for(auto i = dblocks.begin(); i!= dblocks.end();){
            if(upool->tasks_in_queue() >= UPLOADTHREADS){
                break;
            }
            if(i->expired()){
                i = dblocks.erase(i);
                continue;
            }
            auto b = i->lock();
            if(b->staled() >= staled_threshold){
                upool->submit_fire_and_forget([block = *i]{ block_t::push(block); });
                i = dblocks.erase(i);
            }else{
                i++;
            }
        }
        pthread_mutex_unlock(&dblocks_lock);
    }
}


block_t::block_t(int fd, ino_t inode, filekey fk, size_t no, off_t offset, size_t size, unsigned int flags):
    fd(fd),
    inode(inode),
    fk(basename(fk)),
    no(no),
    offset(offset),
    size(size),
    flags(flags),
    atime(0)
{
    assert(fd >= 0);

    if(fk.path == "" && fm_private_key_tostring(fk.private_key)[0] == '\0') {
        this->flags |= BLOCK_SYNC;
    }
    // 检查数据库中的sync状态，如果已同步则设置BLOCK_SYNC标志
    struct block_record record;
    if(!load_block_from_db(inode, no, record)) {
        return;
    }
    this->flags |= BLOCK_SYNC;
    if (record.dirty || record.private_key != fm_private_key_tostring(fk.private_key)) {
        this->flags |= BLOCK_DIRTY;
    }
}

//两种情况下会被调用
// 1. file被销毁了，一般是 drop_mem_cache, BLOCK_STALE标记
// 2. file被truncate了，这个时候需要删除数据
block_t::~block_t() {
    if((flags & BLOCK_STALE) == 0) {
        trim(getkey());
    }
}

std::string block_t::getpath() const {
    // 从 fd 获取当前路径
    string cache_path = getPathFromFd(fd);

    // 从缓存路径推导出远程路径: /cache_dir/cache/a/b/c -> /a/b/c
    return get_remote_path(cache_path);
}

filekey block_t::getkey() const {
    if(fk.path.empty()) {
        return filekey{getpath(), fk.private_key};
    } else {
        return filekey{pathjoin(getpath(), fk.path), fk.private_key};
    }
}

void block_t::pull(std::weak_ptr<block_t> wb) {
    if(wb.expired()) {
        return;
    }
    auto b = wb.lock();
    auto_wlock(b.get());
    if(b->flags & BLOCK_SYNC){
        return;
    }
    buffstruct bs((char*)malloc(b->size), b->size);
    //for chunk file, read from begin
    off_t startp = b->fk.path.size() ? 0 : b->offset;
    if(HANDLE_EAGAIN(fm_download(b->getkey(), startp, b->size, bs))){
        throw "fm_download IO Error";
    }
    assert(bs.size() <= (size_t)b->size);
    // 直接写入缓存文件
    if(b->flags & FILE_ENCODE_F){
        xorcode(bs.mutable_data(), b->offset, bs.size(), opt.secret);
    }
    int ret = TEMP_FAILURE_RETRY(pwrite(b->fd, bs.mutable_data(), bs.size(), b->offset));
    if(ret >= 0){
        b->flags |= BLOCK_SYNC;
        save_block_to_db(b->inode, b->no, b->fk.private_key, false);
    }
}

static std::string random_string() {
     std::string str("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");

     std::random_device rd;
     std::mt19937 generator(rd());

     std::shuffle(str.begin(), str.end(), generator);
     return str.substr(0, 16);    // assumes 16 < number of characters in str
}

void block_t::push(std::weak_ptr<block_t> wb) {
    if(wb.expired()) {
        return;
    }
    auto b = wb.lock();
    auto_rlock(b.get());
    size_t version = b->version;
    if((b->flags & BLOCK_DIRTY) == 0 || (b->flags & BLOCK_STALE)){
        return;
    }
    char *buff = (char*)malloc(b->size);
    int ret = TEMP_FAILURE_RETRY(pread(b->fd, buff, b->size, b->offset));
    if(ret < 0){
        free(buff);
        return;
    }
    size_t len = ret;

    // 检查是否全零
    bool allzero = true;
    for(size_t i = 0; i < len; i++){
        if(buff[i]){
            allzero = false;
            break;
        }
    }
    if (allzero) {
        len = 0;
    } else if(b->flags & FILE_ENCODE_F){
        xorcode(buff, b->offset, len, opt.secret);
    }
    filekey file;
    if(len){
        //It must be chunk file, because native file can't be written
        string path;
        if(opt.flags & FM_RENAME_NOTSUPPRTED) {
            //放到.objs/目录下，使用随机文件名，重命名也不移动它
            path = std::string("/.objs/") + std::to_string(b->no);
        } else {
            path = std::to_string(b->no);
        }
        path +=  '_' + std::to_string(time(nullptr)) + '_' + random_string();
        file = {path, nullptr};
retry:
        int ret = HANDLE_EAGAIN(fm_upload({b->getpath(), nullptr}, file, buff, len, false));
        if(ret != 0 && errno == EEXIST){
            goto retry;
        }
        free(buff);
        if(ret != 0){
            throw "fm_upload IO Error";
        }
        file = basename(file);
    }else{
        free(buff);
        file = filekey{"x", 0};
    }
    __r.upgrade();
    if (version != b->version || (b->flags & BLOCK_STALE)) {
        trim(file);
        return;
    }
    trim(b->getkey());
    b->fk = file;
    // 上传成功，清除dirty标记
    save_block_to_db(b->inode, b->no, b->fk.private_key, false);
    b->flags &= ~BLOCK_DIRTY;
}

void block_t::prefetch(bool wait) {
    auto_rlock(this);
    if(flags & BLOCK_SYNC) {
        return; // 已经同步，不需要预取
    }
    if(wait){
        __r.unlock();
        pull(shared_from_this());
        return;
    }
    if(dpool->tasks_in_queue() < DOWNLOADTHREADS){
        dpool->submit_fire_and_forget([this]{ pull(shared_from_this()); });
    }
}

void block_t::markdirty() {
    version++;
    atime = time(nullptr);
    auto_rlock(this);
    if(flags & BLOCK_DIRTY) {
        return;
    }
    __r.upgrade();
    flags |=  BLOCK_DIRTY;
    // 保存到数据库并标记为dirty
    save_block_to_db(inode, no, fk.private_key, true);
    pthread_mutex_lock(&dblocks_lock);
    dblocks.emplace(shared_from_this());
    pthread_mutex_unlock(&dblocks_lock);
}

void block_t::markstale() {
    auto_wlock(this);
    flags |= BLOCK_STALE;
}

bool block_t::sync(){
    auto_rlock(this);
    if ((flags & BLOCK_DIRTY) == 0) {
        return false;
    }
    pthread_mutex_lock(&dblocks_lock);
    dblocks.emplace(shared_from_this());
    pthread_mutex_unlock(&dblocks_lock);
    return true;
}

void block_t::reset(){
    auto_wlock(this);
    assert((flags & BLOCK_DIRTY) == 0);
    if(dummy()){
        flags = BLOCK_SYNC;
    }else{
        flags = 0;
    }
}

bool block_t::dummy() {
    auto_rlock(this);
    return fk.path == "x";
}


int block_t::staled(){
    return time(nullptr) - atime;
}
