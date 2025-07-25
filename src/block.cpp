#include "common.h"
#include "threadpool.h"
#include "block.h"
#include "file.h"
#include "sqlite.h"

#include <string.h>

#include <semaphore.h>
#include <list>
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
    return std::string(buffer.data(), len);
}

sem_t dirty_sem;
std::list<block_t*> dblocks; // dirty blocks
pthread_mutex_t dblocks_lock = PTHREAD_MUTEX_INITIALIZER;

//static_assert(BLOCKLEN > INLINE_DLEN, "blocklen should not bigger than inline date length");

void writeback_thread(){
    sem_init(&dirty_sem, 0, UPLOADTHREADS*2);
    while(true){
        usleep(100000);
        pthread_mutex_lock(&dblocks_lock);
        if(dblocks.empty()){
            pthread_mutex_unlock(&dblocks_lock);
            continue;
        }else if(taskinqueu(upool) == 0 && dblocks.size() >= UPLOADTHREADS){
            addtask(upool, (taskfunc)block_t::push, dblocks.front(), 0);
            dblocks.pop_front();
        }
        for(auto i = dblocks.begin(); i!= dblocks.end();){
            if((*i)->staled() >= 30){
                addtask(upool, (taskfunc)block_t::push, *i, 0);
                i = dblocks.erase(i);
            }else{
                break;
            }
        }
        pthread_mutex_unlock(&dblocks_lock);
    }
}


block_t::block_t(int fd, filekey fk, size_t no, off_t offset, size_t size, unsigned int flags):
    fd(fd),
    fk(basename(fk)),
    no(no),
    offset(offset),
    size(size),
    flags(flags)
{
    assert(fd >= 0);

    // 检查数据库中的sync状态，如果已同步则设置BLOCK_SYNC标志
    if(is_block_synced_in_db(fk.private_key, no)) {
        this->flags |= BLOCK_SYNC;
    }
}

//两种情况下会被调用
// 1. file被销毁了，一般是 drop_mem_cache, BLOCK_STALE标记
// 2. file被truncate了，这个时候需要删除数据
block_t::~block_t() {
    pthread_mutex_lock(&dblocks_lock);
    for(auto i = dblocks.begin(); i != dblocks.end();){
        if(*i ==  this){
            i = dblocks.erase(i);
        }else{
            i++;
        }
    }
    pthread_mutex_unlock(&dblocks_lock);
    if(flags & BLOCK_DIRTY){
        sem_post(&dirty_sem);
    }
    if((flags & BLOCK_STALE) == 0) {
        trim(getkey());
    }
}

std::string block_t::getpath() {
    // 从 fd 获取当前路径
    string cache_path = getPathFromFd(fd);

    // 从缓存路径推导出远程路径: /cache_dir/cache/a/b/c -> /a/b/c
    string remote_path;
    string cache_prefix = pathjoin(opt.cache_dir, "cache");
    if(cache_path.find(cache_prefix) == 0) {
        remote_path = cache_path.substr(cache_prefix.length());
        if(remote_path.empty()) {
            remote_path = "/";
        }
    } else {
        throw std::runtime_error("Cache path does not start with expected prefix: " + cache_path);
    }
    return remote_path;
}

filekey block_t::getkey(){
    if(fk.path.empty()) {
        return filekey{getpath(), fk.private_key};
    } else {
        return filekey{pathjoin(getpath(), fk.path), fk.private_key};
    }
}

std::tuple<filekey, uint> block_t::getmeta() {
    auto_rlock(this);
    return {fk, flags};
}

void block_t::pull(block_t* b) {
    auto_wlock(b);
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
        // 更新数据库中的sync状态
        save_block_sync_status_to_db(b->fk.private_key, b->no, true);
    }
}

static std::string random_string() {
     std::string str("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");

     std::random_device rd;
     std::mt19937 generator(rd());

     std::shuffle(str.begin(), str.end(), generator);
     return str.substr(0, 16);    // assumes 16 < number of characters in str
}



void block_t::push(block_t* b) {
    auto_wlock(b);
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
    trim(b->getkey());
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
        filekey file{path, 0};
retry:
        int ret = HANDLE_EAGAIN(fm_upload({b->getpath(), nullptr}, file, buff, len, false));
        if(ret != 0 && errno == EEXIST){
            goto retry;
        }
        free(buff);
        if(ret != 0){
            throw "fm_upload IO Error";
        }
        b->fk = basename(file);
    }else{
        free(buff);
        b->fk = filekey{"x", 0};
    }
    save_block_sync_status_to_db(b->fk.private_key, b->no, true);
    b->flags &= ~BLOCK_DIRTY;
    sem_post(&dirty_sem);
}

void block_t::prefetch(bool wait) {
    if(wait){
        pull(this);
        return;
    }
    if(tryrlock()){
        return;
    }
    if(flags & BLOCK_SYNC){
        unrlock();
        return;
    }
    unrlock();
    if(taskinqueu(dpool) < DOWNLOADTHREADS){
        addtask(dpool, (taskfunc)block_t::pull, this, 0);
    }
}

void block_t::markdirty() {
    wlock();
    atime = time(0);
    if(flags & BLOCK_DIRTY){
        unwlock();
        return;
    }
    flags |=  BLOCK_DIRTY;
    unwlock();
    pthread_mutex_lock(&dblocks_lock);
    // 挪到队尾
    for(auto i = dblocks.begin(); i != dblocks.end(); i++ ){
        if(*i == this){
            dblocks.erase(i);
            break;
        }
    }
    dblocks.push_back(this);
    pthread_mutex_unlock(&dblocks_lock);
    sem_wait(&dirty_sem);
}

void block_t::markstale() {
    auto_wlock(this);
    flags |= BLOCK_STALE;
}

void block_t::sync(){
    pthread_mutex_lock(&dblocks_lock);
    // dblocks 是异步搞的，所以挪出来同步搞
    for(auto i = dblocks.begin(); i != dblocks.end();){
        if(*i ==  this){
            i = dblocks.erase(i);
            break;
        }else{
            i++;
        }
    }
    pthread_mutex_unlock(&dblocks_lock);
    push(this);
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
    return fk.path == "x";
}


int block_t::staled(){
    auto_rlock(this);
    return time(0) - atime;
}