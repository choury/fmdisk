#include "common.h"
#include "trdpool.h"
#include "block.h"
#include "file.h"
#include "sqlite.h"
#include "defer.h"
#include "log.h"

#include <string.h>
#include <sys/xattr.h>
#include <fcntl.h>

#include <semaphore.h>
#include <set>
#include <algorithm>

static std::string getRemotePathFromFd(int fd) {
    if (fd < 0) {
        throw std::invalid_argument("File descriptor cannot be negative.");
    }
    //get remoute path from xattr first
    char path[PATH_MAX] = {0};
    if(fgetxattr(fd, FM_REMOTE_PATH_ATTR, path, sizeof(path) - 1) == 0){
        return path;
    }

    std::string procPath = "/proc/self/fd/" + std::to_string(fd);
    std::vector<char> buffer(PATH_MAX);
    ssize_t len = readlink(procPath.c_str(), buffer.data(), buffer.size() - 1);

    if (len == -1) {
        throw std::runtime_error("Failed to readlink for " + procPath + ": " + strerror(errno));
    }
    return get_remote_path({buffer.data(), (size_t)len});
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
    assert(opt.no_cache || fd >= 0);

    std::string fk_private = fm_private_key_tostring(fk.private_key);
    if(fk.path == "" && fk_private[0] == '\0') {
        this->fk.path = "x";
    }
    if(this->fk.path == "x") {
        this->flags |= BLOCK_SYNC;
    }
    // 检查数据库中的sync状态，如果已同步则设置BLOCK_SYNC标志
    struct block_record record;
    if(!load_block_from_db(inode, no, record)) {
        return;
    }
    this->flags |= BLOCK_SYNC;
    if (record.dirty) {
        this->flags |= BLOCK_DIRTY;
    }
    if(record.private_key != fk_private) {
        if(!record.private_key.empty() && !record.path.empty()){
            //数据库中blocks表保存的信息比files表更可靠,因为正常流程是先保存blocks再更新files
            trim(getkey());
            this->fk.path = record.path;
            this->fk.private_key = fm_get_private_key(record.private_key.c_str());
        } else {
            //数据库中blocks表保存的信息不完整，重新上传
            this->flags |= BLOCK_DIRTY;
        }
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
    return getRemotePathFromFd(fd);
}

filekey block_t::getkey() const {
    if(fk.path.empty()) {
        return filekey{getpath(), fk.private_key};
    } else {
        return filekey{pathjoin(getpath(), fk.path), fk.private_key};
    }
}

int block_t::pull(std::weak_ptr<block_t> wb) {
    if(wb.expired()) {
        return 0;
    }
    auto b = wb.lock();
    auto_wlock(b.get());
    if(b->flags & (BLOCK_SYNC | BLOCK_STALE)){
        return 0;
    }
    buffstruct bs((char*)malloc(b->size), b->size);
    //for chunk file, read from begin
    off_t startp = b->fk.path.size() ? 0 : b->offset;
    int ret = HANDLE_EAGAIN(fm_download(b->getkey(), startp, b->size, bs));
    if(ret){
        return ret;
    }
    assert(bs.size() <= (size_t)b->size);
    // 直接写入缓存文件
    if(b->flags & FILE_ENCODE_F){
        xorcode(bs.mutable_data(), b->offset, bs.size(), opt.secret);
    }
    ret = TEMP_FAILURE_RETRY(pwrite(b->fd, bs.mutable_data(), bs.size(), b->offset));
    if(ret >= 0){
        //这里因为没有执行sync操作，进程异常退出不会有问题，但是os crash的话，数据会有不一致的情况
        assert((size_t)ret == bs.size());
        b->flags |= BLOCK_SYNC;
        save_block_to_db(b->inode, b->no, b->fk, false);
    }
    return ret;
}

ssize_t block_t::read(filekey fileat, void* buff, off_t offset, size_t len) {
    auto_rlock(this);
    if(offset < 0 || offset + len > size){
        errno = EINVAL;
        return -1;
    }
    if(len == 0){
        return 0;
    }
    if(dummy() || (flags & BLOCK_STALE)) {
        memset(buff, 0, len);
        return len;
    }
    buffstruct bs((char*)buff, len);
    defer([&bs] { bs.release(); });
    fileat.private_key = fk.private_key;
    if(!fk.path.empty()) {
        fileat.path = pathjoin(fileat.path, fk.path);
    }
    int ret = HANDLE_EAGAIN(fm_download(fileat, offset, len, bs));
    if(ret < 0) {
        return ret;
    }
    assert(bs.size() <= (size_t)len);
    if(flags & FILE_ENCODE_F){
        xorcode(buff, this->offset + offset, bs.size(), opt.secret);
    }
    return bs.size();
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

#if 0
    bool allzero = std::all_of(buff, buff + len, [](char c) { return c == 0; });
#else
    bool allzero = isAllZero(buff, len);
#endif
    if (allzero) {
        len = 0;
    } else if(b->flags & FILE_ENCODE_F){
        xorcode(buff, b->offset, len, opt.secret);
    }
    filekey file;
    if(len){
        //It must be chunk file, because native file can't be written
        file = makeChunkBlockKey(b->no);
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
    // 上传成功，清除dirty标记
    if(save_block_to_db(b->inode, b->no, file, false) == 0) {
        trim(b->getkey());
        b->fk = file;
        b->flags &= ~BLOCK_DIRTY;
    } else {
        trim(file);
    }
}

int block_t::prefetch(bool wait) {
    atime = time(nullptr);
    if(wait) {
        auto_rlock(this);
        if(flags & (BLOCK_SYNC | BLOCK_STALE)) {
            return 0; // 已经同步，不需要预取
        }
        __r.unlock();
        return pull(shared_from_this());
    } else {
        if(dpool->tasks_in_queue() > DOWNLOADTHREADS){
            return 1;
        }
        if(tryrlock() != 0) {
            return 1; // 已经有写锁，说明正在被修改，不需要预取
        }
        defer([this] { unrlock(); });
        if(flags & (BLOCK_SYNC | BLOCK_STALE)) {
            return 0; // 已经同步，不需要预取
        }
        dpool->submit_fire_and_forget([b = std::weak_ptr<block_t>(shared_from_this())]{
            pull(b);
        });
        return 0;
    }
}

void block_t::markdirty() {
    version++;
    atime = time(nullptr);
    auto_rlock(this);
    if(flags & (BLOCK_DIRTY | BLOCK_STALE)) {
        return;
    }
    __r.upgrade();
    flags |=  BLOCK_DIRTY | BLOCK_SYNC;
    // 保存到数据库并标记为dirty
    save_block_to_db(inode, no, fk, true);
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
    if ((flags & BLOCK_DIRTY) == 0 || (flags & BLOCK_STALE)) {
        return false;
    }
    pthread_mutex_lock(&dblocks_lock);
    dblocks.emplace(shared_from_this());
    pthread_mutex_unlock(&dblocks_lock);
    return true;
}

bool block_t::dummy() {
    auto_rlock(this);
    return fk.path == "x";
}


int block_t::staled(){
    return time(nullptr) - atime;
}

size_t block_t::release() {
    auto_wlock(this);
    if((flags & BLOCK_SYNC) == 0 || (flags & (BLOCK_DIRTY | BLOCK_STALE)) || fd < 0 || staled() < 60) {
        return 0;
    }
    fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, size);
    delete_block_from_db(inode, no);
    debuglog("release block %s/%lu[%zd-%zd]\n", getpath().c_str(), no, offset, offset+size);
    flags = flags & FILE_ENCODE_F;
    if(fk.path == "x") {
        flags |= BLOCK_SYNC;
    }
    return size;
}
