#include "common.h"
#include "trdpool.h"
#include "block.h"
#include "file.h"
#include "sqlite.h"
#include "defer.h"
#include "log.h"
#include "transfer_helper.h"

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

std::map<std::weak_ptr<block_t>, filekey, std::owner_less<std::weak_ptr<block_t>>> dblocks; // dirty blocks
pthread_mutex_t dblocks_lock = PTHREAD_MUTEX_INITIALIZER;
static sem_t dirty_blocks_sem;
static std::atomic<long long> dirty_blocks_limit{-1};
static pthread_once_t dirty_blocks_once = PTHREAD_ONCE_INIT;

static void init_dirty_blocks_limit() {
    if(opt.no_cache || opt.cache_size <= 0 || opt.block_len == 0) {
        dirty_blocks_limit = -1;
        return;
    }
    long long limit = opt.cache_size / opt.block_len;
    if(limit < UPLOADTHREADS) {
        limit = UPLOADTHREADS;
    }
    if(sem_init(&dirty_blocks_sem, 0, static_cast<unsigned int>(limit)) != 0) {
        warnlog("init dirty blocks semaphore failed: %s\n", strerror(errno));
        dirty_blocks_limit = -1;
        return;
    }
    dirty_blocks_limit = limit;
}

static void acquire_dirty_block_slot() {
    pthread_once(&dirty_blocks_once, init_dirty_blocks_limit);
    if(dirty_blocks_limit.load() <= 0) {
        return;
    }
    int ret = 0;
    do {
        ret = sem_wait(&dirty_blocks_sem);
    } while(ret == -1 && errno == EINTR);
}

static void release_dirty_block_slot() {
    if(dirty_blocks_limit.load() <= 0) {
        return;
    }
    sem_post(&dirty_blocks_sem);
}

//static_assert(BLOCKLEN > INLINE_DLEN, "blocklen should not bigger than inline date length");

void writeback_thread(bool* done){
    while(!*done){
        usleep(100000);
        std::vector<std::pair<std::shared_ptr<block_t>, filekey>> snapshot;
        pthread_mutex_lock(&dblocks_lock);
        if(dblocks.empty()){
            pthread_mutex_unlock(&dblocks_lock);
            continue;
        }
        snapshot.reserve(dblocks.size());
        for(auto it = dblocks.begin(); it != dblocks.end();){
            auto b = it->first.lock();
            if(b == nullptr){
                it = dblocks.erase(it);
                continue;
            }
            snapshot.emplace_back(b, it->second);
            ++it;
        }
        pthread_mutex_unlock(&dblocks_lock);

        int staled_threshold = 30; // seconds
        if(upool->tasks_in_queue() == 0 && snapshot.size() >= UPLOADTHREADS){
            staled_threshold = 5;
        }

        struct candidate_t {
            std::shared_ptr<block_t> block;
            filekey fileat;
            int staled;
        };
        std::vector<candidate_t> candidates;
        candidates.reserve(snapshot.size());
        for(const auto& item : snapshot){
            int staled = item.first->staled();
            if(staled < staled_threshold){
                continue;
            }
            candidates.push_back(candidate_t{item.first, item.second, staled});
        }
        if(candidates.empty()){
            continue;
        }
        std::sort(candidates.begin(), candidates.end(),
                  [](const candidate_t& a, const candidate_t& b) {
                      return a.staled > b.staled;
                  });

        size_t max_upload_queue = UPLOADTHREADS;
        size_t max_download_queue = DOWNLOADTHREADS;
        size_t upload_queue = upool->tasks_in_queue();
        size_t download_queue = dpool->tasks_in_queue();
        size_t upload_avail = (upload_queue >= max_upload_queue) ? 0 : (max_upload_queue - upload_queue);
        size_t download_avail = (download_queue >= max_download_queue) ? 0 : (max_download_queue - download_queue);
        if(upload_avail == 0 && download_avail == 0){
            continue;
        }

        std::vector<std::weak_ptr<block_t>> pushed_blocks;
        for(const auto& item : candidates){
            if(upload_avail == 0 && download_avail == 0){
                break;
            }
            if(!item.block->full_cached()) {
                if(download_avail == 0){
                    continue;
                }
                dpool->submit_fire_and_forget([block = std::weak_ptr<block_t>(item.block)]{
                    block_t::pull(block, false);
                });
                download_avail--;
            } else {
                if(upload_avail == 0){
                    continue;
                }
                upool->submit_fire_and_forget([block = std::weak_ptr<block_t>(item.block), fileat = item.fileat]{
                    block_t::push(block, fileat);
                });
                pushed_blocks.push_back(item.block);
                upload_avail--;
            }
        }

        if(!pushed_blocks.empty()){
            pthread_mutex_lock(&dblocks_lock);
            for(const auto& block : pushed_blocks){
                auto it = dblocks.find(block);
                if(it != dblocks.end()){
                    dblocks.erase(it);
                }
            }
            pthread_mutex_unlock(&dblocks_lock);
        }
    }
}

static void merge_range(std::vector<Range>& ranges, uint32_t start, uint32_t end) {
    if(ranges.size() == 1 && ranges.front().start == 0 && ranges.front().end == 0) {
        ranges.clear();
    }
    // ranges 已保持有序且不重叠（(start, end] 语义）
    std::vector<Range> merged;
    merged.reserve(ranges.size() + 1);
    bool inserted = false;
    for(const auto& r : ranges) {
        if(end < r.start && !inserted) {
            merged.push_back({start, end});
            inserted = true;
        }
        if(inserted || end < r.start || start > r.end) {
            merged.push_back(r);
        } else {
            start = std::min(start, r.start);
            end = std::max(end, r.end);
        }
    }
    if(!inserted) {
        merged.push_back({start, end});
    }
    ranges.swap(merged);
}

block_t::block_t(const fileInfo& fi, filekey fk, size_t no, off_t offset, size_t size, unsigned int flags):
    fi(fi),
    fk(basename(fk)),
    no(no),
    offset(offset),
    size(size),
    flags(flags),
    atime(0)
{
    assert(opt.no_cache || fi.fd >= 0);

    std::string fk_private = fm_private_key_tostring(fk.private_key);
    if(fk.path == "" && fk_private[0] == '\0') {
        this->fk.path = "x";
    }
    if(this->fk.path == "x") {
        this->ranges = std::vector<Range>{{0, (uint32_t)size}};
    }
    // 检查数据库中的sync状态，如果已同步则设置BLOCK_SYNC标志
    struct block_record record;
    if(!load_block_from_db(fi.inode, no, record)) {
        return;
    }

    // 检查btime确定文件身份，如果btime不匹配则说明文件已被替换
    if(record.btime != 0 && record.btime != fi.btime) {
        infolog("btime mismatch for inode=%ju, no=%zu: db=%lu, file=%lu, treating as new file\n",
                fi.inode, no, record.btime, fi.btime);
        // btime不匹配，说明是不同的文件，不使用数据库中的记录
        return;
    }

    if(this->ranges.size() == 1 && this->ranges.front().start == 0 && this->ranges.front().end == 0) {
        //这个是老数据，表示全缓存
        this->ranges = std::vector<Range>{{0, (uint32_t)size}};
    } else {
        this->ranges = std::move(record.ranges);
    }
    if (record.dirty) {
        this->flags |= BLOCK_DIRTY;
        acquire_dirty_block_slot();
    }
    if(record.private_key != fk_private) {
        if(!record.private_key.empty() && !record.path.empty()){
            infolog("load block from db: %s, inode=%ju, no=%d\n", record.path.c_str(), fi.inode, no);
            //数据库中blocks表保存的信息比files表更可靠,因为正常流程是先保存blocks再更新files
            trim(getkey());
            this->fk.path = record.path;
            this->fk.private_key = fm_get_private_key(record.private_key.c_str());
        } else {
            //数据库中blocks表保存的信息不完整，重新上传
            this->flags |= BLOCK_DIRTY;
            acquire_dirty_block_slot();
        }
    }
}

//两种情况下会被调用
// 1. file被销毁了，一般是 drop_mem_cache, BLOCK_STALE标记
// 2. file被truncate了，这个时候需要删除数据
block_t::~block_t() {
    if(flags & BLOCK_DIRTY) {
        release_dirty_block_slot();
    }
    if((flags & BLOCK_STALE) == 0) {
        trim(getkey());
    }
}

std::string block_t::getpath() const {
    return getRemotePathFromFd(fi.fd);
}

filekey block_t::getkey() const {
    if(fk.path.empty()) {
        return filekey{getpath(), fk.private_key};
    } else if(opt.flags & FM_RENAME_NOTSUPPRTED){
        return filekey{pathjoin(".objs", fk.path), fk.private_key};
    } else {
        return filekey{pathjoin(encodepath(getpath(), file_encode_suffix), fk.path), fk.private_key};
    }
}

int block_t::pull(std::weak_ptr<block_t> wb, bool wait) {
    auto b = wb.lock();
    if(b == nullptr) {
        return -ENOENT;
    }
    b->wlock();
    if((b->flags & BLOCK_STALE) || b->full_cached()){
        b->unwlock();
        return 0;
    }
    if((b->flags & BLOCK_PULLING) && !wait) {
        b->unwlock();
        return -EBUSY;
    }
    while(b->flags & BLOCK_PULLING){
        b->pull_cond.wait_write(b);
    }

    off_t startp = b->fk.path.size() ? 0 : b->offset;
    size_t size = b->size;
    filekey file = b->getkey();
    b->flags |= BLOCK_PULLING;
    b->unwlock();
    buffstruct bs((char*)malloc(size), size);
    //for chunk file, read from begin
    int ret = HANDLE_EAGAIN(fm_download(file, startp, size, bs));
    auto_wlock(b);
    b->flags &= ~BLOCK_PULLING;
    b->pull_cond.notify_all();
    if(ret){
        return ret;
    }
    // 直接写入缓存文件
    if(b->flags & FILE_ENCODE_F){
        xorcode(bs.mutable_data(), b->offset, bs.size(), opt.secret);
    }

    for(const auto& r : b->ranges) {
        pread(b->fi.fd, (char*)bs.mutable_data() + r.start, r.end - r.start, b->offset + r.start);
    }

    ret = TEMP_FAILURE_RETRY(pwrite(b->fi.fd, bs.mutable_data(), bs.size(), b->offset));
    if(ret >= 0){
        //这里因为没有执行sync操作，进程异常退出不会有问题，但是os crash的话，数据会有不一致的情况
        assert((size_t)ret == bs.size());
        b->ranges = std::vector<Range>{{0, (uint32_t)b->size}};
        //save_block_to_db(b->fi, b->no, b->fk, false);
        save_block_to_db(block_record{
            b->fi.inode,
            b->no,
            b->fi.btime,
            b->fk.path,
            fm_private_key_tostring(b->fk.private_key),
            (b->flags & BLOCK_DIRTY) != 0,
            b->ranges
         });
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
    fileat.private_key = fk.private_key;
    size_t got = 0;
    int ret = download_block_common(fileat, fk, no, size, offset, len, flags & FILE_ENCODE_F, (char*)buff, got);
    if(ret < 0) {
        return ret;
    }
    return got;
}

int block_t::push(std::weak_ptr<block_t> wb, filekey fileat) {
    auto b = wb.lock();
    if(b == nullptr) {
        return -ENOENT;
    }
    size_t version = 0;
    char *buff = (char*)malloc(b->size);
    defer(free, buff);
    size_t len = 0;
    {
        auto_rlock(b);
        version = b->version;
        if((b->flags & BLOCK_DIRTY) == 0 ){
            return 0;
        }
        if((b->flags & (BLOCK_STALE | BLOCK_PULLING)) || !b->full_cached()){
            return -EAGAIN;
        }
        int ret = TEMP_FAILURE_RETRY(pread(b->fi.fd, buff, b->size, b->offset));
        if(ret < 0){
            return ret;
        }
        len = ret;

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
    }
    filekey file;
    if(len){
        //It must be chunk file, because native file can't be written
retry:
        file = makeChunkBlockKey(b->no);
        int ret = HANDLE_EAGAIN(fm_upload(fileat, file, buff, len, false));
        if(ret != 0 && errno == EEXIST){
            goto retry;
        }
        if(ret != 0){
            errorlog("fm_upload IO Error %s: %s\n", file.path.c_str(), strerror(-ret));
            return ret;
        }
    }else{
        file = filekey{"x", 0};
    }
    auto stripfile = basename(file);
    auto_wlock(b);
    if (version != b->version || (b->flags & BLOCK_STALE)) {
        infolog("%s version: %zd vs %zd, flags: %x\n", stripfile.path.c_str(), version, b->version.load(), b->flags);
        trim(file);
        return -EAGAIN;
    }
    if (len == 0 && b->fi.fd >= 0) {
        fallocate(b->fi.fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, b->offset, b->size);
    }
    // 上传成功，清除dirty标记
    if(save_block_to_db(block_record{
        b->fi.inode,
        b->no,
        b->fi.btime,
        stripfile.path,
        fm_private_key_tostring(stripfile.private_key),
        false,
        b->ranges
     }) == 0) {
        trim(b->getkey());
        b->fk = stripfile;
        b->flags &= ~BLOCK_DIRTY;
        release_dirty_block_slot();
        return 0;
    } else {
        errorlog("save %s failed: %ju, %d\n", stripfile.path.c_str(), b->fi.inode, b->no);
        trim(file);
        return -EIO;
    }
}

int block_t::prefetch(uint32_t start, uint32_t end, bool wait) {
    atime = time(nullptr);
    if(wait) {
        auto_rlock(this);
        // check range
        for (const auto& r : ranges) {
            if (start >= r.start && end <= r.end) {
                return 0;
            }
        }
        if((flags & BLOCK_STALE)){
            return 0; // 已经被释放，不预取
        }
        __r.unlock();
        return pull(weak_from_this(), true);
    } else {
        if(dpool->tasks_in_queue() > DOWNLOADTHREADS){
            return 1;
        }
        if(tryrlock() != 0) {
            return 1; // 已经有写锁，说明正在被修改，不需要预取
        }
        defer([this] { unrlock(); });
        if((flags & BLOCK_STALE) || full_cached()) {
            return 0; // 已经同步，不需要预取
        }
        dpool->submit_fire_and_forget([b = weak_from_this()]{
            pull(b, false);
        });
        return 1;
    }
}

void block_t::markdirty(filekey fileat, uint32_t start, uint32_t end) {
    version++;
    atime = time(nullptr);
    auto_wlock(this);
    if(flags & BLOCK_STALE) {
        return;
    }
    merge_range(ranges, start, end);
    // 保存ranges到数据库并标记为dirty
    save_block_to_db(block_record{
        fi.inode,
        no,
        fi.btime,
        fk.path,
        fm_private_key_tostring(fk.private_key),
        true,
        ranges
    });

    if(flags & BLOCK_DIRTY) {
        return;
    }
    flags |=  BLOCK_DIRTY;
    __w.unlock();
    pthread_mutex_lock(&dblocks_lock);
    dblocks.emplace(weak_from_this(), fileat);
    pthread_mutex_unlock(&dblocks_lock);
    acquire_dirty_block_slot();
}

void block_t::markstale() {
    auto_wlock(this);
    flags |= BLOCK_STALE;
}

bool block_t::full_cached() {
    auto_rlock(this);
    if(ranges.size() == 1 && ranges.front().start == 0 && ranges.front().end == (uint32_t)size) {
        return true;
    }
    return false;
}

//return if is dirty after sync
bool block_t::sync(filekey fileat, bool wait){
    auto_rlock(this);
    if ((flags & BLOCK_DIRTY) == 0 || (flags & BLOCK_STALE)) {
        return false;
    }
    __r.unlock();
    if(wait) {
        if(pull(weak_from_this(), true) < 0) return true;
        if(push(weak_from_this(), fileat) < 0) return true;
        assert((flags & BLOCK_DIRTY) == 0);
        return false;
    }
    pthread_mutex_lock(&dblocks_lock);
    dblocks.emplace(weak_from_this(), fileat);
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
    if(ranges.empty() || (flags & (BLOCK_DIRTY  | BLOCK_PULLING | BLOCK_STALE)) || fi.fd < 0 || staled() < 60) {
        return 0;
    }
    atime = time(nullptr);
    fallocate(fi.fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, size);
    delete_block_from_db(fi.inode, no);
    debuglog("release block %s/%lu[%zd-%zd]\n", getpath().c_str(), no, offset, offset+size);
    flags = flags & FILE_ENCODE_F;
    if(fk.path == "x") {
        ranges = std::vector<Range>{{0, (uint32_t)size}};
        return 0;
    } else {
        ranges.clear();
        return size;
    }
}
