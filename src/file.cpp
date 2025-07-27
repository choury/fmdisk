#include "common.h"
#include "fmdisk.h"
#include "file.h"
#include "dir.h"
#include "threadpool.h"
#include "defer.h"
#include "sqlite.h"

#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <semaphore.h>
#include <fcntl.h>

#include <list>
#include <random>
#include <algorithm>
#include <dirent.h>


static pthread_mutex_t gcLocker = PTHREAD_MUTEX_INITIALIZER;
static std::vector<filekey> droped;
static std::set<ino_t> opened_inodes;
static std::atomic<bool> gc_stop(false);

// 缓存文件信息，用于GC时扫描
struct cache_file_info {
    string path;
    ino_t inode;   // inode号，用于删除blocks表记录
    time_t atime;  // 访问时间
    size_t size;    // 磁盘占用大小

    cache_file_info(const string& p, ino_t i, time_t a, size_t s) : path(p), inode(i), atime(a), size(s){}

    // 按访问时间排序，最旧的在前
    bool operator<(const cache_file_info& other) const {
        return atime < other.atime;
    }
};

// 扫描缓存目录，获取所有缓存文件的信息
static std::vector<cache_file_info> scan_cache_directory() {
    std::vector<cache_file_info> cache_files;
    string cache_dir = pathjoin(opt.cache_dir, "cache");

    // 递归扫描缓存目录
    std::function<void(const string&)> scan_dir = [&](const string& dir) {
        DIR* d = opendir(dir.c_str());
        if (!d) return;

        defer([d]() { closedir(d); });

        struct dirent* entry;
        while ((entry = readdir(d)) != nullptr) {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
                continue;
            }

            string full_path = pathjoin(dir, entry->d_name);
            struct stat st;
            if (stat(full_path.c_str(), &st) == 0) {
                if (S_ISDIR(st.st_mode)) {
                    scan_dir(full_path);  // 递归扫描子目录
                } else if (S_ISREG(st.st_mode)) {
                    // 常规文件，记录其信息, 实际磁盘占用
                    cache_files.emplace_back(full_path, st.st_ino, st.st_atime, st.st_blksize * 512);
                }
            }
        }
    };

    scan_dir(cache_dir);
    return cache_files;
}


// 清理缓存直到满足大小限制
static bool cleanup_cache_by_size() {
    if (opt.cache_size < 0) {
        return false;  // 不限制缓存大小
    }

    auto_lock(&gcLocker);

    // 扫描缓存目录获取所有文件信息
    std::vector<cache_file_info> cache_files = scan_cache_directory();

    // 计算总大小
    off_t total_size = 0;
    for (const auto& file : cache_files) {
        total_size += file.size;
    }

    if (total_size <= opt.cache_size) {
        return false;  // 未超过限制
    }

    // 按访问时间排序，最旧的在前
    std::sort(cache_files.begin(), cache_files.end());

    // 删除最旧的文件直到满足大小限制
    off_t current_size = total_size;

    bool success = false;
    for (const auto& file : cache_files) {
        if (current_size <= opt.cache_size) {
            break;
        }
        if(opened_inodes.count(file.inode) > 0) {
            continue; // 跳过正在使用的文件
        }

        // 删除对应的blocks表记录
        delete_blocks_from_db(file.inode);

        // 删除缓存文件
        unlink(file.path.c_str());
        current_size -= file.size;
        success = true;
    }
    return success;
}

void trim(const filekey& file) {
    string name = basename(file.path);
    if(name == "" || name == "x"){
        return;
    }
    auto_lock(&gcLocker);
    droped.push_back(file);
}

static void trim(const std::vector<filekey>& files) {
    auto_lock(&gcLocker);
    droped.insert(droped.end(), files.begin(), files.end());
}

void start_gc() {
    while(!gc_stop) {
        bool haswork = false;
        pthread_mutex_lock(&gcLocker);
        //copy dropped and clear
        std::vector<filekey> tmp = std::move(droped);
        droped.clear();
        pthread_mutex_unlock(&gcLocker);

        if(!tmp.empty()){
            // 先清理数据库中的block记录
            delete_blocks_by_key(tmp);
            HANDLE_EAGAIN(fm_batchdelete(std::move(tmp)));
            haswork = true;
        }

        // 定期检查缓存大小并清理
        if (opt.cache_size >= 0 && cleanup_cache_by_size()) {
            haswork = true;
        }

        if(haswork) {
            sleep(1);  // 有工作时每秒检查一次
        }else {
            //wait 30s
            sleep(30);
        }
    }
}

void stop_gc() {
    gc_stop = true;
}

static int persistent_cache_file(const string& remote_path) {
    string cache_path = get_cache_path(remote_path);

    // 确保目录存在
    string cache_dir = dirname(cache_path);
    if(create_dirs_recursive(cache_dir) != 0) {
        fprintf(stderr, "create dirs failed for %s: %s\n", cache_dir.c_str(), strerror(errno));
        return -1;
    }

    // 尝试打开现有文件，如果不存在则创建
    int fd = open(cache_path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd == -1) {
        fprintf(stderr, "open cache file failed %s: %s\n", cache_path.c_str(), strerror(errno));
        return -1;
    }
    return fd;
}

//计算某位置在哪个块中,从0开始计数,分界点算在前一个块中
inline size_t GetBlkNo(size_t p, blksize_t blksize) {
    assert(blksize);
    if (p == 0)
        return 0;
    return (p - 1) / blksize;
}

file_t::file_t(dir_t *parent, const filemeta& meta):
    entry_t(parent, meta),
    private_key(meta.key.private_key),
    blksize(meta.blksize),
    last_meta_sync_time(0)
{
    if(flags & ENTRY_CHUNCED_F){
        fk = decodepath(fk);
    }
    if(flags & META_KEY_ONLY_F) {
        return;
    }
    if(flags & ENTRY_CREATE_F){
    //creata new file
        assert(flags & ENTRY_CHUNCED_F);
        assert(meta.size == 0);
        inline_data = new char[INLINE_DLEN];
        private_key = nullptr;
    }
}

file_t::~file_t() {
    auto_wlock(this);
    reset_wlocked();
    if(inline_data){
        delete[] inline_data;
    }
}

void file_t::reset_wlocked() {
    assert(opened == 0  && (flags & FILE_DIRTY_F) == 0);
    for(auto i: blocks){
        i.second->markstale();
        delete i.second;
    }
    blocks.clear();
    if(fd < 0) {
        return;
    }
    close(fd);
    fd = -1;
    auto_lock(&gcLocker);
    opened_inodes.erase(inode);
}

int file_t::open(){
    auto_wlock(this);
    if((flags & ENTRY_INITED_F) == 0){
        pull_wlocked();
    }
    opened++;
    if(fd >= 0) {
        return 0;
    }
    assert(opened == 1 && blocks.empty());
    fd = persistent_cache_file(fk.path);
    TEMP_FAILURE_RETRY(ftruncate(fd, length));
    struct stat st;
    fstat(fd, &st);
    inode = st.st_ino;
    opened_inodes.insert(inode);
    if(flags & ENTRY_CHUNCED_F) {
        auto fblocks = getfblocks();
        assert(fblocks.size() == GetBlkNo(length, blksize)+1 || (fblocks.size() == 0 && length <= INLINE_DLEN));
        for(size_t i = 0; i < fblocks.size(); i++){
            blocks.emplace(i, new block_t(fd, inode, fblocks[i], i, blksize * i, blksize, flags & FILE_ENCODE_F));
        }
    } else {
        for(size_t i = 0; i <= GetBlkNo(length, blksize); i++ ){
            blocks.emplace(i, new block_t(fd, inode, filekey{"", private_key}, i, blksize * i, blksize, 0));
        }
    }
    return 0;
}

void file_t::clean(file_t* file) {
    auto_wlock(file);
    file->flags &= ~ENTRY_REASEWAIT_F;
    if(file->opened > 0){
        return;
    }
    if(file->flags & ENTRY_DELETED_F){
        if(opt.flags & FM_DELETE_NEED_PURGE) {
            trim(file->getkeys());
        }else{
            trim(file->getkey());
        }
        __w.unlock();
        delete file;
    } else {
        file->reset_wlocked();
    }
}

int file_t::release(){
    auto_wlock(this);
    opened--;
    if(opened > 0) {
        return 0;
    }

    flags |= ENTRY_REASEWAIT_F;
    add_delay_job((taskfunc)clean, this, 60);
    return 0;
}

void file_t::pull_wlocked() {
    filemeta meta;
    entry_t::pull_wlocked(meta);
    private_key = meta.key.private_key;
    blksize = meta.blksize;
    flags |= meta.flags;
    mode &= ~0777;
    mode |= (meta.mode & 0777);
    if(meta.inline_data){
        assert(meta.size < (size_t)meta.blksize);
        inline_data = (char*)meta.inline_data;
        //blocks.emplace(0, new block_t(this, filekey{"x", 0}, 0, 0, blksize, BLOCK_SYNC));
    }
}

int file_t::read(void* buff, off_t offset, size_t size) {
    auto_rlock(this);
    assert(opened);
    if((size_t)offset > length){
        return -EFAULT;
    }
    if(offset + size > length){
        size = length - offset;
    }
    if(inline_data){
        memcpy(buff, inline_data + offset, size);
        return size;
    }
    size_t startc = GetBlkNo(offset, blksize);
    size_t endc = GetBlkNo(offset + size, blksize);
    for(size_t i = startc; i< endc + DOWNLOADTHREADS/ 2 && i<= GetBlkNo(length, blksize); i++){
        blocks[i]->prefetch(false);
    }
    for(size_t i = startc; i<= endc; i++ ){
        blocks[i]->prefetch(true);
    }
    return pread(fd, buff, size, offset);
}

int file_t::truncate_rlocked(off_t offset){
    if((size_t)offset == length){
        return 0;
    }
    size_t newc = GetBlkNo(offset, blksize);
    size_t oldc = GetBlkNo(length, blksize);
    if(newc > oldc){
        if(newc >= MAXFILE){
            errno = EFBIG;
            return -1;
        }
        upgrade();
        for(size_t i = oldc + 1; i<= newc; i++){
            blocks.emplace(i, new block_t(fd, inode, filekey{"x", 0}, i, blksize * i, blksize, BLOCK_SYNC | (flags & FILE_ENCODE_F)));
        }
    }else if(oldc >= newc && inline_data == nullptr){
        blocks[newc]->prefetch(true);
        if((flags & ENTRY_DELETED_F) == 0) blocks[newc]->markdirty();
        upgrade();
        for(size_t i = newc + 1; i<= oldc; i++){
            delete blocks[i];
            blocks.erase(i);
        }
    }else{
        upgrade();
    }
    defer(&file_t::downgrade, dynamic_cast<locker*>(this));
    if(inline_data && (offset >= (off_t)INLINE_DLEN)){
        int ret = pwrite(fd, inline_data, length, 0);
        if(ret < 0){
            return ret;
        }
        assert(blocks.count(0) == 0);
        blocks.emplace(0, new block_t(fd, inode, filekey{"x", 0}, 0, 0, blksize, BLOCK_SYNC | (flags & FILE_ENCODE_F)));
        delete[] inline_data;
        inline_data = nullptr;
        if((flags & ENTRY_DELETED_F) == 0) blocks[0]->markdirty();
    }
    length = offset;
    if(length == 0 && inline_data == nullptr){
        inline_data = new char[INLINE_DLEN];
    }
    flags |= FILE_DIRTY_F;
    assert(fd >= 0);
    return TEMP_FAILURE_RETRY(ftruncate(fd, offset));
}

int file_t::truncate(off_t offset){
    auto_rlock(this);
    assert(opened);
    if((flags & ENTRY_CHUNCED_F) == 0){
        return -EPERM;
    }
    int ret = truncate_rlocked(offset);
    if(ret < 0){
        return -errno;
    }
    mtime = time(NULL);
    if(mtime - last_meta_sync_time >= 60) {
        last_meta_sync_time = mtime;
        addtask(upool, (taskfunc)file_t::upload_meta_async_task, this, 0);
    }
    return 0;
}

int file_t::write(const void* buff, off_t offset, size_t size) {
    auto_rlock(this);
    assert(opened);
    if((flags & ENTRY_CHUNCED_F) == 0){
        return -EPERM;
    }
    if((size_t)offset + size > length){
        int ret = truncate_rlocked(offset + size);
        if(ret < 0){
            return ret;
        }
    }
    assert((!inline_data) || (inline_data && length < INLINE_DLEN));
    const size_t startc = GetBlkNo(offset, blksize);
    const size_t endc = GetBlkNo(offset + size, blksize);
    if(inline_data == nullptr) {
        for(size_t i = startc; i <= endc; i++){
            blocks[i]->prefetch(true);
        }
    }
    assert(fd >= 0);
    int ret = pwrite(fd, buff, size, offset);
    if(ret < 0){
        return ret;
    }
    if(inline_data){
        __r.upgrade();
        memcpy(inline_data + offset, buff, size);
    }else if(flags & ENTRY_DELETED_F) {
        __r.upgrade();
    } else {
        for(size_t i =  startc; i <= endc; i++){
            blocks[i]->markdirty();
        }
        __r.upgrade();
    }
    flags |= FILE_DIRTY_F;
    mtime = time(NULL);
    if(mtime - last_meta_sync_time >= 60) {
        last_meta_sync_time = mtime;
        addtask(upool, (taskfunc)file_t::upload_meta_async_task, this, 0);
    }
    return ret;
}

int file_t::remove_and_release_wlock() {
    // 删除持久化缓存文件
    string cache_path = get_cache_path(getcwd());
    if(unlink(cache_path.c_str()) != 0 && errno != ENOENT) {
        fprintf(stderr, "failed to unlink cache file %s: %s\n", cache_path.c_str(), strerror(errno));
    }
    flags |= ENTRY_DELETED_F;
    if(opened || (flags & ENTRY_REASEWAIT_F)) {
        //do this in clean
        return 0;
    }
    if(opt.flags & FM_DELETE_NEED_PURGE) {
        HANDLE_EAGAIN(fm_batchdelete(getkeys()));
    }else{
        HANDLE_EAGAIN(fm_delete(getkey()));
    }
    return 1;
}

std::vector<filekey> file_t::getfblocks(){
    auto_rlock(this);
    if(inline_data){
        return {};
    }
    assert(flags & ENTRY_INITED_F);
    if(blocks.empty()) {
        filemeta meta;
        std::vector<filekey> fblocks;
        load_file_from_db(getkey().path, meta, fblocks);
        return fblocks;
    }
    std::vector<filekey> fblocks(blocks.size());
    for(auto i : this->blocks){
        if(i.second->dummy()){
            continue;
        }
        fblocks[i.first] = std::get<0>(i.second->getmeta());
    }
    return fblocks;
}

filekey file_t::getmetakey(){
    auto_rlock(this);
    return filekey{METANAME, private_key};
}

std::vector<filekey> file_t::getkeys() {
    auto_rlock(this);
    std::vector<filekey> flist = getfblocks();
    flist.emplace_back(filekey{METANAME, private_key});
    if(fk.private_key == nullptr){
        return flist;
    }
    string path;
    if(flags & ENTRY_CHUNCED_F){
        path = encodepath(getcwd());
    }else{
        path = getcwd();
    }
    flist.emplace_back(filekey{path, fk.private_key});
    return flist;
}



filemeta file_t::getmeta() {
    auto_rlock(this);
    if((flags & ENTRY_INITED_F) == 0){
        __r.upgrade();
        pull_wlocked();
    }
    filemeta meta = initfilemeta(getkey());
    meta.mode = S_IFREG | (mode & 0777);
    meta.inline_data = (unsigned char*)inline_data;
    meta.flags = flags;
    meta.size = length;
    meta.blksize = blksize;
    if(inline_data) {
        meta.blocks = length *3 / 2 / 512 + 1; //base64 encode
    }else if(block_size > 0 && (flags & FILE_DIRTY_F) == 0){
        //sync will call getmeta before clear FILE_DIRTY_F
        //so we no need to recalculate the block size
        meta.blocks = block_size;
    }else {
        meta.blocks = 1; //at least for meta.json
        for(auto i: blocks){
            if(i.second->dummy()){
                continue;
            }
            if(i.first == GetBlkNo(length, blksize)){
                break; // last block not full
            }
            meta.blocks += blksize / 512;
        }
        meta.blocks += (length % blksize) / 512 + 1;
        block_size = meta.blocks;
    }
    meta.ctime = ctime;
    meta.mtime = mtime;
    return meta;
}

int file_t::sync(int dataonly){
    auto_rlock(this);
    if(flags & ENTRY_DELETED_F) {
        return 0;
    }
    assert(flags & ENTRY_INITED_F);
    if((flags & ENTRY_CHUNCED_F) == 0 || (flags & FILE_DIRTY_F) == 0){
        return 0;
    }
    for(auto i: blocks){
        i.second->sync();
    }
    if(!dataonly){
        const filekey& key = getkey();
        filemeta meta = getmeta();
        meta.key = getmetakey();
        std::vector<filekey> fblocks = getfblocks();
        if(upload_meta(key, meta, fblocks)){
            throw "upload_meta IO Error";
        }
        save_file_to_db(key.path, meta, fblocks);
        __r.upgrade();
        private_key = meta.key.private_key;
        flags &= ~FILE_DIRTY_F;
    }
    return 0;
}

int file_t::utime(const struct timespec tv[2]) {
    auto_rlock(this);
    if(flags & ENTRY_DELETED_F) {
        return 0;
    }
    if((flags & ENTRY_INITED_F) == 0){
        __r.upgrade();
        pull_wlocked();
    }
    const filekey& key = getkey();
    std::vector<filekey> fblocks = getfblocks();
    filemeta meta = getmeta();
    meta.key = getmetakey();
    meta.mtime = tv[0].tv_sec;
    meta.ctime = tv[1].tv_sec;
    if(flags & ENTRY_CHUNCED_F){
        int ret = upload_meta(key, meta, fblocks);
        if(ret){
            return -errno;
        }
        private_key = meta.key.private_key;
    } else {
        int ret = HANDLE_EAGAIN(fm_utime(getkey(), tv));
        if(ret){
            return ret;
        }
    }
    mtime = tv[0].tv_sec;
    ctime = tv[1].tv_sec;
    save_file_to_db(key.path, meta, fblocks);
    return 0;
}

void file_t::dump_to_disk_cache(){
    auto_rlock(this);
    if(flags & ENTRY_DELETED_F) {
        return;
    }
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
    save_file_to_db(meta.key.path, meta, getfblocks());
}

int file_t::drop_mem_cache() {
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
    reset_wlocked();
    if(inline_data){
        delete[] inline_data;
    }
    inline_data = nullptr;
    flags &= ~ENTRY_INITED_F;
    return 0;
}

void file_t::upload_meta_async_task(file_t* file) {
    if(file->tryrlock()) {
        return;  // 获取不到锁，放弃本次上传
    }
    defer(&file_t::unrlock, dynamic_cast<locker*>(file));

    try {
        if((file->flags & ENTRY_DELETED_F) || (file->flags & FILE_DIRTY_F) == 0) {
            return;
        }

        const filekey& key = file->getkey();
        filemeta meta = file->getmeta();
        meta.key = file->getmetakey();
        std::vector<filekey> fblocks = file->getfblocks();

        if(upload_meta(key, meta, fblocks) != 0) {
            return;
        }
        save_file_to_db(key.path, meta, fblocks);
    } catch(...) {
        // 如果上传失败，时间戳不更新，下次check时会重新尝试
    }
}
