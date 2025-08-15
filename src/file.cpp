#include "common.h"
#include "fmdisk.h"
#include "file.h"
#include "dir.h"
#include "trdpool.h"
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

// 清理缓存直到满足大小限制
static bool cleanup_cache_by_size() {
    if (opt.cache_size < 0) {
        return false;  // 不限制缓存大小
    }

    auto_lock(&gcLocker);

    // 扫描缓存目录获取所有文件信息
    std::vector<cache_file_info> cache_files = scan_cache_directory();

    // 计算总大小（磁盘占用）
    off_t total_size = 0;
    for (const auto& file : cache_files) {
        total_size += file.st.st_blocks * 512;
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
        if(opened_inodes.contains(file.st.st_ino)) {
            continue; // 跳过正在使用的文件
        }

        // 检查文件是否有dirty标记（从数据库加载）
        if(!file.remote_path.empty()) {
            filemeta meta{};
            std::vector<filekey> fblocks;
            load_file_from_db(encodepath(file.remote_path), meta, fblocks);
            if(meta.blksize == 0) {
                load_file_from_db(file.remote_path, meta, fblocks);
            }

            if(meta.flags & FILE_DIRTY_F) {
                continue; // 跳过有dirty标记的文件
            }
        }

        // 删除对应的blocks表记录
        delete_blocks_from_db(file.st.st_ino);

        // 删除缓存文件
        unlink(file.path.c_str());
        current_size -= file.st.st_blocks * 512;
        success = true;
    }
    return success;
}

void trim(const filekey& file) {
    string name = basename(file.path);
    if(name.empty() || name == "x"){
        return;
    }
    auto_lock(&gcLocker);
    droped.push_back(file);
}

static void trim(const std::vector<filekey>& files) {
    auto_lock(&gcLocker);
    droped.insert(droped.end(), files.begin(), files.end());
}

// 恢复dirty数据并重新上传
void recover_dirty_data(dir_t* root) {
    std::vector<std::string> dirty_files;
    if(get_dirty_files(dirty_files) <= 0) {
        return;
    }
    fprintf(stderr, "Recovering %zu dirty files from previous session\n", dirty_files.size());

    for(const auto& file_path : dirty_files) {
        string path = decodepath(file_path);
        file_t* file = dynamic_cast<file_t*>(root->find(path));
        if(file == nullptr) {
            fprintf(stderr, "File %s not found in cache, skipping\n", file_path.c_str());
            continue;
        }
        file->open();
        file->release();
    }
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
        fk = std::make_shared<filekey>(decodepath(*fk.load()));
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
    flags |= ENTRY_DELETED_F;
    reset_wlocked();
    delete[] inline_data;
}

string file_t::getrealname() {
    if(flags & ENTRY_CHUNCED_F) {
        return encodepath(fk.load()->path);
    }
    return fk.load()->path;
}

void file_t::reset_wlocked() {
    assert(opened == 0  && ((flags & FILE_DIRTY_F) == 0 || (flags & ENTRY_DELETED_F)));
    for(auto it = blocks.rbegin(); it != blocks.rend(); ++it) {
        it->second->markstale();
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
    fd = persistent_cache_file(getcwd());
    TEMP_FAILURE_RETRY(ftruncate(fd, length));
    struct stat st;
    fstat(fd, &st);
    inode = st.st_ino;
    opened_inodes.insert(inode);
    if(flags & ENTRY_CHUNCED_F) {
        auto fblocks = getfblocks();
        assert(fblocks.size() == GetBlkNo(length, blksize)+1 || (fblocks.empty() && length <= INLINE_DLEN));
        for(size_t i = 0; i < fblocks.size(); i++){
            blocks.emplace(i, std::make_shared<block_t>(fd, inode, fblocks[i], i, blksize * i, blksize, flags & FILE_ENCODE_F));
        }
    } else {
        for(size_t i = 0; i <= GetBlkNo(length, blksize); i++ ){
            blocks.emplace(i, std::make_shared<block_t>(fd, inode, filekey{"", private_key}, i, blksize * i, blksize, 0));
        }
    }
    return 0;
}

void file_t::clean(file_t* file) {
    auto_wlock(file);
    if(file->opened > 0){
        file->flags &= ~ENTRY_REASEWAIT_F;
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
        return;
    }
    if (file->flags & FILE_DIRTY_F && file->sync_wlocked()) {
        add_delay_job((taskfunc)clean, file, 60);
        return;
    }
    file->flags &= ~ENTRY_REASEWAIT_F;
    file->reset_wlocked();
}

int file_t::release(){
    auto_wlock(this);
    opened--;
    if(opened > 0) {
        return 0;
    }

    flags |= ENTRY_REASEWAIT_F;
    add_delay_job((taskfunc)clean, this, 0);
    return 0;
}

void file_t::pull_wlocked() {
    filemeta meta{};
    entry_t::pull_wlocked(meta);
    private_key = meta.key.private_key;
    blksize = meta.blksize;
    flags |= meta.flags;
    mode &= ~0777;
    mode |= (meta.mode & 0777);
    if(meta.inline_data){
        assert(meta.size < (size_t)meta.blksize);
        inline_data = (char*)meta.inline_data;
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

int file_t::truncate_wlocked(off_t offset){
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
        for(size_t i = oldc + 1; i<= newc; i++){
            blocks.emplace(i, std::make_shared<block_t>(fd, inode, filekey{"x", 0}, i, blksize * i, blksize, BLOCK_SYNC | (flags & FILE_ENCODE_F)));
        }
    }else if(oldc >= newc && inline_data == nullptr){
        blocks[newc]->prefetch(true);
        if((flags & ENTRY_DELETED_F) == 0) blocks[newc]->markdirty();
        for(size_t i = newc + 1; i<= oldc; i++){
            blocks.erase(i);
        }
    }
    if(inline_data && (offset >= (off_t)INLINE_DLEN)){
        int ret = pwrite(fd, inline_data, length, 0);
        if(ret < 0){
            return ret;
        }
        assert(!blocks.contains(0));
        blocks.emplace(0, std::make_shared<block_t>(fd, inode, filekey{"x", 0}, 0, 0, blksize, BLOCK_SYNC | (flags & FILE_ENCODE_F)));
        delete[] inline_data;
        inline_data = nullptr;
        if((flags & ENTRY_DELETED_F) == 0) blocks[0]->markdirty();
    }
    length = offset;
    if(length == 0 && inline_data == nullptr){
        inline_data = new char[INLINE_DLEN];
    }
    if((flags & FILE_DIRTY_F) == 0){
        flags |= FILE_DIRTY_F;
        sync_wlocked(true);
    }
    assert(fd >= 0);
    return TEMP_FAILURE_RETRY(ftruncate(fd, offset));
}

int file_t::truncate(off_t offset){
    auto_wlock(this);
    assert(opened);
    if((flags & ENTRY_CHUNCED_F) == 0){
        return -EPERM;
    }
    int ret = truncate_wlocked(offset);
    if(ret < 0){
        return -errno;
    }
    mtime = time(nullptr);
    if(mtime - last_meta_sync_time >= 600) {
        last_meta_sync_time = mtime;
        upool->submit_fire_and_forget([this]{ upload_meta_async_task(this); });
    }
    return 0;
}

int file_t::write(const void* buff, off_t offset, size_t size) {
    auto_wlock(this);
    assert(opened);
    if((flags & ENTRY_CHUNCED_F) == 0){
        return -EPERM;
    }
    if((size_t)offset + size > length){
        int ret = truncate_wlocked(offset + size);
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
        memcpy(inline_data + offset, buff, size);
    }else if((flags & ENTRY_DELETED_F) == 0) {
        for(size_t i =  startc; i <= endc; i++){
            blocks[i]->markdirty();
        }
    }
    mtime = time(nullptr);
    if((flags & FILE_DIRTY_F) == 0){
        flags |= FILE_DIRTY_F;
        sync_wlocked(true);
    }else if(mtime - last_meta_sync_time >= 600) {
        last_meta_sync_time = mtime;
        upool->submit_fire_and_forget([this]{ upload_meta_async_task(this); });
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
        for(auto it = blocks.rbegin(); it != blocks.rend(); ++it) {
            it->second->markstale();
        }
        //do trim in clean
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
        filemeta meta{};
        std::vector<filekey> fblocks;
        load_file_from_db(getkey().path, meta, fblocks);
        return fblocks;
    }
    std::vector<filekey> fblocks(blocks.size());
    for(auto i : this->blocks){
        if(i.second->dummy()){
            continue;
        }
        fblocks[i.first] = i.second->getfk();
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
    if(fk.load()->private_key == nullptr){
        return flist;
    }
    string path;
    if(flags & ENTRY_CHUNCED_F){
        path = encodepath(getcwd());
    }else{
        path = getcwd();
    }
    flist.emplace_back(filekey{path, fk.load()->private_key});
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
        const auto fblocks = getfblocks();
        for(size_t i = 0; i < fblocks.size(); i++){
            if(fblocks[i].path == "x" || fblocks[i].path.empty()){
                continue;
            }
            if(i == GetBlkNo(length, blksize)){
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

bool file_t::sync_wlocked(bool forcedirty) {
    bool dirty = false;
    if(forcedirty) {
        dirty = true;
    } else {
        for(auto it = blocks.rbegin(); it != blocks.rend(); ++it) {
            dirty |= it->second->sync();
        }
    }
    assert(!dirty || (flags & FILE_DIRTY_F));
    const filekey& key = getkey();
    filemeta meta = getmeta();
    meta.key = getmetakey();
    std::vector<filekey> fblocks = getfblocks();
    if(upload_meta(key, meta, fblocks)){
        throw "upload_meta IO Error";
    }
    private_key = meta.key.private_key;
    if(!dirty){
        flags &= ~FILE_DIRTY_F;
        meta.flags = flags;
    }
    save_file_to_db(key.path, meta, fblocks);
    return dirty;
}


int file_t::sync(int dataonly){
    auto_wlock(this);
    if(flags & ENTRY_DELETED_F) {
        return 0;
    }
    assert(flags & ENTRY_INITED_F);
    if((flags & ENTRY_CHUNCED_F) == 0 || (flags & FILE_DIRTY_F) == 0){
        return 0;
    }
    fsync(fd);
    if(!dataonly) {
        dump_to_disk_cache(dirname(getcwd()), fk.load()->path);
    }
    return 0;
}

int file_t::utime(const struct timespec tv[2]) {
    auto_wlock(this);
    if(flags & ENTRY_DELETED_F) {
        return 0;
    }
    if((flags & ENTRY_INITED_F) == 0){
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

void file_t::dump_to_disk_cache(const std::string& path, const std::string& name) {
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
        savemeta.key.path = encodepath(name);
        savemeta.mode = S_IFDIR | 0755;
        save_entry_to_db(path, savemeta);
        meta.key.private_key = private_key;
        save_file_to_db(pathjoin(path, encodepath(name)), meta, getfblocks());
    }else{
        meta.key.path = name;
        save_entry_to_db(path, meta);
        save_file_to_db(pathjoin(path, name), meta, getfblocks());
    }
}

int file_t::drop_mem_cache() {
    auto_wlock(this);
    if(opened){
        return -EBUSY;
    }
    if((flags & ENTRY_REASEWAIT_F) || (flags & ENTRY_PULLING_F) || (flags & FILE_DIRTY_F)){
        return -EAGAIN;
    }
    if((flags & ENTRY_INITED_F) == 0){
        return 0;
    }
    reset_wlocked();
    if(inline_data){
        delete[] inline_data;
    }
    inline_data = nullptr;
    flags &= ~ENTRY_INITED_F;
    return 0;
}

void file_t::upload_meta_async_task(file_t* file) {
    if(file->trywlock()) {
        return;  // 获取不到锁，放弃本次上传
    }
    defer(&file_t::unwlock, dynamic_cast<locker*>(file));
    if((file->flags & ENTRY_DELETED_F) || (file->flags & FILE_DIRTY_F) == 0) {
        return;
    }
    file->sync_wlocked();
}
