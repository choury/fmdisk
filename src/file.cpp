#include "common.h"
#include "fmdisk.h"
#include "file.h"
#include "dir.h"
#include "trdpool.h"
#include "threadpool.h"
#include "defer.h"
#include "sqlite.h"
#include "utils.h"
#include "log.h"

#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <semaphore.h>
#include <fcntl.h>
#include <sys/xattr.h>

#include <list>
#include <random>
#include <algorithm>
#include <dirent.h>
#include <utility>
#include <thread>


static pthread_mutex_t droped_lock = PTHREAD_MUTEX_INITIALIZER;
static std::vector<filekey> droped;
static pthread_mutex_t openfile_lock = PTHREAD_MUTEX_INITIALIZER;
static std::map<ino_t, std::shared_ptr<file_t>> opened_inodes;
static std::atomic<bool> gc_stop(false);
static std::thread gc_thread;
static std::thread trim_thread;

// 清理缓存直到满足大小限制
static bool cleanup_cache_by_size() {
    if (opt.cache_size < 0) {
        return false;  // 不限制缓存大小
    }


    // 扫描缓存目录获取所有文件信息
    std::vector<cache_file_info> cache_files = scan_cache_directory();

    // 计算总大小（磁盘占用）
    off_t total_size = 0;
    for (const auto& file : cache_files) {
        total_size += file.st.stx_blocks * 512;
    }

    if (total_size <= opt.cache_size) {
        return false;  // 未超过限制
    }

    // 按访问时间排序，最旧的在前
    std::sort(cache_files.begin(), cache_files.end());

    // 删除最旧的文件直到满足大小限制
    off_t current_size = total_size;

    auto_lock(&openfile_lock);
    for (const auto& file : cache_files) {
        if (current_size <= opt.cache_size) {
            return false;
        }
        if(opened_inodes.contains(file.st.stx_ino)) {
            continue; // 跳过正在使用的文件
        }

        // 检查文件是否有dirty标记（从数据库加载）
        if(!file.remote_path.empty()) {
            filemeta meta{};
            std::vector<filekey> fblocks;
            load_file_from_db(encodepath(file.remote_path, file_encode_suffix), meta, fblocks);
            if(meta.blksize == 0) {
                load_file_from_db(file.remote_path, meta, fblocks);
            }

            if(meta.flags & FILE_DIRTY_F) {
                continue; // 跳过有dirty标记的文件
            }
        }

        infolog("drop cache: %s, inode=%d\n", file.path.c_str(), file.st.stx_ino);
        // 删除对应的blocks表记录
        delete_blocks_from_db(file.st.stx_ino);

        // 删除缓存文件
        unlink(file.path.c_str());
        current_size -= file.st.stx_blocks * 512;
    }
    if(opt.cache_size == 0) {
        return false;
    }
    //如果删完了还是不够,就清理打开文件的block
    std::vector<std::shared_ptr<file_t>> open_files;
    open_files.reserve(opened_inodes.size());
    for (auto& [inode, file] : opened_inodes) {
        open_files.push_back(file);
    }
    std::sort(open_files.begin(), open_files.end(), [](const std::shared_ptr<file_t>& lhs, const std::shared_ptr<file_t>& rhs) {
        return lhs->getatime() < rhs->getatime();
    });
    for(const auto& file : open_files) {
        size_t freed = file->release_clean_blocks();
        current_size -= freed;
        if (current_size <= opt.cache_size) {
            return false;
        }
    }
    return true;
}

void trim(const filekey& file) {
    string name = basename(file.path);
    if(name.empty() || name == "x"){
        return;
    }
    auto_lock(&droped_lock);
    infolog("trim: %s\n", file.path.c_str());
    droped.push_back(file);
}

// 恢复dirty数据并重新上传
void recover_dirty_data() {
    std::vector<std::string> dirty_files;
    if(get_dirty_files(dirty_files) <= 0) {
        return;
    }
    infolog("Recovering %zu dirty files from previous session\n", dirty_files.size());

    for(const auto& file_path : dirty_files) {
        string path = decodepath(file_path, file_encode_suffix);
        auto file = std::dynamic_pointer_cast<file_t>(find_entry(path));
        if(file == nullptr) {
            warnlog("File %s not found in cache, skipping\n", file_path.c_str());
            continue;
        }
        file->open();
        file->release(false);
    }
}

static void trim_worker() {
    while(!gc_stop) {
        pthread_mutex_lock(&droped_lock);
        // copy dropped and clear
        std::vector<filekey> tmp = std::move(droped);
        droped.clear();
        pthread_mutex_unlock(&droped_lock);

        if(tmp.empty()) {
            for(int i = 0; i < 10 && !gc_stop.load(); ++i) {
                sleep(1);
            }
        }else {
            // 先清理数据库中的block记录
            delete_blocks_by_key(tmp);
            HANDLE_EAGAIN(fm_batchdelete(std::move(tmp)));
        }
    }
}

static void gc_worker() {
    while(!gc_stop) {
        bool haswork = false;

        // 定期检查缓存大小并清理
        if (!opt.no_cache && opt.cache_size >= 0 && cleanup_cache_by_size()) {
            haswork = true;
        }

        if(opt.entry_cache_second >= 0) {
            clean_entry_cache();
        }

        if(haswork) {
            sleep(1);  // 有工作时每秒检查一次
        } else {
            for(int i = 0; i < 30 && !gc_stop.load(); ++i) {
                sleep(1); // 可中断的30秒等待
            }
        }
    }
}

void start_gc() {
    if(gc_thread.joinable()) {
        gc_thread.join();
    }
    if(trim_thread.joinable()) {
        trim_thread.join();
    }
    gc_stop = false;
    gc_thread = std::thread(gc_worker);
    trim_thread = std::thread(trim_worker);
}

void stop_gc() {
    gc_stop = true;
    if(gc_thread.joinable()) {
        gc_thread.join();
    }
    if(trim_thread.joinable()) {
        trim_thread.join();
    }
}

static int tempfile() {
    int fd;
    char tmpfilename[PATHLEN];
    snprintf(tmpfilename, sizeof(tmpfilename), "%s/.fm_fuse_XXXXXX", opt.cache_dir);
    if ((fd = mkstemp(tmpfilename)) != -1) {
        /*Unlink the temp file.*/
        unlink(tmpfilename);
    }
    return fd;
}

static int persistent_cache_file(const string& remote_path) {
    string cache_path = get_cache_path(remote_path);

    // 确保目录存在
    string cache_dir = dirname(cache_path);
    if(create_dirs_recursive(cache_dir) != 0) {
        errorlog("create dirs failed for %s: %s\n", cache_dir.c_str(), strerror(errno));
        return -1;
    }

    // 尝试打开现有文件，如果不存在则创建
    int fd = open(cache_path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        errorlog("open cache file failed %s: %s\n", cache_path.c_str(), strerror(errno));
        fd = tempfile();
        if(fd < -1) {
            return -1;
        }
        fsetxattr(fd, FM_TEMP_FILE_ATTR, "1", 1, 0);
    }
    fsetxattr(fd, FM_REMOTE_PATH_ATTR, remote_path.c_str(), remote_path.size(), 0);
    return fd;
}

//计算某位置在哪个块中,从0开始计数,分界点算在前一个块中
inline size_t GetBlkNo(size_t p, blksize_t blksize) {
    assert(blksize);
    if (p == 0)
        return 0;
    return (p - 1) / blksize;
}

file_t::file_t(std::shared_ptr<dir_t> parent, const filemeta& meta):
    entry_t(parent, meta),
    fi{-1, 0, 0},
    blksize(meta.blksize),
    storage(meta.storage),
    last_meta_sync_time(time(nullptr))
{
    mode = (meta.mode & ~S_IFMT) | S_IFREG;
    if(flags & ENTRY_CHUNCED_F){
        fk = std::make_shared<filekey>(decodepath(*fk.load(), file_encode_suffix));
    }else {
        private_key = meta.key.private_key;
    }
    if(flags & ENTRY_CREATE_F){
        //creata new file
        assert(flags & ENTRY_CHUNCED_F);
        assert(meta.size == 0);
        //inline_data.resize(INLINE_DLEN);
        private_key = nullptr;
    }
    if(length == 0) {
        inline_data.resize(1);
    }
}

file_t::~file_t() {
    //这里不需要加锁, 因为走到这里的话其他地方已经没有shared_ptr指向这个对象了
    flags |= ENTRY_DELETED_F;
    reset_wlocked();
}

string file_t::getrealname() {
    if(flags & ENTRY_CHUNCED_F) {
        return encodepath(fk.load()->path, file_encode_suffix);
    }
    return fk.load()->path;
}

filekey file_t::getblockdir() {
    if(opt.flags & FM_RENAME_NOTSUPPRTED) {
        filekey objs = {".objs"};
        fm_getattrat({"/"}, objs);
        return objs;
    }
    return *fk.load();
}

void file_t::reset_wlocked() {
    assert(opened == 0  && ((flags & FILE_DIRTY_F) == 0 || (flags & ENTRY_DELETED_F)));
    for(auto it = blocks.rbegin(); it != blocks.rend(); ++it) {
        it->second->markstale();
    }
    blocks.clear();
    if(fi.fd < 0) {
        return;
    }
    //delete blocks if is temp file
    if(fgetxattr(fi.fd, FM_TEMP_FILE_ATTR, nullptr, 0) >= 0) {
        delete_blocks_from_db(fi.inode);
    }
    close(fi.fd);
    infolog("close file: %s, inode=%ju\n", getcwd().c_str(), fi.inode);
    fi = {-1, 0, 0};

}

int file_t::open(){
    atime = time(nullptr);
    auto_lock(&openfile_lock); //for putting inode into opened_inodes
    auto_wlock(this);
    if((flags & ENTRY_INITED_F) == 0){
        int ret = pull_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    opened++;
    if(fi.fd >= 0 || opt.no_cache) {
        return 0;
    }
    assert(opened == 1 && blocks.empty());
    fi.fd = persistent_cache_file(getcwd());
    if(fi.fd < 0) {
        return -errno;
    }
    TEMP_FAILURE_RETRY(ftruncate(fi.fd, length));
    struct statx st;
    //use statx to get st_ino and btime
    if (statx(fi.fd, "", AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW, STATX_BASIC_STATS | STATX_BTIME, &st) < 0) {
        int err = errno;
        close(fi.fd);
        fi = {-1, 0, 0};
        errorlog("statx failed for %s: %s\n", getcwd().c_str(), strerror(err));
        return -err;
    }

    fi.inode = st.stx_ino;
    fi.btime = st.stx_btime.tv_sec * 1000000000LL + st.stx_btime.tv_nsec;
    infolog("open file: %s, inode=%ju\n", getcwd().c_str(), fi.inode);
    opened_inodes.emplace(fi.inode, shared_file_from_this());
    if(flags & ENTRY_CHUNCED_F) {
        auto fblocks = getfblocks();
        assert(fblocks.size() == GetBlkNo(length, blksize)+1 || (fblocks.empty() && length <= INLINE_DLEN));
        for(size_t i = 0; i < fblocks.size(); i++){
            blocks.emplace(i, std::make_shared<block_t>(fi, fblocks[i], i, blksize * i, blksize, flags & FILE_ENCODE_F));
        }
    } else {
        for(size_t i = 0; i <= GetBlkNo(length, blksize); i++ ){
            blocks.emplace(i, std::make_shared<block_t>(fi, filekey{"", private_key}, i, blksize * i, blksize, 0));
        }
    }
    return 0;
}

void file_t::clean(std::weak_ptr<file_t> file_) {
    auto file = file_.lock();
    if(file == nullptr) {
        return;
    }
    auto_lock(&openfile_lock); //for removing inode from opened_inodes
    auto_wlock(file);
    if(file->opened > 0){
        file->flags &= ~ENTRY_REASEWAIT_F;
        return;
    }
    if(file->flags & ENTRY_DELETED_F){
        opened_inodes.erase(file->fi.inode);
        return;
    }
    if (file->flags & FILE_DIRTY_F && file->sync_wlocked(false, false)) {
        submit_delay_job([file_]() {
            file_t::clean(file_);
        }, 60);
        return;
    }
    file->flags &= ~ENTRY_REASEWAIT_F;
    opened_inodes.erase(file->fi.inode);
    __l.unlock();
    file->reset_wlocked();
}

int file_t::release(bool waitsync){
    atime = time(nullptr);
    auto_wlock(this);
    opened--;
    if(opened > 0) {
        return 0;
    }
    if(opt.no_cache) {
        assert((flags & FILE_DIRTY_F) == 0);
        flags &= ~ENTRY_INITED_F;
        for(auto it = blocks.rbegin(); it != blocks.rend(); ++it) {
            it->second->markstale();
        }
        blocks.clear();
        return 0;
    }
    if(waitsync && (flags & FILE_DIRTY_F)) {
        //如果不等待的话，这个流程会在clean -> sync_wlocked 触发
        for(auto it = blocks.rbegin(); it != blocks.rend(); ++it) {
            it->second->sync(getblockdir(), true);
        }
    }

    flags |= ENTRY_REASEWAIT_F;
    if(waitsync){
        __w.unlock();
        clean(std::weak_ptr<file_t>(shared_file_from_this()));
        return 0;
    } else {
        submit_delay_job([file = std::weak_ptr<file_t>(shared_file_from_this())]() {
            file_t::clean(file);
        }, 0);
        return 0;
    }
}

int file_t::pull_wlocked() {
    assert((flags & ENTRY_INITED_F) == 0);
    const filekey& key = getkey();
    filemeta meta = initfilemeta(key);
    meta.mode = this->mode;
    std::vector<filekey> fblocks;
    load_file_from_db(key.path, meta, fblocks);
    if(flags & ENTRY_CHUNCED_F){
        if(meta.blksize == 0){
            filekey metakey{METANAME, 0};
            int ret = HANDLE_EAGAIN(fm_getattrat(key, metakey));
            if (ret < 0) {
                return ret;
            }
            ret = download_meta(metakey, meta, fblocks);
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
        assert(meta.inline_data.empty());
    }
    mode = (meta.mode & ~S_IFMT) | S_IFREG;
    length = meta.size;
    ctime = meta.ctime;
    mtime = meta.mtime;
    private_key = meta.key.private_key;
    blksize = meta.blksize;
    block_size = 0;
    flags |= meta.flags;
    if(meta.inline_data.size()){
        assert(meta.size < (size_t)meta.blksize);
        inline_data = std::move(meta.inline_data);
    }else if(length == 0) {
        inline_data.resize(1);
    }else{
        inline_data.clear();
    }
    flags |= ENTRY_INITED_F;
    flags &= ~META_KEY_ONLY_F;
    if(!opt.no_cache) {
        return 0;
    }
    if(flags & ENTRY_CHUNCED_F) {
        assert(fblocks.size() == GetBlkNo(length, blksize)+1 || (fblocks.empty() && length <= INLINE_DLEN));
        for(size_t i = 0; i < fblocks.size(); i++){
            blocks.emplace(i, std::make_shared<block_t>(fi, fblocks[i], i, blksize * i, blksize, flags & FILE_ENCODE_F));
        }
    } else {
        for(size_t i = 0; i <= GetBlkNo(length, blksize); i++ ){
            blocks.emplace(i, std::make_shared<block_t>(fi, filekey{"", private_key}, i, blksize * i, blksize, 0));
        }
    }
    return 0;
}

int file_t::fetchmeta(const filekey& parent, filekey& file, filemeta& meta) {
    if(meta.flags & FILE_ENCODE_F) {
        file.path = encodepath(file.path, file_encode_suffix);
        int ret = HANDLE_EAGAIN(fm_getattrat(parent, file));
        if (ret < 0) {
            return ret;
        }
        file.path = pathjoin(parent.path, file.path);
        filekey metakey = {METANAME, 0};
        ret = HANDLE_EAGAIN(fm_getattrat(file, metakey));
        if (ret < 0) {
            return ret;
        }
        metakey.path = pathjoin(file.path, METANAME);
        std::vector<filekey> fblocks;
        return download_meta(metakey, meta, fblocks);
    } else {
        int ret = HANDLE_EAGAIN(fm_getattrat(parent, file));
        if (ret < 0) {
            return ret;
        }
        file.path = pathjoin(parent.path, file.path);
        return HANDLE_EAGAIN(fm_getattr(file, meta));
    }
}

int file_t::read(void* buff, off_t offset, size_t size) {
    atime = time(nullptr);
    auto_rlock(this);
    assert(opened);
    if((size_t)offset > length){
        return -EFAULT;
    }
    if(offset + size > length){
        size = length - offset;
    }
    if(size == 0) {
        return 0;
    }
    if(inline_data.size()){
        assert(inline_data.size() == length);
        memcpy(buff, inline_data.c_str() + offset, size);
        return size;
    }
    size_t startc = GetBlkNo(offset, blksize);
    size_t endc = GetBlkNo(offset + size, blksize);
    if(!opt.no_cache) {
        int left_size = 10 * 1024 * 1024; //10M
        int left_block = 20;
        for(size_t i = startc; i<= GetBlkNo(length, blksize); i++){
            if(blocks.at(i)->prefetch(0, blksize, false) > 0){
                left_size -= blksize;
            }
            left_block --;
            if(left_size <= 0 || left_block <= 0) {
                break;
            }
        }
        for(size_t i = startc; i<= endc; i++ ){
            uint32_t startp = std::max(i * (size_t)blksize, (size_t)offset);
            uint32_t endp = std::min((i + 1) * (size_t)blksize, (size_t)offset + size);
            int ret = blocks.at(i)->prefetch(startp - i * blksize, endp - i * blksize, true);
            if (ret < 0) {
                return ret;
            }
        }
        return pread(fi.fd, buff, size, offset);
    }
    if(flags & ENTRY_CHUNCED_F) {
        //use block_t::read to read directly from startc to endc
        offset -= startc * blksize;
        size_t left = size;
        for(size_t i = startc; i <= endc && left > 0; i++){
            ssize_t ret = blocks.at(i)->read(getkey(), buff, offset, std::min(left, (size_t)blksize - offset));
            if(ret < 0) {
                return ret;
            }
            if(ret < (int)std::min(left, (size_t)blksize - offset)) {
                return size - left + ret; // 可能是最后一个块不满
            }
            offset = 0;
            left -= ret;
            buff = (char*)buff + ret;
        }
        return size - left;
    }
    assert((flags & FILE_ENCODE_F) == 0);
    buffstruct bs((char*)buff, size);
    defer([&bs] { bs.release(); });
    int ret = HANDLE_EAGAIN(fm_download(getkey(), offset, size, bs));
    if(ret < 0) {
        return ret;
    }
    assert(bs.size() <= (size_t)size);
    return bs.size();
}

int file_t::truncate_wlocked(off_t offset){
    if((size_t)offset == length){
        return 0;
    }
    size_t newc = GetBlkNo(offset, blksize);
    size_t oldc = GetBlkNo(length, blksize);
    if(newc > oldc){
        if(newc >= MAXFILE && (opt.flags & FM_RENAME_NOTSUPPRTED) == 0){
            errno = EFBIG;
            return -1;
        }
        if(blocks.contains(oldc)) {
            blocks.at(oldc)->markdirty(getblockdir(), length - oldc * blksize, blksize);
        }
        for(size_t i = oldc + 1; i<= newc; i++){
            blocks.emplace(i, std::make_shared<block_t>(fi, filekey{"x", 0}, i, blksize * i, blksize, (flags & FILE_ENCODE_F)));
        }
    }else if(oldc >= newc && inline_data.empty()){
        // 如果offset正好在newc块的结束位置，不需要修改该块
        if((size_t)offset != (newc + 1) * blksize) {
            int ret = blocks.at(newc)->prefetch(0, blksize, true);
            if(ret < 0) {
                return ret;
            }
            if((flags & ENTRY_DELETED_F) == 0) blocks.at(newc)->markdirty(getblockdir(), 0, blksize);
        }
        for(size_t i = newc + 1; i<= oldc; i++){
            blocks.erase(i);
        }
    }
    if(inline_data.size()) {
        if(offset >= (off_t)INLINE_DLEN){
            int ret = pwrite(fi.fd, inline_data.data(), length, 0);
            if(ret < 0){
                return ret;
            }
            assert(!blocks.contains(0));
            blocks.emplace(0, std::make_shared<block_t>(fi, filekey{"x", 0}, 0, 0, blksize, (flags & FILE_ENCODE_F)));
            inline_data.clear();
            if((flags & ENTRY_DELETED_F) == 0) blocks.at(0)->markdirty(getblockdir(), 0, blksize);
        } else if(offset == 0) {
            inline_data.resize(1);
        } else if(offset > (off_t)inline_data.size()) {
            inline_data.resize(offset);
        }
    }
    length = offset;
    if((flags & FILE_DIRTY_F) == 0){
        flags |= FILE_DIRTY_F;
        sync_wlocked(true, false);
    }
    assert(fi.fd >= 0 && length == (size_t)offset);
    return TEMP_FAILURE_RETRY(ftruncate(fi.fd, offset));
}

int file_t::truncate(off_t offset){
    atime = time(nullptr);
    auto_wlock(this);
    assert(opened);
    if((flags & ENTRY_CHUNCED_F) == 0){
        return -EPERM;
    }
    int ret = truncate_wlocked(offset);
    if(ret < 0){
        return -errno;
    }
    version++;
    ctime = mtime = time(nullptr);
    if(mtime - last_meta_sync_time >= 600) {
        last_meta_sync_time = mtime;
        upool->submit_fire_and_forget([file = std::weak_ptr<file_t>(shared_file_from_this())]{
            upload_meta_async_task(file);
        });
    }
    return 0;
}

int file_t::write(const void* buff, off_t offset, size_t size) {
    atime = time(nullptr);
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
    assert(inline_data.empty() || (inline_data.size() && length < INLINE_DLEN));
    assert(fi.fd >= 0);
    int ret = pwrite(fi.fd, buff, size, offset);
    if(ret < 0){
        return -errno;
    }
    if(inline_data.size()){
        assert(inline_data.size() == length);
        memcpy(inline_data.data() + offset, buff, size);
    }else if((flags & ENTRY_DELETED_F) == 0) {
        auto blockdir = getblockdir();
        const size_t startc = GetBlkNo(offset, blksize);
        const size_t endc = GetBlkNo(offset + size, blksize);
        for(size_t i = startc; i <= endc; i++){
            size_t startp = std::max(i * (size_t)blksize, (size_t)offset);
            size_t endp =  std::min((i + 1) * (size_t)blksize, (size_t)offset + size);
            blocks.at(i)->markdirty(blockdir, startp - i * blksize, endp - i * blksize);
        }
    }
    version++;
    ctime = mtime = time(nullptr);
    if((flags & FILE_DIRTY_F) == 0){
        flags |= FILE_DIRTY_F;
        sync_wlocked(true, true);
    }else if(mtime - last_meta_sync_time >= 600) {
        last_meta_sync_time = mtime;
        upool->submit_fire_and_forget([file = std::weak_ptr<file_t>(shared_file_from_this())]{
             upload_meta_async_task(file);
        });
    }
    return ret;
}

int file_t::remove_wlocked(bool skip_entry) {
    if(flags & FILE_UPMETA_F) {
        return -EBUSY;
    }
    auto key = getkey();
    int ret = 0;
    if((opt.flags & FM_DELETE_NEED_PURGE) && (flags & FILE_ENCODE_F)) {
        ret = HANDLE_EAGAIN(fm_batchdelete(skip_entry? getfblocks() : getkeys()));
    }else if(!skip_entry){
        ret = HANDLE_EAGAIN(fm_delete(key));
    }
    if (ret < 0 && errno != ENOENT) {
        return ret;
    }

    version++;
    for(auto it = blocks.rbegin(); it != blocks.rend(); ++it) {
        it->second->markstale();
    }
    delete_entry_from_db(key.path);
    if(fi.inode) {
        delete_blocks_from_db(fi.inode);       //打开过的文件，可以直接用inode删除
    }else if(flags & FILE_ENCODE_F) {
        delete_blocks_by_key(getfblocks()); //未打开的文件，inode为空，只能用key删除
    }else{
        delete_blocks_by_key({key});
    }
    delete_file_from_db(key.path);
    infolog("remove file: %s, inode=%ju\n", getcwd().c_str(), fi.inode);

    // 删除持久化缓存文件
    string cache_path = get_cache_path(getcwd());
    if(unlink(cache_path.c_str()) != 0 && errno != ENOENT) {
        warnlog("failed to unlink cache file %s: %s\n", cache_path.c_str(), strerror(errno));
    }
    flags |= ENTRY_DELETED_F;
    parent.reset();
    if(opened || (flags & ENTRY_REASEWAIT_F)) {
        //do delete in clean
        return 0;
    }
    return 1;
}

std::vector<filekey> file_t::getfblocks(){
    auto_rlock(this);
    if(inline_data.size() || length == 0){
        assert(length <= INLINE_DLEN);
        return {};
    }
    if((flags & FILE_ENCODE_F) == 0) {
        return {};
    }
    if(blocks.empty()) {
        filemeta meta{};
        std::vector<filekey> fblocks;
        load_file_from_db(getkey().path, meta, fblocks);
        if(meta.blksize == 0) {
            assert((flags & ENTRY_INITED_F) == 0);
        }else {
            assert(fblocks.size() == GetBlkNo(length, blksize)+1);
        }
        return fblocks;
    }
    assert(flags & ENTRY_INITED_F);
    assert(blocks.size() == GetBlkNo(length, blksize)+1);
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
    if(flags & FILE_ENCODE_F) {
        return filekey{pathjoin(getkey().path, METANAME), private_key};
    } else {
        return getkey();
    }
}

std::vector<filekey> file_t::getkeys() {
    auto_rlock(this);
    std::vector<filekey> flist = getfblocks();
    for(auto& fblock: flist){
        if(fblock.path == "x" || fblock.path.empty()){
            continue;
        }
        if(opt.flags & FM_RENAME_NOTSUPPRTED) {
            fblock.path = pathjoin(".objs", fblock.path);
        } else {
            fblock.path = pathjoin(getkey().path, fblock.path);
        }
    }
    flist.emplace_back(getmetakey());
    if(fk.load()->private_key == nullptr){
        return flist;
    }
    string path;
    if(flags & ENTRY_CHUNCED_F){
        path = encodepath(getcwd(), file_encode_suffix);
    }else{
        path = getcwd();
    }
    flist.emplace_back(filekey{path, fk.load()->private_key});
    return flist;
}

int file_t::getmeta(filemeta& meta) {
    atime = time(nullptr);
    auto_rlock(this);
    //assert(fm_private_key_tostring(fk.load()->private_key)[0] != '\0');
    if((flags & ENTRY_INITED_F) == 0){
        __r.upgrade();
        int ret = pull_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    meta = initfilemeta(getkey());
    meta.mode = mode;
    meta.inline_data = inline_data;
    meta.flags = flags;
    meta.size = length;
    meta.blksize = blksize;
    if(inline_data.size()) {
        meta.blocks = length *3 / 2 / 512 + 1; //base64 encode
    }else if(block_size > 0 && (flags & FILE_DIRTY_F) == 0){
        //sync will call getmeta before clear FILE_DIRTY_F
        //so we no need to recalculate the block size
        meta.blocks = block_size;
    }else if((flags & ENTRY_CHUNCED_F) == 0){
        meta.blocks = length / 512 + 1;
    }else {
        meta.blocks = 1; //at least for meta.json
        const auto fblocks = getfblocks();
        // last block may be not full, skip it first
        for(size_t i = 0; i + 1 < fblocks.size(); i++){
            if(fblocks[i].path == "x" || fblocks[i].path.empty()){
                continue;
            }
            meta.blocks += blksize / 512;
        }
        if(!fblocks.empty()){
            const size_t consumed_bytes = blksize * (fblocks.size() - 1);
            const size_t remaining_bytes = length > consumed_bytes ? length - consumed_bytes : 0;
            meta.blocks += remaining_bytes / 512 + 1;
        }
        block_size = meta.blocks;
    }
    meta.ctime = ctime;
    meta.mtime = mtime;
    return 0;
}

bool file_t::sync_wlocked(bool forcedirty, bool lockfree) {
    if(flags & ENTRY_DELETED_F) {
        flags &= ~FILE_DIRTY_F;
        return false;
    }
    bool dirty = false;
    if(forcedirty) {
        dirty = true;
    } else {
        for(auto it = blocks.rbegin(); it != blocks.rend(); ++it) {
            dirty |= it->second->sync(getblockdir(), false);
        }
    }
    assert(!dirty || (flags & FILE_DIRTY_F));
    const filekey& key = getkey();
    filemeta meta;
    if(getmeta(meta) < 0) {
        return true;
    }
    meta.key = basename(getmetakey());
    std::vector<filekey> fblocks = getfblocks();
    const size_t version_snapshot = version.load();
    if(lockfree){
        flags |= FILE_UPMETA_F;
        unwlock();
    }
    int ret = upload_meta(key, meta, fblocks);
    int errorno = ret ? errno : 0;
    if(lockfree){
        wlock();
        flags &= ~FILE_UPMETA_F;
    }
    if(ret){
        errorlog("upload_meta IO Error: %s, err=%s\n", key.path.c_str(), strerror(errorno));
        return true;
    }
    private_key = meta.key.private_key;
    if(version_snapshot != version.load()) {
        infolog("file version: %s version %zu vs %zu, flags: %x\n", key.path.c_str(), version_snapshot, version.load(), flags);
        return true;
    }
    if(!dirty){
        flags &= ~FILE_DIRTY_F;
        meta.flags = flags;
    }
    save_file_to_db(key.path, meta, fblocks);
    return dirty;
}


int file_t::sync(int dataonly){
    atime = time(nullptr);
    auto_wlock(this);
    if(flags & ENTRY_DELETED_F) {
        return 0;
    }
    assert(flags & ENTRY_INITED_F);
    if((flags & ENTRY_CHUNCED_F) == 0 || (flags & FILE_DIRTY_F) == 0){
        return 0;
    }
    fsync(fi.fd);
    if(!dataonly) {
        dump_to_db(dirname(getcwd()), fk.load()->path);
    }
    return 0;
}

int file_t::update_meta_wlocked(filemeta& meta, std::function<void(filemeta&)> meta_updater) {
    if(flags & ENTRY_DELETED_F) {
        int ret = getmeta(meta);
        if(ret < 0) {
            return ret;
        }
        meta_updater(meta);
        return 0;
    }
    if((flags & ENTRY_INITED_F) == 0){
        int ret = pull_wlocked();
        if(ret < 0) {
            return ret;
        }
    }

    const filekey& key = getkey();
    std::vector<filekey> fblocks = getfblocks();
    int ret = getmeta(meta);
    if(ret < 0) {
        return ret;
    }

    // Apply the user-defined modifications
    meta_updater(meta);
    version++;
    if(flags & ENTRY_CHUNCED_F){
        meta.key = basename(getmetakey());
        int ret = upload_meta(key, meta, fblocks);
        if(ret){
            return -errno;
        }
        private_key = meta.key.private_key;
    } else {
        // For non-chunked files, we only update the times using utime
        time_t ttv[2];
        ttv[0] = meta.ctime;
        ttv[1] = meta.mtime;
        int ret = HANDLE_EAGAIN(fm_utime(getkey(), ttv));
        if(ret){
            return ret;
        }
    }
    save_file_to_db(key.path, meta, fblocks);
    return 0;
}

int file_t::utime(const struct timespec tv[2]) {
    atime = time(nullptr);
    //ignore atime
    if(tv[1].tv_nsec == UTIME_OMIT) {
        return 0; // no change
    }
    filemeta meta;
    auto_wlock(this);
    int ret = update_meta_wlocked(meta, [&tv](filemeta& meta) {
        meta.ctime = time(nullptr);
        if(tv[1].tv_nsec == UTIME_NOW) {
            meta.mtime = time(nullptr);
        }else{
            meta.mtime = tv[1].tv_sec;
        }
    });
    if (ret < 0) {
        return ret;
    }
    // Update the file_t object state from the modified meta
    ctime = meta.ctime;
    mtime = meta.mtime;
    return 0;
}

int file_t::chmod(mode_t mode) {
    atime = time(nullptr);
    filemeta meta;
    auto_wlock(this);
    int ret = update_meta_wlocked(meta, [mode](filemeta& meta) {
        meta.mode = (mode & ~S_IFMT) | S_IFREG;
        meta.ctime = time(nullptr);
    });
    if(ret < 0) {
        return ret;
    }
    this->mode = meta.mode;
    this->ctime = meta.ctime;
    return 0;
}

void file_t::dump_to_db(const std::string& path, const std::string& name) {
    auto_rlock(this);
    if(flags & ENTRY_DELETED_F) {
        return;
    }
    if((flags & ENTRY_INITED_F) == 0){
        return;
    }
    filemeta meta;
    if(getmeta(meta) < 0) {
        return;
    }
    if(flags & ENTRY_CHUNCED_F){
        auto savemeta = meta;
        savemeta.key.path = encodepath(name, file_encode_suffix);
        savemeta.mode = S_IFDIR | 0755;
        save_entry_to_db(path, savemeta);
        meta.key.private_key = private_key;
        save_file_to_db(pathjoin(path, savemeta.key.path), meta, getfblocks());
    }else{
        meta.key.path = name;
        save_entry_to_db(path, meta);
        save_file_to_db(pathjoin(path, name), meta, {});
    }
}

int file_t::drop_cache_wlocked(bool mem_only, time_t before) {
    if(opened || fi.fd >= 0){
        return -EBUSY;
    }
    if((flags & ENTRY_REASEWAIT_F) || (flags & ENTRY_PULLING_F) || (flags & FILE_DIRTY_F)){
        return -EAGAIN;
    }
    if((flags & ENTRY_INITED_F) == 0){
        return 0;
    }
    if(before && atime > before) {
        return -EAGAIN;
    }
    //fblocks的获取不能后置，因为可能是通过blocks获取的
    auto fblocks = getfblocks();
    for(auto it = blocks.rbegin(); it != blocks.rend(); ++it) {
        it->second->markstale();
    }
    blocks.clear();
    if(inline_data.size()) {
        inline_data.clear();
        inline_data.resize(1);
    }
    flags &= ~ENTRY_INITED_F;
    if(opt.no_cache || mem_only) {
        return 0;
    }
    if(fi.inode) {
        delete_blocks_from_db(fi.inode);
    }else if(flags & ENTRY_CHUNCED_F) {
        delete_blocks_by_key(fblocks);
    }else {
        delete_blocks_by_key({getkey()});
    }
    infolog("drop cache: %s, inode=%ju\n", getcwd().c_str(), fi.inode);
    return delete_file_from_db(getkey().path);
}

void file_t::upload_meta_async_task(std::weak_ptr<file_t> file_) {
    auto file = file_.lock();
    if(file == nullptr) {
        return;
    }
    if(file->trywlock()) {
        return;  // 获取不到锁，放弃本次上传
    }
    defer([file]{ file->unwlock(); });
    if((file->flags & ENTRY_DELETED_F) || (file->flags & FILE_DIRTY_F) == 0) {
        return;
    }
    file->sync_wlocked(false, true);
}

static void add_meta_to_storage_info(storage_class_info& info, const filemeta& meta) {
    info.size_store[meta.storage] += meta.size;
    if(meta.storage == STORAGE_ARCHIVE) {
        if (meta.restore_in_progress) {
            info.size_archive_restoring += meta.size;
        }
        if (meta.restore_expiry_date) {
            info.size_archive_restored += meta.size;
        }
    }
    if(meta.storage == STORAGE_DEEP_ARCHIVE) {
        if (meta.restore_in_progress) {
            info.size_deep_archive_restoring += meta.size;
        }
        if (meta.restore_expiry_date) {
            info.size_deep_archive_restored += meta.size;
        }
    }
}

int file_t::collect_storage_classes(TrdPool* pool, std::vector<std::future<std::pair<int, storage_class_info>>>& futures) {
    assert(pool != nullptr);
    auto_rlock(this);
    if(flags & ENTRY_DELETED_F) {
        return -ENOENT;
    }
    if((flags & ENTRY_INITED_F) == 0) {
        __r.upgrade();
        int ret = pull_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    if((flags & ENTRY_CHUNCED_F) == 0) {
        auto file_key = getkey();
        futures.emplace_back(pool->submit([file_key]() mutable -> std::pair<int, storage_class_info> {
            storage_class_info info{};
            filemeta meta = initfilemeta(file_key);
            int ret = HANDLE_EAGAIN(fm_getattr(file_key, meta));
            if(ret < 0) {
                return std::make_pair(ret, info);
            }
            add_meta_to_storage_info(info, meta);
            return std::make_pair(0, info);
        }));
        return 0;
    }
    auto fblocks = getfblocks();
    std::string pwd = getkey().path;
    for(auto fblock : fblocks) {
        if(fblock.path.empty() || fblock.path == "x") {
            continue; // 跳过空或占位块
        }
        futures.emplace_back(pool->submit([fblock, pwd]() mutable -> std::pair<int, storage_class_info> {
            storage_class_info info{};
            if(opt.flags & FM_RENAME_NOTSUPPRTED) {
                fblock.path = pathjoin(".objs", fblock.path);
            } else {
                fblock.path = pathjoin(pwd, fblock.path);
            }
            filemeta meta = initfilemeta(fblock);
            int ret = HANDLE_EAGAIN(fm_getattr(fblock, meta));
            if(ret < 0) {
                errorlog("collect_storage_classes failed for block %s: %s\n", fblock.path.c_str(), strerror(-ret));
                return std::make_pair(ret, info);
            }
            add_meta_to_storage_info(info, meta);
            return std::make_pair(0, info);
        }));
    }
    return 0;
}

enum storage_action {
    STORAGE_ACTION_INVALID = 0,
    STORAGE_ACTION_NONE = 1,
    STORAGE_ACTION_CHANGE = 2,
    STORAGE_ACTION_RESTOR = 3,
    STORAGE_ACTION_RESTOR_DUP = 4,
    STORAGE_ACTION_RESTORED = 5,
};

static storage_action get_storage_action(const filemeta& meta, enum storage_class new_storage) {
    if(meta.storage == STORAGE_UNKNOWN || new_storage == STORAGE_UNKNOWN) {
        return STORAGE_ACTION_INVALID;
    }
    if(meta.storage == new_storage) {
        return STORAGE_ACTION_NONE;
    }
    if(((meta.storage == STORAGE_ARCHIVE) || (meta.storage == STORAGE_DEEP_ARCHIVE))){
        if(meta.restore_expiry_date) {
            return STORAGE_ACTION_RESTORED;
        }
        if(new_storage != STORAGE_STANDARD) {
            return STORAGE_ACTION_INVALID; // 归档类只能恢复到标准存储
        }
        if(meta.restore_in_progress) {
            return STORAGE_ACTION_RESTOR_DUP;
        }
        return STORAGE_ACTION_RESTOR;
    }
    return STORAGE_ACTION_CHANGE;
}

static int _set_storage_class(filekey file, enum storage_class storage){
    filemeta meta = initfilemeta(file);
    if(HANDLE_EAGAIN(fm_getattr(file, meta))) {
        return -EAGAIN;
    }
    if(meta.size < 256 * 1024){
        // 小于256KB的块，忽略下沉请求，IA恢复到STANDARD
        if(meta.storage == STORAGE_STANDARD) {
            return 0;
        }
        if(meta.storage == STORAGE_IA) {
            storage = STORAGE_STANDARD;
        }
        if(storage != STORAGE_STANDARD) {
            return 0;
        }
    }
    switch(get_storage_action(meta, storage)) {
    case STORAGE_ACTION_INVALID: default:
        return -EINVAL;
    case STORAGE_ACTION_RESTORED:
        return -EEXIST;
    case STORAGE_ACTION_NONE:
        return 0; // 无需更改
    case STORAGE_ACTION_CHANGE:
        return HANDLE_EAGAIN(fm_change_storage_class(file, storage));
    case STORAGE_ACTION_RESTOR:
        return HANDLE_EAGAIN(fm_restore_archive(file, 3, 3));
    case STORAGE_ACTION_RESTOR_DUP:
        return HANDLE_EAGAIN(fm_restore_archive(file, 3, meta.storage == STORAGE_DEEP_ARCHIVE ? 2 : 1));
    }
}

int file_t::set_storage_class(enum storage_class storage, TrdPool* pool, std::vector<std::future<int>>& futures) {
    auto_rlock(this);
    if(flags & (ENTRY_DELETED_F | FILE_DIRTY_F)) {
        return -EAGAIN; // 已删除或有未同步的更改，跳过
    }
    if((flags & ENTRY_INITED_F) == 0) {
        __r.upgrade();
        int ret = pull_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    if((flags & ENTRY_CHUNCED_F) == 0) {
        futures.emplace_back(pool->submit([file = getkey(), storage] {
            return _set_storage_class(file, storage);
        }));
        return 0;
    }
    auto fblocks = getfblocks();
    std::string pwd = getkey().path;
    for(auto& fblock : fblocks) {
        if (fblock.path.empty() || fblock.path == "x") {
            continue; // 跳过空或占位块
        }
        futures.emplace_back(pool->submit([fblock, storage, pwd]() mutable {
            if(opt.flags & FM_RENAME_NOTSUPPRTED) {
                fblock.path = pathjoin(".objs", fblock.path);
            } else {
                fblock.path = pathjoin(pwd, fblock.path);
            }
            int ret = _set_storage_class(fblock, storage);
            if (ret < 0 && ret != -EEXIST) {
                errorlog("set_storage_class failed for block %s: %s\n", fblock.path.c_str(), strerror(-ret));
                return ret;
            }
            return 0;
        }));
    }
    return 0;
}

int file_t::get_etag(std::string& etag) {
    auto_rlock(this);
    if((flags & ENTRY_INITED_F) == 0) {
        __r.upgrade();
        int ret = pull_wlocked();
        if(ret < 0) {
            return ret;
        }
    }
    if((flags & ENTRY_CHUNCED_F) == 0) {
        filemeta meta = initfilemeta(getkey());
        int ret = HANDLE_EAGAIN(fm_getattr(getkey(), meta));
        if(ret < 0) {
            return ret;
        }
        etag = std::move(meta.etag);
        return 0;
    }
    auto fblocks = getfblocks();
    if(fblocks.empty()) {
        assert(!inline_data.empty());
        etag.resize(inline_data.size() * 2);
        int ret = Base64En(inline_data.c_str(), inline_data.length(), etag.data());
        if(ret < 0 ){
            return -EIO;
        }
        etag.resize(ret);
        etag = etag.substr(std::max(ret - 32, 0));
        return 0;
    }
    filekey lastblock;
    for(auto it = fblocks.rbegin(); it != fblocks.rend(); ++it) {
        if(it->path != "x" && !it->path.empty()) {
            lastblock = *it;
            break;
        }
    }
    if(lastblock.path.empty() || lastblock.path == "x") {
        etag = "x"; // all blocks are empty
        return 0;
    }

    if(opt.flags & FM_RENAME_NOTSUPPRTED) {
        lastblock.path = pathjoin(".objs", lastblock.path);
    } else {
        lastblock.path = pathjoin(getkey().path, lastblock.path);
    }

    filemeta meta = initfilemeta(lastblock);
    int ret = HANDLE_EAGAIN(fm_getattr(lastblock, meta));
    if(ret < 0) {
        return ret;
    }
    etag = std::move(meta.etag);
    return 0;
}


size_t file_t::release_clean_blocks() {
    auto_wlock(this);
    if((flags & ENTRY_INITED_F) == 0 || (flags & (ENTRY_DELETED_F | ENTRY_REASEWAIT_F)) || fi.fd < 0) {
        return 0;
    }
    size_t released = 0;
    for(auto& [_, block]: blocks) {
        released += block->release();
    }
    infolog("released %zd for %s\n", released, getcwd().c_str());
    return released;
}
