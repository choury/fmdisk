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

#include <list>
#include <random>
#include <algorithm>
#include <atomic>


static pthread_mutex_t gcLocker = PTHREAD_MUTEX_INITIALIZER;
static std::vector<filekey> droped;
static std::atomic<bool> gc_stop(false);

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
        pthread_mutex_lock(&gcLocker);
        //copy dropped and clear
        std::vector<filekey> tmp = std::move(droped);
        droped.clear();
        pthread_mutex_unlock(&gcLocker);
        if(tmp.empty()){
            //wait 30s
            sleep(30);
            continue;
        }
        HANDLE_EAGAIN(fm_batchdelete(std::move(tmp)));
    }
}

void stop_gc() {
    gc_stop = true;
}

static int tempfile() {
    int fd;
    char tmpfilename[PATHLEN];
    snprintf(tmpfilename, sizeof(tmpfilename), "%s/.fm_fuse_XXXXXX", opt.cache_dir);
    if ((fd = mkstemp(tmpfilename)) != -1) {
        /*Unlink the temp file.*/
        unlink(tmpfilename);
    } else {
        fprintf(stderr, "mkstem failed: %s", strerror(errno));
        abort();
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
//        blocks.emplace(0, new block_t(this, filekey{"x", 0}, 0, 0, blksize, BLOCK_SYNC));
        return;
    }
    //for the non-chunk file
    for(size_t i = 0; i <= GetBlkNo(length, blksize); i++ ){
        blocks.emplace(i, new block_t(this, filekey{"", meta.key.private_key}, i, blksize * i, blksize, 0));
    }
}

file_t::~file_t() {
    auto_wlock(this);
    for(auto i: blocks){
        i.second->flags |= BLOCK_STALE;
        delete i.second;
    }
    if(fd >= 0){
        close(fd);
    }
    if(inline_data){
        delete[] inline_data;
    }
}

int file_t::open(){
    auto_wlock(this);
    if((flags & ENTRY_INITED_F) == 0){
        pull_wlocked();
    }
    opened++;
    if(fd >= 0){
        return 0;
    }
    fd = tempfile();
    if(fd >= 0){
        TEMP_FAILURE_RETRY(ftruncate(fd, length));
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
    }else{
        assert((file->flags & FILE_DIRTY_F) == 0);
        for(auto i: file->blocks){
            i.second->reset();
        }
        if(file->fd >= 0){
            close(file->fd);
        }
        file->fd = -1;
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
    std::vector<filekey> fblocks;
    entry_t::pull_wlocked(meta, fblocks);
    private_key = meta.key.private_key;
    blksize = meta.blksize;
    flags |= meta.flags;
    mode &= ~0777;
    mode |= (meta.mode & 0777);
    if(flags & ENTRY_CHUNCED_F) {
        assert(blocks.empty());
        if(meta.inline_data){
            assert(meta.size < (size_t)meta.blksize);
            inline_data = (char*)meta.inline_data;
            //blocks.emplace(0, new block_t(this, filekey{"x", 0}, 0, 0, blksize, BLOCK_SYNC));
        }else{
            //zero is the first block
            assert(fblocks.size() == GetBlkNo(length, blksize)+1);
        }
        for(size_t i = 0; i < fblocks.size(); i++ ){
            blocks.emplace(i, new block_t(this, fblocks[i], i, blksize * i, blksize, 0));
        }
    } else {
        for(size_t i = 0; i <= GetBlkNo(length, blksize); i++ ){
            blocks.emplace(i, new block_t(this, filekey{"", meta.key.private_key}, i, blksize * i, blksize, 0));
        }
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
            blocks.emplace(i, new block_t(this, filekey{"x", 0}, i, blksize * i, blksize, BLOCK_SYNC));
        }
    }else if(oldc >= newc && inline_data == nullptr){
        blocks[newc]->prefetch(true);
        if((flags & ENTRY_DELETED_F) == 0) blocks[newc]->makedirty();
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
        blocks.emplace(0, new block_t(this, filekey{"x", 0}, 0, 0, blksize, BLOCK_SYNC));
        delete[] inline_data;
        inline_data = nullptr;
        if((flags & ENTRY_DELETED_F) == 0) blocks[0]->makedirty();
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
            blocks[i]->makedirty();
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
    parent = nullptr;
    flags |= ENTRY_DELETED_F;
    if(opened || (flags & ENTRY_REASEWAIT_F)) {
        //do this in clean
        unwlock();
        return 0;
    }
    if(opt.flags & FM_DELETE_NEED_PURGE) {
        HANDLE_EAGAIN(fm_batchdelete(getkeys()));
    }else{
        HANDLE_EAGAIN(fm_delete(getkey()));
    }
    unwlock();
    delete this;
    return 0;
}

std::vector<filekey> file_t::getfblocks(){
    auto_rlock(this);
    if(inline_data){
        return std::vector<filekey>();
    }
    std::vector<filekey> fblocks(blocks.size());
    for(auto i : this->blocks){
        if(i.second->dummy()){
            continue;
        }
        fblocks[i.first] = basename(i.second->getkey());
    }
    return fblocks;
}

int file_t::getbuffer(void* buffer, off_t offset, size_t size) {
    auto_rlock(this);
    if(fd < 0){
        return -1;
    }
    int ret = TEMP_FAILURE_RETRY(pread(fd, buffer, size, offset));
    bool allzero = true;
    for(size_t i = 0; i < size; i++){
        if(((char*)buffer)[i]){
            allzero = false;
            break;
        }
    }
    if(allzero){
        return 0;
    }
    if(flags & FILE_ENCODE_F){
        xorcode(buffer, offset, ret, opt.secret);
    }
    return ret;
}

int file_t::putbuffer(void* buffer, off_t offset, size_t size) {
    auto_rlock(this);
    if(fd < 0){
        return -1;
    }
    if(flags & FILE_ENCODE_F){
        xorcode(buffer, offset, size, opt.secret);
    }
    return TEMP_FAILURE_RETRY(pwrite(fd, buffer, size, offset));
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
    for(auto i: blocks){
        i.second->flags |= BLOCK_STALE;
        delete i.second;
    }
    blocks.clear();
    assert(fd == -1); //it should be closed in clean
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
