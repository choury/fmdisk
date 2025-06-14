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

static void trim(const filekey& file) {
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

std::string random_string() {
     std::string str("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");

     std::random_device rd;
     std::mt19937 generator(rd());

     std::shuffle(str.begin(), str.end(), generator);
     return str.substr(0, 16);    // assumes 16 < number of characters in str
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

block_t::block_t(file_t* file, filekey fk, size_t no, off_t offset, size_t size, unsigned int flags):
    file(file),
    fk(basename(fk)),
    no(no),
    offset(offset),
    size(size),
    flags(flags)
{
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

filekey block_t::getkey(){
    filekey key = file->getkey();
    if(fk.path.empty()) {
        return filekey{key.path, fk.private_key};
    } else {
        return filekey{pathjoin(key.path, fk.path), fk.private_key};
    }
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
    if(b->file->putbuffer(bs.mutable_data(), b->offset, bs.size()) >= 0){
        b->flags |= BLOCK_SYNC;
    }
}

void block_t::push(block_t* b) {
    auto_wlock(b);
    if((b->flags & BLOCK_DIRTY) == 0 || (b->flags & BLOCK_STALE)){
        return;
    }
    char *buff = (char*)malloc(b->size);
    size_t len = b->file->getbuffer(buff, b->offset, b->size);
    if(len < 0){
        return;
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
        int ret = HANDLE_EAGAIN(fm_upload(b->file->getkey(), file, buff, len, false));
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

void block_t::makedirty() {
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
    blksize(meta.blksize)
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

/*
file_t::file_t(dir_t* parent, const filemeta& meta, std::vector<filekey> fblocks):
    entry_t(parent, meta),
    private_key(meta.key.private_key),
    blksize(meta.blksize),
    flags(meta.flags)
{
    flags |=  ENTRY_CHUNCED_F;
    if(meta.inline_data){
        assert(meta.size < (size_t)meta.blksize);
        inline_data = (char*)meta.inline_data;
        blocks.emplace(0, new block_t(this, filekey{"x", 0}, 0, 0, blksize));
    }else{
        //zero is the first block
        assert(fblocks.size() == GetBlkNo(length, blksize)+1);
    }
    for(size_t i = 0; i < fblocks.size(); i++ ){
        blocks.emplace(i, new block_t(this, fblocks[i], i, blksize * i, blksize));
    }
}
*/

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
    this->mtime = time(NULL);
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
