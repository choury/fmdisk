#include "fmdisk.h"
#include "file.h"
#include "cache.h"
#include "threadpool.h"
#include "defer.h"

#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <semaphore.h>

#include <list>

sem_t dirty_sem;
std::list<block_t*> dblocks;
pthread_mutex_t dblocks_lock = PTHREAD_MUTEX_INITIALIZER;

static_assert(BLOCKLEN > INLINE_DLEN, "blocklen should not bigger than inline date length");

void writeback_thread(){
    sem_init(&dirty_sem, 0, UPLOADTHREADS/2);
    while(true){
        usleep(100000);
        pthread_mutex_lock(&dblocks_lock);
        if(dblocks.empty()){
            pthread_mutex_unlock(&dblocks_lock);
            continue;
        }else if(taskinqueu(upool) == 0){
            addtask(upool, (taskfunc)block_t::push, dblocks.front(), 0);
            dblocks.pop_front();
            sem_post(&dirty_sem);
        }
        for(auto i = dblocks.begin(); i!= dblocks.end();){
            if((*i)->staled() >= 10){
                addtask(upool, (taskfunc)block_t::push, *i, 0);
                i = dblocks.erase(i);
                sem_post(&dirty_sem);
            }else{
                break;
            }
        }
        pthread_mutex_unlock(&dblocks_lock);
    }
}

block_t::block_t(file_t* file, filekey fk, size_t no, off_t offset, size_t size):
    file(file),
    fk(basename(fk)),
    no(no),
    offset(offset),
    size(size)
{
    if(basename(fk.path) == "x"){
        flags = BLOCK_SYNC;
    }
}

block_t::~block_t() {
    pthread_mutex_lock(&dblocks_lock);
    for(auto i = dblocks.begin(); i != dblocks.end();){
        if(*i ==  this){
            i = dblocks.erase(i);
            sem_post(&dirty_sem);
        }else{
            i++;
        }
    }
    pthread_mutex_unlock(&dblocks_lock);
    auto_wlock(this);
    file->trim(getkey());
}

filekey block_t::getkey(){
    filekey key = file->getDirkey();
    return filekey{pathjoin(key.path, fk.path), fk.private_key};
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
    assert(bs.offset <= (size_t)b->size);
    b->file->putbuffer(bs.buf, b->offset, bs.offset);
    b->flags |= BLOCK_SYNC;
}

void block_t::push(block_t* b) {
    auto_wlock(b);
    if((b->flags & BLOCK_DIRTY) == 0){
        return;
    }
    char *buff = (char*)malloc(b->size);
    size_t len = b->file->getbuffer(buff, b->offset, b->size);
    b->file->trim(b->getkey());
    if(len){
        //It must be chunk file, because native file can't be written
        filekey file{std::to_string(b->no), 0};
retry:
        int ret = HANDLE_EAGAIN(fm_upload(b->file->getDirkey(), file, buff, len, false));
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
    pthread_mutex_lock(&dblocks_lock);
    bool found = false;
    for(auto i = dblocks.begin(); i != dblocks.end(); i++ ){
        if(*i == this){
            found = true;
            dblocks.erase(i);
            break;
        }
    }
    dblocks.push_back(this);
    pthread_mutex_unlock(&dblocks_lock);
    flags |=  BLOCK_DIRTY;
    unwlock();
    if(!found){
        sem_wait(&dirty_sem);
    }
}

void block_t::sync(){
    pthread_mutex_lock(&dblocks_lock);
    for(auto i = dblocks.begin(); i != dblocks.end();){
        if(*i ==  this){
            i = dblocks.erase(i);
            sem_post(&dirty_sem);
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
    if(fk.path == "x"){
        flags = BLOCK_SYNC;
    }else{
        flags = 0;
    }
}

bool block_t::zero() {
    return fk.path == "x";
}


int block_t::staled(){
    auto_rlock(this);
    return time(0) - atime;
}

static int tempfile() {
    int fd;
    char tmpfilename[PATHLEN];
    snprintf(tmpfilename, sizeof(tmpfilename), "%s/.fm_fuse_XXXXXX", fm_getcachepath());
    if ((fd = mkstemp(tmpfilename)) != -1) {
        /*Unlink the temp file.*/
        unlink(tmpfilename);
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

file_t::file_t(entry_t *entry, const filemeta& meta):
    entry(entry),
    private_key(meta.key.private_key),
    size(meta.size),
    blksize(meta.blksize),
    mtime(meta.mtime),
    flags(meta.flags)
{
    if(flags & ENTRY_CREATE_F){
    //creata new file
        assert(flags & ENTRY_CHUNCED_F);
        assert(size == 0);
        inline_data = new char[INLINE_DLEN];
        private_key = nullptr;
        blocks.emplace(0, new block_t(this, filekey{"x", 0}, 0, 0, blksize));
        return;
    }
    //for the non-chunk file
    for(size_t i = 0; i <= GetBlkNo(size, blksize); i++ ){
        blocks.emplace(i, new block_t(this, filekey{"", meta.key.private_key}, i, blksize * i, blksize));
    }
}

file_t::file_t(entry_t* entry, const filemeta& meta, std::vector<filekey> fblocks):
    entry(entry),
    private_key(meta.key.private_key),
    size(meta.size),
    blksize(meta.blksize),
    mtime(meta.mtime),
    flags(meta.flags)
{
    flags |=  ENTRY_CHUNCED_F;
    if(meta.inline_data){
        assert(meta.size < (size_t)meta.blksize);
        inline_data = (char*)meta.inline_data;
        blocks.emplace(0, new block_t(this, filekey{"x", 0}, 0, 0, blksize));
    }else{
        //zero is the first block
        assert(fblocks.size() == GetBlkNo(size, blksize)+1);
    }
    for(size_t i = 0; i < fblocks.size(); i++ ){
        blocks.emplace(i, new block_t(this, fblocks[i], i, blksize * i, blksize));
    }
}

file_t::~file_t() {
    auto_wlock(this);
    for(auto i: blocks){
        delete i.second;
    }
    if(fd){
        close(fd);
    }
    if(inline_data){
        delete[] inline_data;
    }
    pthread_mutex_destroy(&extraLocker);
}

int file_t::open(){
    auto_wlock(this);
    if(fd){
        return 0;
    }
    fd = tempfile();
    if(fd > 0){
        TEMP_FAILURE_RETRY(ftruncate(fd, size));
    }
    return fd;
}

int file_t::read(void* buff, off_t offset, size_t size) {
    auto_rlock(this);
    if((size_t)offset > this->size){
        return -EFAULT;
    }
    if(offset + size > this->size){
        size = this->size - offset;
    }
    if(inline_data){
        memcpy(buff, inline_data + offset, size);
        return size;
    }
    size_t startc = GetBlkNo(offset, blksize);
    size_t endc = GetBlkNo(offset + size, blksize);
    for(size_t i = startc; i< endc + DOWNLOADTHREADS && i<= GetBlkNo(this->size, blksize); i++){
        blocks[i]->prefetch(false);
    }
    for(size_t i = startc; i<= endc; i++ ){
        blocks[i]->prefetch(true);
    }
    return pread(fd, buff, size, offset);
}

int file_t::truncate_rlocked(off_t offset){
    if((size_t)offset == size){
        return 0;
    }
    size_t newc = GetBlkNo(offset, blksize);
    size_t oldc = GetBlkNo(size, blksize);
    if(newc > oldc){
        if(newc >= MAXFILE){
            errno = EFBIG;
            return -1;
        }
        upgrade();
        for(size_t i = oldc + 1; i<= newc; i++){
            blocks.emplace(i, new block_t(this, filekey{"x", 0}, i, blksize * i, blksize));
        }
    }else if(oldc >= newc && inline_data == nullptr){
        blocks[newc]->prefetch(true);
        blocks[newc]->makedirty();
        upgrade();
        for(size_t i = newc + 1; i<= oldc; i++){
            delete blocks[i];
            blocks.erase(i);
        }
    }else{
        upgrade();
    }
    defer(&file_t::downgrade, dynamic_cast<locker*>(this));
    size = offset;
    if(inline_data && (size > (off_t)INLINE_DLEN)){
        delete[] inline_data;
        inline_data = nullptr;
    }
    if(size == 0 && inline_data == nullptr){
        inline_data = new char[INLINE_DLEN];
    }
    mtime = time(0);
    flags |= FILE_DIRTY_F;
    assert(fd);
    return TEMP_FAILURE_RETRY(ftruncate(fd, offset));
}

int file_t::truncate(off_t offset){
    auto_rlock(this);
    return truncate_rlocked(offset);
}

int file_t::write(const void* buff, off_t offset, size_t size) {
    auto_rlock(this);
    if((size_t)offset + size > this->size){
        int ret = truncate_rlocked(offset + size);
        if(ret < 0){
            return ret;
        }
    }
    assert((!inline_data) || (inline_data && this->size < INLINE_DLEN));
    if(inline_data){
        __r.upgrade();
        memcpy(inline_data + offset, buff, size);
    }else{
        size_t startc = GetBlkNo(offset, blksize);
        size_t endc = GetBlkNo(offset + size, blksize);
        for(size_t i = startc; i <= endc; i++){
            blocks[i]->prefetch(true);
        }
        for(size_t i =  startc; i <= endc; i++){
            blocks[i]->makedirty();
        }
        __r.upgrade();
    }
    mtime = time(0);
    flags |= FILE_DIRTY_F;
    return pwrite(fd, buff, size, offset);
}


int file_t::sync(){
    auto_rlock(this);
    for(auto i: blocks){
        i.second->sync();
    }
    return 0;
}

std::vector<filekey> file_t::getfblocks(){
    auto_rlock(this);
    if(inline_data){
        return std::vector<filekey>();
    }
    std::vector<filekey> fblocks(blocks.size());
    for(auto i : this->blocks){
        fblocks[i.first] = basename(i.second->getkey());
    }
    return fblocks;
}

int file_t::release(){
    auto_rlock(this);
    for(auto i: blocks){
        i.second->reset();
    }
    __r.upgrade();
    if(fd){
        close(fd);
    }
    fd = 0;
    return 0;
}

int file_t::getbuffer(void* buffer, off_t offset, size_t size) {
    auto_rlock(this);
    assert(fd);
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
        xorcode(buffer, offset, ret, fm_getsecret());
    }
    return ret;
}

int file_t::putbuffer(void* buffer, off_t offset, size_t size) {
    auto_rlock(this);
    assert(fd);
    if(flags & FILE_ENCODE_F){
        xorcode(buffer, offset, size, fm_getsecret());
    }
    return TEMP_FAILURE_RETRY(pwrite(fd, buffer, size, offset));
}

filekey file_t::getDirkey() {
    auto_rlock(this);
    return entry->getkey();
}

filemeta file_t::getmeta() {
    auto_rlock(this);
    filemeta meta = initfilemeta(filekey{METANAME, private_key});
    meta.inline_data = (unsigned char*)inline_data;
    meta.flags = flags;
    meta.size = size;
    meta.blksize = blksize;
    meta.mtime = mtime;
    meta.blocks = 1; //at least for meta.json
    for(auto i: blocks){
        if(i.second->zero()){
            continue;
        }
        meta.blocks ++ ;
    }
    return meta;
}


void file_t::trim(const filekey& file) {
    string name = basename(file.path);
    if(name == "" || name == "x"){
        return;
    }
    auto_lock(&extraLocker);
    droped.emplace_back(file);
}

void file_t::post_sync(const filekey& file) {
    wlock();
    private_key = file.private_key;
    flags &= ~FILE_DIRTY_F;
    unwlock();
    auto_lock(&extraLocker);
    if(!droped.empty()){
        fm_batchdelete(droped);
        droped.clear();
    }
}


void file_t::setmtime(time_t mtime) {
    auto_wlock(this);
    flags |= FILE_DIRTY_F;
    this->mtime = mtime;
}

