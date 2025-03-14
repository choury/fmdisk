#ifndef FILE_H__
#define FILE_H__
#include "locker.h"
#include "utils.h"

#include <vector>
#include <map>

using std::string;

class entry_t;
class file_t;
class block_t: locker {
    file_t* file;
    filekey fk;
    const size_t no;
    const off_t offset;
    const size_t size;
#define BLOCK_SYNC   1
#define BLOCK_DIRTY  2
    unsigned int flags = 0;
    time_t atime;
    int staled();
    static void pull(block_t* b);
    static void push(block_t* b);
    friend void writeback_thread();
public:
    block_t(file_t* file, filekey fk, size_t no, off_t offset, size_t size);
    ~block_t();
    filekey getkey();
    void prefetch(bool wait);
    void makedirty();
    void sync();
    void reset();
    bool zero();
};

class file_t: locker {
    entry_t* entry;
    int fd = -1;
    std::shared_ptr<void> private_key; //for meta.json
    char* inline_data = nullptr;
    size_t size;
    blksize_t blksize;
    uint32_t flags;
    std::map<uint32_t, block_t*> blocks;
    pthread_mutex_t dropLocker = PTHREAD_MUTEX_INITIALIZER;
    std::vector<filekey> droped;
    int truncate_rlocked(off_t offset);
public:
    //for simple native file, use st.st_ino as flags
    file_t(entry_t* entry, const filemeta& meta);
    //for chunck block file
    file_t(entry_t* entry, const filemeta& meta, std::vector<filekey> fblocks);
    virtual ~file_t();

    filekey getDirkey();
    filekey getkey();
    filemeta getmeta();
    int putbuffer(void* buffer, off_t offset, size_t size);
    int getbuffer(void* buffer, off_t offset, size_t size);

    int open();
    int read(void* buff, off_t offset, size_t size);
    int truncate(off_t offset);
    int write(const void* buff, off_t offset, size_t size);
    int syncfblocks();
    std::vector<filekey> getfblocks();
    int release();
    void trim(const filekey& fk);
    void post_sync(const filekey& fk);
};


void writeback_thread();

#endif
