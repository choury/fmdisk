#ifndef FILE_H__
#define FILE_H__
#include "utils.h"
#include "cache.h"

#include <vector>
#include <map>

class file_t;
class block_t: locker {
    file_t* file;
    filekey fk;
    const size_t no;
    const off_t offset;
    const size_t size;
#define BLOCK_SYNC   1
#define BLOCK_DIRTY  2
    unsigned int flags;
    time_t atime;
    int staled();
    static void pull(block_t* b);
    static void push(block_t* b);
    friend void writeback_thread();
public:
    block_t(file_t* file, filekey fk, size_t no, off_t offset, size_t size, unsigned int flags);
    ~block_t();
    filekey getkey();
    void prefetch(bool wait);
    void makedirty();
    void sync();
    void reset();
    bool dummy();
};

class file_t: public entry_t {
    int fd = -1;
    std::shared_ptr<void> private_key; //for meta.json
    char* inline_data = nullptr;
    blksize_t blksize;
    std::map<uint32_t, block_t*> blocks;
    pthread_mutex_t dropLocker = PTHREAD_MUTEX_INITIALIZER;
    std::vector<filekey> droped;
    int truncate_rlocked(off_t offset);
    virtual void pull_wlocked() override;
    static void clean(file_t* file);
public:
    //for simple native file, use st.st_ino as flags
    file_t(dir_t* parent, const filemeta& meta);
    //for chunck block file
    file_t(dir_t* parent, const filemeta& meta, std::vector<filekey> fblocks);
    virtual ~file_t();

    filekey getDirkey();
    filekey getmetakey();
    std::vector<filekey> getkeys();
    virtual filemeta getmeta() override;
    int putbuffer(void* buffer, off_t offset, size_t size);
    int getbuffer(void* buffer, off_t offset, size_t size);

    virtual int open() override;
    virtual int release() override;
    virtual int sync(int dataonly) override;
    virtual int utime(const struct timespec tv[2]) override;
    int read(void* buff, off_t offset, size_t size);
    int truncate(off_t offset);
    int write(const void* buff, off_t offset, size_t size);
    std::vector<filekey> getfblocks();
    void trim(const filekey& fk);


    virtual void dump_to_disk_cache() override;
    virtual int drop_mem_cache() override;
};


void writeback_thread();

#endif
