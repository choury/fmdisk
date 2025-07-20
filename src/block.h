#ifndef BLOCK_H__
#define BLOCK_H__
#include "utils.h"
#include "entry.h"

class file_t;
class block_t: locker {
    file_t* file;
    filekey fk;
    const size_t no;
    const off_t offset;
    const size_t size;
#define BLOCK_SYNC   1
#define BLOCK_DIRTY  2
#define BLOCK_STALE  4
    unsigned int flags;
    time_t atime;
    int staled();
    static void pull(block_t* b);
    static void push(block_t* b);
    friend void writeback_thread();
    friend class file_t;
public:
    block_t(file_t* file, filekey fk, size_t no, off_t offset, size_t size, unsigned int flags);
    ~block_t();
    filekey getkey();
    void prefetch(bool wait);
    void makedirty();
    void sync();
    void sync_local_only();
    void reset();
    bool dummy();
};

void recover_dirty_blocks();
#endif