#ifndef BLOCK_H__
#define BLOCK_H__
#include "utils.h"
#include "entry.h"

class block_t: locker {
    int fd;
    filekey fk;
    const size_t no;
    const off_t offset;
    const size_t size;
    unsigned int flags;
    time_t atime;
    int staled();
    static void pull(block_t* b);
    static void push(block_t* b);
    friend void writeback_thread();
    std::string getpath();
    filekey getkey();
public:
    block_t(int fd, filekey fk, size_t no, off_t offset, size_t size, unsigned int flags);
    ~block_t();
    std::tuple<filekey, uint> getmeta();
    void prefetch(bool wait);
    void markdirty();
    void markstale();
    void sync();
    void reset();
    bool dummy();
};

#endif