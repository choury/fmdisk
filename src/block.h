#ifndef BLOCK_H__
#define BLOCK_H__
#include "utils.h"
#include "entry.h"

class block_t: locker {
    int fd;
    ino_t inode;  // 缓存文件的inode
    filekey fk;
    const size_t no;
    const off_t offset;
    const size_t size;
    unsigned int flags;
    time_t atime;
    size_t version = 0;
    int staled();
    static void pull(block_t* b);
    static void push(block_t* b);
    friend void writeback_thread(bool* done);
    [[nodiscard]] std::string getpath() const;
    [[nodiscard]] filekey getkey() const;
public:
    block_t(int fd, ino_t inode, filekey fk, size_t no, off_t offset, size_t size, unsigned int flags);
    ~block_t() override;
    filekey getfk() {
        auto_rlock(this);
        return fk;
    }
    void prefetch(bool wait);
    void markdirty();
    void markstale();
    bool sync();
    void reset();
    bool dummy();
};

#endif