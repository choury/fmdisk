#ifndef BLOCK_H__
#define BLOCK_H__
#include "utils.h"
#include "entry.h"

#include <atomic>

class block_t: public locker, public std::enable_shared_from_this<block_t> {
    const fileInfo fi;
    filekey fk;   //fk.path = 'x'表示空块(同时private_key也为空), fk.path = ''表示未分块的文件
    const size_t no;
    const off_t offset;
    const size_t size;
    unsigned int flags;
    std::atomic<time_t> atime;
    std::atomic<size_t> version = 0;
    int staled();
    static int pull(std::weak_ptr<block_t> b);
    static void push(std::weak_ptr<block_t> b, filekey fileat);
    friend void writeback_thread(bool* done);
    [[nodiscard]] std::string getpath() const;
    [[nodiscard]] filekey getkey() const;
public:
    block_t(const fileInfo& fi, filekey fk, size_t no, off_t offset, size_t size, unsigned int flags);
    ~block_t() override;
    filekey getfk() {
        auto_rlock(this);
        return fk;
    }
    int prefetch(bool wait);
    ssize_t read(filekey fileat, void* buff, off_t offset, size_t len);
    void markdirty(filekey fileat);
    void markstale();
    bool sync(filekey fileat, bool wait);
    [[nodiscard]] bool dummy();
    [[nodiscard]] size_t release();
};

#endif