#ifndef FILE_H__
#define FILE_H__
#include "utils.h"
#include "entry.h"
#include "block.h"

#include <vector>
#include <map>

class file_t: public entry_t {
    int fd = -1;
    ino_t inode = 0;
    std::shared_ptr<void> private_key; //for meta.json
    char* inline_data = nullptr;
    blksize_t blksize;
    std::map<uint32_t, block_t*> blocks;
    size_t block_size = 0; // cache for getmeta
    int truncate_wlocked(off_t offset);
    virtual void pull_wlocked() override;
    static void clean(file_t* file);
public:
    file_t(dir_t* parent, const filemeta& meta);
    virtual ~file_t();
    void reset_wlocked();

    filekey getmetakey();
    std::vector<filekey> getkeys();
    virtual filemeta getmeta() override;

    virtual bool isDir() override {
        return false;
    }
    virtual int open() override;
    virtual int release() override;
    //如果仍然是dirty状态，返回true
    bool sync_wlocked();
    virtual int sync(int dataonly) override;
    virtual int utime(const struct timespec tv[2]) override;
    int read(void* buff, off_t offset, size_t size);
    int truncate(off_t offset);
    int write(const void* buff, off_t offset, size_t size);
    //It will release the wlock
    int remove_and_release_wlock();
    std::vector<filekey> getfblocks();

    virtual void dump_to_disk_cache() override;
    virtual int drop_mem_cache() override;

    time_t last_meta_sync_time;  // 上次创建upload_meta_async_task的时间
    static void upload_meta_async_task(file_t* file);  // 异步上传meta的静态函数

    friend class dir_t;
};

void writeback_thread(bool* done);
void start_gc();
void stop_gc();
void trim(const filekey& file);
void recover_dirty_data(dir_t* root);

#endif
