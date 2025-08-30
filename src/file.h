#ifndef FILE_H__
#define FILE_H__
#include "utils.h"
#include "entry.h"
#include "block.h"

#include <memory>
#include <vector>
#include <map>

#define FM_REMOTE_PATH_ATTR "user.fm_remote_path"

class file_t: public entry_t {
    int fd = -1;
    ino_t inode = 0;
    std::shared_ptr<void> private_key; //for meta.json
    char* inline_data = nullptr;
    blksize_t blksize;
    enum storage_class storage = STORAGE_UNKNOWN;
    std::map<uint32_t, std::shared_ptr<block_t>> blocks;
    size_t block_size = 0; // cache for getmeta
    int truncate_wlocked(off_t offset);
    virtual int pull_wlocked() override;
    static void clean(file_t* file);
    virtual std::string getrealname() override;
    void set_private_key_wlocked(std::shared_ptr<void> key) {
        private_key = key;
        if(fm_private_key_tostring(fk.load()->private_key)[0] == '\0') {
            fk = std::make_shared<filekey>(filekey{fk.load()->path, key});
        }
    }

    time_t last_meta_sync_time;  // 上次创建upload_meta_async_task的时间
    static void upload_meta_async_task(file_t* file);  // 异步上传meta的静态函数
    virtual int drop_cache_wlocked() override;
    virtual int remove_wlocked() override;
public:
    file_t(dir_t* parent, const filemeta& meta);
    virtual ~file_t();
    void reset_wlocked();

    filekey getmetakey();
    std::vector<filekey> getkeys();
    virtual int getmeta(filemeta& meta) override;

    virtual bool isDir() override {
        return false;
    }
    virtual int open() override;
    virtual int release() override;
    //如果仍然是dirty状态，返回true
    bool sync_wlocked(bool forcedirty = false);
    virtual int sync(int dataonly) override;
    virtual int utime(const struct timespec tv[2]) override;
    int read(void* buff, off_t offset, size_t size);
    int truncate(off_t offset);
    int write(const void* buff, off_t offset, size_t size);
    //It will release the wlock
    std::vector<filekey> getfblocks();

    virtual void dump_to_db(const std::string& path, const std::string& name) override;
    virtual int get_storage_classes(storage_class_info& info) override;
    virtual int set_storage_class(enum storage_class storage) override;

    // 使用fallocate释放clean状态block的磁盘空间，返回释放的字节数
    size_t release_clean_blocks();

    friend class dir_t;
};

void writeback_thread(bool* done);
void start_gc();
void stop_gc();
void trim(const filekey& file);
void recover_dirty_data(dir_t* root);

#endif
