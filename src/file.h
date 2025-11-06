#ifndef FILE_H__
#define FILE_H__
#include "utils.h"
#include "entry.h"
#include "block.h"

#include <memory>
#include <vector>
#include <map>
#include <functional>

#define FM_REMOTE_PATH_ATTR "user.fm_remote_path"
#define FM_TEMP_FILE_ATTR "user.fm_temp_file"
#define file_encode_suffix ".def"

class file_t: public entry_t {
    fileInfo fi;
    std::shared_ptr<void> private_key; //for meta.json
    std::string inline_data;
    blksize_t blksize;
    enum storage_class storage = STORAGE_UNKNOWN;
    std::map<uint32_t, std::shared_ptr<block_t>> blocks;
    size_t block_size = 0; // cache for getmeta
    int truncate_wlocked(off_t offset);
    int update_meta_wlocked(filemeta& meta, std::function<void(filemeta&)> meta_updater);
    virtual int pull_wlocked() override;
    static void clean(std::weak_ptr<file_t> file);
    virtual std::string getrealname() override;
    std::shared_ptr<file_t> shared_file_from_this() {
        return std::static_pointer_cast<file_t>(shared_from_this());
    }
    filekey getblockdir();

    time_t last_meta_sync_time;  // 上次创建upload_meta_async_task的时间
    static void upload_meta_async_task(std::weak_ptr<file_t> file);  // 异步上传meta的静态函数
    virtual int drop_cache_wlocked(bool mem_only, time_t before) override;
    virtual int remove_wlocked(bool skip_entry) override;
    virtual int set_storage_class(enum storage_class storage, TrdPool* pool, std::vector<std::future<int>>& futures) override;
public:
    file_t(std::shared_ptr<dir_t> parent, const filemeta& meta);
    virtual ~file_t();
    void reset_wlocked();

    filekey getmetakey();
    std::vector<filekey> getkeys();
    virtual int getmeta(filemeta& meta) override;

    virtual int open() override;
    virtual int release(bool waitsync) override;
    //如果仍然是dirty状态，返回true
    bool sync_wlocked(bool forcedirty = false);
    virtual int sync(int dataonly) override;
    virtual int utime(const struct timespec tv[2]) override;
    virtual int chmod(mode_t mode) override;
    int read(void* buff, off_t offset, size_t size);
    int truncate(off_t offset);
    int write(const void* buff, off_t offset, size_t size);
    //It will release the wlock
    std::vector<filekey> getfblocks();

    virtual void dump_to_db(const std::string& path, const std::string& name) override;
    virtual int collect_storage_classes(TrdPool* pool, std::vector<std::future<std::pair<int, storage_class_info>>>& futures) override;
    virtual int get_etag(std::string& etag) override;

    // 使用fallocate释放clean状态block的磁盘空间，返回释放的字节数
    size_t release_clean_blocks();

    friend class dir_t;
};

void writeback_thread(bool* done);
void start_gc();
void stop_gc();
void trim(const filekey& file);
void recover_dirty_data();

#endif
