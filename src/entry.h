#ifndef CACHE_H__
#define CACHE_H__

#include "locker.h"
#include "fmdisk.h"

#include <sys/stat.h>
#include <time.h>
#include <atomic>
#include <future>
#include <utility>

using std::string;

struct filemeta;
class dir_t;
class TrdPool;

struct storage_class_info {
    size_t size_store[5]; // 0: UNKNOWN, 1: STANDARD, 2: IA, 3: ARCHIVE, 4: DEEP_ARCHIVE
    size_t size_archive_restored;
    size_t size_archive_restoring;
    size_t size_deep_archive_restored;
    size_t size_deep_archive_restoring;
};

class entry_t: public locker, public std::enable_shared_from_this<entry_t> {
protected:
    std::weak_ptr<dir_t> parent;
    std::atomic<std::shared_ptr<filekey>> fk;
    std::atomic<mode_t> mode;
    std::atomic<time_t> atime;
    size_t length;
    time_t mtime = 0;
    time_t ctime = 0;
    uint32_t flags = 0;
    uint32_t opened = 0;
    string getcwd();
    virtual string getrealname() = 0;
    virtual int pull_wlocked() = 0;
    virtual int drop_cache_wlocked(bool mem_only, time_t before) = 0;
    virtual int remove_wlocked() = 0;
    static void pull(std::weak_ptr<entry_t> entry);
    virtual int set_storage_class(enum storage_class storage, TrdPool* pool, std::vector<std::future<int>>& futures) {
        return -EINVAL;
    }
    virtual int collect_storage_classes(TrdPool* pool, std::vector<std::future<std::pair<int, storage_class_info>>>&) {
        return -EINVAL;
    }
public:
    static int statfs(const char* path, struct statvfs *sf);
    entry_t(std::shared_ptr<dir_t> parent, const filemeta& meta);
    virtual ~entry_t() override;
    filekey getkey();
    mode_t getmode() const {
        return mode;
    }
    virtual int getmeta(filemeta& meta) = 0;
    [[nodiscard]] size_t size() const {
        return length;
    }
    virtual int open() = 0;
    virtual int sync(int dataonly) = 0;
    virtual int release(bool waitsync) = 0;
    virtual int utime(const struct timespec tv[2]) = 0;
    virtual int chmod(mode_t mode);
    virtual void dump_to_db(const std::string& path, const std::string& name) = 0;
    int drop_cache(bool mem_only, time_t before = 0) {
        if(before == 0) {
            auto_wlock(this);
            return drop_cache_wlocked(mem_only, 0);
        } else {
            if(trywlock() != 0) {
                return -EBUSY;
            }
            int ret = drop_cache_wlocked(mem_only, before);
            unwlock();
            return ret;
        }
    }
    int get_storage_classes(storage_class_info& info);
    virtual int get_etag(std::string&) {
        return -ENODATA;
    }

    int set_storage_class(enum storage_class storage);
    friend class dir_t;
};

class TrdPool;
extern TrdPool* upool;
extern TrdPool* dpool;

int cache_prepare();
std::shared_ptr<dir_t> cache_root();
void cache_destroy();
int create_dirs_recursive(const string& path);
void clean_entry_cache();

#endif
