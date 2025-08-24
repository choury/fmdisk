#ifndef CACHE_H__
#define CACHE_H__

#include "locker.h"
#include "fmdisk.h"

#include <sys/stat.h>
#include <time.h>

using std::string;

struct filemeta;
class dir_t;

struct storage_class_info {
    size_t size_store[5]; // 0: UNKNOWN, 1: STANDARD, 2: IA, 3: ARCHIVE, 4: DEEP_ARCHIVE
    size_t size_archive_restored;
    size_t size_archive_restoring;
    size_t size_deep_archive_restored;
    size_t size_deep_archive_restoring;
};

class entry_t: public locker {
protected:
    dir_t* parent;
    std::atomic<std::shared_ptr<filekey>> fk;
    mode_t mode;
    size_t length;
    time_t mtime = 0;
    time_t ctime = 0;
    uint32_t flags = 0;
    uint32_t opened = 0;
    string getcwd();
    virtual string getrealname() = 0;
    virtual void pull_wlocked() = 0;
    void pull_wlocked(filemeta& meta, std::vector<filekey>& fblocks);
    virtual int drop_cache_wlocked() = 0;
    static void pull(entry_t* entry);
public:
    static int statfs(const char* path, struct statvfs *sf);
    entry_t(dir_t* parent, const filemeta& meta);
    virtual ~entry_t() override;
    filekey getkey();
    virtual filemeta getmeta() = 0;
    [[nodiscard]] size_t size() const {
        return length;
    }
    virtual bool isDir() = 0;
    virtual int open() = 0;
    virtual int sync(int dataonly) = 0;
    virtual int release() = 0;
    virtual int utime(const struct timespec tv[2]) = 0;
    virtual void dump_to_db(const std::string& path, const std::string& name) = 0;
    int drop_cache() {
        auto_wlock(this);
        return drop_cache_wlocked();
    }
    virtual storage_class_info get_storage_classes() {
        return {};
    }
    virtual int set_storage_class(enum storage_class storage) {
        return -EINVAL;
    }
    friend class dir_t;
};

class TrdPool;
extern TrdPool* upool;
extern TrdPool* dpool;

filekey basename(const filekey& file);
filekey decodepath(const filekey& file);
int create_dirs_recursive(const string& path);

#endif
