#ifndef CACHE_H__
#define CACHE_H__

#include "locker.h"
#include "fmdisk.h"

#include <sys/stat.h>
#include <time.h>

using std::string;

struct filemeta;
class dir_t;
class entry_t: public locker {
protected:
    dir_t* parent;
    filekey fk;
    mode_t mode;
    size_t length;
    time_t mtime = 0;
    time_t ctime = 0;
    uint32_t flags = 0;
    uint32_t opened = 0;
    string getcwd();
    virtual void pull_wlocked() = 0;
    void pull_wlocked(filemeta& meta, std::vector<filekey>& fblocks);
    static void pull(entry_t* entry);
public:
    static int statfs(const char* path, struct statvfs *sf);
    entry_t(dir_t* parent, const filemeta meta);
    virtual ~entry_t();
    filekey getkey();
    virtual filemeta getmeta() = 0;
    size_t size() {
        return length;
    }
    virtual bool isDir() = 0;
    virtual int open() = 0;
    virtual int sync(int dataonly) = 0;
    virtual int release() = 0;
    virtual int utime(const struct timespec tv[2]) = 0;
    virtual void dump_to_disk_cache() = 0;
    virtual int drop_mem_cache() = 0;
    int drop_disk_cache();

    friend class dir_t;
    friend class file_t;
};

struct thrdpool;
extern thrdpool* upool;
extern thrdpool* dpool;

int cache_prepare();
void cache_destroy(dir_t* root);
filekey basename(const filekey& file);
filekey decodepath(const filekey& file);

#endif
