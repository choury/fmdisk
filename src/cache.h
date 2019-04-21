#ifndef CACHE_H__
#define CACHE_H__

#include "locker.h"
#include "fmdisk.h"

#include <sys/stat.h>
#include <time.h>

#include <map>



using std::string;

class dir_t;
class file_t;
struct filemeta;

class entry_t: locker {
    entry_t* parent;
    struct filekey fk;
    mode_t mode;
    union{
        dir_t* dir = nullptr;
        file_t* file;
    };
//    time_t ctime = 0;
    uint32_t flags = 0;
    uint32_t opened = 0;
    void erase_child_rlocked(entry_t* child);
    string getcwd();
    void pull_wlocked();
    static void pull(entry_t* entry);
    static void clean(entry_t* entry);
public:
    static int statfs(const char* path, struct statvfs *sf);
    entry_t(entry_t* parent, const filemeta meta);
    virtual ~entry_t();
    filekey getkey();
    struct filemeta getmeta();
    entry_t* find(string path);
    entry_t* create(string name);
    entry_t* mkdir(string name);
    int open();
    const std::map<string, entry_t*>& entrys();
    int read(void* buff, off_t offset, size_t size);
    int truncate(off_t offset);
    int write(const void* buff, off_t offset, size_t size);
    int sync(int datasync);
    int flush();
    int release();
    int moveto(entry_t* newparent, string oldname, string newname);
    int utime(const struct timespec tv[2]);
    int unlink(const string& name);
    int rmdir(const string& name);
    void dump_to_disk_cache();
    int drop_mem_cache();
    int drop_disk_cache();
};

struct thrdpool;
extern thrdpool* upool;
extern thrdpool* dpool;

int cache_prepare();
entry_t* cache_root();
void cache_destroy(entry_t* root);
filekey basename(const filekey& file);
filekey decodepath(const filekey& file);

#endif
