#ifndef CACHE_H__
#define CACHE_H__

#include "locker.h"
#include "fmdisk.h"

#include <sys/stat.h>
#include <time.h>

#include <map>

#define ENTRY_INITED_F    (1<<0)
#define ENTRY_CHUNCED_F   (1<<1)
#define ENTRY_DELETED_F   (1<<2)
#define ENTRY_REASEWAIT_F (1<<3)
#define ENTRY_CREATE_F    (1<<4)
#define FILE_ENCODE_F  (1<<5)
#define FILE_DIRTY_F   (1<<6)
#define DIR_PULLED_F   (1<<7)


using std::string;

class dir_t;
class file_t;
struct filemeta;

class entry_t: locker {
    pthread_mutex_t init_lock = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t  init_cond = PTHREAD_COND_INITIALIZER;
    entry_t* parent;
    struct filekey fk;
    mode_t mode;
    union{
        dir_t* dir = nullptr;
        file_t* file;
    };
    time_t ctime = 0;
    uint32_t flags = 0;
    uint32_t opened = 0;
    void init_wait();
    void erase(string name);
    void insert(string name, entry_t* entry);
    static void pull(entry_t* entry);
    static void push(entry_t* entry);
    static void clean(entry_t* entry);
public:
    static int statfs(const char* path, struct statvfs *sf);
    entry_t(entry_t* parent, const filemeta meta);
    virtual ~entry_t();
    string getcwd();
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
    int move(entry_t* newparent, string name);
    int utime(const struct timespec tv[2]);
    int unlink();
    int rmdir();
};

int cache_prepare();
entry_t* cache_root();
filekey basename(const filekey& file);
filekey decodepath(const filekey& file);

#endif
