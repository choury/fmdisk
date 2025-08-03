#ifndef DIR_H__
#define DIR_H__
#include "entry.h"

#include <map>

using std::string;

class file_t;
class dir_t: public entry_t {
    std::map<string, entry_t*> entrys;
    virtual void pull_wlocked() override {
        filemeta meta;
        entry_t::pull_wlocked(meta);
    }
    void pull_entrys_wlocked();
    entry_t* insert_child_wlocked(const std::string& name, entry_t* entry);
    void erase_child_wlocked(const std::string& path, const std::string& name);
public:
    dir_t(dir_t* parent, const filemeta& meta);
    virtual ~dir_t() override;
    virtual filemeta getmeta() override;
    entry_t* find(std::string path);
    const std::map<string, entry_t*>& get_entrys();
    size_t children();

    virtual bool isDir() override {
        return true;
    }
    virtual int open() override {
        auto_wlock(this);
        if((flags & ENTRY_INITED_F) == 0){
            pull_wlocked();
        }
        opened++;
        return 0;
    }
    virtual int release() override{
        auto_wlock(this);
        assert(opened > 0);
        opened--;
        if(opened == 0 && flags & ENTRY_DELETED_F) {
            __w.unlock();
            delete this;
        }
        return 0;
    }
    virtual int sync(int dataonly) override;
    virtual int utime(const struct timespec tv[2]) override;

    file_t* create(const string& name);
    dir_t*  mkdir(const string& name);
    int unlink(const string& name);
    int rmdir(const string& name);
    int moveto(dir_t* newparent, const string& oldname, const string& newname);

    virtual void dump_to_disk_cache() override;
    virtual int drop_mem_cache() override;
};


int cache_prepare();
dir_t* cache_root();
void cache_destroy(dir_t* root);

#endif
