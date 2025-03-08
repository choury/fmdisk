#ifndef DIR_H__
#define DIR_H__
#include "locker.h"

#include <map>

using std::string;

class entry_t;

class dir_t: locker {
    uint32_t flags = 0;
    size_t length;
    std::map<string, entry_t*> entrys;
    void pull_wlocked();
public:
    dir_t(entry_t* entry, entry_t* parent, const filemeta& meta);
    virtual ~dir_t();
    entry_t* find(std::string path);
    const std::map<string, entry_t*>& get_entrys();
    entry_t* insert(std::string name, entry_t* entry);
    void erase(std::string name);
    size_t children();
    size_t size() {
        return length;
    }
    void dump_to_disk_cache();
    int drop_mem_cache();
};

#endif
