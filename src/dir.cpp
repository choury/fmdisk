#include "fmdisk.h"
#include "dir.h"
#include "cache.h"
#include "sqlite.h"

#include <string.h>
#include <assert.h>

dir_t::dir_t(entry_t* entry, entry_t* parent, time_t mtime): mtime(mtime){
    entrys.emplace(".", entry);
    entrys.emplace("..", parent ? parent: entry);
}

dir_t::~dir_t(){
    this->wlock();
    for(auto i: entrys){
        if(i.first != "." && i.first != ".."){
            delete i.second;
        }
    }
}

// Must wlock before call this function
void dir_t::pull_wlocked() {
    entry_t* current = entrys["."];
    bool cached = false;
    std::vector<filemeta> flist;
    if(load_entry_from_db(current->getkey().path, flist) == 0){
        printf("Miss from localcache\n");
        if(HANDLE_EAGAIN(fm_list(current->getkey(), flist))){
            throw "fm_list IO Error";
        }
    }else{
        cached = true;
    }
    for(auto i: flist){
        string bname = basename(i.key.path);
        if(endwith(bname, ".def") && S_ISDIR(i.mode)){
            bname = decodepath(bname);
            i.flags |= ENTRY_CHUNCED_F | META_KEY_ONLY_F;
        }
        if(entrys.count(bname) == 0){
            entrys.emplace(bname, new entry_t(current, i));
        }
        if(!cached){
            save_entry_to_db(current->getkey(), i);
            if((i.flags & META_KEY_ONLY_F) == 0){
                save_file_to_db(entrys[bname]->getkey().path, i, std::vector<filekey>{});
            }
        }
    }
    flags |= DIR_PULLED_F;
}

entry_t* dir_t::find(std::string path) {
    auto_rlock(this);
    if((flags & DIR_PULLED_F) == 0){
        __r.upgrade();
        pull_wlocked();
    }
    if(entrys.count(path)){
        return entrys[path];
    }else{
        return nullptr;
    }
}

const std::map<string, entry_t*>& dir_t::get_entrys(){
    auto_rlock(this);
    if((flags & DIR_PULLED_F) == 0){
        __r.upgrade();
        pull_wlocked();
    }
    //at least '.' and '..'
    assert(entrys.size() >= 2);
    return entrys;
}

entry_t* dir_t::insert(string name, entry_t* entry){
    auto_wlock(this);
    assert(entrys.count(name) == 0);
    assert(entrys.size() < MAXFILE);
    mtime = time(0);
    entry->dump_to_disk_cache();
    return entrys[name] = entry;
}

void dir_t::erase(std::string name) {
    auto_wlock(this);
    assert(entrys.count(name));
    auto path = entrys[name]->getkey().path;
    delete_entry_from_db(path);
    delete_entry_prefix_from_db(path);
    entrys.erase(name);
    mtime = time(0);
}

void dir_t::setmtime(time_t mtime) {
    auto_wlock(this);
    this->mtime = mtime;
}


time_t dir_t::getmtime() {
    auto_rlock(this);
    return mtime;
}

size_t dir_t::size() {
    auto_rlock(this);
    return entrys.size();
}

void dir_t::dump_to_disk_cache(){
    auto_rlock(this);
    if((flags & DIR_PULLED_F) == 0){
        return;
    }
    for(auto i: entrys){
        if(i.first == "." || i.first == ".."){
            continue;
        }
        i.second->dump_to_disk_cache();
    }
}

int dir_t::drop_mem_cache(){
    int ret = 0;
    auto_rlock(this);
    for(auto i : entrys){
        if(i.first == "." || i.first == ".."){
            continue;
        }
        ret |= i.second->drop_mem_cache();
    }
    return ret?-EAGAIN:0;
}
