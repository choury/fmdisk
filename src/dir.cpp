#include "fmdisk.h"
#include "dir.h"
#include "cache.h"

#include <assert.h>


dir_t::dir_t(entry_t* entry, entry_t* parent, time_t mtime): mtime(mtime){
    entrys.emplace(".", entry);
    entrys.emplace("..", parent ? parent: entry);
}

dir_t::~dir_t(){
    for(auto i: entrys){
        if(i.first != "." && i.first != ".."){
            delete i.second;
        }
    }
}

// Must wlock before call this function
void dir_t::pull() {
    entry_t* entry = entrys["."];
    std::vector<filemeta> flist;
    if(HANDLE_EAGAIN(fm_list(entry->getkey(), flist))){
        throw "fm_list IO Error";
    }
    for(auto i: flist){
        string bname = basename(i.key.path);
        if(endwith(bname, ".def") && S_ISDIR(i.mode)){
            bname = decodepath(bname);
        }
        entrys.emplace(bname, new entry_t(entry, i));
    }
    flags |= DIR_PULLED_F;
}

entry_t* dir_t::find(std::string path) {
    auto_wlock(this);
    if((flags & DIR_PULLED_F) == 0){
        pull();
    }
    assert(flags & DIR_PULLED_F);
    if(entrys.count(path)){
        return entrys[path];
    }else{
        return nullptr;
    }
}

const std::map<string, entry_t*>& dir_t::get_entrys(){
    auto_wlock(this);
    if((flags & DIR_PULLED_F) == 0){
        pull();
    }
    assert(flags & DIR_PULLED_F);
#if  0
    for(auto i: entrys){
        if(i.second == nullptr){
            entrys.emplace(i.first, new entry_t(entrys["."], i.first));
        }
    }
#endif
    //at least '.' and '..'
    assert(entrys.size() >= 2);
    return entrys;
}

entry_t* dir_t::insert(string name, entry_t* entry){
    auto_wlock(this);
    assert(entrys.count(name) == 0);
    assert(entrys.size() < MAXFILE);
    mtime = time(0);
    return entrys[name] = entry;
}

void dir_t::erase(std::string name) {
    auto_wlock(this);
    assert(entrys.count(name));
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

