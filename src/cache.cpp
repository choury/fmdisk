#include "cache.h"
#include "dir.h"
#include "file.h"
#include "threadpool.h"

#include <string.h>
#include <assert.h>
#include <json-c/json.h>


static string childname(const string& path) {
    size_t pos = path.find_first_of("/");
    if(pos == string::npos) {
        return path;
    }
    if(pos == 0 ) {
        string path_truncate = path.substr(1, path.length());
        return childname(path_truncate);
    }
    return path.substr(0, pos);
}

static string subname(const string& path) {
    size_t pos = path.find_first_of("/");
    if(pos == string::npos || pos == path.length()-1) {
        return ".";
    }
    if(pos == 0 ) {
        string path_truncate = path.substr(1, path.length());
        return subname(path_truncate);
    }
    return path.substr(pos+1, path.length());
}

filekey basename(const filekey& file) {
    return filekey{basename(file.path), file.private_key};
}

filekey decodepath(const filekey& file) {
    return filekey{decodepath(file.path), file.private_key};
}



int cache_prepare() {
    return fm_prepare();
}

entry_t* cache_root() {
    creatpool(THREADS);
    start_prefetch();
    start_writeback();
    struct filemeta meta = initfilemeta(filekey{"/", 0});
    if(HANDLE_EAGAIN(fm_getattr(filekey{"/", 0}, meta))){
        throw "getattr of root failed";
    }
    return new entry_t(nullptr, meta);
}

int entry_t::statfs(const char*, struct statvfs* sf) {
    return HANDLE_EAGAIN(fm_statfs(sf));
}

#if 0
entry_t::entry_t(entry_t* parent, string name, struct stat* st):
    parent(parent),
    name(name),
    mode(st->st_mode),
    ctime(st->st_ctime),
    flags(ENTRY_INITED)
{
    if(S_ISDIR(mode)){
        dir = new dir_t(this, parent, st->st_mtime);
    }else{
        file = new file_t(this, st);
    }
}

entry_t::entry_t(entry_t* parent, string name):
    parent(parent),
    name(name),
    mode(S_IFREG | 0666),
    flags(ENTRY_CHUNCED)
{
    addtask((taskfunc)pull, this, 0, 0);
}
#endif


entry_t::entry_t(entry_t* parent, filemeta meta):
    parent(parent),
    fk(basename(meta.key)),
    mode(meta.mode),
    ctime(meta.ctime),
    flags(meta.flags)
{
    assert((flags & ENTRY_INITED_F) == 0);
    if(endwith(fk.path, ".def") && S_ISDIR(meta.mode) && (flags & ENTRY_CREATE_F) == 0){
        fk = decodepath(fk);
        mode = S_IFREG | 0666;
        flags |= ENTRY_CHUNCED_F;
        addtask((taskfunc)pull, this, 0, 0);
        return;
    }
    if(meta.flags & METE_KEY_ONLY){
        addtask((taskfunc)pull, this, 0, 0);
        return;
    }
    if(flags & ENTRY_CHUNCED_F){
        fk = decodepath(fk);
    }
    if(S_ISDIR(mode)){
        dir = new dir_t(this, parent, meta.mtime);
    }else{
        file = new file_t(this, meta);
    }
    flags |= ENTRY_INITED_F;
}

entry_t::~entry_t() {
    assert(opened == 0);
    pthread_mutex_destroy(&init_lock);
    pthread_cond_destroy(&init_cond);
    if(S_ISDIR(mode)){
        delete dir;
    }else{
        delete file;
    }
}

void entry_t::init_wait() {
    pthread_mutex_lock(&init_lock);
    while((flags & ENTRY_INITED_F) == 0){
        pthread_cond_wait(&init_cond, &init_lock);
    }
    pthread_mutex_unlock(&init_lock);
}

void entry_t::pull(entry_t* entry) {
    if(entry->flags & ENTRY_CHUNCED_F){
        filekey metakey{METANAME, 0};
        if(HANDLE_EAGAIN(fm_getattrat(entry->getkey(), metakey))){
            throw "fm_getattr IO Error";
        }
        buffstruct bs;
        if(HANDLE_EAGAIN(fm_download(metakey, 0, 0, bs))){
            throw "fm_download IO Error";
        }
        json_object *json_get = json_tokener_parse(bs.buf);
        if(json_get ==  nullptr){
            throw "Json parse error";
        }
        struct filemeta meta = initfilemeta(metakey);
        json_object* jctime;
        int ret = json_object_object_get_ex(json_get, "ctime", &jctime);
        assert(ret);
        entry->ctime = json_object_get_int64(jctime);

        json_object* jmtime;
        ret = json_object_object_get_ex(json_get, "mtime", &jmtime);
        assert(ret);
        meta.mtime = json_object_get_int64(jmtime);

        json_object* jsize;
        ret = json_object_object_get_ex(json_get, "size", &jsize);
        assert(ret);
        meta.size = json_object_get_int64(jsize);

        json_object *jencoding;
        ret = json_object_object_get_ex(json_get, "encoding", &jencoding);
        assert(ret);
        const char* encoding = json_object_get_string(jencoding);
        if(strcasecmp(encoding, "xor") == 0){
            meta.flags = FILE_ENCODE_F;
        }else{
            assert(strcasecmp(encoding, "none") == 0);
        }

        json_object *jblksize;
        ret = json_object_object_get_ex(json_get, "blksize", &jblksize);
        assert(ret);
        meta.blksize = json_object_get_int64(jblksize);

        std::vector<filekey> fblocks;
        json_object* jblocks;
        if(json_object_object_get_ex(json_get, "blocks", &jblocks)){
            fblocks.reserve(json_object_array_length(jblocks));
            for(int i=0; i < json_object_array_length(jblocks); i++){
                json_object *jblock = json_object_array_get_idx(jblocks, i);
                json_object *jname;
                ret = json_object_object_get_ex(jblock, "name", &jname);
                assert(ret);
                json_object *jkey;
                ret = json_object_object_get_ex(jblock, "key", &jkey);
                assert(ret);
                const char* name = json_object_get_string(jname);
                const char* private_key = json_object_get_string(jkey);
                fblocks.push_back(filekey{name, fm_get_private_key(private_key)});
            }
        }

        json_object *jblock_list;
        if(json_object_object_get_ex(json_get, "block_list", &jblock_list)){
            fblocks.reserve(json_object_array_length(jblock_list));
            for(int i=0; i < json_object_array_length(jblock_list); i++){
                json_object *block = json_object_array_get_idx(jblock_list, i);
                const char* name = json_object_get_string(block);
                fblocks.push_back(filekey{name, 0});
            }
        }

        json_object *jinline_data;
        ret = json_object_object_get_ex(json_get, "inline_data", &jinline_data);
        if(ret){
            char* inline_data = new char[INLINE_DLEN];
            Base64Decode(json_object_get_string(jinline_data), json_object_get_string_len(jinline_data), inline_data);
            meta.inline_data = (unsigned char*)inline_data;
        }
        json_object_put(json_get);
        entry->file = new file_t(entry, meta, fblocks);
    }else{
        assert(entry->flags & METE_KEY_ONLY);
        struct filemeta meta = initfilemeta(entry->getkey());
        if(HANDLE_EAGAIN(fm_getattr(meta.key, meta))){
            throw "fm_getattr IO Error";
        }
        entry->ctime = meta.ctime;
        if(S_ISDIR(meta.mode)){
            entry->dir = new dir_t(entry, entry->parent, meta.mtime);
        }else{
            entry->file = new file_t(entry, meta);
        }
    }

    entry->flags |= ENTRY_INITED_F;
    pthread_cond_broadcast(&entry->init_cond);
}


void entry_t::clean(entry_t* entry) {
    auto_wlock(entry);
    entry->flags &= ~ENTRY_REASEWAIT_F;
    if(entry->opened > 0){
        return;
    }
    assert((entry->file->getmeta().flags & FILE_DIRTY_F) == 0);
    assert(S_ISREG(entry->mode));
    if(entry->flags & ENTRY_DELETED_F){
        __w.unlock();
        delete entry;
    }else{
        entry->file->release();
    }
}


filekey entry_t::getkey() {
    auto_rlock(this);
    string path;
    if(flags & ENTRY_CHUNCED_F){
        path = encodepath(getcwd());
    }else{
        path = getcwd();
    }
    return filekey{path, fk.private_key};
}

string entry_t::getcwd() {
    auto_rlock(this);
    if(parent == nullptr){
        return "/";
    }
    if(S_ISDIR(mode)){
        return parent->getcwd() + fk.path + "/";
    }else{
        return parent->getcwd() + fk.path;
    }
}

filemeta entry_t::getmeta() {
    init_wait();
    filemeta meta{getkey()};
    auto_rlock(this);
    meta.mode = mode;
    meta.ctime = ctime;
    if(!S_ISDIR(mode)){
        filemeta fmeta = file->getmeta();
        meta.size = fmeta.size;
        meta.inline_data = fmeta.inline_data;
        meta.blksize = fmeta.blksize;
        meta.mtime = fmeta.mtime;
    }else{
        meta.size = 0;
        meta.inline_data = 0;
        meta.blksize =  0;
        meta.mtime = dir->getmtime();
    }
    return meta;
}

entry_t* entry_t::find(string path){
    auto_rlock(this);
    if(path == "." || path == "/"){
        return this;
    }
    string cname = childname(path);
    entry_t* entry = dir->find(cname);
    if(entry){
        return entry->find(subname(path));
    }
    return nullptr;
}

entry_t* entry_t::create(string name){
    auto_rlock(this);
    if(dir->size() >= MAXFILE){
        errno = ENOSPC;
        return nullptr;
    }
    assert(S_ISDIR(mode));
    struct filemeta meta = initfilemeta(filekey{encodepath(name), 0});
    if(HANDLE_EAGAIN(fm_mkdir(getkey(), meta.key))){
        return nullptr;
    }

    meta.ctime = time(NULL);
    meta.mtime = time(NULL);
    meta.flags =  ENTRY_CHUNCED_F | ENTRY_CREATE_F | FILE_ENCODE_F | FILE_DIRTY_F ;
    meta.blksize = BLOCKLEN;
    meta.mode = S_IFREG | 0644;
    entry_t* entry = new entry_t(this, meta);
    return dir->insert(name, entry);
}

entry_t* entry_t::mkdir(string name) {
    if(endwith(name, ".def")){
        errno = EINVAL;
        return nullptr;
    }
    if(dir->size() >= MAXFILE){
        errno = ENOSPC;
        return nullptr;
    }
    auto_rlock(this);
    assert(S_ISDIR(mode));
    struct filemeta meta = initfilemeta(filekey{name, 0});
    if(HANDLE_EAGAIN(fm_mkdir(getkey(), meta.key))){
        return nullptr;
    }

    meta.ctime = time(NULL);
    meta.mtime = time(NULL);
    meta.flags = ENTRY_CREATE_F;
    meta.mode = S_IFDIR | 0755;
    entry_t* entry = new entry_t(this, meta);
    return dir->insert(name, entry);
}

int entry_t::open() {
    auto_wlock(this);
    opened++;
    if(S_ISREG(mode) && file->open() < 0){
        return -errno;
    }
    return 0;
}


const std::map<string, entry_t*>& entry_t::entrys(){
    auto_rlock(this);
    return dir->get_entrys();
}


int entry_t::read(void* buff, off_t offset, size_t size) {
    auto_rlock(this);
    return file->read(buff, offset, size);
}

int entry_t::truncate(off_t offset){
    auto_rlock(this);
    if((flags & ENTRY_CHUNCED_F) == 0){
        return -EPERM;
    }
    int ret = file->truncate(offset);
    if(ret < 0){
        return -errno;
    }
    return ret;
}


int entry_t::write(const void* buff, off_t offset, size_t size) {
    auto_rlock(this);
    if((flags & ENTRY_CHUNCED_F) == 0){
        return -EPERM;
    }
    int ret = file->write(buff, offset, size);
    if(ret < 0){
        return -errno;
    }
    return ret;
}


int entry_t::sync(int datasync){
    auto_rlock(this);
    if((flags & ENTRY_CHUNCED_F) == 0){
        return 0;
    }
    assert(S_ISREG(mode));
    file->sync();
    filemeta meta = file->getmeta();
    if((!datasync && (meta.flags & FILE_DIRTY_F))){
        json_object *jobj = json_object_new_object();
        json_object_object_add(jobj, "size", json_object_new_int64(meta.size));
        json_object_object_add(jobj, "ctime", json_object_new_int64(ctime));
        json_object_object_add(jobj, "mtime", json_object_new_int64(meta.mtime));
        json_object_object_add(jobj, "blksize", json_object_new_int64(meta.blksize));
        if(meta.flags & FILE_ENCODE_F){
            json_object_object_add(jobj, "encoding", json_object_new_string("xor"));
        }else{
            json_object_object_add(jobj, "encoding", json_object_new_string("none"));
        }
        if(meta.inline_data){
            char* inline_data = new char[INLINE_DLEN * 2];
            Base64Encode((const char*)meta.inline_data, meta.size, inline_data);
            json_object_object_add(jobj, "inline_data", json_object_new_string(inline_data));
            delete[] inline_data;
        }

        json_object *jblocks = json_object_new_array();
        auto fblocks = file->getfblocks();
        for(auto block: fblocks){
            json_object *jblock = json_object_new_object();
            json_object_object_add(jblock, "name", json_object_new_string(block.path.c_str()));
            json_object_object_add(jblock, "key", json_object_new_string(fm_private_key_tostring(block.private_key).c_str()));
            json_object_array_add(jblocks, jblock);
        }

        json_object_object_add(jobj, "blocks", jblocks);
        const char *jstring = json_object_to_json_string(jobj);

retry:
        int ret = HANDLE_EAGAIN(fm_upload(getkey(), meta.key, jstring, strlen(jstring), true));
        if(ret != 0 && errno == EEXIST){
            goto retry;
        }
        assert(ret == 0);
        json_object_put(jobj);
        file->post_sync(meta.key);
    }
    return 0;
}

int entry_t::flush(){
    sync(0);
    return 0;
}

int entry_t::release() {
    {
        auto_wlock(this);
        opened--;
        if(opened || !S_ISREG(mode)){
            return 0;
        }
        flags |= ENTRY_REASEWAIT_F;
    }
    addtask(taskfunc(clean), this, 0, 60);
    return 0;
}

int entry_t::utime(const struct timespec  tv[2]) {
    auto_rlock(this);
    if((flags & ENTRY_CHUNCED_F) == 0){
        return -EACCES;
    }
    file->setmtime(tv[1].tv_sec);
    sync(0);
    return 0;
}


void entry_t::insert(string name, entry_t* entry) {
    auto_rlock(this);
    assert(S_ISDIR(mode));
    dir->insert(name, entry);
}


int entry_t::move(entry_t* newparent, string name) {
    auto_wlock(this);
    if(newparent->dir->size() >= MAXFILE){
        return -ENOSPC;
    }
    filekey newfile{(flags & ENTRY_CHUNCED_F)?encodepath(name):name, 0};
    int ret = HANDLE_EAGAIN(fm_rename(parent->getkey(), getkey(), newparent->getkey(), newfile));
    if(ret){
        return ret;
    }
    parent->erase(fk.path);
    parent = newparent;
    fk = filekey{name, newfile.private_key};
    parent->insert(name, this);
    return 0;
}

void entry_t::erase(string name) {
    auto_rlock(this);
    assert(S_ISDIR(mode));
    return dir->erase(name);
}

int entry_t::unlink() {
    auto_wlock(this);
    assert(opened == 0);
    if(!S_ISREG(mode)){
        return -EISDIR;
    }
    int ret = HANDLE_EAGAIN(fm_delete(getkey()));
    if(ret){
        return ret;
    }
    parent->erase(fk.path);
    flags |= ENTRY_DELETED_F;
    if(flags & ENTRY_REASEWAIT_F){
        //delete this in clean
        return 0;
    }
    __w.unlock();
    delete this;
    return 0;
}

int entry_t::rmdir() {
    auto_wlock(this);
    if(!S_ISDIR(mode)){
        return -ENOTDIR;
    }
    if(opened){
        return -EBUSY;
    }
    if(dir->get_entrys().size() != 2){
        return -ENOTEMPTY;
    }
    int ret = HANDLE_EAGAIN(fm_delete(getkey()));
    if(ret){
        return ret;
    }
    parent->erase(fk.path);
    flags |= ENTRY_DELETED_F;
    if(flags & ENTRY_REASEWAIT_F){
        return 0;
    }
    __w.unlock();
    delete this;
    return 0;
}
