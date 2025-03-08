#include "fuse.h"
#include "cache.h"
#include "utils.h"
#include "fmdisk.h"

#include <errno.h>
#include <string.h>
#include <memory>

std::unique_ptr<struct statvfs> fs = nullptr;
struct fmoption opt;

int fm_fuse_prepare(){
    return cache_prepare();
}

void *fm_fuse_init(struct fuse_conn_info *conn){
#ifndef __APPLE__
    conn->capable = conn->want & FUSE_CAP_BIG_WRITES & FUSE_CAP_EXPORT_SUPPORT;
    conn->max_background = 20;
#endif
    conn->max_readahead = 10*1024*1024;
    return cache_root();
}

void fm_fuse_destroy(void* root){
    fs = nullptr;
    cache_destroy((entry_t*)root);
}

int fm_fuse_statfs(const char *path, struct statvfs *sf){
    if(fs){
        memcpy(sf, fs.get(), sizeof(struct statvfs));
        return 0;
    }
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    auto ret = root->statfs(path, sf);
    if(ret >= 0) {
        fs = std::unique_ptr<struct statvfs>(new struct statvfs);
        memcpy(fs.get(), sf, sizeof(struct statvfs));
    }
    return ret;
}

int fm_fuse_opendir(const char *path, struct fuse_file_info *fi){
    return fm_fuse_open(path, fi);
}

int fm_fuse_readdir(const char*, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi){
    entry_t* entry = (entry_t*)fi->fh;
    auto entrys = entry->entrys();
    for(auto i: entrys){
    /*
        struct stat st;
        i.second->getattr(&st);
        filler(buf, i.first.c_str(), &st, 0);
    */
        filler(buf, i.first.c_str(), nullptr, 0);
    }
    return 0;
}

int fm_fuse_releasedir(const char*, struct fuse_file_info *fi){
    return fm_fuse_release(nullptr, fi);
}

int fm_fuse_access(const char* path, int mask){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t* entry = root->find(path);
    if(entry == nullptr){
        return -ENOENT;
    }
    return 0;
}

int fm_fuse_getattr(const char *path, struct stat *st){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t* entry = root->find(path);
    if(entry == nullptr){
        return -ENOENT;
    }
    struct fuse_file_info fi;
    fi.fh = (unsigned long)entry;
    return fm_fuse_fgetattr(path, st, &fi);
}


int fm_fuse_mkdir(const char *path, mode_t mode){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t* entry =  root->find(dirname(path));
    if(entry == nullptr){
        return -ENOENT;
    }
    if(entry->mkdir(basename(path)) == nullptr){
        return -errno;
    }
    fs = nullptr;
    return 0;
}

int fm_fuse_unlink(const char *path){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t* parent = root->find(dirname(path));
    if(parent == nullptr){
        return -ENOENT;
    }
    fs = nullptr;
    return parent->unlink(basename(path));
}

int fm_fuse_rmdir(const char *path){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t* parent = root->find(dirname(path));
    if(parent == nullptr){
        return -ENOENT;
    }
    fs = nullptr;
    return parent->rmdir(basename(path));
}

int fm_fuse_rename(const char *oldname, const char *newname){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t* parent =  root->find(dirname(oldname));
    if(parent == nullptr){
        return -ENOENT;
    }
    entry_t* newparent = root->find(dirname(newname));
    if(newparent == nullptr){
        return -ENOENT;
    }
    fs = nullptr;
    return parent->moveto(newparent, basename(oldname), basename(newname));
}

int fm_fuse_create(const char *path, mode_t mode, struct fuse_file_info *fi){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t *entry = root->find(dirname(path));
    assert(entry);
    entry_t *nentry = entry->create(basename(path));
    if(nentry == nullptr){
        return -errno;
    }
    fi->fh = (uint64_t)nentry;
    fi->direct_io = 1;
    fs = nullptr;
    return nentry->open();
}

int fm_fuse_open(const char *path, struct fuse_file_info *fi){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t* entry = root->find(path);
    if(entry == nullptr){
        return -ENOENT;
    }
    fi->fh = (uint64_t)entry;
    fi->direct_io = 1;
    return entry->open();
}

int fm_fuse_truncate(const char* path, off_t offset){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t* entry = root->find(path);
    if(entry == nullptr){
        return -ENOENT;
    }
    fs = nullptr;
    return entry->truncate(offset);
}

int fm_fuse_fgetattr(const char*, struct stat* st, struct fuse_file_info* fi){
    entry_t* entry = (entry_t*)fi->fh;
    filemeta meta = entry->getmeta();
    memset(st, 0, sizeof(struct stat));
    assert((meta.flags & META_KEY_ONLY_F) == 0);
    st->st_mode = meta.mode;
    st->st_size = meta.size;
    st->st_blksize = 4096;
    st->st_nlink = 1;
    st->st_blocks = meta.size/4096 + 1;
    st->st_ctime = meta.ctime;
    st->st_mtime = meta.mtime;
    st->st_uid = getuid();
    st->st_gid = getgid();
    return 0;
}

int fm_fuse_read(const char *, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    entry_t* entry = (entry_t*)fi->fh;
    return entry->read(buf, offset, size);
}

int fm_fuse_ftruncate(const char*, off_t offset, struct fuse_file_info *fi){
    entry_t* entry = (entry_t*)fi->fh;
    fs = nullptr;
    return entry->truncate(offset);
}

int fm_fuse_write(const char *, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    entry_t* entry = (entry_t*)fi->fh;
    fs = nullptr;
    return entry->write(buf, offset, size);
}

int fm_fuse_fsync(const char *, int dataonly, struct fuse_file_info *fi){
    entry_t* entry = (entry_t*)fi->fh;
    return entry->sync(dataonly);
}

int fm_fuse_flush(const char*, struct fuse_file_info *fi){
    entry_t* entry = (entry_t*)fi->fh;
    return entry->flush();
}

int fm_fuse_release(const char *, struct fuse_file_info *fi){
    entry_t* entry = (entry_t*)fi->fh;
    return entry->release();
}


int fm_fuse_fsyncdir (const char *, int dataonly, struct fuse_file_info *fi) {
    entry_t* entry = (entry_t*)fi->fh;
    return entry->sync(dataonly);
}

int fm_fuse_utimens(const char *path, const struct timespec tv[2]){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t* entry = (entry_t*)root->find(path);
    if(entry == nullptr){
        return -ENOENT;
    }
    return entry->utime(tv);
}


int fm_fuse_chmod (const char *path, mode_t mode){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t* entry = (entry_t*)root->find(path);
    if(entry == nullptr){
        return -ENOENT;
    }
    return 0;
}

int fm_fuse_setxattr(const char *path, const char *name, const char *value, size_t size, int flags){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t* entry = (entry_t*)root->find(path);
    if(entry == nullptr){
        return -ENOENT;
    }
    if(strcmp(name, "user.drop_disk_cache") == 0){
        return entry->drop_disk_cache();
    }
    if(strcmp(name, "user.drop_mem_cache") == 0){
        return entry->drop_mem_cache();
    }
    return -ENODATA;
}

int fm_fuse_getxattr(const char *path, const char *name, char *value, size_t len){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t* entry = (entry_t*)root->find(path);
    if(entry == nullptr){
        return -ENOENT;
    }
    if(strcmp(name, "user.underlay_path")){
        return -ENODATA;
    }
    string underlay_path = entry->getkey().path;
    if(len == 0){
        return underlay_path.length();
    }
    if(underlay_path.length() >= len){
        return -ERANGE;
    }
    strcpy(value, underlay_path.c_str());
    return underlay_path.length();
}
