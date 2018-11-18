#include "fuse.h"
#include "cache.h"
#include "utils.h"
#include "fmdisk.h"

#include <errno.h>
#include <string.h>


int fm_fuse_prepare(){
    return cache_prepare();
}

void *fm_fuse_init(struct fuse_conn_info *conn){
    conn->capable = conn->want & FUSE_CAP_BIG_WRITES & FUSE_CAP_EXPORT_SUPPORT;
    conn->max_background = 20;
    conn->max_write = 1024*1024;
    conn->max_readahead = 10*1024*1024;
    return cache_root();
}

void fm_fuse_destroy(void* root){
    cache_deinit();
    delete (entry_t*)root;
}

int fm_fuse_statfs(const char *path, struct statvfs *sf){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    return root->statfs(path, sf);
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
    return 0;
}

int fm_fuse_unlink(const char *path){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t *entry = root->find(path);
    if(entry == nullptr){
        return -ENOENT;
    }
    return entry->unlink();
}

int fm_fuse_rmdir(const char *path){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t *entry = root->find(path);
    if(entry == nullptr){
        return -ENOENT;
    }
    return entry->rmdir();
}

int fm_fuse_rename(const char *oldname, const char *newname){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t* entry =  root->find(oldname);
    if(entry == nullptr){
        return -ENOENT;
    }
    entry_t* nentry = root->find(newname);
    if(nentry){
        nentry->unlink();
    }
    entry_t* newparent = root->find(dirname(newname));
    if(newparent == nullptr){
        return -ENOENT;
    }
    return entry->move(newparent, basename(newname));
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
    return entry->truncate(offset);
}

int fm_fuse_fgetattr(const char*, struct stat* st, struct fuse_file_info* fi){
    entry_t* entry = (entry_t*)fi->fh;
    filemeta meta = entry->getmeta();
    memset(st, 0, sizeof(struct stat));
    assert((meta.flags & METE_KEY_ONLY_F) == 0);
    st->st_mode = meta.mode;
    st->st_size = meta.size;
    st->st_blksize = meta.blksize;
    st->st_blocks = (meta.blocks * meta.blksize)/512;
    st->st_ctime = meta.ctime;
    st->st_mtime = meta.mtime;
    return 0;
}

int fm_fuse_read(const char *, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    entry_t* entry = (entry_t*)fi->fh;
    return entry->read(buf, offset, size);
}

int fm_fuse_ftruncate(const char*, off_t offset, struct fuse_file_info *fi){
    entry_t* entry = (entry_t*)fi->fh;
    return entry->truncate(offset);
}

int fm_fuse_write(const char *, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    entry_t* entry = (entry_t*)fi->fh;
    return entry->write(buf, offset, size);
}

int fm_fuse_fsync(const char *, int datasync, struct fuse_file_info *fi){
    entry_t* entry = (entry_t*)fi->fh;
    return entry->sync(datasync);
}

int fm_fuse_flush(const char*, struct fuse_file_info *fi){
    entry_t* entry = (entry_t*)fi->fh;
    return entry->flush();
}

int fm_fuse_release(const char *, struct fuse_file_info *fi){
    entry_t* entry = (entry_t*)fi->fh;
    return entry->release();
}

int fm_fuse_utimens(const char *path, const struct timespec tv[2]){
    entry_t* root = (entry_t*)fuse_get_context()->private_data;
    entry_t* entry = (entry_t*)root->find(path);
    if(entry == nullptr){
        return -ENOENT;
    }
    return entry->utime(tv);
}

int fm_fuse_setxattr(const char *path, const char *name, const char *value, size_t size, int flags){
    return -EOPNOTSUPP;
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
