#include "fmdisk.h"

int fm_prepare(){
    return 0;
}

int fm_statfs(const char *path, struct statvfs *sf){
    return 0;
}

int fm_download(const char* path, size_t startp, size_t len, buffstruct& bs){
    return 0;
}

int fm_upload(const char* path, const char* input, size_t len, bool overwrite, char outpath[PATHLEN]){
    return 0;
}

int fm_list(const char* path, std::map<std::string, struct stat>& stmap){
    return 0;
}

int fm_getattr(const char *path, struct stat *st){
    return 0;
}

int fm_mkdir(const char *path, struct stat* st){
    return 0;
}
int fm_delete(const char *path){
    return 0;
}

int fm_batchdelete(std::set<std::string> flist){
    for(auto i: flist){
        int ret = fm_delete(i.c_str());
        if(ret){
            return ret;
        }
    }
    return 0;
}
int fm_rename(const char *oldname, const char *newname){
    return 0;
}

const char* fm_getsecret(){
    return "FM_STUB";
}

const char* fm_getcachepath(){
    return "/tmp";
}
