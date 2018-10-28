#include "fmdisk.h"

int fm_prepare(){
    return 0;
}

int fm_download(const filekey& file, size_t startp, size_t len, buffstruct& bs){
    return 0;
}

int fm_upload(const filekey& fileat, filekey& file, size_t len, bool overwrite){
    return 0;
}

int fm_list(const filekey& file, std::vector<struct filemeta>& flist){
    return 0;
}

int fm_getattr(const filekey& file, struct filemeta& meta){
    return 0;
}

int fm_getattrat(const filekey& fileat, struct filekey& file) {
    return 0;
}


int fm_delete(const filekey& file){
    return 0;
}

int fm_batchdelete(std::vector<struct filekey> flist){
    for(auto i: flist){
        int ret = fm_delete(i);
        if(ret){
            return ret;
        }
    }
    return 0;
}

int fm_rename(const filekey& oldfile, filekey& newfile){
    return 0;
}

std::shared_ptr<void> fm_get_private_key(const char* private_key_str){
    return std::shared_ptr<void>(nullptr, [](void*){});
}

