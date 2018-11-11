#include "fmdisk.h"

int fm_prepare(){
    return 0;
}

int fm_download(const filekey&, size_t, size_t, buffstruct&){
    return 0;
}

int fm_upload(const filekey&, filekey&, const char*, size_t, bool){
    return 0;
}

int fm_list(const filekey&, std::vector<struct filemeta>&){
    return 0;
}

int fm_getattr(const filekey&, struct filemeta&){
    return 0;
}

int fm_getattrat(const filekey&, struct filekey&) {
    return 0;
}


int fm_delete(const filekey&){
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

std::shared_ptr<void> fm_get_private_key(const char*){
    return std::shared_ptr<void>(nullptr, [](void*){});
}

std::string fm_private_key_tostring(std::shared_ptr<void>) {
    return "";
}

