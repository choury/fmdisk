#include "common.h"
#include "fmdisk.h"

#define BLOCKLEN       (uint64_t)0x100000          //1M,缓存分块大小 必须为4K的倍数

int fm_prepare(){
    opt.cache_dir = "/tmp";
    opt.secret = "STUB_XXXXX";
    opt.block_len = BLOCKLEN;
    return 0;
}

int fm_download(const filekey&, off_t, size_t, buffstruct&){
    errno = EACCES;
    return -EACCES;
}

int fm_upload(const filekey&, filekey&, const char*, size_t, bool){
    errno = EACCES;
    return -EACCES;
}

int fm_list(const filekey&, std::vector<struct filemeta>&){
    return 0;
}

int fm_getattr(const filekey&, struct filemeta&){
    errno = EACCES;
    return -EACCES;
}

// 有些情况下，需要通过文件名获取 private key,就通过这个函数
int fm_getattrat(const filekey& fileat, struct filekey& file) {
    file.path = pathjoin(fileat.path, file.path);
    return 0;
}


int fm_delete(const filekey&){
    errno = EACCES;
    return -EACCES;
}

int fm_batchdelete(std::vector<struct filekey>&& flist){
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

const char* fm_private_key_tostring(std::shared_ptr<void>) {
    return "";
}
