#ifndef STUB_API_H__
#define STUB_API_H__
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <vector>
#include <string>
#include <unistd.h>

#include "common.h"
#include "utils.h"


int fm_prepare();

int fm_statfs(struct statvfs* sf);

int fm_download(const filekey& file, size_t startp, size_t len, buffstruct& bs);

//upload `file` in `fileat`, return key in `file`
int fm_upload(const filekey& fileat, filekey& file, const char* data, size_t len, bool overwrite);

//list files in `file`, if only key got, set flags of METE_KEY_ONLY in `flist`
int fm_list(const filekey& file, std::vector<struct filemeta>& flist);

// get meta info of `file`
int fm_getattr(const filekey& file, struct filemeta& meta);

// Get key of `file` in dir `fileat`
int fm_getattrat(const filekey& fileat, struct filekey& file);

//mkdir in `fileat`, return key in `file`
int fm_mkdir(const filekey& fileat, struct filekey& file);

int fm_delete(const filekey& file);

//delete file in `flist`
int fm_batchdelete(std::vector<struct filekey> flist);

//move `file` at `oldat` to `newat` with new name of `newfile`, new key return in `newfile`
int fm_rename(const filekey& oldat, const filekey& file, const filekey& newat, filekey& newfile);

std::string fm_private_key_tostring(std::shared_ptr<void> private_key);
std::shared_ptr<void> fm_get_private_key(const char* private_key_str);

const char* fm_getsecret();

const char* fm_getcachepath();

#define HANDLE_EAGAIN(x) ({      \
  __typeof__(x) _result;          \
  auto _retry = 0;                \
  while(true && _retry<20){       \
    _result = (x);                \
    _retry++;                     \
    if(_result &&                 \
      (errno == EAGAIN ||         \
       errno == ETIMEDOUT));      \
    else if(_result &&            \
        errno == EBUSY)           \
        sleep(1<<(_retry-1));     \
    else break;                   \
  }                               \
  _result;                        \
})

#endif
