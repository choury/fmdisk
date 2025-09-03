#ifndef STUB_API_H__
#define STUB_API_H__
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <vector>
#include <unistd.h>

#include "utils.h"

#ifdef __cplusplus
extern "C" {
#endif

int fm_prepare();

#ifdef __cplusplus
}
#endif

int fm_statfs(struct statvfs* sf);

int fm_download(const filekey& file, off_t startp, size_t len, buffstruct& bs);

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
int fm_batchdelete(std::vector<struct filekey>&& flist);

//move `file` at `oldat` to `newat` with new name of `newfile`, new key return in `newfile`
int fm_rename(const filekey& oldat, const filekey& file, const filekey& newat, filekey& newfile);

//optional, return -EACCES if not supported. tv[0]: ctime, tv[1]: mtime
int fm_utime(const filekey& file, const time_t tv[2]);

const char* fm_private_key_tostring(std::shared_ptr<void> private_key);
std::shared_ptr<void> fm_get_private_key(const char* private_key_str);
int fm_copy(const filekey& file, const filekey& newat, filekey& newfile);
int fm_change_storage_class(const filekey& file, enum storage_class storage);
int fm_restore_archive(const filekey& file, int days, unsigned int mode);

#define HANDLE_EAGAIN(x) ({      \
  __typeof__(x) _result;          \
  while(true){                    \
    _result = (x);                \
    if(_result &&                 \
      (errno == EAGAIN ||         \
       errno == ETIMEDOUT));      \
    else if(_result &&            \
        errno == EBUSY)           \
        sleep(1);                 \
    else break;                   \
  }                               \
  _result;                        \
})

#ifndef TEMP_FAILURE_RETRY
#define TEMP_FAILURE_RETRY(exp)            \
  ({                                       \
    decltype(exp) _rc;                     \
    do {                                   \
      _rc = (exp);                         \
    } while (_rc == -1 && errno == EINTR); \
    _rc;                                   \
  })
#endif

#endif
