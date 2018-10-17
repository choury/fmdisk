#ifndef STUB_API_H__
#define STUB_API_H__
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <map>
#include <set>
#include <string>
#include <unistd.h>

#include "common.h"
#include "utils.h"


int fm_prepare();
int fm_statfs(const char* path, struct statvfs* sf);
int fm_download(const char* path, size_t startp, size_t len, buffstruct& bs);
int fm_upload(const char* path, const char* input, size_t len, bool overwrite, char outpath[PATHLEN]);
int fm_list(const char* path, std::map<std::string, struct stat>& stmap);
int fm_getattr(const char* path, struct stat* st);
int fm_mkdir(const char* path, struct stat* st);
int fm_delete(const char* path);
int fm_batchdelete(std::set<std::string> flist);
int fm_rename(const char* oldname, const char* newname);
const char* fm_getsecret();
const char* fm_getcachepath();

#define HANDLE_EAGAIN(x) ({      \
  __typeof__(x) _result;          \
  while(true){                    \
    _result = (x);                \
    if(_result != 0               \
  && (errno == EAGAIN ||          \
      errno == ETIMEDOUT ||       \
      errno == EBUSY))            \
      sleep(1);                   \
    else                          \
      break;                      \
  }                               \
  _result;                        \
})

#endif
