#ifndef FM_FUSE_H__
#define FM_FUSE_H__

#define FUSE_USE_VERSION 30

#include <sys/stat.h>
#include <sys/statvfs.h>
#include <fuse.h>

#ifdef  __cplusplus
extern "C" {
#endif

int fm_fuse_prepare();
void *fm_fuse_init(struct fuse_conn_info *conn, struct fuse_config *cfg);
void fm_fuse_destroy(void *);
int fm_fuse_statfs(const char *path, struct statvfs *sf);
int fm_fuse_opendir(const char *path, struct fuse_file_info *fi);
int fm_fuse_readdir(const char* path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi, enum fuse_readdir_flags flags);
int fm_fuse_releasedir(const char* path, struct fuse_file_info *fi);
int fm_fuse_access(const char* path, int mask);
int fm_fuse_getattr(const char *path, struct stat *st, struct fuse_file_info *fi);
int fm_fuse_mkdir(const char *path, mode_t mode);
int fm_fuse_unlink(const char *path);
int fm_fuse_rmdir(const char *path);
int fm_fuse_fsyncdir(const char *path, int dataonly, struct fuse_file_info *fi);
int fm_fuse_rename(const char *oldname, const char *newname, unsigned int flags);
int fm_fuse_create(const char *path, mode_t mode, struct fuse_file_info *fi);
int fm_fuse_open(const char *path, struct fuse_file_info *fi);
int fm_fuse_truncate(const char* path, off_t offset, struct fuse_file_info *fi);
int fm_fuse_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int fm_fuse_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int fm_fuse_fsync(const char *path, int dataonly, struct fuse_file_info *fi);
int fm_fuse_flush(const char *path, struct fuse_file_info *fi);
int fm_fuse_release(const char *path, struct fuse_file_info *fi);
int fm_fuse_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi);
int fm_fuse_chown(const char* path, uid_t uid, gid_t gid, struct fuse_file_info *fi);
#ifdef __APPLE__
int fm_fuse_setxattr(const char *path, const char *name, const char *value, size_t size, int flags, uint32_t);
int fm_fuse_getxattr(const char *path, const char *name, char *value, size_t len, uint32_t);
#else
int fm_fuse_setxattr(const char *path, const char *name, const char *value, size_t size, int flags);
int fm_fuse_getxattr(const char *path, const char *name, char *value, size_t len);
#endif
int fm_fuse_chmod (const char *path, mode_t mode, struct fuse_file_info *fi);

#ifdef  __cplusplus
}
#endif

#endif
