#include "common.h"
#include "fuse.h"
#include "log.h"
#include <stdio.h>

struct fuse_operations fm_oper = {
    .init       = fm_fuse_init,
    .destroy    = fm_fuse_destroy,
    .access     = fm_fuse_access,
    .getattr    = fm_fuse_getattr,
    .opendir    = fm_fuse_opendir,
    .readdir    = fm_fuse_readdir,
    .releasedir = fm_fuse_releasedir,
    .fsyncdir   = fm_fuse_fsyncdir,
    .mkdir      = fm_fuse_mkdir,
    .unlink     = fm_fuse_unlink,
    .rmdir      = fm_fuse_rmdir,
    .rename     = fm_fuse_rename,
    .statfs     = fm_fuse_statfs,
    .open       = fm_fuse_open,
    .truncate   = fm_fuse_truncate,
    .read       = fm_fuse_read,
    .create     = fm_fuse_create,
    .write      = fm_fuse_write,
    .flush      = fm_fuse_flush,
    .release    = fm_fuse_release,
    .fsync      = fm_fuse_fsync,
    .utimens    = fm_fuse_utimens,
    .getxattr   = fm_fuse_getxattr,
    .setxattr   = fm_fuse_setxattr,
    .chmod      = fm_fuse_chmod,
    .chown      = fm_fuse_chown,
    .symlink    = fm_fuse_symlink,
    .readlink   = fm_fuse_readlink,
};

int fm_prepare();

int fm_main(int argc, char *argv[]) {
    int ret = fm_prepare();
    if(ret){
        return ret;
    }
    log_init(opt.log_path);
    return fuse_main(argc, argv, &fm_oper, NULL);
}
