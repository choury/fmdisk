#include "fuse.h"
#include <stdio.h>

struct fuse_operations fm_oper = {
    .init       = fm_fuse_init,
    .destroy    = fm_fuse_destroy,
    .access     = fm_fuse_access,
    .getattr    = fm_fuse_getattr,
    .fgetattr   = fm_fuse_fgetattr,
    .opendir    = fm_fuse_opendir,
    .readdir    = fm_fuse_readdir,
    .releasedir = fm_fuse_releasedir,
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
    .ftruncate  = fm_fuse_ftruncate,
    .fsync      = fm_fuse_fsync,
    .utimens    = fm_fuse_utimens,
    .getxattr   = fm_fuse_getxattr,
    .setxattr   = fm_fuse_setxattr,
#ifndef __APPLE__
    .flag_nullpath_ok = 1,
    .flag_nopath = 1,
#endif
};

int fm_main(int argc, char *argv[]) {
    if(fm_fuse_prepare()){
        fprintf(stderr, "fm_fuse_prepare failed!");
        return -1;
    }
    return fuse_main(argc, argv, &fm_oper, NULL);
}
