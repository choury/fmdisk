#ifndef TRANSFER_HELPER_H__
#define TRANSFER_HELPER_H__

#include "common.h"
#include "utils.h"
#include "fmdisk.h"

#include <sys/stat.h>
#include <vector>

// Upload a single block; fills block_key (generates one if empty) and keeps name basename-only.
int upload_block_common(const filekey& block_parent,
                        filekey& block_key,
                        size_t block_no,
                        size_t blksize,
                        off_t offset,
                        const char* data,
                        size_t len,
                        bool encode);

// Upload whole file from an open fd into chunked layout; fills meta_out and fblocks_out.
int upload_file_from_fd(const filekey& file_dir,
                        const filekey& block_parent,
                        int fd,
                        const struct stat& st,
                        bool encode,
                        filemeta& meta_out,
                        std::vector<filekey>& fblocks_out);

// Download a block (chunked or whole-file) into caller buffer.
int download_block_common(filekey base,
                          const filekey& block_entry,
                          size_t block_no,
                          size_t blksize,
                          off_t offset,
                          size_t len,
                          bool encode,
                          char* out,
                          size_t& out_size);

#endif
