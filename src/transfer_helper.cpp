#include "transfer_helper.h"

#include "log.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <vector>

int upload_block_common(const filekey& block_parent,
                        filekey& block_key,
                        size_t block_no,
                        size_t blksize,
                        off_t offset,
                        const char* data,
                        size_t len,
                        bool encode) {
    if(isAllZero(data, len)) {
        block_key = filekey{"x", nullptr};
        return 0;
    }

    std::vector<char> payload(data, data + len);
    if(encode) {
        xorcode(payload.data(), offset, len, opt.secret);
    }

    if(block_key.path.empty()) {
        block_key = makeChunkBlockKey(block_no);
    }

    int ret = HANDLE_EAGAIN(fm_upload(block_parent, block_key, payload.data(), len, false));
    if(ret) {
        return ret;
    }
    block_key = basename(block_key);
    return 0;
}

int upload_file_from_fd(const filekey& file_dir,
                        const filekey& block_parent,
                        int fd,
                        const struct stat& st,
                        bool encode,
                        filemeta& meta_out,
                        std::vector<filekey>& fblocks_out) {
    meta_out = initfilemeta(filekey{METANAME, nullptr});
    meta_out.mode = S_IFREG | (st.st_mode & 0777);
    meta_out.ctime = st.st_ctime;
    meta_out.mtime = st.st_mtime;
    meta_out.size = st.st_size;
    meta_out.blksize = opt.block_len ? opt.block_len : 1024 * 1024;
    meta_out.flags = ENTRY_CHUNCED_F | (encode ? FILE_ENCODE_F : 0);

    fblocks_out.clear();

    if((size_t)meta_out.size <= INLINE_DLEN) {
        meta_out.inline_data.resize(meta_out.size);
        if(meta_out.size > 0) {
            ssize_t rlen = TEMP_FAILURE_RETRY(read(fd, meta_out.inline_data.data(), meta_out.size));
            if(rlen < 0) {
                return -errno;
            }
            if((size_t)rlen != meta_out.size) {
                return -EIO;
            }
        }
        return 0;
    }

    std::vector<char> buffer(meta_out.blksize);
    size_t idx = 0;
    while(true) {
        ssize_t rlen = TEMP_FAILURE_RETRY(read(fd, buffer.data(), buffer.size()));
        if(rlen < 0) {
            return -errno;
        }
        if(rlen == 0) {
            break;
        }
        filekey block_key;
        int ret = upload_block_common(block_parent, block_key, idx, meta_out.blksize, idx * meta_out.blksize, buffer.data(), static_cast<size_t>(rlen), encode);
        if(ret) {
            return ret;
        }
        fblocks_out.push_back(basename(block_key));
        idx++;
    }
    return 0;
}

int download_block_common(filekey base,
                          const filekey& block_entry,
                          size_t block_no,
                          size_t blksize,
                          off_t offset,
                          size_t len,
                          bool encode,
                          char* out,
                          size_t& out_size) {
    filekey target = block_entry;
    if(!block_entry.path.empty()) {
        target.path = pathjoin(base.path, block_entry.path);
        if(!target.private_key) {
            filekey lookup{block_entry.path, nullptr};
            int ret = HANDLE_EAGAIN(fm_getattrat(base, lookup));
            if(ret) {
                return ret;
            }
            target.private_key = lookup.private_key;
        }
    } else {
        target.path = base.path;
        if(block_entry.private_key) {
            target.private_key = block_entry.private_key;
        } else if(base.private_key) {
            target.private_key = base.private_key;
        }
    }

    buffstruct bs;
    int ret = HANDLE_EAGAIN(fm_download(target, offset, len, bs));
    if(ret) {
        return ret;
    }
    out_size = bs.size();
    if(out_size == 0) {
        return 0;
    }
    if(encode && out) {
        xorcode(bs.mutable_data(), block_no * blksize + offset, out_size, opt.secret);
        memcpy(out, bs.data(), out_size);
    }
    return 0;
}
