#include "common.h"
#include "fmdisk.h"
#include "utils.h"
#include "log.h"
#include "transfer_helper.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

struct fmoption opt{};

static constexpr const char* FILE_SUFFIX = ".def";

static std::string normalize_path(const std::string& raw_path) {
    if(raw_path.empty()) {
        return "/";
    }
    std::filesystem::path p(raw_path);
    std::string normalized = p.lexically_normal().string();
    if(normalized.empty() || normalized[0] != '/') {
        normalized = "/" + normalized;
    }
    if(normalized.size() > 1 && normalized.back() == '/') {
        normalized.pop_back();
    }
    return normalized;
}

static int resolve_dir(const std::string& raw_path, filekey& out) {
    std::filesystem::path p(normalize_path(raw_path));
    filekey current{"/", nullptr};
    if(p == std::filesystem::path("/")) {
        out = current;
        return 0;
    }

    for(const auto& part : p) {
        auto name = part.string();
        if(name.empty() || name == "/") {
            continue;
        }
        if(name == ".") {
            continue;
        }
        filekey child{name, nullptr};
        int ret = HANDLE_EAGAIN(fm_getattrat(current, child));
        if(ret) {
            return ret;
        }
        current = child;
    }
    out = current;
    return 0;
}

static int resolve_file(const std::string& raw_path, filekey& parent, filekey& file_dir) {
    std::string path = normalize_path(raw_path);
    std::string parent_path = dirname(path);
    if(parent_path.empty() || parent_path == ".") {
        parent_path = "/";
    }
    int ret = resolve_dir(parent_path, parent);
    if(ret) {
        return ret;
    }

    std::string encoded = encodepath(basename(path), FILE_SUFFIX);
    file_dir = filekey{encoded, nullptr};
    ret = HANDLE_EAGAIN(fm_getattrat(parent, file_dir));
    if(ret == 0 && !file_dir.private_key) {
        errno = ENOENT;
        return -ENOENT;
    }
    return ret;
}

static int ensure_block_parent(filekey& block_parent) {
    if(!(opt.flags & FM_RENAME_NOTSUPPRTED)) {
        return 0;
    }
    filekey root{"/", nullptr};
    filekey objs{".objs", nullptr};
    int ret = HANDLE_EAGAIN(fm_getattrat(root, objs));
    if(ret == 0) {
        block_parent = objs;
        return 0;
    }
    if(errno != ENOENT) {
        return ret;
    }
    ret = HANDLE_EAGAIN(fm_mkdir(root, objs));
    if(ret && errno != EEXIST) {
        return ret;
    }
    return HANDLE_EAGAIN(fm_getattrat(root, objs));
}

static int list_dir(const filekey& dir) {
    filemeta meta = initfilemeta(dir);
    if(HANDLE_EAGAIN(fm_getattr(dir, meta)) != 0 || !S_ISDIR(meta.mode)) {
        std::cerr << "ls: " << strerror(errno) << "\n";
        return -errno;
    }

    std::vector<filemeta> flist;
    int ret = HANDLE_EAGAIN(fm_list(dir, flist));
    if(ret) {
        std::cerr << "ls: " << strerror(errno) << "\n";
        return ret;
    }

    for(auto& entry : flist) {
        std::string name = basename(entry.key.path);
        bool is_dir = S_ISDIR(entry.mode);
        bool chunked = is_dir && endwith(name, FILE_SUFFIX);

        if(chunked) {
            name = decodepath(name, FILE_SUFFIX);
            is_dir = false;
        }
        if(name == METANAME) {
            continue;
        }
        if(is_dir && name == ".objs" && (opt.flags & FM_RENAME_NOTSUPPRTED)) {
            continue;
        }

        size_t fsize = entry.size;
        if(!is_dir) {
            filemeta meta = initfilemeta(entry.key);
            bool meta_loaded = false;
            if(chunked) {
                filekey metakey{METANAME, nullptr};
                if(HANDLE_EAGAIN(fm_getattrat(entry.key, metakey)) == 0) {
                    std::vector<filekey> dummy;
                    if(download_meta(metakey, meta, dummy) == 0) {
                        fsize = meta.size;
                        meta_loaded = true;
                    }
                }
            }
            if(!meta_loaded && (entry.flags & META_KEY_ONLY_F || entry.size == 0)) {
                if(HANDLE_EAGAIN(fm_getattr(entry.key, meta)) == 0) {
                    fsize = meta.size;
                }
            }
        }

        std::cout << name;
        if(is_dir) {
            std::cout << "/";
        } else {
            std::cout << "\t" << bytes2human(fsize);
        }
        std::cout << "\n";
    }
    return 0;
}

static int command_ls(const std::string& raw_path) {
    std::string target = normalize_path(raw_path.empty() ? "/" : raw_path);
    filekey dir;
    int ret_dir = resolve_dir(target, dir);
    if(ret_dir == 0) {
        filemeta meta = initfilemeta(dir);
        if(HANDLE_EAGAIN(fm_getattr(dir, meta)) == 0) {
            if(S_ISDIR(meta.mode)) {
                return list_dir(dir);
            } else {
                std::cout << basename(normalize_path(target)) << "\t" << bytes2human(meta.size) << "\n";
                return 0;
            }
        }
    }

    // Fallback: chunked file resolution (.def directory)
    filekey parent;
    filekey file_dir;
    int ret_file = resolve_file(target, parent, file_dir);
    if(ret_file) {
        std::cerr << "ls: " << strerror(errno) << "\n";
        return ret_file;
    }

    bool chunked = endwith(file_dir.path, FILE_SUFFIX);
    size_t fsize = 0;
    if(chunked) {
        filekey metakey{METANAME, nullptr};
        ret_file = HANDLE_EAGAIN(fm_getattrat(file_dir, metakey));
        if(ret_file == 0) {
            filemeta meta;
            std::vector<filekey> dummy;
            ret_file = download_meta(metakey, meta, dummy);
            if(ret_file == 0) {
                fsize = meta.size;
            }
        }
    } else {
        filemeta meta = initfilemeta(file_dir);
        ret_file = HANDLE_EAGAIN(fm_getattr(file_dir, meta));
        if(ret_file == 0) {
            fsize = meta.size;
        }
    }
    if(ret_file) {
        std::cerr << "ls: " << strerror(errno) << "\n";
        return ret_file;
    }
    std::cout << basename(normalize_path(target)) << "\t" << bytes2human(fsize) << "\n";
    return 0;
}

static int download_blocks(const filekey& file_dir, const filemeta& meta, const std::vector<filekey>& fblocks, int fd) {
    size_t blksize = meta.blksize ? meta.blksize : opt.block_len;
    if(blksize == 0) {
        blksize = 1024 * 1024;
    }
    size_t blocks = meta.size ? (meta.size + blksize - 1) / blksize : 0;
    filekey block_parent = file_dir;
    int ret = ensure_block_parent(block_parent);
    if(ret) {
        return ret;
    }

    std::vector<char> buffer;
    buffer.resize(blksize);
    for(size_t idx = 0; idx < blocks; ++idx) {
        size_t chunk = std::min(blksize, meta.size - idx * blksize);
        const filekey* block_entry = idx < fblocks.size() ? &fblocks[idx] : nullptr;
        if(block_entry == nullptr || block_entry->path == "x" || block_entry->path.empty()) {
            std::vector<char> zeros(chunk, 0);
            ssize_t written = TEMP_FAILURE_RETRY(write(fd, zeros.data(), chunk));
            if(written < 0) {
                return -errno;
            }
            continue;
        }
        size_t got = 0;
        ret = download_block_common(block_parent, *block_entry, idx, blksize, 0, chunk, meta.flags & FILE_ENCODE_F, buffer.data(), got);
        if(ret) {
            return ret;
        }
        ssize_t written = TEMP_FAILURE_RETRY(write(fd, buffer.data(), got));
        if(written < 0) {
            return -errno;
        }
    }
    return 0;
}

static int command_get(const std::string& remote_raw, const std::string& local_raw) {
    std::string remote = normalize_path(remote_raw);
    std::string local = local_raw.empty() ? basename(remote) : local_raw;

    filekey parent;
    filekey file_dir;
    int ret = resolve_file(remote, parent, file_dir);
    if(ret) {
        std::cerr << "get: " << strerror(errno) << "\n";
        return ret;
    }

    filekey meta_key{METANAME, nullptr};
    ret = HANDLE_EAGAIN(fm_getattrat(file_dir, meta_key));
    if(ret) {
        std::cerr << "get: meta lookup failed: " << strerror(errno) << "\n";
        return ret;
    }

    filemeta meta;
    std::vector<filekey> fblocks;
    ret = download_meta(meta_key, meta, fblocks);
    if(ret) {
        std::cerr << "get: meta download failed: " << strerror(errno) << "\n";
        return ret;
    }

    std::filesystem::path local_path(local);
    if(local_path.has_parent_path()) {
        std::error_code ec;
        std::filesystem::create_directories(local_path.parent_path(), ec);
    }

    int fd = TEMP_FAILURE_RETRY(open(local.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644));
    if(fd < 0) {
        std::cerr << "get: open " << local << " failed: " << strerror(errno) << "\n";
        return -errno;
    }

    if(!meta.inline_data.empty()) {
        ssize_t written = TEMP_FAILURE_RETRY(write(fd, meta.inline_data.data(), meta.size));
        ret = (written < 0) ? -errno : 0;
    } else {
        ret = download_blocks(file_dir, meta, fblocks, fd);
    }
    close(fd);
    if(ret == 0) {
        std::cout << "saved to " << local << "\n";
    }
    return ret;
}

static int command_put(const std::string& local, const std::string& remote_raw) {
    std::string remote = normalize_path(remote_raw);
    bool remote_had_slash = !remote_raw.empty() && remote_raw.back() == '/';

    struct stat st{};
    if(stat(local.c_str(), &st) < 0) {
        std::cerr << "put: stat " << local << " failed: " << strerror(errno) << "\n";
        return -errno;
    }
    if(!S_ISREG(st.st_mode)) {
        std::cerr << "put: " << local << " is not a regular file\n";
        return -EINVAL;
    }

    filekey parent;
    std::string filename;

    // If target is an existing dir (or trailing slash), upload into it using local basename.
    bool target_is_dir = false;
    int dir_ret = resolve_dir(remote, parent);
    if(dir_ret == 0) {
        filemeta dir_meta = initfilemeta(parent);
        if(HANDLE_EAGAIN(fm_getattr(parent, dir_meta)) == 0 && S_ISDIR(dir_meta.mode)) {
            target_is_dir = true;
        }
    }

    if(target_is_dir || remote_had_slash) {
        if(dir_ret != 0) {
            std::cerr << "put: parent lookup failed: " << strerror(errno) << "\n";
            return dir_ret;
        }
        filename = basename(local);
    } else {
        std::string parent_path = dirname(remote);
        if(parent_path.empty() || parent_path == ".") {
            parent_path = "/";
        }
        int ret = resolve_dir(parent_path, parent);
        if(ret) {
            std::cerr << "put: parent lookup failed: " << strerror(errno) << "\n";
            return ret;
        }
        filename = basename(remote);
    }

    std::string encoded = encodepath(filename, FILE_SUFFIX);
    filekey file_dir{encoded, nullptr};
    int ret = HANDLE_EAGAIN(fm_getattrat(parent, file_dir));
    if(ret == 0 && !file_dir.private_key) {
        errno = ENOENT;
        ret = -ENOENT;
    }
    if(ret == 0 && file_dir.private_key) {
        std::cerr << "put: existing file" << "\n";
        return -EEXIST;
    } else if(ret != 0 && errno != ENOENT) {
        std::cerr << "put: lookup failed: " << strerror(errno) << "\n";
        return ret;
    }

    if(!file_dir.private_key) {
        ret = HANDLE_EAGAIN(fm_mkdir(parent, file_dir));
        if(ret && errno != EEXIST) {
            std::cerr << "put: mkdir failed: " << strerror(errno) << "\n";
            return ret;
        }
        if(ret) {
            ret = HANDLE_EAGAIN(fm_getattrat(parent, file_dir));
            if(ret) {
                std::cerr << "put: fetch existing dir failed: " << strerror(errno) << "\n";
                return ret;
            }
        }
    }

    filekey block_parent = file_dir;
    ret = ensure_block_parent(block_parent);
    if(ret) {
        std::cerr << "put: prepare block parent failed: " << strerror(errno) << "\n";
        return ret;
    }

    int fd = TEMP_FAILURE_RETRY(open(local.c_str(), O_RDONLY));
    if(fd < 0) {
        std::cerr << "put: open " << local << " failed: " << strerror(errno) << "\n";
        return -errno;
    }

    filemeta meta{};
    std::vector<filekey> fblocks;
    ret = upload_file_from_fd(file_dir, block_parent, fd, st, true, meta, fblocks);
    close(fd);
    if(ret) {
        std::cerr << "put: upload data failed: " << strerror(errno) << "\n";
        return ret;
    }

    ret = upload_meta(file_dir, meta, fblocks);
    if(ret) {
        std::cerr << "put: upload meta failed: " << strerror(errno) << "\n";
    }
    return ret;
}

static int command_rm(const std::string& remote_raw) {
    std::string remote = normalize_path(remote_raw);

    filekey parent;
    filekey file_dir;
    int ret = resolve_file(remote, parent, file_dir);
    if(ret) {
        std::cerr << "rm: " << strerror(errno) << "\n";
        return ret;
    }

    bool chunked = endwith(file_dir.path, FILE_SUFFIX);
    if(!chunked) {
        ret = HANDLE_EAGAIN(fm_delete(file_dir));
        if(ret && errno != ENOENT) {
            std::cerr << "rm: " << strerror(errno) << "\n";
        }
        return ret;
    }

    filekey metakey{METANAME, nullptr};
    ret = HANDLE_EAGAIN(fm_getattrat(file_dir, metakey));
    if(ret) {
        std::cerr << "rm: meta lookup failed: " << strerror(errno) << "\n";
        return ret;
    }

    filemeta meta;
    std::vector<filekey> fblocks;
    ret = download_meta(metakey, meta, fblocks);
    if(ret) {
        std::cerr << "rm: read meta failed: " << strerror(errno) << "\n";
        return ret;
    }

    filekey block_parent = file_dir;
    ret = ensure_block_parent(block_parent);
    if(ret) {
        std::cerr << "rm: prepare block parent failed: " << strerror(errno) << "\n";
        return ret;
    }

    for(auto& blk : fblocks) {
        if(blk.path == "x" || blk.path.empty()) {
            continue;
        }
        filekey del = blk;
        del.path = pathjoin(block_parent.path, blk.path);
        if(!del.private_key) {
            filekey lookup{blk.path, nullptr};
            if(HANDLE_EAGAIN(fm_getattrat(block_parent, lookup)) == 0) {
                del.private_key = lookup.private_key;
            }
        }
        int del_ret = HANDLE_EAGAIN(fm_delete(del));
        if(del_ret && errno != ENOENT) {
            std::cerr << "rm: delete block failed: " << strerror(errno) << "\n";
            return del_ret;
        }
    }

    ret = HANDLE_EAGAIN(fm_delete(metakey));
    if(ret && errno != ENOENT) {
        std::cerr << "rm: delete meta failed: " << strerror(errno) << "\n";
        return ret;
    }

    ret = HANDLE_EAGAIN(fm_delete(file_dir));
    if(ret && errno != ENOENT) {
        std::cerr << "rm: delete dir failed: " << strerror(errno) << "\n";
    }
    return ret;
}

static int command_rmdir(const std::string& remote_raw) {
    std::string remote = normalize_path(remote_raw);
    if(remote == "/") {
        std::cerr << "rmdir: cannot remove root\n";
        return -EINVAL;
    }

    filekey dir;
    int ret = resolve_dir(remote, dir);
    if(ret) {
        std::cerr << "rmdir: " << strerror(errno) << "\n";
        return ret;
    }

    filemeta meta = initfilemeta(dir);
    ret = HANDLE_EAGAIN(fm_getattr(dir, meta));
    if(ret) {
        std::cerr << "rmdir: " << strerror(errno) << "\n";
        return ret;
    }
    if(!S_ISDIR(meta.mode)) {
        std::cerr << "rmdir: not a directory\n";
        return -ENOTDIR;
    }

    std::vector<filemeta> flist;
    ret = HANDLE_EAGAIN(fm_list(dir, flist));
    if(ret) {
        std::cerr << "rmdir: " << strerror(errno) << "\n";
        return ret;
    }
    if(!flist.empty()) {
        std::cerr << "rmdir: directory not empty\n";
        return -ENOTEMPTY;
    }

    ret = HANDLE_EAGAIN(fm_delete(dir));
    if(ret && errno != ENOENT) {
        std::cerr << "rmdir: " << strerror(errno) << "\n";
    }
    return ret;
}

static int command_mkdir(const std::string& remote_raw) {
    std::string remote = normalize_path(remote_raw);
    if(remote == "/") {
        std::cerr << "mkdir: cannot create root\n";
        return -EINVAL;
    }

    std::string parent_path = dirname(remote);
    if(parent_path.empty() || parent_path == ".") {
        parent_path = "/";
    }
    filekey parent;
    int ret = resolve_dir(parent_path, parent);
    if(ret) {
        std::cerr << "mkdir: parent lookup failed: " << strerror(errno) << "\n";
        return ret;
    }

    filekey dir{basename(remote), nullptr};
    ret = HANDLE_EAGAIN(fm_getattrat(parent, dir));
    if(ret == 0 && dir.private_key) {
        std::cerr << "mkdir: existing path\n";
        return -EEXIST;
    } else if(ret != 0 && errno != ENOENT) {
        std::cerr << "mkdir: lookup failed: " << strerror(errno) << "\n";
        return ret;
    }

    ret = HANDLE_EAGAIN(fm_mkdir(parent, dir));
    if(ret && errno != EEXIST) {
        std::cerr << "mkdir: create failed: " << strerror(errno) << "\n";
    }
    return ret;
}

static void usage() {
    std::cout << "Usage:\n"
              << "  fmcli ls <remote_path>\n"
              << "  fmcli get <remote_path> [local_path]\n"
              << "  fmcli put <local_path> <remote_path>\n"
              << "  fmcli mkdir <remote_dir>\n"
              << "  fmcli rm <remote_path>\n"
              << "  fmcli rmdir <remote_dir>\n";
}

int main(int argc, char** argv) {
    if(argc < 2) {
        usage();
        return 1;
    }

    if(fm_prepare()) {
        std::cerr << "fm_prepare failed\n";
        return 2;
    }
    opt.no_cache = 1;
    opt.cache_size = 0;
    opt.entry_cache_second = -1;
    log_init(opt.log_path);

    std::string cmd = argv[1];
    if(cmd == "ls") {
        if(argc < 3) {
            return command_ls("/");
        }
        return command_ls(argv[2]);
    } else if(cmd == "get") {
        if(argc < 3) {
            usage();
            return 1;
        }
        std::string local = (argc >= 4) ? argv[3] : "";
        return command_get(argv[2], local);
    } else if(cmd == "put") {
        if(argc < 4) {
            usage();
            return 1;
        }
        return command_put(argv[2], argv[3]);
    } else if(cmd == "mkdir") {
        if(argc < 3) {
            usage();
            return 1;
        }
        return command_mkdir(argv[2]);
    } else if(cmd == "rm") {
        if(argc < 3) {
            usage();
            return 1;
        }
        return command_rm(argv[2]);
    } else if(cmd == "rmdir") {
        if(argc < 3) {
            usage();
            return 1;
        }
        return command_rmdir(argv[2]);
    }

    usage();
    return 1;
}
