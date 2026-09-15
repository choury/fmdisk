#include "common.h"
#include "fmdisk.h"
#include "fmbed.h"
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

struct ls_ctx {
    const std::string* dir;
};

static int ls_callback(void* ud, const char* name) {
    auto* ctx = (ls_ctx*)ud;
    struct stat st{};
    std::string child = pathjoin(*ctx->dir, name);
    if(fmbed_stat(child.c_str(), &st) != 0) {
        memset(&st, 0, sizeof(st));
    }
    std::cout << name;
    if(S_ISDIR(st.st_mode)) {
        std::cout << "/";
    } else {
        std::cout << "\t" << bytes2human(st.st_size);
    }
    std::cout << "\n";
    return 0;
}

static int command_ls(const std::string& raw_path) {
    std::string target = normalize_path(raw_path.empty() ? "/" : raw_path);
    struct stat st{};
    int ret = fmbed_stat(target.c_str(), &st);
    if(ret == 0 && S_ISDIR(st.st_mode)) {
        ls_ctx ctx{&target};
        return fmbed_listdir(target.c_str(), ls_callback, &ctx);
    }
    if(ret == 0) {
        std::cout << basename(target) << "\t" << bytes2human(st.st_size) << "\n";
        return 0;
    }
    std::cerr << "ls: " << strerror(-ret) << "\n";
    return ret;
}

static int command_get(const std::string& remote_raw, const std::string& local_raw) {
    std::string remote = normalize_path(remote_raw);
    std::string local = local_raw.empty() ? basename(remote) : local_raw;

    struct stat st{};
    int ret = fmbed_stat(remote.c_str(), &st);
    if(ret) {
        std::cerr << "get: " << strerror(-ret) << "\n";
        return ret;
    }
    if(S_ISDIR(st.st_mode)) {
        std::cerr << "get: " << strerror(EISDIR) << "\n";
        return -EISDIR;
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

    // fmcli 跑在 no_cache 模式, 没有本地缓存自然无预取概念(advise 是空操作),
    // 顺序读由 fmbed_read 逐段直拉远端; 句柄只开一次, 跨整个下载复用
    fmbed_file* file = fmbed_open(remote.c_str(), O_RDONLY);
    if(file == nullptr) {
        std::cerr << "get: open failed: " << strerror(errno) << "\n";
        close(fd);
        return -errno;
    }
    std::vector<char> buffer;
    buffer.resize(1024 * 1024);
    uint64_t offset = 0;
    bool short_read = false;
    while(offset < (uint64_t)st.st_size) {
        size_t want = std::min<uint64_t>(buffer.size(), st.st_size - offset);
        int64_t got = fmbed_read(file, buffer.data(), want, offset);
        if(got < 0) {
            std::cerr << "get: read failed: " << strerror((int)-got) << "\n";
            fmbed_close(file, 0);
            close(fd);
            return (int)got;
        }
        if(got == 0) {
            std::cerr << "get: short read at offset " << offset << " (remote file changed?)\n";
            short_read = true;
            break;
        }
        size_t written_total = 0;
        while(written_total < (size_t)got) {
            ssize_t written = TEMP_FAILURE_RETRY(write(fd, buffer.data() + written_total, got - written_total));
            if(written < 0) {
                std::cerr << "get: write " << local << " failed: " << strerror(errno) << "\n";
                fmbed_close(file, 0);
                close(fd);
                return -errno;
            }
            written_total += written;
        }
        offset += got;
    }
    fmbed_close(file, 0);
    close(fd);
    if(short_read) {
        return -EIO; // 截尾文件不能报成功
    }
    std::cout << "saved to " << local << "\n";
    return 0;
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

    // If target is an existing dir (or trailing slash), upload into it using local basename.
    std::string target = remote;
    bool target_is_dir = false;
    struct stat rst{};
    if(fmbed_stat(remote.c_str(), &rst) == 0 && S_ISDIR(rst.st_mode)) {
        target_is_dir = true;
    }
    if(target_is_dir || remote_had_slash) {
        if(!target_is_dir) {
            std::cerr << "put: parent lookup failed: " << strerror(ENOENT) << "\n";
            return -ENOENT;
        }
        target = pathjoin(remote, basename(local));
    }

    int fd = TEMP_FAILURE_RETRY(open(local.c_str(), O_RDONLY));
    if(fd < 0) {
        std::cerr << "put: open " << local << " failed: " << strerror(errno) << "\n";
        return -errno;
    }
    // fmcli 跑在 no_cache 模式: 句柄写路径只读, 直传走 fmbed_upload
    int ret = fmbed_upload(target.c_str(), fd);
    close(fd);
    if(ret == -EEXIST) {
        std::cerr << "put: existing file" << "\n";
    } else if(ret < 0) {
        std::cerr << "put: " << strerror(-ret) << "\n";
    }
    return ret;
}

static int command_rm(const std::string& remote_raw) {
    std::string remote = normalize_path(remote_raw);
    int ret = fmbed_unlink(remote.c_str());
    if(ret && ret != -ENOENT) {
        std::cerr << "rm: " << strerror(-ret) << "\n";
    }
    return ret;
}

static int command_rmdir(const std::string& remote_raw) {
    std::string remote = normalize_path(remote_raw);
    if(remote == "/") {
        std::cerr << "rmdir: cannot remove root\n";
        return -EINVAL;
    }
    int ret = fmbed_rmdir(remote.c_str());
    if(ret) {
        std::cerr << "rmdir: " << strerror(-ret) << "\n";
    }
    return ret;
}

static int command_mkdir(const std::string& remote_raw) {
    std::string remote = normalize_path(remote_raw);
    if(remote == "/") {
        std::cerr << "mkdir: cannot create root\n";
        return -EINVAL;
    }
    int ret = fmbed_mkdir(remote.c_str(), 0755);
    if(ret) {
        std::cerr << "mkdir: " << strerror(-ret) << "\n";
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

    struct fmbed_opts fopts{};
    fopts.mask = FMBED_OPT_NO_CACHE | FMBED_OPT_CACHE_SIZE | FMBED_OPT_ENTRY_CACHE_SECOND;
    fopts.no_cache = 1;
    fopts.cache_size = 0;
    fopts.entry_cache_second = -1;
    if(fmbed_init(&fopts)) {
        std::cerr << "fmbed_init failed\n";
        return 2;
    }

    std::string cmd = argv[1];
    int ret = 1;
    if(cmd == "ls") {
        if(argc < 3) {
            ret = command_ls("/");
        } else {
            ret = command_ls(argv[2]);
        }
    } else if(cmd == "get") {
        if(argc < 3) {
            usage();
        } else {
            std::string local = (argc >= 4) ? argv[3] : "";
            ret = command_get(argv[2], local);
        }
    } else if(cmd == "put") {
        if(argc < 4) {
            usage();
        } else {
            ret = command_put(argv[2], argv[3]);
        }
    } else if(cmd == "mkdir") {
        if(argc < 3) {
            usage();
        } else {
            ret = command_mkdir(argv[2]);
        }
    } else if(cmd == "rm") {
        if(argc < 3) {
            usage();
        } else {
            ret = command_rm(argv[2]);
        }
    } else if(cmd == "rmdir") {
        if(argc < 3) {
            usage();
        } else {
            ret = command_rmdir(argv[2]);
        }
    } else {
        usage();
    }

    fmbed_destroy();
    return ret;
}
