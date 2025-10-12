#include "mock_backend.h"

#include "common.h"
#include "fmdisk.h"
#include "utils.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <map>
#include <unordered_map>
#include <vector>
#include <stdexcept>
#include <mutex>
#include <unistd.h>

struct RemoteEntry {
    uint64_t id = 0;
    bool is_dir = true;
    mode_t mode = S_IFDIR | 0755;
    std::map<std::string, uint64_t> children;
    std::string content;
    time_t ctime = 0;
    time_t mtime = 0;
};

static uint64_t next_id = 1;
static std::unordered_map<uint64_t, RemoteEntry> id_map;
std::mutex remote_lock;

std::shared_ptr<void> make_private_key(uint64_t id) {
    return std::shared_ptr<void>( reinterpret_cast<void*>(id), [](void*) {});
}

uint64_t get_entry_id(const filekey& file) {
    if(file.path.empty() || file.path == "/") {
        return 1;
    }
    return reinterpret_cast<uint64_t>(file.private_key.get());
}

RemoteEntry* find_entry_unlocked(const std::string& path) {
    if(id_map.empty()) {
        return nullptr;
    }

    uint64_t current_id = 1;
    for(const auto& part : std::filesystem::path(path)) {
        if (part.string().empty() || part.string() == "/") {
            continue;
        }
        auto current_it = id_map.find(current_id);
        if(current_it == id_map.end() || !current_it->second.is_dir) {
            return nullptr;
        }
        auto child_it = current_it->second.children.find(part);
        if(child_it == current_it->second.children.end()) {
            return nullptr;
        }
        current_id = child_it->second;
    }
    auto final_it = id_map.find(current_id);
    return (final_it == id_map.end()) ? nullptr : &final_it->second;
}

RemoteEntry* ensure_directory_unlocked(RemoteEntry* parent, const std::string& path) {
    RemoteEntry* current_entry = parent;
    for (const auto& part : std::filesystem::path(path)) {
        if (part == "/" || part.empty()) continue;

        auto it = current_entry->children.find(part);
        if (it != current_entry->children.end()) {
            current_entry = &id_map.at(it->second);
            if (!current_entry->is_dir) {
                return nullptr;
            }
        } else {
            uint64_t new_id = next_id++;
            RemoteEntry dir;
            dir.id = new_id;
            dir.is_dir = true;
            dir.mode = S_IFDIR | 0755;
            dir.ctime = dir.mtime = time(nullptr);
            auto [new_dir_it, _] = id_map.emplace(new_id, std::move(dir));

            current_entry->children[part] = new_id;
            current_entry->mtime = time(nullptr);

            current_entry = &new_dir_it->second;
        }
    }
    return current_entry;
}

void reset_remote_unlocked() {
    id_map.clear();
    next_id = 1;
    time_t now = time(nullptr);
    RemoteEntry root;
    root.id = next_id++; // root id is 1
    root.is_dir = true;
    root.mode = S_IFDIR | 0755;
    root.ctime = root.mtime = now;
    id_map.emplace(root.id, std::move(root));
}

void backend_clear_cache_dir() {
    std::error_code ec;
    if(opt.cache_dir == nullptr || *opt.cache_dir == '\0') {
        return;
    }
    std::filesystem::remove_all(opt.cache_dir, ec);
    opt.cache_dir = nullptr;
}

int fm_prepare() {
    std::lock_guard<std::mutex> guard(remote_lock);
    if(id_map.empty()) {
        reset_remote_unlocked();
    }
    std::filesystem::create_directories(opt.cache_dir);
    return 0;
}

int fm_statfs(struct statvfs* sf) {
    memset(sf, 0, sizeof(*sf));
    sf->f_bsize = opt.block_len;
    sf->f_frsize = opt.block_len;
    sf->f_blocks = 1024;
    sf->f_bfree = 512;
    sf->f_bavail = 512;
    sf->f_files = 1024;
    sf->f_ffree = 512;
    sf->f_favail = 512;
    sf->f_namemax = 255;
    return 0;
}

int fm_getattr(const filekey& file, filemeta& meta) {
    std::lock_guard<std::mutex> guard(remote_lock);
    uint64_t entry_id = get_entry_id(file);
    if(!id_map.contains(entry_id)) {
        errno = ENOENT;
        return -errno;
    }
    RemoteEntry& entry = id_map[entry_id];
    meta = initfilemeta(filekey{file.path, make_private_key(entry.id)});
    meta.mode = entry.mode;
    meta.flags = 0;
    meta.blksize = opt.block_len;
    meta.ctime = entry.ctime;
    meta.mtime = entry.mtime;
    if(entry.is_dir) {
        meta.size = 0;
        meta.blocks = 1;
    } else {
        meta.size = entry.content.size();
        meta.blocks = meta.size / 512 + 1;
    }
    return 0;
}

int fm_list(const filekey& file, std::vector<filemeta>& flist) {
    std::lock_guard<std::mutex> guard(remote_lock);
    uint64_t entry_id = get_entry_id(file);
    if(!id_map.contains(entry_id)) {
        errno = ENOENT;
        return -errno;
    }
    auto& parent = id_map[entry_id];
    if(!parent.is_dir) {
        errno = ENOTDIR;
        return -ENOTDIR;
    }
    for(const auto& [name, child_id] : parent.children) {
        auto child_it = id_map.find(child_id);
        if(child_it == id_map.end()) {
            continue;
        }
        RemoteEntry& child = child_it->second;
        std::string child_path = pathjoin(file.path, name);
        filemeta meta = initfilemeta(filekey{child_path, make_private_key(child.id)});
        meta.mode = child.mode;
        meta.flags = 0;
        meta.blksize = opt.block_len;
        meta.ctime = child.ctime;
        meta.mtime = child.mtime;
        if(child.is_dir) {
            meta.size = 0;
            meta.blocks = 1;
        } else {
            meta.size = child.content.size();
            meta.blocks = meta.size / 512 + 1;
        }
        flist.emplace_back(std::move(meta));
    }
    return 0;
}

int fm_getattrat(const filekey& fileat, struct filekey& file) {
    std::lock_guard<std::mutex> guard(remote_lock);
    uint64_t parent_id = get_entry_id(fileat);
    if(!id_map.contains(parent_id)) {
        errno = ENOENT;
        return -errno;
    }
    auto& parent = id_map[parent_id];
    if(!parent.is_dir) {
        errno = ENOTDIR;
        return -errno;
    }
    if(!parent.children.contains(file.path)) {
        errno = ENOENT;
        return -errno;
    }
    file.private_key = make_private_key(parent.children[file.path]);
    return 0;
}

int fm_mkdir(const filekey& fileat, struct filekey& file) {
    std::lock_guard<std::mutex> guard(remote_lock);
    uint64_t parent_id = get_entry_id(fileat);
    if(!id_map.contains(parent_id)) {
        errno = ENOENT;
        return -errno;
    }
    RemoteEntry& parent = id_map[parent_id];
    if(parent.children.contains(file.path)) {
        errno = EEXIST;
        return -errno;
    }
    uint64_t new_id = next_id++;
    RemoteEntry dir;
    dir.id = new_id;
    dir.is_dir = true;
    dir.mode = S_IFDIR | 0755;
    dir.ctime = dir.mtime = time(nullptr);
    id_map.emplace(new_id, std::move(dir));
    parent.children[file.path] = new_id;
    parent.mtime = time(nullptr);
    file.private_key = make_private_key(new_id);
    return 0;
}

int fm_upload(const filekey& fileat, filekey& file, const char* data, size_t len, bool override) {
    std::lock_guard<std::mutex> guard(remote_lock);
    uint64_t parent_id = get_entry_id(fileat);
    if(!id_map.contains(parent_id)) {
        errno = ENOENT;
        return -errno;
    }
    RemoteEntry& parent = id_map[parent_id];
    if(!parent.children.contains(file.path)) {
        uint64_t new_id = next_id++;
        RemoteEntry entry;
        entry.id = new_id;
        entry.is_dir = false;
        entry.mode = S_IFREG | 0644;
        entry.content.assign(data, len);
        entry.ctime = entry.mtime = time(nullptr);

        id_map.emplace(new_id, std::move(entry));
        parent.children[file.path] = new_id;
        parent.mtime = time(nullptr);
        file.private_key = make_private_key(new_id);
        return 0;
    }
    if(!override) {
        errno = EEXIST;
        return -errno;
    }

    RemoteEntry& existing_entry = id_map[parent.children[file.path]];
    existing_entry.is_dir = false;
    existing_entry.mode = S_IFREG | 0644;
    existing_entry.content.assign(data, len);
    existing_entry.mtime = time(nullptr);
    file.private_key = make_private_key(existing_entry.id);
    return 0;
}

int fm_download(const filekey& file, off_t off, size_t size, buffstruct& bs) {
    std::lock_guard<std::mutex> guard(remote_lock);
    uint64_t entry_id = get_entry_id(file);
    if(!id_map.contains(entry_id)) {
        errno = ENOENT;
        return -errno;
    }
    RemoteEntry& entry = id_map[entry_id];
    if(entry.is_dir) {
        errno = EISDIR;
        return -errno;
    }
    if((size_t)off > entry.content.size()) {
        errno = ERANGE;
        return -errno;
    }
    size_t want = std::min(size ?: entry.content.size(), entry.content.size() - off);
    if(want == 0) {
        return 0;
    }
    buffstruct::savetobuff(const_cast<char*>(entry.content.data()) + off, 1, want, &bs);
    return 0;
}

int fm_delete(const filekey& file) {
    std::lock_guard<std::mutex> guard(remote_lock);
    if(file.path == "/") {
        errno = EPERM;
        return -EPERM;
    }
    RemoteEntry* parent = find_entry_unlocked(dirname(file.path));
    std::string bname = basename(file.path);
    if(parent == nullptr || !parent->children.contains(bname)) {
        errno = ENOENT;
        return -ENOENT;
    }
    RemoteEntry& entry = id_map[parent->children[bname]];
    if(entry.id != get_entry_id(file)) {
        errno = EBADF;
        return -errno;
    }
    if(entry.is_dir && !entry.children.empty()) {
        errno = ENOTEMPTY;
        return -ENOTEMPTY;
    }

    parent->children.erase(bname);
    parent->mtime = time(nullptr);
    id_map.erase(entry.id);
    return 0;
}

int fm_batchdelete(std::vector<filekey>&& flist) {
    for(auto& key : flist) {
        int ret = fm_delete(key);
        if(ret != 0 && errno != ENOENT) {
            return ret;
        }
    }
    return 0;
}

int fm_rename(const filekey& oldat, const filekey& file, const filekey& newat, filekey& newfile) {
    std::lock_guard<std::mutex> guard(remote_lock);
    uint64_t old_parent_id = get_entry_id(oldat);
    uint64_t new_parent_id = get_entry_id(newat);
    uint64_t entry_id = get_entry_id(file);
    if(!id_map.contains(old_parent_id) || !id_map.contains(new_parent_id) || !id_map.contains(entry_id)) {
        errno = ENOENT;
        return -errno;
    }

    RemoteEntry& old_parent = id_map[old_parent_id];
    std::string bname = basename(file.path);
    if(!old_parent.children.contains(bname)) {
        errno = ENOENT;
        return -errno;
    }
    if(old_parent.children[bname] != entry_id) {
        errno = EBADF;
        return -errno;
    }
    old_parent.children.erase(bname);
    old_parent.mtime = time(nullptr);

    RemoteEntry& new_parent = id_map[new_parent_id];
    new_parent.children[basename(newfile.path)] = entry_id;
    new_parent.mtime = time(nullptr);

    newfile.private_key = make_private_key(entry_id);
    return 0;
}

int fm_copy(const filekey& file, const filekey& newat, filekey& newfile) {
    std::lock_guard<std::mutex> guard(remote_lock);
    uint64_t entry_id = get_entry_id(file);
    uint64_t parent_id = get_entry_id(newat);
    if(!id_map.contains(entry_id) || !id_map.contains(parent_id)) {
        errno = ENOENT;
        return -errno;
    }

    RemoteEntry* parent = ensure_directory_unlocked(&id_map[parent_id], dirname(newfile.path));
    if(parent == nullptr) {
        errno = ENOTDIR;
        return -errno;
    }

    RemoteEntry& src = id_map[entry_id];
    uint64_t new_id = next_id++;
    RemoteEntry cloned;
    cloned.id = new_id;
    cloned.is_dir = src.is_dir;
    cloned.mode = src.mode;
    cloned.content = src.content;
    cloned.children = src.children;
    cloned.ctime = src.ctime;
    cloned.mtime = src.mtime;
    id_map.emplace(new_id, std::move(cloned));
    parent->children[basename(newfile.path)] = new_id;
    parent->mtime = time(nullptr);
    newfile.private_key = make_private_key(new_id);
    return 0;
}

int fm_utime(const filekey& file, const time_t tv[2]) {
    std::lock_guard<std::mutex> guard(remote_lock);
    uint64_t entry_id = get_entry_id(file);
    if(!id_map.contains(entry_id)) {
        errno = ENOENT;
        return -errno;
    }
    RemoteEntry& entry = id_map[entry_id];
    entry.ctime = tv[0];
    entry.mtime = tv[1];
    return 0;
}

std::shared_ptr<void> fm_get_private_key(const char* key) {
    if(key == nullptr) {
        return nullptr;
    }
    uint64_t id = 0;
    errno = 0;
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(key, &end, 10);
    if(errno == 0 && end != nullptr && *end == '\0') {
        id = static_cast<uint64_t>(parsed);
    }
    return make_private_key(id);
}

const char* fm_private_key_tostring(std::shared_ptr<void> key) {
    if(!key) {
        return "";
    }
    uint64_t value = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(key.get()));
    static thread_local char buf[21];
    std::snprintf(buf, sizeof(buf), "%lu", value);
    return buf;
}

void backend_reset_state() {
    std::lock_guard<std::mutex> guard(remote_lock);
    reset_remote_unlocked();
}

void backend_seed_file(const std::string& path, const std::string& content) {
    std::lock_guard<std::mutex> guard(remote_lock);
    ensure_directory_unlocked(&id_map[1], dirname(path));

    RemoteEntry* existing_entry = find_entry_unlocked(path);
    if(existing_entry == nullptr) {
        uint64_t new_id = next_id++;
        RemoteEntry entry;
        entry.id = new_id;
        entry.is_dir = false;
        entry.mode = S_IFREG | 0644;
        entry.content = content;
        entry.ctime = entry.mtime = time(nullptr);
        id_map.emplace(new_id, std::move(entry));

        RemoteEntry* parent = find_entry_unlocked(dirname(path));
        if(parent != nullptr) {
            parent->children[basename(path)] = new_id;
            parent->mtime = time(nullptr);
        }
    } else {
        existing_entry->is_dir = false;
        existing_entry->mode = S_IFREG | 0644;
        existing_entry->content = content;
        existing_entry->mtime = time(nullptr);
    }
}

std::string backend_read_file(const std::string& path) {
    std::lock_guard<std::mutex> guard(remote_lock);
    RemoteEntry* entry = find_entry_unlocked(path);
    assert(entry != nullptr);
    assert(!entry->is_dir);
    return entry->content;
}

void backend_set_mount_option(const struct fmoption* opt_) {
    opt = *opt_;
}

bool backend_path_exists(const std::string& path) {
    std::lock_guard<std::mutex> guard(remote_lock);
    return find_entry_unlocked(path) != nullptr;
}
