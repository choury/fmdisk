#include "common.h"
#include "fmdisk.h"
#include "defer.h"
#include "trdpool.h"
#include "file.h"
#include <unistd.h>
#include <iostream>
#include <vector>
#include <map>
#include <unordered_map>
#include <shared_mutex>
#include <limits>
#include <mutex>
#include <string>
#include <json.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "sqlite.h"
#include "utils.h"

using namespace std;

struct fmoption opt{};
static bool verbose = false;
static bool autofix = false;
static bool no_db = false;
static bool recursive = false;
static bool deleteall = false;
static bool sync_dirty = false;
static TrdPool *pool;
static std::unordered_map<std::string, filemeta> objs;
static std::mutex objs_lock;

// 全局变量存储本地缓存文件信息

// 直接使用utils.h中的cache_file_info
static std::vector<cache_file_info> local_cache_files;
static std::unordered_map<string, cache_file_info*> remote_path_to_cache; // 远程路径到缓存文件的映射
static std::unordered_map<ino_t, cache_file_info*> inode_to_cache;        // inode到缓存文件的映射
static std::shared_mutex cache_index_lock;

static mutex console_lock;

std::ostream& lock(std::ostream& os) {
    console_lock.lock();
    return os;
}

std::ostream& unlock(std::ostream& os) {
    console_lock.unlock();
    return os;
}

// 前向声明
static void checkcache(const filekey& file, filemeta& meta, std::vector<filekey>& blks);
static filekey* getpath(string path);
static filekey uploadBlockFromCache(const std::string& remote_path, const filemeta& meta, uint64_t block_no);

// 上传单个block数据，返回block key
static int upload_block_data(filekey& file, const char* data, size_t size, off_t offset, bool encode) {
    // 检查是否全零
    if(isAllZero(data, size)){
        file = filekey{"x", 0};
        return 0;
    }
    int ret = 0;
    if(encode) {
        char* buff = (char*)malloc(size);
        if (!buff) {
            cerr << "Failed to allocate memory for block" << endl;
            return -1;
        }
        defer(free, buff);

        memcpy(buff, data, size);
        xorcode(buff, offset, size, opt.secret);

        // 上传block，fm_upload会更新result_key的private_key
        ret = HANDLE_EAGAIN(fm_upload(dirname(file), file, buff, size, false));
    } else {
        // 直接上传原始数据
        ret = HANDLE_EAGAIN(fm_upload(dirname(file), file, data, size, false));
    }
    if (ret != 0) {
        cerr << "Failed to upload block " << file.path << " error: " << ret << endl;
        return -1;
    }

    if (verbose) {
        cout << "Successfully uploaded block " << file.path << " (size: " << size << ")" << endl;
    }
    return 0;
}

// 从本地cache上传指定的block，返回新的block key
static filekey uploadBlockFromCache(const std::string& remote_path, const filemeta& meta, uint64_t block_no) {
    cache_file_info* cache_file = nullptr;
    {
        std::shared_lock<std::shared_mutex> lock(cache_index_lock);
        auto it = remote_path_to_cache.find(remote_path);
        if (it == remote_path_to_cache.end()) {
            return {{"x"}, 0};
        }
        cache_file = it->second;
    }

    // 验证文件大小
    if ((size_t)cache_file->st.stx_size != meta.size) {
        return {{"x"}, 0};
    }

    // 检查block表中该block的状态
    block_record record;
    if (!load_block_from_db(cache_file->st.stx_ino, block_no, record)) {
        if (verbose) {
            cerr << lock << "Block " << block_no << " not found in database for: " << remote_path << endl << unlock;
        }
        return {{"x"}, 0};
    }

    // 计算block参数
    off_t offset = block_no * meta.blksize;
    size_t block_size = std::min((size_t)meta.blksize, (size_t)(meta.size - offset));
    if (offset >= (off_t)meta.size) {
        return {{"x"}, 0};
    }

    // 打开和mmap cache文件
    int fd = open(cache_file->path.c_str(), O_RDONLY);
    if (fd < 0) return {{"x"}, 0};
    defer(close, fd);

    char* mmap_data = (char*)mmap(nullptr, meta.size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mmap_data == MAP_FAILED) return {{"x"}, 0};
    defer(munmap, mmap_data, meta.size);

    // 上传block
    filekey new_block_key = makeChunkBlockKey(block_no);
    if (upload_block_data(new_block_key, mmap_data + offset, block_size, offset, meta.flags & FILE_ENCODE_F) != 0) {
        return {{"x"}, 0};
    }

    return basename(new_block_key);
}

static int sync_dirty_file(const string& path){
    filemeta meta{};
    std::vector<filekey> fblocks;

    // 从数据库加载文件元数据
    load_file_from_db(path, meta, fblocks);

    if ((meta.flags & FILE_DIRTY_F) == 0) {
        if (verbose) {
            cout << "File " << path << " no longer dirty, skipping" << endl;
        }
        return 0;
    }
    string decoded_path = decodepath(path, file_encode_suffix);

    bool succeed = false;
    defer([&path, &decoded_path, &succeed, &meta, &fblocks]{
        if(!succeed) {
            return;
        }
        meta.flags &= ~FILE_DIRTY_F;
        save_file_to_db(path, meta, fblocks);
        if (verbose) {
            cout << "Successfully synced: " << decoded_path << endl;
        }
    });

    if (verbose) {
        cout << "Syncing dirty file: " << decoded_path << endl;
    }

    filekey* file = getpath(path);
    if (file == nullptr) {
        cerr << "Failed to get filekey for: " << path << endl;
        return -1;
    }
    defer([file] { delete file; });

    if(meta.size == 0 || meta.inline_data.size() > 0) {
        int ret = upload_meta(*file, meta, {});
        if (ret != 0) {
            cerr << "Failed to upload meta for file: " << decoded_path << " error: " << ret << endl;
            return -1;
        }
        return 0;
    }

    // 获取缓存文件路径
    string cache_path = get_cache_path(decoded_path);

    // 打开并mmap缓存文件
    int fd = open(cache_path.c_str(), O_RDONLY);
    if (fd < 0) {
        cerr << "Failed to open cache file: " << cache_path << " error: " << strerror(errno) << endl;
        return -1;
    }
    defer(close, fd);

    struct statx st;
    if (statx(fd, "", AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW, STATX_INO | STATX_BTIME | STATX_SIZE, &st) != 0) {
        cerr << "Failed to statx cache file: " << cache_path << " error: " << strerror(errno) << endl;
        return -1;
    }
    if(st.stx_size != meta.size) {
        cerr << "Cache file size mismatch for " << cache_path << ": expected " << meta.size << ", got " << st.stx_size << endl;
        return -1;
    }

    // mmap文件
    char* mmap_data = (char*)mmap(nullptr, st.stx_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mmap_data == MAP_FAILED) {
        cerr << "Failed to mmap cache file: " << cache_path << " error: " << strerror(errno) << endl;
        return -1;
    }
    defer(munmap, mmap_data, st.stx_size);

    // 1. 先同步所有dirty blocks
    bool block_sync_failed = false;
    std::vector<block_record> blocks;
    get_blocks_for_inode(st.stx_ino, blocks);
    if(blocks.size() > fblocks.size()) {
        cerr << "Block count mismatch for file: " << decoded_path << ", db has " << blocks.size() << ", meta has " << fblocks.size() << endl;
        return -1;
    }
    uint64_t btime = st.stx_btime.tv_sec * 1000000000LL + st.stx_btime.tv_nsec;
    for (auto& block_rec : blocks) {
        if(block_rec.btime != 0 && block_rec.btime != btime) {
            // btime不匹配，说明是不同的文件，数据可能已被覆盖
            if(block_rec.dirty) {
                cerr << "Block " << block_rec.block_no << " btime mismatch but dirty for: " << decoded_path << ", db=" << block_rec.btime
                     << ", file=" << btime << endl;
                return -1;
            }
            block_rec.dirty = true; // 强制重新上传
        }
        if (!block_rec.dirty && !block_rec.path.empty() && !block_rec.private_key.empty()) {
            fblocks[block_rec.block_no].path = block_rec.path;
            fblocks[block_rec.block_no].private_key = fm_get_private_key(block_rec.private_key.c_str());
            continue;
        }
        off_t offset = block_rec.block_no * meta.blksize;
        size_t block_size = std::min((size_t)meta.blksize, (size_t)(meta.size - offset));

        filekey new_block_key = makeChunkBlockKey(block_rec.block_no);
        int ret = upload_block_data(new_block_key, mmap_data + offset, block_size, offset, meta.flags & FILE_ENCODE_F);
        if (ret != 0) {
            block_sync_failed = true;
            break;
        }

        // 更新fblocks和数据库
        fblocks[block_rec.block_no] = basename(new_block_key);
        save_block_to_db({-1, st.stx_ino, btime}, block_rec.block_no, new_block_key, false);
    }

    if (block_sync_failed) {
        cerr << "Failed to sync blocks for file: " << decoded_path << endl;
        return -1;
    }

    // 上传元数据到远程，使用更新后的fblocks
    int ret = upload_meta(*file, meta, fblocks);
    if (ret != 0) {
        cerr << "Failed to upload meta for file: " << decoded_path << " error: " << ret << endl;
        return -1;
    }
    // 更新数据库中的状态
    meta.flags &= ~FILE_DIRTY_F;
    save_file_to_db(path, meta, fblocks);
    return 0;
}

// 同步所有dirty文件到远程存储
static int sync_all_dirty_files() {
    std::vector<std::string> dirty_files;
    if(get_dirty_files(dirty_files) <= 0) {
        cout << "No dirty files found." << endl;
        return 0;
    }

    cout << "Found " << dirty_files.size() << " dirty files, syncing..." << endl;

    int synced = 0;
    int failed = 0;

    for(const auto& file_path : dirty_files) {
        if (sync_dirty_file(file_path) != 0) {
            failed++;
        }else{
            synced++;
        }
    }

    cout << "Sync completed: " << synced << " succeeded, " << failed << " failed" << endl;
    return failed > 0 ? -1 : 0;
}

void fixNoMeta(const filekey& file, const std::map<std::string, struct filekey>& flist) {
    cerr << lock;
    defer([]{cerr<<unlock;});
    if (flist.empty()) {
        cerr << "there is no blocks of file: " << decodepath(file.path, file_encode_suffix) << ", so delete it" << endl;
        goto del;
    }
    cerr << decodepath(file.path, file_encode_suffix)<<" has blocks:" << endl;
    for (auto f : flist) {
        cerr << f.first<<" ";
    }
    cerr<<endl;
    if(deleteall)
        goto del;
    do {
        fflush(stdin);
        cerr << "delete this file or ignore it([D]elete/[I]gnore) I?";
        char a;
        cin>>a;
        if (a == '\n') {
            a = 'I';
        } else if (a != 'D' && a != 'I') {
            cin.ignore(numeric_limits<streamsize>::max(), '\n');
            continue;
        }
        if (a == 'I') {
            return;
        } else {
            goto del;
        }
    } while (true);
del:
    int ret = HANDLE_EAGAIN(fm_delete(file));
    if (ret != 0) {
        cerr << "delete dir " << file.path << "failed: " << ret << endl;
        return;
    }
    delete_entry_from_db(file.path);
    delete_file_from_db(file.path);
}

filekey fixMissBlock(const std::string& path, const std::map<std::string, struct filekey>& flist, uint64_t no, const filemeta& meta) {
    cerr << lock;
    defer([]{cerr<<unlock;});

    // 首先尝试从本地cache上传
    if (autofix) {
        string remote_path = decodepath(path, file_encode_suffix);
        filekey cache_result = uploadBlockFromCache(remote_path, meta, no);
        if (cache_result.path != "x") {
            cerr << "Successfully uploaded block " << no << " from local cache for " << remote_path << endl;
            return cache_result;
        }
    }

    std::vector<filekey> fit;
    string No = to_string(no);
    for (auto i : flist) {
        if (i.first == No || startwith(i.first, No + '_')) {
            fit.push_back(filekey{basename(i.second.path), i.second.private_key});
        }
    }
    if (fit.empty()) {
        cout<< path << " has no block fit for " << No << ", should reset it to 'x'"<<endl;
        return {{"x"}, 0};
    }
    size_t n = 0;
pick:
    cerr << path <<" has some block fit for " << No << ", please pick one:" << endl;
    for(size_t i = 0; i < fit.size(); i++) {
        cerr << i << " -> " << fit[i].path << endl;
    }
    cout<<"? ";
    cin>>n;
    if(n >= fit.size()){
        goto pick;
    }
    cerr << fit[n].path << " is selected." << endl;
    return fit[n];
}

bool blockMatchNo(string block, uint64_t no) {
    if (block == "x") {
        return true;
    }
    try{
        return (uint64_t)stoi(block) == no;
    }catch(const std::logic_error& e) {
        return false;
    }
}

void checkchunk(filekey* file) {
    defer([file]{delete file;});
    std::vector<filemeta> flist;
    int ret  = HANDLE_EAGAIN(fm_list(*file, flist));
    if (ret != 0) {
        cerr<<lock<< "list dir "<<file->path<<" failed: "<<ret<<endl<<unlock;
        return;
    }
    std::map<string, filekey> fs;
    for (auto f : flist) {
        fs[basename(f.key.path)] =  f.key;
    }
    std::string path = decodepath(file->path, file_encode_suffix);
    if (fs.count(METANAME) == 0) {
        cerr<<lock<< "file: "<<path<<" have no meta.json"<<endl<<unlock;
        if (autofix) {
            fixNoMeta(*file, fs);
        }
        return;
    }
    filemeta meta = initfilemeta(fs[METANAME]);
    std::vector<filekey> fblocks;
    ret = download_meta(meta.key, meta, fblocks);
    if(ret == 0){
        if(meta.inline_data.size() && fblocks.size()){
            cerr<<lock<<"get inline_data and blocks/block_list: "<< meta.key.path <<endl <<unlock;
            ret = 4;
        }

        if(meta.size && meta.inline_data.empty() && fblocks.empty()){
            cerr<<lock<<"get none of inline_data/blocks/block_list: "<< meta.key.path <<endl <<unlock;
            ret = 6;
        }
    }
    if (ret != 0) {
        cerr<<lock<<"file: "<<path<<" have malformed meta.json"<<endl<<unlock;
        if (autofix) {
            fixNoMeta(*file, fs);
        }
        return;
    }
    fs.erase(METANAME);
    bool fixed = false;
    for(size_t i = 0; i < fblocks.size(); i++) {
        if(fblocks[i].path == "x" || fblocks[i].path.empty()){
            continue;
        }
        bool haswrong = false;
        if (!blockMatchNo(fblocks[i].path, i)) {
            cerr<<lock<<"file: "<<path<<" has block "<<fblocks[i].path<<" on No."<<i<<endl<<unlock;
            haswrong = true;
        }
        if(opt.flags & FM_RENAME_NOTSUPPRTED) {
            bool matched = false;
            {
                std::lock_guard<std::mutex> guard(objs_lock);
                auto it = objs.find(fblocks[i].path);
                if(it != objs.end()) {
                    it->second.flags |= META_KEY_CHECKED_F;
                    matched = true;
                }
            }
            if(!matched) {
                haswrong = true;
            }
        } else if (fs.count(fblocks[i].path) == 0) {
            haswrong = true;
        }
        if (haswrong) {
            cerr<<lock<<"file: "<<path<<" miss block: "<<fblocks[i].path<<endl<<unlock;
            if (autofix) {
                fixed = true;
                fblocks[i] = basename(fixMissBlock(path, fs, i, meta));
            }
        }
    }
    if(fixed){
        ret = upload_meta(*file, meta, fblocks);
        if (ret != 0) {
            cerr<<lock<< "upload meta "<<meta.key.path<<" failed: "<<ret<<endl<<unlock;
            return;
        }
    }
    if (autofix) {
        save_file_to_db(file->path, meta, fblocks);
    }
    std::vector<filekey> ftrim;
    for (auto f : fs) {
        if (!isdigit(f.first[0])){
            cerr<<lock<<"file: "<<path<<" has unwanted block: "<<f.first<<endl<<unlock;
            if(autofix){
                ftrim.push_back(f.second);
            }
            continue;
        }
        int i = stoi(f.first);
        if(i < 0 || fblocks.size() <= (size_t)i){
            cerr<<lock<<"file: "<<path<<" has unwanted block: "<<f.first<<endl<<unlock;
            if(autofix){
                ftrim.push_back(f.second);
            }
            continue;
        }
        if (fblocks[i].path != f.first) {
            cerr<<lock<<"file: "<<path<<" has lagecy block: "<<pathjoin(f.first, fblocks[i].path)<<endl<<unlock;
            if (autofix) {
                ftrim.push_back(f.second);
            }
        }
    }
    if (!ftrim.empty()) {
        int ret = HANDLE_EAGAIN(fm_batchdelete(std::move(ftrim)));
        if (ret != 0) {
            cerr<<lock<< "delete lagecy block in: "<<file->path<<" failed"<<endl<<unlock;
        }
    }

    // 检查本地缓存一致性
    checkcache(*file, meta, fblocks);
    if (verbose) {
        cout <<lock<< path << " check finish" << endl<<unlock;
    }
    return;
}

void checkdir(filekey* file) {
    defer([file]{delete file;});

    std::vector<filemeta> flist;
    int ret  = HANDLE_EAGAIN(fm_list(*file, flist));
    if (ret != 0) {
        cerr<<lock<< "list dir "<<file->path<<" failed: "<<ret<<endl<<unlock;
        return;
    }
    bool isRoot = file->path == "/";
    if (autofix) {
        delete_entry_prefix_from_db(file->path);
    }
    for (auto f : flist) {
        if(isRoot && (opt.flags & FM_RENAME_NOTSUPPRTED) && (f.key.path == ".objs" || f.key.path == ".objs/")){
            continue;
        }
        std::string path;
        if(f.key.path[0] != '/') {
            path = "/" + f.key.path;
        } else{
            path = f.key.path;
        }
        if(path[path.length() - 1] == '/') {
            path.resize(path.size() - 1);
        }
        f.key.path = path;
        if (autofix) {
            save_entry_to_db(file->path, f);
        }
        if (S_ISREG(f.mode)) {
            if (verbose) {
                cout<<lock << f.key.path << " skip check" << endl<<unlock;
            }
            continue;
        }

        if(endwith(f.key.path, file_encode_suffix)){
            pool->submit_fire_and_forget([key = new filekey(f.key)]() { checkchunk(key); });
        }else if(recursive){
            pool->submit_fire_and_forget([key = new filekey(f.key)]() { checkdir(key); });
        }
    }
}

// 删除不一致的缓存文件和相关block条目
void fixCacheInconsistency(cache_file_info* cache_file) {
    if (cache_file == nullptr) {
        return;
    }

    const std::string cache_path = cache_file->path;
    const std::string remote_path = cache_file->remote_path;
    const ino_t inode = cache_file->st.stx_ino;

    if (verbose) {
        cout << lock << "Deleted inconsistent cache file: " << cache_file->path << endl << unlock;
    }

    // 删除本地缓存文件
    int ret = unlink(cache_path.c_str());
    bool removed = true;
    if (ret != 0) {
        if (errno != ENOENT) {
            cerr << lock << "Failed to delete cache file: " << cache_path
                 << " error: " << strerror(errno) << endl << unlock;
            removed = false;
        }
    }

    if (removed) {
        delete_blocks_from_db(inode);
        std::unique_lock<std::shared_mutex> lock(cache_index_lock);
        remote_path_to_cache.erase(remote_path);
        inode_to_cache.erase(inode);
    } else {
        cache_file->checked = false;
    }
}

// 检查本地缓存文件与远程meta信息的一致性
static void checkcache(const filekey& file, filemeta& meta, std::vector<filekey>& blks) {
    string remote_path = decodepath(file.path, file_encode_suffix);

    cache_file_info* cache_file = nullptr;
    {
        std::shared_lock<std::shared_mutex> lock(cache_index_lock);
        // 查找对应的本地缓存文件
        auto it = remote_path_to_cache.find(remote_path);
        if (it == remote_path_to_cache.end()) {
            return;
        }
        cache_file = it->second;
    }
    cache_file->checked = true;  // 标记为已检查

    // 1. 验证文件大小是否一致
    if ((size_t)cache_file->st.stx_size != meta.size) {
        cerr << lock << "Cache file size mismatch: [" << remote_path
             << "] local=" << cache_file->st.stx_size << " remote=" << meta.size << endl << unlock;
        if (autofix) {
            fixCacheInconsistency(cache_file);
        }
        return;
    }

    // 2. 验证数据库blocks表中该文件相关记录的private_key
    std::vector<block_record> db_blocks;
    if (get_blocks_for_inode(cache_file->st.stx_ino, db_blocks) <= 0) {
        cerr << lock << "blocks for: [" << remote_path << "] not found"<< endl << unlock;
        if(autofix) {
            fixCacheInconsistency(cache_file);
        }
        return;
    }
    // 为每个数据库中的block找到对应的远程block并比较private_key
    for (const auto& db_block : db_blocks) {
        uint64_t btime = cache_file->st.stx_btime.tv_sec * 1000000000LL + cache_file->st.stx_btime.tv_nsec;
        if (db_block.btime != 0 && db_block.btime != btime) {
            cerr << lock << "Block " << db_block.block_no << " btime mismatch for: [" << remote_path
                 << "] db_btime=" << db_block.btime << " file_btime=" << btime << endl << unlock;
            if (autofix) {
                fixCacheInconsistency(cache_file);
            }
            return;
        }
        if (db_block.block_no >= blks.size()) {
            cerr << lock << "Block table has invalid block_no: [" << remote_path
                 << "] block_no=" << db_block.block_no << " max_blocks=" << blks.size() << endl << unlock;
            if (autofix) {
                fixCacheInconsistency(cache_file);
            }
            return;
        }
        string expected_key = fm_private_key_tostring(blks[db_block.block_no].private_key);
        if (db_block.private_key != expected_key) {
            cerr << lock << "Block table private_key mismatch: [" << remote_path
                 << "] block_no=" << db_block.block_no << " db_key=" << db_block.private_key
                 << " expected=" << expected_key << endl << unlock;
            if (autofix) {
                fixCacheInconsistency(cache_file);
            }
            return;
        }
    }
    if (verbose) {
        cout << lock << "Cache check completed for: [" << remote_path << "] (consistent)" << endl << unlock;
    }
}

// 检查孤儿文件和孤儿block条目
void checkOrphanedFiles(const char* checkpath) {
    cout << lock << "Checking for orphaned cache files and block entries..." << endl << unlock;

    // 1. 检查孤儿缓存文件（没有被任何远程文件引用的本地缓存文件）
    for (auto& cache_file : local_cache_files) {
        if (cache_file.checked) {
            continue;
        }
        cerr << lock << "Found arphaned cache file  " << cache_file.path << ", inode: "
             << cache_file.st.stx_ino << ", size: " << cache_file.st.stx_size << endl << unlock;

        if (autofix) {
            fixCacheInconsistency(&cache_file);
        }
    }

    // 2. 检查孤儿block条目（blocks表中对应的inode在本地缓存中不存在）
    // 只有在检查根路径时才进行孤儿block条目检查，避免误删其他路径下的正常文件对应的block
    if (strcmp(checkpath, "/") != 0) {
        return;
    }
    std::vector<ino_t> db_inodes;
    if (get_all_block_inodes(db_inodes) <= 0) {
        return;
    }
    for (ino_t db_inode : db_inodes) {
        if (inode_to_cache.count(db_inode) ) {
            continue;
        }
        cerr << lock << "inode=" << db_inode << " (no corresponding cache file)" << endl << unlock;
        if (autofix) {
            // 删除孤儿block条目
            delete_blocks_from_db(db_inode);
        }
    }
}

// 扫描本地缓存目录，收集指定路径下的文件信息
void scanLocalCacheFiles(const char* checkpath) {
    // 获取指定路径下的缓存文件
    local_cache_files = scan_cache_directory(checkpath);

    // 构建映射关系
    std::unique_lock<std::shared_mutex> index_lock(cache_index_lock);
    remote_path_to_cache.clear();
    inode_to_cache.clear();
    remote_path_to_cache.reserve(local_cache_files.size());
    inode_to_cache.reserve(local_cache_files.size());

    for (auto& cache_file : local_cache_files) {
        remote_path_to_cache[cache_file.remote_path] = &cache_file;
        inode_to_cache[cache_file.st.stx_ino] = &cache_file;
    }

    if (verbose) {
        cout << lock << "Scanned " << local_cache_files.size() << " local cache files under " << checkpath << endl << unlock;
    }
}

filekey* getpath(string path){
    if(path == "/" || path == "."){
        return new filekey{"/", 0};
    }
    filekey* fileat = getpath(dirname(path));
    if(fileat == nullptr){
        return fileat;
    }
    filekey file{basename(path), 0};
    if(HANDLE_EAGAIN(fm_getattrat(*fileat, file))){
        delete fileat;
        return nullptr;
    }
    delete fileat;
    return new filekey(file);
}


int main(int argc, char **argv) {
    if(fm_prepare()){
        cerr<<"fm_prepare failed!"<<endl;
        return -1;
    }

    char ch;
    bool isfile = false;
    int concurrent =  CHECKTHREADS;
    while ((ch = getopt(argc, argv, "evfrdsc:n")) != -1)
        switch (ch) {
        case 'e':
            cout<< "treat it as file"<<endl;
            isfile = true;
            break;
        case 'c':
            cout<< "concurrent: "<<optarg<<endl;
            concurrent = atoi(optarg);
            break;
        case 'v':
            cout << "verbose mode" << endl;
            verbose = true;
            break;
        case 'f':
            cout << "will try fix error" << endl;
            autofix = true;
            break;
        case 'n':
            cout << "no db operation" << endl;
            no_db = true;
            break;
        case 'r':
            cout << "will check recursive" <<endl;
            recursive = true;
            break;
        case 'd':
            cout<<"will delete all failed file"<<endl;
            deleteall = true;
            break;
        case 's':
            cout << "will sync dirty data before fsck" << endl;
            sync_dirty = true;
            break;
        case '?':
            return 1;
        }
    if(!no_db){
        sqlinit();
    }
    defer(sqldeinit);
    const char *checkpath;
    if (argv[optind]) {
        checkpath = argv[optind];
    } else {
        checkpath = "/";
    }
    cout << "will check path: " << checkpath << endl;

    // 处理dirty数据
    if (sync_dirty) {
        // 如果指定了同步dirty数据，执行同步后退出
        cout << "Syncing dirty data..." << endl;
        int sync_result = sync_all_dirty_files();
        if (sync_result != 0) {
            cerr << "Failed to sync dirty data" << endl;
            return -5;
        }
        cout << "Dirty data sync completed successfully" << endl;
        return 0;
    } else {
        // 检查是否有dirty数据，如果有则报错退出
        std::vector<std::string> dirty_files;
        if (get_dirty_files(dirty_files) > 0) {
            cerr << "ERROR: Found " << dirty_files.size() << " dirty files with uncommitted changes:" << endl;
            for (const auto& file_path : dirty_files) {
                cerr << "  " << decodepath(file_path, file_encode_suffix) << endl;
            }
            cerr << "Please sync dirty data first using '-s' option or mount the filesystem to commit changes." << endl;
            cerr << "Running fsck on dirty data may cause data loss!" << endl;
            return -6;
        }
    }

    // 扫描本地缓存文件
    scanLocalCacheFiles(checkpath);

    pool = new TrdPool(concurrent);
    filekey* file = nullptr;
    if(isfile){
        file = getpath(encodepath(checkpath, file_encode_suffix));
    }else{
        file = getpath(checkpath);
    }
    if(file == nullptr){
        cerr<<"dir/file "<<checkpath<<" not found."<<endl;
        return -2;
    }
    if(opt.flags & FM_RENAME_NOTSUPPRTED){
        cout<<"rename notsupported set, listing objs"<<endl;
        filekey obj = {".objs/", 0};
        if(fm_getattrat(filekey{"/"}, obj) < 0){
            cerr<<"get .objs failed"<<endl;
            return -3;
        }
        std::vector<filemeta> objslist;
        if(fm_list(obj, objslist) < 0){
            cerr<<"list .objs failed"<<endl;
            return -4;
        }
        for(auto i: objslist){
            objs.emplace(basename(i.key.path), i);
        }
        cout<<"objs list done, size: "<<objs.size()<<endl;
    }
    if(isfile){
        checkchunk(file);
    }else{
        checkdir(file);
    }
    pool->wait_all();
    if(objs.size() && strcmp(checkpath, "/") == 0 && recursive){
        //only check objs for full check
        std::vector<filekey> ftrim;
        cerr<<"objs not used: "<<endl;
        for(auto [path, meta]: objs){
            if(meta.flags & META_KEY_CHECKED_F) continue;
            char timestr[64];
            strftime(timestr, sizeof(timestr), "%Y-%m-%d %H:%M:%S", localtime(&meta.ctime));
            cerr<<path<<", create: "<<timestr<<", size: "<<meta.size<<endl;
            if(autofix){
                ftrim.emplace_back(meta.key);
            }
        }
        if(!ftrim.empty()){
            int ret = HANDLE_EAGAIN(fm_batchdelete(std::move(ftrim)));
            if (ret != 0) {
                cerr<< "delete lagecy block in: "<<file->path<<" failed"<<endl;
            }
        }
    }

    if(recursive) {
        // 检查孤儿文件和block条目
        checkOrphanedFiles(checkpath);
    }
    delete pool;
    if(opt.clean) {
        opt.clean();
    }
    return 0;
}
