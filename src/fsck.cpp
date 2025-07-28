#include "common.h"
#include "fmdisk.h"
#include "defer.h"
#include "threadpool.h"
#include <unistd.h>
#include <iostream>
#include <vector>
#include <map>
#include <unordered_map>
#include <limits>
#include <mutex>
#include <string>
#include <json.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <functional>
#include "sqlite.h"

using namespace std;

struct fmoption opt;
static bool verbose = false;
static bool autofix = false;
static bool recursive = false;
static bool deleteall = false;
static thrdpool *pool;
static std::unordered_map<std::string, filemeta> objs;

// 全局变量存储本地缓存文件信息

struct local_cache_file {
    string path;           // 本地缓存文件路径
    string remote_path;    // 对应的远程路径
    ino_t inode;          // 文件inode
    size_t size;          // 文件大小
    time_t mtime;         // 修改时间
    bool checked;         // 是否已被远程文件检查过

    local_cache_file(const string& p, const string& rp, ino_t i, size_t s, time_t m)
        : path(p), remote_path(rp), inode(i), size(s), mtime(m), checked(false) {}
};

static std::vector<local_cache_file> local_cache_files;
static std::map<string, local_cache_file*> remote_path_to_cache; // 远程路径到缓存文件的映射
static std::map<ino_t, local_cache_file*> inode_to_cache;        // inode到缓存文件的映射

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
void checkcache(const filekey& file, const filemeta& meta, const std::vector<filekey>& blks);

void fixNoMeta(const filekey& file, const std::map<std::string, struct filekey>& flist) {
    cerr << lock;
    defer([]{cerr<<unlock;});
    if (flist.empty()) {
        cerr << "there is no blocks of file: " << decodepath(file.path) << ", so delete it" << endl;
        goto del;
    }
    cerr << decodepath(file.path)<<" has blocks:" << endl;
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

filekey fixMissBlock(const filekey& file, const std::map<std::string, struct filekey>& flist, uint64_t no) {
    cerr << lock;
    defer([]{cerr<<unlock;});
    std::vector<filekey> fit;
    string No = to_string(no);
    for (auto i : flist) {
        if (i.first == No || startwith(i.first, No + '_')) {
            fit.push_back(filekey{basename(i.second.path), i.second.private_key});
        }
    }
    filemeta meta{file};
    if (fit.empty()) {
        cout<<decodepath(file.path) << " has no block fit for " << No << ", should reset it to 'x'"<<endl;
        return {{"x"}, 0};
    }
    size_t n = 0;
pick:
    cerr << decodepath(file.path) <<" has some block fit for " << No << ", please pick one:" << endl;
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
    if (fs.count(METANAME) == 0) {
        cerr<<lock<< "file: "<<decodepath(file->path)<<" have no meta.json"<<endl<<unlock;
        if (autofix) {
            fixNoMeta(*file, fs);
        }
        return;
    }
    filemeta meta;
    std::vector<filekey> blks;
    ret = download_meta(*file, meta, blks);
    defer([&meta]{delete[] meta.inline_data;});
    if(ret == 0){
        if(meta.inline_data && blks.size()){
            cerr<<lock<<"get inline_data and blocks/block_list: "<< meta.key.path <<endl <<unlock;
            ret = 4;
        }

        if(meta.size && !meta.inline_data && blks.empty()){
            cerr<<lock<<"get none of inline_data/blocks/block_list: "<< meta.key.path <<endl <<unlock;
            ret = 6;
        }
    }
    if (ret != 0) {
        cerr<<lock<<"file: "<<decodepath(file->path)<<" have malformed meta.json"<<endl<<unlock;
        if (autofix) {
            fixNoMeta(*file, fs);
        }
        return;
    }
    fs.erase(METANAME);
    bool fixed = false;
    for(size_t i = 0; i < blks.size(); i++) {
        if(blks[i].path == "x"){
            continue;
        }
        bool haswrong = false;
        if (!blockMatchNo(blks[i].path, i)) {
            cerr<<lock<<"file: "<<decodepath(file->path)<<" has block "<<blks[i].path<<" on No."<<i<<endl<<unlock;
            haswrong = true;
        }
        if(opt.flags & FM_RENAME_NOTSUPPRTED) {
            if(objs.count(blks[i].path) == 0) {
                haswrong = true;
            }else{
                objs[blks[i].path].flags |= META_KEY_CHECKED_F;
            }
        } else if (fs.count(blks[i].path) == 0) {
            haswrong = true;
        }
        if (haswrong) {
            cerr<<lock<<"file: "<<decodepath(file->path)<<" miss block: "<<blks[i].path<<endl<<unlock;
            if (autofix) {
                fixed = true;
                blks[i] = fixMissBlock(*file, fs, i);
            }
        }
    }
    if(fixed){
        ret = upload_meta(*file, meta, blks);
        if (ret != 0) {
            cerr<<lock<< "upload meta "<<meta.key.path<<" failed: "<<ret<<endl<<unlock;
            return;
        }
    }
    save_file_to_db(file->path, meta, blks);
    std::vector<filekey> ftrim;
    for (auto f : fs) {
        if (!isdigit(f.first[0])){
            cerr<<lock<<"file: "<<decodepath(file->path)<<" has unwanted block: "<<f.first<<endl<<unlock;
            if(autofix){
                ftrim.push_back(f.second);
            }
            continue;
        }
        int i = stoi(f.first);
        if(i < 0 || blks.size() <= (size_t)i){
            cerr<<lock<<"file: "<<decodepath(file->path)<<" has unwanted block: "<<f.first<<endl<<unlock;
            if(autofix){
                ftrim.push_back(f.second);
            }
            continue;
        }
        if (blks[i].path != f.first) {
            cerr<<lock<<"file: "<<decodepath(file->path)<<" has lagecy block: "<<pathjoin(f.first, blks[i].path)<<endl<<unlock;
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
    checkcache(*file, meta, blks);
    if (verbose) {
        cout <<lock<< decodepath(file->path) << " check finish" << endl<<unlock;
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
    delete_entry_prefix_from_db(file->path);
    for (auto f : flist) {
        if(isRoot && (opt.flags & FM_RENAME_NOTSUPPRTED) && (f.key.path == ".objs" || f.key.path == ".objs/")){
            continue;
        }
        std::string path;
        if(f.key.path[0] != '/') {
            path = "/";
            path += f.key.path;
        } else{
            path = f.key.path;
        }
        if(path[path.length() - 1] == '/') {
            path.resize(path.size() - 1);
        }
        f.key.path = path;
        save_entry_to_db(*file, f);
        if (S_ISREG(f.mode)) {
            if (verbose) {
                cout<<lock << f.key.path << " skip check" << endl<<unlock;
            }
            continue;
        }

        if(endwith(f.key.path, ".def")){
            addtask(pool, (taskfunc)checkchunk, new filekey(f.key), 0);
        }else if(recursive){
            addtask(pool, (taskfunc)checkdir, new filekey(f.key), 0);
        }
    }
}

// 删除不一致的缓存文件和相关block条目
void fixCacheInconsistency(local_cache_file* cache_file) {
    if (verbose) {
        cout << lock << "Deleted inconsistent cache file: " << cache_file->path << endl << unlock;
    }

    // 删除本地缓存文件
    if (unlink(cache_file->path.c_str()) != 0) {
        cerr << lock << "Failed to delete cache file: " << cache_file->path
             << " error: " << strerror(errno) << endl << unlock;
    }

    delete_blocks_from_db(cache_file->inode);
    inode_to_cache.erase(cache_file->inode);
}

// 检查本地缓存文件与远程meta信息的一致性
void checkcache(const filekey& file, const filemeta& meta, const std::vector<filekey>& blks) {
    string remote_path = file.path;

    // 查找对应的本地缓存文件
    auto it = remote_path_to_cache.find(remote_path);
    if (it == remote_path_to_cache.end()) {
        return;
    }

    local_cache_file* cache_file = it->second;
    cache_file->checked = true;  // 标记为已检查

    // 1. 验证文件大小是否一致
    if (cache_file->size != meta.size) {
        cerr << lock << "Cache file size mismatch: [" << remote_path
             << "] local=" << cache_file->size << " remote=" << meta.size << endl << unlock;
        if (autofix) {
            fixCacheInconsistency(cache_file);
            return;
        }
    }

    // 2. 验证数据库blocks表中该文件相关记录的private_key
    std::vector<block_record> db_blocks;
    if (get_blocks_for_inode(cache_file->inode, db_blocks) <= 0) {
        cerr << lock << "blocks for: [" << remote_path << "] not found"<< endl << unlock;
        if(autofix) {
            fixCacheInconsistency(cache_file);
            return;
        }
    }
    // 为每个数据库中的block找到对应的远程block并比较private_key
    for (const auto& db_block : db_blocks) {
        if (db_block.block_no >= blks.size()) {
            cerr << lock << "Block table has invalid block_no: [" << remote_path
                    << "] block_no=" << db_block.block_no << " max_blocks=" << blks.size() << endl << unlock;
            if (autofix) {
                fixCacheInconsistency(cache_file);
                return;
            }
        }
        string expected_key = fm_private_key_tostring(blks[db_block.block_no].private_key);
        if (db_block.private_key != expected_key) {
            cerr << lock << "Block table private_key mismatch: [" << remote_path
                    << "] block_no=" << db_block.block_no << " db_key=" << db_block.private_key
                    << " expected=" << expected_key << endl << unlock;

            if (autofix) {
                fixCacheInconsistency(cache_file);
                return;  // 修复后直接返回
            }
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
             << cache_file.inode << ", size: " << cache_file.size << endl << unlock;

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
    string cache_dir = pathjoin(opt.cache_dir, "cache");
    string target_cache_dir;

    // 如果检查根路径，扫描整个缓存目录；否则只扫描对应的子目录
    if (strcmp(checkpath, "/") == 0) {
        target_cache_dir = cache_dir;
    } else {
        target_cache_dir = pathjoin(cache_dir, checkpath);
    }

    std::function<void(const string&)> scan_dir = [&](const string& dir) {
        DIR* d = opendir(dir.c_str());
        if (!d) return;

        defer([d]() { closedir(d); });

        struct dirent* entry;
        while ((entry = readdir(d)) != nullptr) {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
                continue;
            }

            string full_path = pathjoin(dir, entry->d_name);
            struct stat st;
            if (stat(full_path.c_str(), &st) < 0) {
                cerr << lock << "Failed to stat file: " << full_path
                     << " error: " << strerror(errno) << endl << unlock;
                abort();
            }
            if (S_ISDIR(st.st_mode)) {
                scan_dir(full_path);
                continue;
            }
            if (S_ISREG(st.st_mode)) {
                // 计算对应的远程路径：去掉cache_dir/cache前缀
                string remote_path;
                string cache_prefix = pathjoin(opt.cache_dir, "cache");
                if (full_path.find(cache_prefix) == 0) {
                    remote_path = full_path.substr(cache_prefix.length());
                    if (remote_path.empty()) {
                        remote_path = "/";
                    }
                } else {
                    // 不应该发生，但为了安全起见
                    remote_path = full_path;
                }

                // 添加到全局列表
                local_cache_files.emplace_back(full_path, remote_path, st.st_ino, st.st_size, st.st_mtime);

                // 建立映射关系
                local_cache_file* file_ptr = &local_cache_files.back();
                remote_path_to_cache[remote_path] = file_ptr;
                inode_to_cache[st.st_ino] = file_ptr;
            }
        }
    };

    scan_dir(target_cache_dir);

    if (verbose) {
        cout << lock << "Scanned " << local_cache_files.size() << " local cache files under " << checkpath << endl << unlock;
    }
}

filekey* getpath(string path){
    if(path == "/" || path == "."){
        return new filekey{"/", 0};
    }else{
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
}


int main(int argc, char **argv) {
    if(fm_prepare()){
        cerr<<"fm_prepare failed!"<<endl;
        return -1;
    }
    sqlinit();
    defer(sqldeinit);

    char ch;
    bool isfile = false;
    int concurrent =  CHECKTHREADS;
    while ((ch = getopt(argc, argv, "evfrdc:")) != -1)
        switch (ch) {
        case 'e':
            cout<< "treat it as file"<<endl;
            isfile = true;
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
        case 'r':
            cout << "will check recursive" <<endl;
            recursive = true;
            break;
        case 'd':
            cout<<"will delete all failed file"<<endl;
            deleteall = true;
            break;
        case '?':
            return 1;
        }
    const char *checkpath;
    if (argv[optind]) {
        checkpath = argv[optind];
    } else {
        checkpath = "/";
    }
    cout << "will check path: " << checkpath << endl;

    // 扫描本地缓存文件
    scanLocalCacheFiles(checkpath);

    pool = creatpool(concurrent);
    filekey* file = nullptr;
    if(isfile){
        file = getpath(encodepath(checkpath));
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
    void* result;
    waittask(pool, 0, &result);
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
    return 0;
}
