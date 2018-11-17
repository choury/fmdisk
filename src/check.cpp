#include "fmdisk.h"
#include "defer.h"
#include "threadpool.h"
#include <unistd.h>
#include <iostream>
#include <vector>
#include <map>
#include <limits>
#include <mutex>
#include <string>
#include <json-c/json.h>
#include <string.h>

using namespace std;

static bool verbose = false;
static bool autofix = false;
static bool recursive = false;
static thrdpool *pool;

static mutex console_lock;

std::ostream& lock(std::ostream& os) {
    console_lock.lock();
    return os;
}

std::ostream& unlock(std::ostream& os) {
    console_lock.unlock();
    return os;
}

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
    do {
        fflush(stdin);
        cerr << "delete this file or ignore it([D]elete/[I]gnore) I?";
        char a = getchar();
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
    }
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
        return filekey{"x"};
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
    return (uint64_t)stoi(block) == no;
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
    ret = downlod_meta(*file, meta, blks);
    defer1([&meta]{delete meta.inline_data;});
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
        bool haswrong = false;
        if (!blockMatchNo(blks[i].path, i)) {
            cerr<<lock<<"file: "<<decodepath(file->path)<<" has block "<<blks[i].path<<" on No."<<i<<endl<<unlock;
            haswrong = true;
        }
        if (blks[i].path != "x" && fs.count(blks[i].path) == 0) {
            cerr<<lock<<"file: "<<decodepath(file->path)<<" miss block: "<<blks[i].path<<endl<<unlock;
            haswrong = true;
        }
        if (haswrong && autofix) {
            fixed = true;
            blks[i] = fixMissBlock(*file, fs, i);
        }
    }
    if(fixed){
        ret = upload_meta(*file, meta, blks);
        if (ret != 0) {
            cerr<<lock<< "upload meta "<<meta.key.path<<" failed: "<<ret<<endl<<unlock;
            return;
        }
    }
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
        int ret = HANDLE_EAGAIN(fm_batchdelete(ftrim));
        if (ret != 0) {
            cerr<<lock<< "delete lagecy block in: "<<file->path<<" failed"<<endl<<unlock;
        }
    }
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
    for (auto f : flist) {
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
        return new filekey{file};
    }
}

int main(int argc, char **argv) {
    if(fm_prepare()){
        cerr<<"fm_prepare failed!"<<endl;
        return -1;
    }
    char ch;
    bool isfile = false;
    while ((ch = getopt(argc, argv, "evfr")) != -1)
        switch (ch) {
        case 'e':
            cout<< "treat it as file"<<endl;
            isfile = true;
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
        case '?':
            return 1;
        }
    const char *path;
    if (argv[optind]) {
        path = argv[optind];
    } else {
        path = "/";
    }
    cout << "will check path: " << path << endl;
    pool = creatpool(CHECKTHREADS);
    filekey* file = nullptr;
    if(isfile){
        file = getpath(encodepath(path));
    }else{
        file = getpath(path);
    }
    if(file == nullptr){
        cerr<<"dir/file "<<path<<" not found."<<endl;
        return -2;
    }
    if(isfile){
        checkchunk(file);
    }else{
        checkdir(file);
    }
    void* result;
    waittask(pool, 0, &result);
    return 0;
}
