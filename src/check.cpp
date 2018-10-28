#include "fmdisk.h"
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

static mutex console_lock;

std::ostream& lock(std::ostream& os) {
    console_lock.lock();
    return os;
}

std::ostream& unlock(std::ostream& os) {
    console_lock.unlock();
    return os;
}

int readblklist(const filekey& metafile, std::vector<filekey>& fblocks) {
    buffstruct bs;
    int ret = HANDLE_EAGAIN(fm_download(metafile, 0, 0, bs));
    if (ret != 0) {
        cerr<<lock<<"read meta file "<< metafile.path<<" failed: "<<ret<<endl<<unlock;
        return ret;
    }
    json_object *json_get = json_tokener_parse(bs.buf);
    if (json_get == NULL) {
        cerr<<lock<<"json_tokener_parse filed: " << bs.buf << endl <<unlock;
        return ret;
    }

    bool inline_data_found = false;
    bool blocks_found = false;
    bool block_list_found = false;
    json_object *jsize;
    if(json_object_object_get_ex(json_get, "size", &jsize) == 0){
        cerr<<lock<<"no size found in meta.json: "<< bs.buf <<endl <<unlock;
        ret = 2;
        goto json_err;
    }

    json_object *jblksize;
    if(json_object_object_get_ex(json_get, "blksize", &jblksize) == 0){
        cerr<<lock<<"no blksize found in meta.json: "<< bs.buf <<endl <<unlock;
        ret = 2;
        goto json_err;
    }

    json_object *jinline_data;
    if(json_object_object_get_ex(json_get, "inline_data", &jinline_data)){
        inline_data_found = true;
    }

    json_object* jblocks;
    if(json_object_object_get_ex(json_get, "blocks", &jblocks)){
        blocks_found = true;
        fblocks.reserve(json_object_array_length(jblocks));
        for(int i=0; i < json_object_array_length(jblocks); i++){
            json_object *jblock = json_object_array_get_idx(jblocks, i);
            json_object *jname;
            if(json_object_object_get_ex(jblock, "name", &jname) == 0){
                cerr<<lock<<"no name found in blocks: "<< bs.buf <<endl <<unlock;
                ret = 3;
                goto json_err;
            }
            json_object *jkey;
            if(json_object_object_get_ex(jblock, "key", &jkey) == 0){
                cerr<<lock<<"no key found in blocks: "<< bs.buf <<endl <<unlock;
                ret = 3;
                goto json_err;
            }
            const char* name = json_object_get_string(jname);
            const char* private_key = json_object_get_string(jkey);
            fblocks.push_back(filekey{name, fm_get_private_key(private_key)});
        }
    }

    json_object *jblock_list;
    if(json_object_object_get_ex(json_get, "block_list", &jblock_list)){
        block_list_found = true;
        fblocks.reserve(json_object_array_length(jblock_list));
        for(int i=0; i < json_object_array_length(jblock_list); i++){
            json_object *block = json_object_array_get_idx(jblock_list, i);
            const char* name = json_object_get_string(block);
            fblocks.push_back(filekey{name, 0});
        }
    }

    if(inline_data_found & (blocks_found || block_list_found)){
        cerr<<lock<<"get inline_data and blocks/block_list: "<< bs.buf <<endl <<unlock;
        ret = 4;
        goto json_err;
    }
    if(blocks_found && block_list_found){
        cerr<<lock<<"get blocks and block_list together: "<< bs.buf <<endl <<unlock;
        ret = 5;
        goto json_err;
    }

    if(json_object_get_int64(jsize) && !blocks_found && !block_list_found){
        cerr<<lock<<"get none of blocks and block_list: "<< bs.buf <<endl <<unlock;
        ret = 6;
        goto json_err;
    }

    json_object_put(json_get);
    return 0;
json_err:
    json_object_put(json_get);
    return ret;
}

void fixNoMeta(const filekey& file, const std::map<std::string, struct filekey>& flist) {
    if (flist.empty()) {
        cerr <<lock<< "there is no blocks of file: " << decodepath(file.path) << ", so delete it" << endl<<unlock;
        goto del;
        return;
    }
    cerr<<lock << decodepath(file.path)<<" has blocks:" << endl;
    for (auto f : flist) {
        cerr << f.first<<endl;
    }
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
        cerr<<unlock;
        if (a == 'I') {
            return;
        } else {
            goto del;
        }
    } while (true);
del:
    int ret = HANDLE_EAGAIN(fm_delete(file));
    if (ret != 0) {
        cerr<<lock << "delete dir " << file.path << "failed: " << ret << endl << unlock;
    }
}

void fixMissBlock(const filekey& file, const std::map<std::string, struct filekey>& flist, uint64_t no) {
    std::vector<string> fit;
    string No = to_string(no);
    for (auto i : flist) {
        if (i.first == No || startwith(i.first, No + '_')) {
            fit.push_back(i.first);
        }
    }
    if (fit.empty()) {
        cerr<<lock<< decodepath(file.path) << "has no block fit for " << No << ", should reset it to 'x' (not implement)" << endl<<unlock;
        return;
    }
    cerr <<lock<< decodepath(file.path) <<"has some block fit for " << No << ", please pick one:" << endl;
    size_t n = 0;
    for (auto i : fit) {
        cerr << n << ". " << i << endl;
    }
    getchar();
    cerr << "not implement now" << endl<<unlock;
}

bool blockMatchNo(string block, uint64_t no) {
    if (block == "x") {
        return true;
    }
    return (uint64_t)stoi(block) == no;
}

void checkchunk(filekey* file) {
    std::vector<filemeta> flist;
    int ret  = HANDLE_EAGAIN(fm_list(*file, flist));
    if (ret != 0) {
        delete file;
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
        delete file;
        return;
    }
    std::vector<filekey> blks;
    ret = readblklist(fs[METANAME], blks);
    if (ret != 0) {
        cerr<<lock<<"file: "<<decodepath(file->path)<<" have malformed meta.json"<<endl<<unlock;
        if (autofix) {
            fixNoMeta(*file, fs);
        }
        delete file;
        return;
    }
    fs.erase(METANAME);
    int no = 0;
    for (auto b : blks) {
        bool haswrong = false;
        if (!blockMatchNo(b.path, no)) {
            cerr<<lock<<"file: "<<decodepath(file->path)<<" has block "<<b.path<<" on No."<<no<<endl<<unlock;
            haswrong = true;
        }
        if (b.path != "x" && fs.count(b.path) == 0) {
            cerr<<lock<<"file: "<<decodepath(file->path)<<" miss block: "<<b.path<<endl<<unlock;
            haswrong = true;
        }
        if (haswrong && autofix) {
            fixMissBlock(*file, fs, no);
        }
        no++;
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
            cerr<<lock<<"file: "<<decodepath(file->path)<<" has lagecy block: "<<f.first<<"/"<<blks[i].path<<endl<<unlock;
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
    delete file;
    return;
}

void checkfile(filekey* file) {
    std::vector<filemeta> flist;
    int ret  = HANDLE_EAGAIN(fm_list(*file, flist));
    if (ret != 0) {
        cerr<<lock<< "list dir "<<file->path<<" failed: "<<ret<<endl<<unlock;
        delete file;
        return;
    }
    delete file;
    for (auto f : flist) {
        if (S_ISREG(f.mode)) {
            if (verbose) {
                cout<<lock << f.key.path << " skip check" << endl<<unlock;
            }
            continue;
        }

        if(endwith(f.key.path, ".def")){
            addtask((taskfunc)checkchunk, new filekey(f.key), 0, 0);
        }else if(recursive){
            addtask((taskfunc)checkfile, new filekey(f.key), 0, 0);
        }
    }
}

filekey getpath(string path){
    if(path == "/" || path == "."){
        return filekey{"/", 0};
    }else{
        filekey fileat = getpath(dirname(path));
        if(fileat.path == ""){
            return fileat;
        }
        filekey file{basename(path), 0};
        if(HANDLE_EAGAIN(fm_getattrat(fileat, file))){
            return filekey{"", 0};
        }
        return file;
    }
}

int main(int argc, char **argv) {
    if(fm_prepare()){
        cerr<<"fm_prepare failed!"<<endl;
        return -1;
    }
    char ch;
    while ((ch = getopt(argc, argv, "vfr")) != -1)
        switch (ch) {
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
    creatpool(CHECKTHREADS);
    filekey file = getpath(path);
    if(file.path == ""){
        cerr<<"dir "<<path<<" not found."<<endl;
        return -2;
    }
    checkfile(new filekey{file});
    waittask(0);
    return 0;
}
