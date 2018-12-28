#include "sqlite.h"
#include "fmdisk.h"

#include <assert.h>
#include <string.h>
#include <errno.h>
#include <sqlite3.h>
#include <json-c/json.h>

sqlite3* cachedb = nullptr;

using std::string;

int sqlinit(){
    string cachefile = pathjoin(fm_getcachepath(), "cache.db");
    struct stat st;
    if(stat(cachefile.c_str(), &st) == 0 || errno == ENOENT){
        bool failed = false;
        do{
            if(sqlite3_open(cachefile.c_str(), &cachedb)){
                fprintf(stderr, "sqlite3_open failed %s: %s\n", cachefile.c_str(), sqlite3_errmsg(cachedb));
                failed = true;
                break;
            }
            char *err_msg;
            if(sqlite3_exec(cachedb, 
                "CREATE TABLE IF NOT EXISTS entrys("
                "parent text,"
                "path text,"
                "private_key text,"
                "mode integer,"
                "primary key (parent, path))", nullptr, nullptr, &err_msg))
            {
                fprintf(stderr, "create table entrys failed: %s\n", err_msg);
                sqlite3_free(err_msg);
                failed = true;
                break;
            }
            if(sqlite3_exec(cachedb, 
                "CREATE TABLE IF NOT EXISTS files("
                "path text PRIMARY KEY,"
                "meta text)", nullptr, nullptr, &err_msg))
            {
                fprintf(stderr, "create table files failed: %s\n", err_msg);
                sqlite3_free(err_msg);
                failed = true;
                break;
            }
        }while(0);
        if(failed){
            sqlite3_close(cachedb);
            cachedb = nullptr;
            return -1;
        }
        return 0;
    }else{
        fprintf(stderr, "stat cache db failed %s: %s", cachefile.c_str(), strerror(errno));
        return -1;
    }
}

void save_file_to_db(const string& path, const char* json){
    string sql = "replace into files (path, meta) values('" + path + "', '" + json + "')";
    char* err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "insert file to db failed: %s\n", err_msg);
        sqlite3_free(err_msg);
    }
}

void save_file_to_db(const string& path, const filemeta& meta){
    json_object* jobj = marshal_meta(meta, std::vector<filekey>{});
    save_file_to_db(path, json_object_to_json_string(jobj));
    json_object_put(jobj);
}

static int files_callback(void *data, int columns, char **field, char **colum){
    buffstruct* bs = (buffstruct*)data;
    assert(columns = 1);
    size_t len = strlen(field[0]);
    bs->expand(len);
    strcpy(bs->buf, field[0]);
    bs->offset = len;
    return 0;
}

int load_file_from_db(const string& path, buffstruct& bs){
    string sql = "select meta from files where path='" + path + "'";
    char* err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), files_callback, &bs, &err_msg)){
        fprintf(stderr, "select file from db failed: %s\n", err_msg);
        sqlite3_free(err_msg);
    }
    return bs.offset;
}

int delete_file_from_db(const string& path){
    string sql = "delete from files where path='" + path + "'";
    char* err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "delete file from db failed: %s\n", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    return 0;
}


void save_entry_to_db(const filekey& fileat, const filemeta& meta){
    string sql = "replace into entrys(parent, path, private_key, mode) values('" + fileat.path 
    + "', '" + basename(meta.key.path) + "', '" + fm_private_key_tostring(meta.key.private_key)
    + "', " + std::to_string(meta.mode) + ")";
    char* err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "insert entry to db failed: %s\n", err_msg);
        sqlite3_free(err_msg);
    }
    if(endwith(meta.key.path, ".def") && S_ISDIR(meta.mode)){
        return;
    }
    if((meta.flags & META_KEY_ONLY_F) == 0){
        save_file_to_db(pathjoin(fileat.path, basename(meta.key.path)), meta);
    }
}

static int entrys_callback(void *data, int columns, char **field, char **colum){
    std::vector<filemeta>* flist = (std::vector<filemeta>*)data;
    filemeta meta;
    for(int i = 0; i < columns; i++){
        if(strcmp(colum[i], "path") == 0){
            meta.key.path = field[i];
            continue;
        }
        if(strcmp(colum[i], "private_key") == 0){
            meta.key.private_key = fm_get_private_key(field[i]);
            continue;
        }
        if(strcmp(colum[i], "mode") == 0){
            meta.mode = std::stoi(field[i]);
            continue;
        }
    }
    meta.flags = META_KEY_ONLY_F;
    flist->emplace_back(meta);
    return 0;
}

int load_entry_from_db(const string& path, std::vector<filemeta>& flist){
    string sql = "select path, private_key, mode from entrys where parent = '" + path + "'";
    char *err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), entrys_callback, &flist, &err_msg)){
        fprintf(stderr, "select entry from db failed: %s\n", err_msg);
        sqlite3_free(err_msg);
        flist.clear();
    }
    return flist.size();
}

int delete_entry_from_db(const string& path){
    string sql = "delete from entrys where parent = '" + dirname(path) + "' and path= '" + basename(path) + "'";
    char *err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "delete entry from db failed: %s\n", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    return delete_file_from_db(path);
}