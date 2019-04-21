#include "sqlite.h"
#include "fmdisk.h"

#include <assert.h>
#include <string.h>
#include <errno.h>
#include <sqlite3.h>
#include <json.h>
#include <functional>

static sqlite3* cachedb = nullptr;

using std::string;
using std::make_pair;
using std::reference_wrapper;

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
                "private_key text,"
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

void sqldeinit(){
    if(cachedb){
        sqlite3_close(cachedb);
    }
}

static void save_file_to_db(const string& path, const filekey& metakey, const char* json){
    if(cachedb ==  nullptr){
        return;
    }
    string sql = "replace into files (path, private_key, meta) values('"
     + path + "', '" + fm_private_key_tostring(metakey.private_key) + "', '"+ json + "')";
    char* err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
    }
}

void save_file_to_db(const string& path, const filemeta& meta, const std::vector<filekey>& fblocks){
    json_object* jobj = marshal_meta(meta, fblocks);
    save_file_to_db(path, meta.key, json_object_to_json_string(jobj));
    json_object_put(jobj);
}

static int files_callback(void *data, int columns, char **field, char **colum){
    auto param = (std::pair<reference_wrapper<filemeta>, reference_wrapper<std::vector<filekey>>>*)data;
    assert(columns = 2);
    for(int i = 0; i < columns; i++){
        if(strcmp(colum[i], "private_key") == 0){
            param->first.get().key.private_key = fm_get_private_key(field[i]);
            continue;
        }
        if(strcmp(colum[i], "meta") == 0){
            json_object *json_get = json_tokener_parse(field[i]);
            if(json_get ==  nullptr){
                throw "Json parse error";
            }
            unmarshal_meta(json_get, param->first, param->second);
            json_object_put(json_get);
            continue;
        }
    }
    return 0;
}

void load_file_from_db(const std::string& path, filemeta& meta, std::vector<filekey>& fblocks){
    if(cachedb == nullptr){
        return;
    }
    string sql = "select private_key, meta from files where path='" + path + "'";
    char* err_msg;
    std::pair<reference_wrapper<filemeta>, reference_wrapper<std::vector<filekey>>> data = 
        make_pair<reference_wrapper<filemeta>, reference_wrapper<std::vector<filekey>>>(meta, fblocks);
    if(sqlite3_exec(cachedb, sql.c_str(), files_callback, &data, &err_msg)){
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
    }
}

int delete_file_from_db(const string& path){
    if(cachedb == nullptr){
        return -1;
    }
    string sql = "delete from files where path='" + path + "'";
    char* err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    return 0;
}


void save_entry_to_db(const filekey& fileat, const filemeta& meta){
    if(cachedb == nullptr){
        return;
    }
    string sql = "replace into entrys(parent, path, private_key, mode) values('" + fileat.path 
    + "', '" + basename(meta.key.path) + "', '" + fm_private_key_tostring(meta.key.private_key)
    + "', " + std::to_string(meta.mode) + ")";
    char* err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
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
    if(cachedb == nullptr){
        return 0;
    }
    string sql = "select path, private_key, mode from entrys where parent = '" + path + "'";
    char *err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), entrys_callback, &flist, &err_msg)){
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
        flist.clear();
    }
    return flist.size();
}

static string escapSQL(const string& sql){
    string s = replaceAll(sql, "\\", "\\\\");
    s = replaceAll(s, "%", "\\%");
    s = replaceAll(s, "_", "\\_");
    s = replaceAll(s, "[", "\\[");
    return replaceAll(s, "]", "\\]");
}

int delete_entry_prefix_from_db(const string& path){
    if(cachedb == nullptr){
        return -1;
    }
    string sql = "delete from entrys where parent = '" + path + "' or parent like '" + escapSQL(path) + "/%' escape '\\'";
    char *err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    sql = "delete from files where path  = '"+ path + "' or path like '" + escapSQL(path) + "/%' escape '\\'";
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    return 0;
}

int delete_entry_from_db(const string& path){
    if(cachedb == nullptr){
        return -1;
    }
    string sql = "delete from entrys where parent = '" + dirname(path) + "' and path= '" + basename(path) + "'";
    char *err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    return 0;
}
