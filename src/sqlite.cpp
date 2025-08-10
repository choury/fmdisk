#include "common.h"
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
    string cachefile = pathjoin(opt.cache_dir, "cache.db");
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
                "meta text,"
                "dirty integer DEFAULT 0)", nullptr, nullptr, &err_msg))
            {
                fprintf(stderr, "create table files failed: %s\n", err_msg);
                sqlite3_free(err_msg);
                failed = true;
                break;
            }
            if(sqlite3_exec(cachedb,
                "CREATE TABLE IF NOT EXISTS blocks("
                "inode integer,"
                "block_no integer,"
                "private_key text,"
                "dirty integer DEFAULT 0,"
                "primary key (inode, block_no))", nullptr, nullptr, &err_msg))
            {
                fprintf(stderr, "create table blocks failed: %s\n", err_msg);
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
        //set journal_mode=wal
        char *err_msg;
        if(sqlite3_exec(cachedb, "PRAGMA journal_mode=WAL", nullptr, nullptr, &err_msg)){
            fprintf(stderr, "set journal_mode failed: %s\n", err_msg);
            sqlite3_free(err_msg);
        }
        //set synchronous=normal
        if(sqlite3_exec(cachedb, "PRAGMA synchronous=NORMAL", nullptr, nullptr, &err_msg)){
            fprintf(stderr, "set synchronous failed: %s\n", err_msg);
            sqlite3_free(err_msg);
        }
        //set cache_size 128M
        if(sqlite3_exec(cachedb, "PRAGMA cache_size=-131072", nullptr, nullptr, &err_msg)){
            fprintf(stderr, "set cache_size failed: %s\n", err_msg);
            sqlite3_free(err_msg);
        }
        //set temp_store=memory
        if(sqlite3_exec(cachedb, "PRAGMA temp_store=MEMORY", nullptr, nullptr, &err_msg)){
            fprintf(stderr, "set temp_store failed: %s\n", err_msg);
            sqlite3_free(err_msg);
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

static string escapQuote(const string& s){
    return replaceAll(s, "'", "''");
}

static void save_file_to_db(const string& path, const filekey& metakey, const char* json, uint32_t flags){
    if(cachedb ==  nullptr){
        return;
    }
    string sql = "replace into files (path, private_key, meta, dirty) values('"
     + escapQuote(path) + "', '" + fm_private_key_tostring(metakey.private_key) + "', '"+ escapQuote(json) + "', " + ((flags & FILE_DIRTY_F) ? '1' : '0') + ")";
    char* err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
    }
}

void save_file_to_db(const string& path, const filemeta& meta, const std::vector<filekey>& fblocks){
    json_object* jobj = marshal_meta(meta, fblocks);
    save_file_to_db(path, meta.key, json_object_to_json_string(jobj), meta.flags);
    json_object_put(jobj);
}

static int files_callback(void *data, int columns, char **field, char **colum){
    auto param = (std::pair<reference_wrapper<filemeta>, reference_wrapper<std::vector<filekey>>>*)data;
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
        if (strcmp(colum[i], "dirty") == 0) {
            if (field[i] && strcmp(field[i], "1") == 0) {
                param->first.get().flags |= FILE_DIRTY_F;
            } else {
                param->first.get().flags &= ~FILE_DIRTY_F;
            }
            continue;
        }
    }
    return 0;
}

void load_file_from_db(const std::string& path, filemeta& meta, std::vector<filekey>& fblocks){
    if(cachedb == nullptr){
        return;
    }
    string sql = "select private_key, meta, dirty from files where path='" + escapQuote(path) + "'";
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
    string sql = "delete from files where path='" + escapQuote(path) + "'";
    char* err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    return 0;
}


void save_entry_to_db(const string& path, const filemeta& meta){
    if(cachedb == nullptr){
        return;
    }
    string sql = "replace into entrys(parent, path, private_key, mode) values('" + escapQuote(path)
    + "', '" + escapQuote(basename(meta.key.path)) + "', '" + fm_private_key_tostring(meta.key.private_key)
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
    string sql = "select path, private_key, mode from entrys where parent = '" + escapQuote(path) + "'";
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
    string sql = "delete from entrys where parent = '" + escapQuote(path) + "' or parent like '" + escapSQL(path) + "/%' escape '\\'";
    char *err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    sql = "delete from files where path  = '"+ escapQuote(path) + "' or path like '" + escapSQL(path) + "/%' escape '\\'";
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
    string sql = "delete from entrys where parent = '" + escapQuote(dirname(path)) + "' and path= '" + escapQuote(basename(path)) + "'";
    char *err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    return 0;
}

void save_block_to_db(ino_t inode, size_t block_no, std::shared_ptr<void> file_private_key, bool dirty){
    if(cachedb == nullptr || inode == 0){
        return;
    }
    string key_str = fm_private_key_tostring(file_private_key);
    string sql = "replace into blocks (inode, block_no, private_key, dirty) values("
        + std::to_string(inode) + ", " + std::to_string(block_no) + ", '" + escapQuote(key_str) + "', " + (dirty ? '1' : '0') + ")";
    char* err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
    }
}


bool load_block_from_db(ino_t inode, size_t block_no, struct block_record& record) {
    if(cachedb == nullptr || inode == 0){
        return false;
    }
    string sql = "select inode, block_no, private_key, dirty from blocks where inode = " + std::to_string(inode)
                 + " and block_no = " + std::to_string(block_no);
    //use sqlite3_prepare_v2 to prepare the SQL statement
    sqlite3_stmt* stmt;
    if(sqlite3_prepare_v2(cachedb, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), sqlite3_errmsg(cachedb));
        return false;
    }
    int ret = sqlite3_step(stmt);
    if(ret == SQLITE_ROW) {
        record.inode = sqlite3_column_int64(stmt, 0);
        record.block_no = sqlite3_column_int64(stmt, 1);
        record.private_key = (const char*)sqlite3_column_text(stmt, 2);
        record.dirty = (sqlite3_column_int(stmt, 3) == 1);
        sqlite3_finalize(stmt);
        return true;
    }

    if(ret == SQLITE_DONE) {
        sqlite3_finalize(stmt);
        return false; // No record found
    }

    fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), sqlite3_errmsg(cachedb));
    sqlite3_finalize(stmt);
    return false; // Error occurred
}

int delete_blocks_from_db(ino_t inode) {
    if(cachedb == nullptr){
        return 0;
    }
    assert(inode != 0);
    string sql = "delete from blocks where inode = " + std::to_string(inode);
    char* err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    return 0;
}

int delete_blocks_by_key(const std::vector<filekey>& filekeys){
    if(cachedb == nullptr || filekeys.empty()){
        return 0;
    }

    // 构建批量删除的SQL语句
    string sql = "delete from blocks where private_key in (";
    bool first = true;
    for(const auto& fkey : filekeys) {
        if(!first) sql += ", ";
        string key_str = fm_private_key_tostring(fkey.private_key);
        sql += "'" + escapQuote(key_str) + "'";
        first = false;
    }
    sql += ")";
    if(first) return 0; // 没有有效的private_key

    char* err_msg;
    if(sqlite3_exec(cachedb, sql.c_str(), nullptr, nullptr, &err_msg)){
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    return 0;
}

int get_blocks_for_inode(ino_t inode, std::vector<block_record>& blocks) {
    if(cachedb == nullptr) {
        return -1;
    }

    string sql = "SELECT inode, block_no, private_key, dirty FROM blocks WHERE inode = " + std::to_string(inode);
    char* err_msg;

    auto blocks_callback = [](void *data, int columns, char **field, char **colum) -> int {
        std::vector<block_record>* blocks = (std::vector<block_record>*)data;
        if (field[0] && field[1] && field[2] && field[3]) {
            block_record record;
            record.inode = std::stoll(field[0]);
            record.block_no = std::stoull(field[1]);
            record.private_key = field[2];
            record.dirty = (std::stoi(field[3]) == 1);
            blocks->push_back(record);
        }
        return 0;
    };

    if(sqlite3_exec(cachedb, sql.c_str(), blocks_callback, &blocks, &err_msg)) {
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    return blocks.size();
}

int get_all_block_inodes(std::vector<ino_t>& inodes) {
    if(cachedb == nullptr) {
        return -1;
    }

    string sql = "SELECT DISTINCT inode FROM blocks WHERE inode > 0";
    char* err_msg;

    auto inodes_callback = [](void *data, int columns, char **field, char **colum) -> int {
        std::vector<ino_t>* inodes = (std::vector<ino_t>*)data;
        if (field[0]) {
            inodes->push_back(std::stoll(field[0]));
        }
        return 0;
    };

    if(sqlite3_exec(cachedb, sql.c_str(), inodes_callback, &inodes, &err_msg)) {
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    return inodes.size();
}

// 获取所有dirty的文件路径
int get_dirty_files(std::vector<std::string>& dirty_files) {
    if(cachedb == nullptr) {
        return -1;
    }

    string sql = "SELECT path FROM files WHERE dirty = 1";
    char* err_msg;

    auto files_callback = [](void *data, int columns, char **field, char **colum) -> int {
        std::vector<std::string>* files = (std::vector<std::string>*)data;
        if (field[0]) {
            files->push_back(field[0]);
        }
        return 0;
    };

    if(sqlite3_exec(cachedb, sql.c_str(), files_callback, &dirty_files, &err_msg)) {
        fprintf(stderr, "SQL [%s]: %s\n", sql.c_str(), err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    return dirty_files.size();
}
