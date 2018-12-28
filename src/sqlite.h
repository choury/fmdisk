#ifndef SQLITE_H__
#define SQLITE_H__
#include "utils.h"

int sqlinit();
void save_file_to_db(const std::string& path, const char* json);
void save_file_to_db(const std::string& path, const filemeta& meta);
int load_file_from_db(const std::string& path, buffstruct& bs);
void save_entry_to_db(const filekey& fileat, const filemeta& meta);
int load_entry_from_db(const std::string& path, std::vector<filemeta>& flist);
int delete_entry_from_db(const std::string& path);
#endif