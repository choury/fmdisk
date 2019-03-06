#ifndef SQLITE_H__
#define SQLITE_H__
#include "utils.h"

int sqlinit();
void sqldeinit();

void save_file_to_db(const std::string& path, const filemeta& meta, const std::vector<filekey>& fblocks);
void load_file_from_db(const std::string& path, filemeta& meta, std::vector<filekey>& fblocks);
int delete_file_from_db(const std::string& path);

void save_entry_to_db(const filekey& fileat, const filemeta& meta);
int load_entry_from_db(const std::string& path, std::vector<filemeta>& flist);
int delete_entry_from_db(const std::string& path);
int delete_entry_prefix_from_db(const std::string& path);
#endif
