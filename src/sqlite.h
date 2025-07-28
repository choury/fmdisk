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

void save_block_sync_status_to_db(ino_t inode, size_t block_no, std::shared_ptr<void> file_private_key, bool synced);
bool is_block_synced_in_db(ino_t inode, size_t block_no);
int delete_blocks_from_db(ino_t inode);
int delete_blocks_by_key(const std::vector<filekey>& filekeys);

// fsck相关的数据库获取函数
struct block_record {
    ino_t inode;
    size_t block_no;
    std::string private_key;
};

int get_blocks_for_inode(ino_t inode, std::vector<block_record>& blocks);
int get_all_block_inodes(std::vector<ino_t>& inodes);
#endif
