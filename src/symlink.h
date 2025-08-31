#ifndef SYMLINK_H__
#define SYMLINK_H__
#include "entry.h"

#define symlink_encode_suffix ".lnk"

class symlink_t: public entry_t {
    std::string target_path;  // 符号链接目标路径

    virtual int pull_wlocked() override;
    virtual std::string getrealname() override;
    virtual int drop_cache_wlocked() override;
    virtual int remove_wlocked() override;

public:
    symlink_t(dir_t* parent, const filemeta& meta);
    virtual ~symlink_t() override;
    virtual int getmeta(filemeta& meta) override;

    virtual int open() override {
        return -ELOOP;
    };
    virtual int release() override {
        return 0;
    };
    virtual int sync(int dataonly) override {
        return 0;
    };
    virtual int utime(const struct timespec tv[2]) override;
    virtual void dump_to_db(const std::string& path, const std::string& name) override;

    friend class dir_t;
};

#endif