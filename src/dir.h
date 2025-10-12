#ifndef DIR_H__
#define DIR_H__
#include "entry.h"

#include <map>

using std::string;

class file_t;
class symlink_t;
class dir_t: public entry_t {
    std::map<string, std::shared_ptr<entry_t>> entrys;
    int pull_wlocked() override;
    int pull_entrys_wlocked();
    std::shared_ptr<entry_t> insert_child_wlocked(const std::string& name, std::shared_ptr<entry_t> entry);
    std::set<std::string> insert_meta_wlocked(const std::vector<filemeta>& flist, bool save);

    // Helper to get shared_ptr<dir_t> from shared_from_this()
    std::shared_ptr<dir_t> shared_dir_from_this() {
        return std::static_pointer_cast<dir_t>(shared_from_this());
    }

    virtual std::string getrealname() override {
        return fk.load()->path;
    }
    virtual int drop_cache_wlocked(bool mem_only, time_t before) override;
    virtual int remove_wlocked() override;
    virtual int set_storage_class(enum storage_class storage, TrdPool* pool, std::vector<std::future<int>>& futures) override;
public:
    dir_t(std::shared_ptr<dir_t> parent, const filemeta& meta);
    virtual ~dir_t() override;
    virtual int getmeta(filemeta& meta) override;
    std::shared_ptr<entry_t> find(std::string path);
    const std::map<string, std::shared_ptr<entry_t>>& get_entrys();
    size_t children();

    virtual int open() override;
    virtual int release(bool) override{
        atime = time(nullptr);
        auto_wlock(this);
        assert(opened > 0);
        opened--;
        return 0;
    }
    virtual int sync(int dataonly) override;
    virtual int utime(const struct timespec tv[2]) override;

    std::shared_ptr<file_t> create(const string& name, mode_t mode);
    std::shared_ptr<dir_t>  mkdir(const string& name, mode_t mode);
    std::shared_ptr<symlink_t> symlink(const string& name, const string& target);
    int unlink(const string& name);
    int rmdir(const string& name);
    int moveto(std::shared_ptr<dir_t> newparent, const string& oldname, const string& newname, unsigned int flags);

    virtual void dump_to_db(const std::string& path, const std::string& name) override;
};



#endif
