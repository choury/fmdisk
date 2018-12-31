#ifndef UTILS_H__
#define UTILS_H__

#include <string>
#include <memory>
#include <vector>

class buffstruct {
    bool const_buff = false;
public:
    size_t offset = 0;
    char *buf;
    size_t len;
    buffstruct(char* buf = nullptr, size_t len = 0);
    buffstruct(const char* buf, size_t len);
    void expand(size_t size);
    ~buffstruct();
};

struct filekey {
    std::string path;
    std::shared_ptr<void> private_key;
};

#define ENTRY_INITED_F    (1<<0)
#define ENTRY_CHUNCED_F   (1<<1)
#define ENTRY_DELETED_F   (1<<2)
#define ENTRY_REASEWAIT_F (1<<3)
#define ENTRY_CREATE_F    (1<<4)
#define FILE_ENCODE_F     (1<<5)
#define FILE_DIRTY_F      (1<<6)
#define DIR_PULLED_F      (1<<7)
#define META_KEY_ONLY_F   (1<<16)

struct filemeta{
    struct filekey key;
    mode_t mode;
    uint32_t flags;
    size_t size;
    blksize_t blksize;
    blkcnt_t blocks;
    time_t ctime;
    time_t mtime;
    unsigned char* inline_data;
};


std::string URLEncode(const std::string& str);
std::string URLDecode(const std::string& str);
size_t Base64Encode(const char *src, size_t len, char *dst);
size_t Base64Decode(const char *src, size_t len, char* dst);

void xorcode(void* buf, size_t offset, size_t len, const char* key);

std::string dirname(const std::string& path);
std::string basename(const std::string& path);
std::string encodepath(const std::string& path);
std::string decodepath(const std::string& path);

std::string pathjoin(const std::string& dir, const std::string& name);

template <typename... T>
std::string pathjoin(const std::string& dir, const std::string& name, const T&... others){
    return pathjoin(dir, pathjoin(name, others...));
}

bool startwith(const std::string& s1, const std::string& s2);
bool endwith(const std::string& s1, const std::string& s2);
std::string replaceAll(const std::string &s, const std::string &search, const std::string &replace);

size_t savetobuff(void *buffer, size_t size, size_t nmemb, void *user_p);
size_t readfrombuff(void *buffer, size_t size, size_t nmemb, void *user_p);

filemeta initfilemeta(const filekey& key);

struct json_object;
json_object* marshal_meta(const filemeta& meta, const std::vector<filekey>& fblocks);
int unmarshal_meta(json_object *jobj, filemeta& meta, std::vector<filekey>& fblocks);
int download_meta(const filekey& file, filemeta& meta);
int downlod_meta(const filekey& fileat, filemeta& meta, std::vector<filekey>& fblocks);
int upload_meta(const filekey& fileat, filemeta& meta, const std::vector<filekey>& fblocks);
#endif
