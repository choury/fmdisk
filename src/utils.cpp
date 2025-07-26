#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <sys/stat.h>
#include <json.h>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#define ARCH_X86
#elif defined(__arm__) || defined(__aarch64__)
#include <arm_neon.h>
#define ARCH_ARM
#endif

#include "common.h"
#include "utils.h"
#include "fmdisk.h"

using std::string;

char COFPATH[4096];

static int hex2num(char c) {
    if (c>='0' && c<='9') return c - '0';
    if (c>='a' && c<='z') return c - 'a' + 10;//这里+10的原因是:比如16进制的a值为10
    if (c>='A' && c<='Z') return c - 'A' + 10;

    fprintf(stderr, "unexpected char: %c", c);
    return '0';
}

string URLEncode(const string& str) {
    string result;
    for (size_t i=0; i<str.size(); ++i) {
        unsigned char ch = str[i];
        if (((ch>='A') && (ch<='Z')) ||
            ((ch>='a') && (ch<='z')) ||
            ((ch>='0') && (ch<='9'))) {
            result += ch;
        } else if (ch == ' ') {
            result += '+';
        } else if (ch == '.' || ch == '-' || ch == '_' || ch == '*') {
            result += ch;
        } else {
            char tmp[4];
            sprintf(tmp, "%%%02X", ch);
            result += tmp;
        }
    }
    return result;
}


string URLDecode(const string& str) {
    string result;
    for (size_t i=0; i<str.size(); ++i) {
        char ch = str[i];
        switch (ch) {
        case '+':
            result += ' ';
            break;
        case '%':
            if (i+2<str.size()) {
                char ch1 = hex2num(str[i+1]);//高4位
                char ch2 = hex2num(str[i+2]);//低4位
                if ((ch1!='0') && (ch2!='0'))
                    result += (char)((ch1<<4) | ch2);
                i += 2;
                break;
            } else {
                break;
            }
        default:
            result += ch;
            break;
        }
    }
    return result;
}

static const char *base64_endigs_normal ="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
static const char *base64_endigs_mod ="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

static size_t Base64(const char* endigs, const char *s, size_t len, char *dst){
    size_t i=0,j=0;
    const unsigned char* src = (const unsigned char *)s;
    for(;i+2<len;i+=3){
        dst[j++] = endigs[src[i]>>2];
        dst[j++] = endigs[((src[i]<<4) & 0x30) | src[i+1]>>4];
        dst[j++] = endigs[((src[i+1]<<2) & 0x3c) | src[i+2]>>6];
        dst[j++] = endigs[src[i+2] & 0x3f];
    }
    if(i == len-1){
        dst[j++] = endigs[src[i]>>2];
        dst[j++] = endigs[(src[i]<<4) & 0x30];
        dst[j++] = '=';
        dst[j++] = '=';
    }else if(i == len-2){
        dst[j++] = endigs[src[i]>>2];
        dst[j++] = endigs[((src[i]<<4) & 0x30) | src[i+1]>>4];
        dst[j++] = endigs[(src[i+1]<<2) & 0x3c];
        dst[j++] = '=';
    }
    dst[j] = 0;
    return j;
}

size_t Base64Encode(const char *s, size_t len, char *dst){
    return Base64(base64_endigs_mod, s, len, dst);
}

size_t Base64En(const char *s, size_t len, char *dst){
    return Base64(base64_endigs_normal, s, len, dst);
}

static const char base64_dedigs[128] =
{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
 0,0,0,0,0,0,0,0,0,0,0,0,0,62,0,0,
 52,53,54,55,56,57,58,59,60,61,0,0,0,0,0,0,
 0,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,
 15,16,17,18,19,20,21,22,23,24,25,0,0,0,0,63,
 0,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
 41,42,43,44,45,46,47,48,49,50,51,0,0,0,0,0
};

size_t Base64Decode(const char *src, size_t len, char* dst){
    size_t i=0, j = 0;
    for(;i<len; i+= 4){
        char ch1 = (base64_dedigs[(int)src[i]]<<2) | (base64_dedigs[(int)src[i+1]] >>4);
        dst[j++] = ch1;
        if(i+2 >= len || src[i+2] == '='){
            break;
        }
        char ch2 = (base64_dedigs[(int)src[i+1]]<<4) | (base64_dedigs[(int)src[i+2]] >>2);
        dst[j++] = ch2;
        if(i+3 >= len || src[i+3] == '='){
            break;
        }
        char ch3 = (base64_dedigs[(int)src[i+2]]<<6) | base64_dedigs[(int)src[i+3]];
        dst[j++] = ch3;
    }
    dst[j] = 0;
    return j;
}

void xorcode(void* buf, size_t offset, size_t len, const char* key) {
    unsigned char* buff = (unsigned char*)buf;
    size_t klen = strlen(key);
    size_t k_idx = offset % klen;

    // 处理非常小的缓冲区，避免SIMD的初始化开销
    if (len < 16) {
        for (size_t i = 0; i < len; i++) {
            buff[i] ^= key[k_idx];
            k_idx = (k_idx + 1) % klen;
        }
        return;
    }

    // 准备扩展密钥数组
    unsigned char extended_key[1024]; // 足够容纳大多数密钥情况
    size_t extended_len = 0;

#if defined(__AVX512F__) && defined(__AVX512BW__)
    size_t target_size = std::max(klen * 2, (size_t)128);
#elif defined(__AVX2__)
    size_t target_size = std::max(klen * 2, (size_t)64);
#else
    size_t target_size = std::max(klen * 2, (size_t)32);
#endif
    while (extended_len < target_size) {
        extended_key[extended_len] = key[extended_len % klen];
        extended_len++;
    }

    size_t i = 0;

#ifdef ARCH_X86
    // 使用x86 SIMD指令
    #if defined(__AVX512F__) && defined(__AVX512BW__)
    // 使用AVX-512 (64字节向量)
    while (i + 64 <= len) {
        __m512i data = _mm512_loadu_si512((__m512i*)(buff + i));
        __m512i key_vec = _mm512_loadu_si512((__m512i*)(extended_key + k_idx));
        __m512i result = _mm512_xor_si512(data, key_vec);
        _mm512_storeu_si512((__m512i*)(buff + i), result);

        k_idx = (k_idx + 64) % klen;
        i += 64;
    }
    #endif
    #if defined(__AVX2__)
    // 使用AVX2 (32字节向量)
    while (i + 32 <= len) {
        __m256i data = _mm256_loadu_si256((__m256i*)(buff + i));
        __m256i key_vec = _mm256_loadu_si256((__m256i*)(extended_key + k_idx));
        __m256i result = _mm256_xor_si256(data, key_vec);
        _mm256_storeu_si256((__m256i*)(buff + i), result);

        k_idx = (k_idx + 32) % klen;
        i += 32;
    }
    #endif

    // 使用SSE (16字节向量)
    while (i + 16 <= len) {
        __m128i data = _mm_loadu_si128((__m128i*)(buff + i));
        __m128i key_vec = _mm_loadu_si128((__m128i*)(extended_key + k_idx));
        __m128i result = _mm_xor_si128(data, key_vec);
        _mm_storeu_si128((__m128i*)(buff + i), result);

        k_idx = (k_idx + 16) % klen;
        i += 16;
    }
#elif defined(ARCH_ARM)
    // 使用ARM NEON SIMD指令
    while (i + 16 <= len) {
        uint8x16_t data = vld1q_u8(buff + i);
        uint8x16_t key_vec = vld1q_u8(extended_key + k_idx);
        uint8x16_t result = veorq_u8(data, key_vec);
        vst1q_u8(buff + i, result);

        k_idx = (k_idx + 16) % klen;
        i += 16;
    }
#endif

    // 处理剩余部分的字节 - 现在也使用extended_key
    for (; i < len; i++) {
        buff[i] ^= extended_key[k_idx];
        k_idx = (k_idx + 1) % klen;
    }
}

string basename(const string& path) {
    size_t pos = path.find_last_of('/');
    if(pos == string::npos) {
        return path;
    }
    if(path.length() <= 1){
        return path;
    }
    if(pos == path.length() -1 ) {
        string path_truncate = path.substr(0, path.length()-1);
        return basename(path_truncate);
    }
    return path.substr(pos+1, path.length());
}

string dirname(const string& path) {
    size_t pos = path.find_last_of('/');
    if(pos == string::npos) {
        return ".";
    }
    if(pos == 0){
        return "/";
    }
    if(path.length() <= 1){
        return path;
    }
    if(pos == path.length() -1 ) {
        string path_truncate = path.substr(0, path.length()-1);
        return dirname(path_truncate);
    }
    return path.substr(0, pos);
}


bool endwith(const string& s1, const string& s2){
    auto l1 = s1.length();
    auto l2 = s2.length();
    if(l1 < l2)
        return 0;
    return !memcmp(s1.data()+l1-l2, s2.data(), l2);
}

bool startwith(const string& s1, const string& s2){
    auto l1 = s1.length();
    auto l2 = s2.length();
    if(l1 < l2)
        return 0;
    return !memcmp(s1.data(), s2.data(), l2);
}

string replaceAll(const std::string &s, const std::string &search, const std::string &replace ){
    string str = s;
    for(size_t pos = 0; ; pos += replace.length()) {
        // Locate the substring to replace
        pos = str.find( search, pos);
        if( pos == std::string::npos) break;
        // Replace by erasing and inserting
        str.erase(pos, search.length());
        str.insert(pos, replace);
    }
    return str;
}

string encodepath(const string& path){
    string bname = basename(path);
    char dst[bname.length()*2 < 10 ? 10 : bname.length()*2];
    Base64Encode(bname.data(), bname.length(), dst);
    if(dirname(path) == "."){
        return string(dst) + ".def";
    }else{
        return pathjoin(dirname(path), string(dst) + ".def");
    }
}

string decodepath(const string& path){
    assert(endwith(path, ".def"));
    string bname = basename(path);
    char dst[bname.length()];
    Base64Decode(bname.substr(0, bname.length()-4).data(), bname.length() - 4, dst);
    if(dirname(path) == "."){
        return dst;
    }else{
        return pathjoin(dirname(path),  dst);
    }
}

string pathjoin(const string& dir, const string& name){
    bool endwithslash = endwith(dir, "/");
    bool startwithslash = startwith(name, "/");

    if(endwithslash && startwithslash){
        return dir + (name.c_str()+1);
    }else if(endwithslash || startwithslash){
        return dir + name;
    }else{
        return dir +'/'+ name;
    }
}

filemeta initfilemeta(const filekey& key){
    return filemeta{key,
        0, 0, 0, 0, 0, 0, 0, nullptr};
}


buffstruct::buffstruct(): buf((char*)calloc(1024, 1)), cap(1024) {
}

buffstruct::buffstruct(char* buf, size_t len):buf(buf),cap(len) {
    assert(buf != nullptr && len > 0);
}

buffstruct::buffstruct(const char* buf, size_t len):cbuf(buf), cap(len) {
    assert(cbuf != nullptr && len > 0);
}


void buffstruct::expand(size_t size){
    if(cbuf){
        assert(0);
        return;
    }
    if(offset + size >= cap){
        cap = ((offset + size)&0xfffffffffc00)+1024;
        buf = (char*)realloc(buf, cap);
        memset(buf + offset, 0, cap - offset);
    }
}

buffstruct::~buffstruct() {
    free(buf);
}



//顾名思义，将服务器传回的数据写到buff中
size_t buffstruct::savetobuff(char* buffer, size_t size, size_t nmemb, void *user_p) {
    buffstruct *bs = (buffstruct *) user_p;
    size_t len = size * nmemb;
    bs->expand(len);
    memcpy(bs->buf + bs->offset, buffer, len);
    bs->offset += len;
    return len;
}

//你猜
size_t buffstruct::readfrombuff(char* buffer, size_t size, size_t nmemb, void *user_p) {
    buffstruct *bs = (buffstruct *) user_p;
    size_t len = std::min(size * nmemb, (bs->cap) - (size_t)bs->offset);
    if(bs->cbuf) {
        memcpy(buffer, bs->cbuf + bs->offset, len);
    }else {
        memcpy(buffer, bs->buf + bs->offset, len);
    }
    bs->offset += len;
    return len;
}


int unmarshal_meta(json_object *jobj, filemeta& meta, std::vector<filekey>& fblocks){
    json_object* jctime;
    int ret = json_object_object_get_ex(jobj, "ctime", &jctime);
    assert(ret);
    meta.ctime = json_object_get_int64(jctime);

    json_object* jmtime;
    ret = json_object_object_get_ex(jobj, "mtime", &jmtime);
    assert(ret);
    meta.mtime = json_object_get_int64(jmtime);

    json_object* jsize;
    ret = json_object_object_get_ex(jobj, "size", &jsize);
    assert(ret);
    meta.size = json_object_get_int64(jsize);

    json_object* jmode;
    if(json_object_object_get_ex(jobj, "mode", &jmode)){
        meta.mode = json_object_get_int64(jmode);
    }else{
        meta.mode = S_IFREG | 0444;
    }

    json_object *jencoding;
    ret = json_object_object_get_ex(jobj, "encoding", &jencoding);
    assert(ret);
    const char* encoding = json_object_get_string(jencoding);
    if(strcasecmp(encoding, "xor") == 0){
        meta.flags = FILE_ENCODE_F;
    }else{
        assert(strcasecmp(encoding, "none") == 0);
    }

    json_object *jblksize;
    ret = json_object_object_get_ex(jobj, "blksize", &jblksize);
    assert(ret);
    meta.blksize = json_object_get_int64(jblksize);

    json_object* jblocks;
    if(json_object_object_get_ex(jobj, "blocks", &jblocks)){
        fblocks.reserve(json_object_array_length(jblocks));
        for(size_t i=0; i < json_object_array_length(jblocks); i++){
            json_object *jblock = json_object_array_get_idx(jblocks, i);
            json_object *jname;
            ret = json_object_object_get_ex(jblock, "name", &jname);
            assert(ret);
            json_object *jkey;
            ret = json_object_object_get_ex(jblock, "key", &jkey);
            assert(ret);
            const char* name = json_object_get_string(jname);
            const char* private_key = json_object_get_string(jkey);
            fblocks.emplace_back(filekey{name, fm_get_private_key(private_key)});
        }
    }

    json_object *jblock_list;
    if(json_object_object_get_ex(jobj, "block_list", &jblock_list)){
        fblocks.reserve(json_object_array_length(jblock_list));
        for(size_t i=0; i < json_object_array_length(jblock_list); i++){
            json_object *block = json_object_array_get_idx(jblock_list, i);
            const char* name = json_object_get_string(block);
            fblocks.emplace_back(filekey{name, 0});
        }
    }

    json_object *jinline_data;
    ret = json_object_object_get_ex(jobj, "inline_data", &jinline_data);
    if(ret){
        char* inline_data = new char[INLINE_DLEN];
        Base64Decode(json_object_get_string(jinline_data), json_object_get_string_len(jinline_data), inline_data);
        meta.inline_data = (unsigned char*)inline_data;
    }
    return 0;
}


int download_meta(const filekey& fileat, filemeta& meta, std::vector<filekey>& fblocks){
    filekey metakey{METANAME, 0};
    int ret;
    if((ret = HANDLE_EAGAIN(fm_getattrat(fileat, metakey)))){
        return ret;
    }
    buffstruct bs;
    if((ret = HANDLE_EAGAIN(fm_download(metakey, 0, 0, bs)))){
        return ret;
    }
    std::string json_str = std::string(bs.data(), bs.size());
    json_object *json_get = json_tokener_parse(json_str.c_str());
    if(json_get ==  nullptr){
        throw "Json parse error";
    }
    meta = initfilemeta(metakey);
    unmarshal_meta(json_get, meta, fblocks);
    json_object_put(json_get);
    return 0;
}

json_object* marshal_meta(const filemeta& meta, const std::vector<filekey>& fblocks){
    json_object *jobj = json_object_new_object();
    json_object_object_add(jobj, "size", json_object_new_int64(meta.size));
    if(meta.flags & ENTRY_CHUNCED_F){
        json_object_object_add(jobj, "mode", json_object_new_int64(S_IFDIR | (meta.mode & 0777)));
    }else{
        json_object_object_add(jobj, "mode", json_object_new_int64(meta.mode));
    }
    json_object_object_add(jobj, "ctime", json_object_new_int64(meta.ctime));
    json_object_object_add(jobj, "mtime", json_object_new_int64(meta.mtime));
    json_object_object_add(jobj, "blksize", json_object_new_int64(meta.blksize));
    if(meta.flags & FILE_ENCODE_F){
        json_object_object_add(jobj, "encoding", json_object_new_string("xor"));
    }else{
        json_object_object_add(jobj, "encoding", json_object_new_string("none"));
    }
    if(meta.inline_data){
        char* inline_data = new char[INLINE_DLEN * 2 < 10 ? 10 : INLINE_DLEN * 2];
        Base64Encode((const char*)meta.inline_data, meta.size, inline_data);
        json_object_object_add(jobj, "inline_data", json_object_new_string(inline_data));
        delete[] inline_data;
    }

    json_object *jblocks = json_object_new_array();
    for(auto block: fblocks){
        json_object *jblock = json_object_new_object();
        json_object_object_add(jblock, "name", json_object_new_string(block.path.c_str()));
        json_object_object_add(jblock, "key", json_object_new_string(fm_private_key_tostring(block.private_key)));
        json_object_array_add(jblocks, jblock);
    }

    json_object_object_add(jobj, "blocks", jblocks);
    return jobj;
}

int upload_meta(const filekey& fileat, filemeta& meta, const std::vector<filekey>& fblocks){
    json_object *jobj = marshal_meta(meta, fblocks);
    const char *jstring = json_object_to_json_string(jobj);

retry:
    int ret = HANDLE_EAGAIN(fm_upload(fileat, meta.key, jstring, strlen(jstring), true));
    if(ret != 0 && errno == EEXIST){
        goto retry;
    }
    json_object_put(jobj);
    return ret;
}
