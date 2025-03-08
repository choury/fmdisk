#ifndef __NET_H__
#define __NET_H__

#include <curl/curl.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BUNDARY "----------choury-fmdisk"
#define errorlog(...)  fprintf(stderr, __VA_ARGS__)

typedef struct Httprequest {
    enum {head, get, put,  post, patch, Delete} method;
    enum {none, post_x_www_form_urlencoded, post_formdata, post_related, post_json} posttype;
    const char *url;
    size_t length;
    const char *range;
    const char *cookies;
    const char *useragent;
    struct curl_slist *headers;
    uint32_t timeout;
    curl_read_callback readfunc;                //传送给服务器的数据
    void *readprame;
    curl_write_callback writefunc;               //读取服务器返回的数据
    void *writeprame;
    const char* token;
    CURL *curl_handle;
}Http;

const char* getMethod(const Http *r);

typedef struct content_list{
    struct content_list* next;
    const void* data;
    size_t      left;
}contents;

void netinit();
Http * Httpinit(const char *url);                   //根据url生成一个Http结构体，并返回它的指针，必须用HttpDestroy销毁，不然会内存泄漏
void Httpdestroy(Http *hh); 
CURLcode request(Http *r);                        //发送请求

size_t readfromcontentlist(char* buffer, size_t size, size_t nmemb, void *user_p);

#ifdef __cplusplus
}
#endif

#endif
