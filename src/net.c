#include <curl/curl.h>
#include <pthread.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

#include "net.h"
#include "log.h"

//int(*errorlog)( const char *__restrict fmt, ... );

//用来存放已有的CURL的链表
struct connect_data{
    CURL *handle;
    struct connect_data* next;
};

static struct connect_data* conhead;                    //链表头指针
static pthread_mutex_t lockcon;                         //操作那个链表使用的锁

struct config {
    char trace_ascii; /* 1 or 0 */
};


//下面个函数来自于curl的文档，调试用（包括上面那个结构体)
static void dump(const char *text,
                  FILE *stream, unsigned char *ptr, size_t size,
                  char nohex){
    size_t i;
    size_t c;
    unsigned int width = 0x10;
    if(nohex)
        /* without the hex output, we can fit more on screen */
        width = 0x40;

    debuglog("%s, %10.10ld bytes(0x%8.8lx)\n",
             text,(long)size, ( long )size );

    for(i = 0; i < size; i += width){
        debuglog("%4.4lx: ",( long)i );
        if(!nohex){
            /* hex not disabled, show it */
            for(c = 0; c < width; c++)
                if(i + c < size)
                    debuglog("%02x ", ptr[i + c]);
                else
                    fputs("   ", stream);
        }

        for(c = 0; ( c < width)&& ( i + c < size ); c++ ) {
            /* check for 0D0A; if found, skip past and start a new line of output */
            if(nohex && ( i + c + 1 < size)&& ptr[i + c] == 0x0D && ptr[i + c + 1] == 0x0A ) {
                i +=(c + 2 - width);
                break;
            }

            debuglog("%c",
                    (ptr[i + c] >= 0x20)&& ( ptr[i + c] < 0x80 ) ? ptr[i + c] : '.' );

            /* check again for 0D0A, to avoid an extra \n if it's at width */
            if(nohex && ( i + c + 2 < size)&& ptr[i + c + 1] == 0x0D && ptr[i + c + 2] == 0x0A ) {
                i +=(c + 3 - width);
                break;
            }
        }
        fputc('\n', stream); /* newline */
    }
    fflush(stream);
}

static int my_trace(CURL *handle, curl_infotype type,
                     char *data, size_t size,
                     void *userp){
    struct config *config =(struct config *)userp;
    const char *text;
    (void)handle; /* prevent compiler warning */

    switch(type){
    case CURLINFO_TEXT:
        debuglog("== Info: %s", data);

    default: /* in case a new one is introduced to shock us */
        return 0;

    case CURLINFO_HEADER_OUT:
        text = "=> Send header";
        break;

    case CURLINFO_DATA_OUT:
        text = "=> Send data";
        break;

    case CURLINFO_SSL_DATA_OUT:
        text = "=> Send SSL data";
        break;

    case CURLINFO_HEADER_IN:
        text = "<= Recv header";
        break;

    case CURLINFO_DATA_IN:
        text = "<= Recv data";
        break;

    case CURLINFO_SSL_DATA_IN:
        text = "<= Recv SSL data";
        break;
    }

    dump(text, stderr,( unsigned char *)data, size, config->trace_ascii );
    return 0;
}

//获得一个可用的CURL结构
static CURL* getcurl(){
    struct config config = {
        .trace_ascii = 1,  /* enable ascii tracing */
    };
    CURL *curl;
    pthread_mutex_lock(&lockcon);
    struct connect_data *tmp = conhead;
    if(tmp){                                //如果链表中已有一个现有的CURL结构，直接取下来返回
        conhead = conhead->next;
        curl = tmp->handle;
        free(tmp);
        curl_easy_reset(curl);
    }else{                                  //否则新生成一个
        curl = curl_easy_init();
    }
    pthread_mutex_unlock(&lockcon);
    curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, my_trace);
    curl_easy_setopt(curl, CURLOPT_DEBUGDATA, &config);
    curl_easy_setopt(curl, CURLOPT_FILETIME, 1);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
    curl_easy_setopt(curl, CURLOPT_FRESH_CONNECT, 0);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2);
    curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 5);
    curl_easy_setopt(curl, CURLOPT_HEADER, 0);
    //curl_easy_setopt(curl, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 60);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 0);
    //curl_easy_setopt(curl, CURLOPT_PROXY, "proxy.com:8080");
    return curl;
}


//释放，就是把它加到链表中，
//如果申请内存成功的话并不真的释放
static void releasecurl(CURL *curl){
    struct connect_data* tmp = (struct connect_data*)malloc(sizeof(struct connect_data));
    if (tmp==NULL) {
        curl_easy_cleanup(curl);
        return;
    }
    tmp->handle=curl;
    pthread_mutex_lock(&lockcon);
    tmp->next = conhead;
    conhead = tmp;
    pthread_mutex_unlock(&lockcon);
}

const char* getMethod(const Http *r){
    switch(r->method){
    case head:
        return "HEAD";
    case get:
        return "GET";
    case post:
        return "POST";
    case put:
        return "PUT";
    case patch:
        return "PATCH";
    case Delete:
        return "DELETE";
    default:
        return "UNKNOW";
    }
}

CURLcode request(Http *r){
//    errorlog("request: %s\n", r->url);

    curl_mime *mime = NULL;

    curl_easy_setopt(r->curl_handle, CURLOPT_URL, r->url);
    curl_easy_setopt(r->curl_handle, CURLOPT_REFERER, r->url);
    curl_easy_setopt(r->curl_handle, CURLOPT_TIMEOUT, r->timeout);

    char errbuf[CURL_ERROR_SIZE] = {0};
    curl_easy_setopt(r->curl_handle, CURLOPT_ERRORBUFFER, errbuf);
    //curl_easy_setopt(r->curl_handle, CURLOPT_FRESH_CONNECT, 1);
    if (r->timeout > 60) {
        curl_easy_setopt(r->curl_handle, CURLOPT_LOW_SPEED_LIMIT, 5);
        curl_easy_setopt(r->curl_handle, CURLOPT_LOW_SPEED_TIME, r->timeout/2);
    }
    if (r->range) {
        curl_easy_setopt(r->curl_handle, CURLOPT_RANGE, r->range);
    }

    if (r->useragent) {
        curl_easy_setopt(r->curl_handle, CURLOPT_USERAGENT, r->useragent);
    }

    if (r->writefunc) {
        curl_easy_setopt(r->curl_handle, CURLOPT_WRITEFUNCTION, r->writefunc);
        curl_easy_setopt(r->curl_handle, CURLOPT_WRITEDATA, r->writeprame);
    }

    if (r->readfunc) {
        curl_easy_setopt(r->curl_handle, CURLOPT_READFUNCTION, r->readfunc);
        curl_easy_setopt(r->curl_handle, CURLOPT_READDATA, r->readprame);
    }

    if (r->token) {
        size_t tlen = strlen(r->token);
        size_t header_len = tlen + strlen("Authorization: Bearer ") + 1;
        char *bearer = malloc(header_len);
        if (!bearer) {
            return CURLE_OUT_OF_MEMORY;
        }
        snprintf(bearer, header_len, "Authorization: Bearer %s", r->token);
        r->headers = curl_slist_append(r->headers, bearer);
        free(bearer);
    }

    switch(r->method){
    case head:
        curl_easy_setopt(r->curl_handle, CURLOPT_CUSTOMREQUEST, getMethod(r));
        curl_easy_setopt(r->curl_handle, CURLOPT_NOBODY, 1);
        break;

    case Delete:
        curl_easy_setopt(r->curl_handle, CURLOPT_CUSTOMREQUEST, getMethod(r));
    case get:
        curl_easy_setopt(r->curl_handle,CURLOPT_HTTPGET,1);
        break;

    case put:
    case patch:
        curl_easy_setopt(r->curl_handle, CURLOPT_CUSTOMREQUEST, getMethod(r));
    case post:
        curl_easy_setopt(r->curl_handle, CURLOPT_POST, 1);
        curl_easy_setopt(r->curl_handle, CURLOPT_POSTFIELDSIZE, r->length);
        break;

    default:
        errorlog("Unimplise Method!\n");
        return -1;
    }
    switch(r->posttype){
    case none:
        break;
    case post_x_www_form_urlencoded:
        r->headers = curl_slist_append(r->headers, "Content-Type: application/x-www-form-urlencoded");
        break;
    case post_formdata: {
        mime = curl_mime_init(r->curl_handle);
        curl_mimepart *part = curl_mime_addpart(mime);
        curl_mime_name(part, "file");
        curl_mime_filename(part, "tmpfile");
        curl_mime_type(part, "application/octet-stream");
        curl_mime_data_cb(part, r->length, r->readfunc, NULL, NULL, r->readprame);
        curl_easy_setopt(r->curl_handle, CURLOPT_MIMEPOST, mime);
        break;
    }
    case post_multipart: {
        char content_type[256];
        snprintf(content_type, sizeof(content_type), "Content-Type: multipart/%s; boundary=%s",
                 r->multipart_subtype, BUNDARY);
        r->headers = curl_slist_append(r->headers, content_type);
        break;
    }
    case post_json:
        r->headers = curl_slist_append(r->headers, "Content-Type: application/json");
        break;
    }

    curl_easy_setopt(r->curl_handle, CURLOPT_HTTPHEADER, r->headers);
    CURLcode curl_code = curl_easy_perform(r->curl_handle);
    long http_code = 0;
    curl_easy_getinfo(r->curl_handle, CURLINFO_RESPONSE_CODE, &http_code);
    if(curl_code != CURLE_OK && strlen(errbuf)){
        r->should_destory = 1;
        errorlog("libcurl error: %s [%d]\n", errbuf, curl_code);
    }
    if(mime){
        curl_mime_free(mime);
    }
    if(curl_code == CURLE_OK && (http_code >= 300 || http_code < 200)){
        return http_code;
    }
    return curl_code;
}


void netinit(){
    while(curl_global_init(CURL_GLOBAL_ALL)!= CURLE_OK) ;     //初始化curl
    conhead = NULL;
    pthread_mutex_init(&lockcon, NULL);
    //curl_global_cleanup();
}

Http *Httpinit(const char *url){
    Http *hh = malloc(sizeof(Http));
    assert(hh);
    memset(hh, 0, sizeof(Http));
    hh->curl_handle = getcurl();
    assert(hh->curl_handle);
    hh->url = url;
    hh->method = get;
    hh->posttype = none;
    hh->timeout = 60;
    hh->multipart_subtype = NULL;
    return hh;
}

void Httpdestroy(Http *hh){
    if(hh->should_destory) {
        curl_easy_cleanup(hh->curl_handle);
    }else{
        releasecurl(hh->curl_handle);
    }
    curl_slist_free_all(hh->headers);
    free(hh);
}

size_t readfromcontentlist(char* buffer, size_t size, size_t nmemb, void *user_p){
    contents** cnt =  (contents **)user_p;
    contents* current = *cnt;
    if(current == NULL){
        return 0;
    }
    size_t len = current->left > size*nmemb ? size*nmemb : current->left;
    assert(len);
    memcpy(buffer, current->data, len);
    current->left -= len;
    current->data = current->data + len;
    if(current->left == 0){
        *cnt = current->next;
    }
    return len;
}
