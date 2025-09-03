#ifndef LOG_H__
#define LOG_H__

#include <fuse3/fuse_log.h>

#ifdef __cplusplus
extern "C" {
#endif

// Initialize logging system with optional file path
int log_init(const char* log_path);

// Cleanup logging system
void log_cleanup(void);

// FUSE log callback function that handles file output
void fuse_log_handler(enum fuse_log_level level, const char *fmt, va_list ap);

// Convenient logging macros
#define errorlog(...)  fuse_log(FUSE_LOG_ERR, __VA_ARGS__)
#define debuglog(...)  fuse_log(FUSE_LOG_DEBUG, __VA_ARGS__)
#define infolog(...)   fuse_log(FUSE_LOG_INFO, __VA_ARGS__)
#define warnlog(...)   fuse_log(FUSE_LOG_WARNING, __VA_ARGS__)

#ifdef __cplusplus
}
#endif

#endif // LOG_H__