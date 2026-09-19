#include "log.h"
#include "common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>

static FILE* log_file = NULL;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
static enum fuse_log_level current_log_level = FUSE_LOG_INFO;

static const char* level_strings[] = {
    "EMERG",
    "ALERT",
    "CRIT",
    "ERR",
    "WARNING",
    "NOTICE",
    "INFO",
    "DEBUG"
};

void log_set_level(enum fuse_log_level level) {
    pthread_mutex_lock(&log_mutex);
    current_log_level = level;
    pthread_mutex_unlock(&log_mutex);
}

int log_init(const char* log_path) {
    pthread_mutex_lock(&log_mutex);

    // Close existing log file if any
    if (log_file) {
        fclose(log_file);
        log_file = NULL;
    }

    if (log_path) {
        log_file = fopen(log_path, "a");
        if (!log_file) {
            fprintf(stderr, "Failed to open log file: %s\n", log_path);
            log_file = stderr;
            pthread_mutex_unlock(&log_mutex);
            return -1;
        }
    } else {
        log_file = stderr;
    }

    // Set FUSE log handler
    fuse_set_log_func(fuse_log_handler);

    pthread_mutex_unlock(&log_mutex);
    return 0;
}

//fd 是否指向 /dev/null(fuse 后台化会把 fd 0/1/2 都换成它)
static int is_devnull_fd(int fd) {
    struct stat st, nullst;
    return fstat(fd, &st) == 0 && S_ISCHR(st.st_mode) &&
           stat("/dev/null", &nullst) == 0 &&
           st.st_rdev == nullst.st_rdev;
}

void log_cleanup(void) {
    pthread_mutex_lock(&log_mutex);

    // Reset FUSE log handler to default
    fuse_set_log_func(NULL);

    //log_init(NULL) 时 log_file 就是 stderr, 不能把进程的 stderr 关掉
    if (log_file && log_file != stderr) {
        fclose(log_file);
    }
    log_file = NULL;

    pthread_mutex_unlock(&log_mutex);
}

void fuse_log_handler(enum fuse_log_level level, const char *fmt, va_list ap) {
    if(level > current_log_level) {
        return;
    }

    // Get current time
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    char time_str[64];
    strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", tm_info);

    pthread_mutex_lock(&log_mutex);
    if(log_file != stderr) {
        //只在发现 fd 已被 fuse 后台化换成 /dev/null 时才收编
        int log_fd = fileno(log_file);
        if(is_devnull_fd(STDOUT_FILENO)) {
            dup2(log_fd, STDOUT_FILENO);
        }
        if(is_devnull_fd(STDERR_FILENO)) {
            dup2(log_fd, STDERR_FILENO);
        }
    }
    // Print timestamp, level, and PID
    fprintf(log_file, "[%s] [%s] [%d] ",
            time_str,
            level < 8 ? level_strings[level] : "UNKNOWN",
            getpid());

    // Print the actual log message
    vfprintf(log_file, fmt, ap);

    // Ensure immediate write
    fflush(log_file);
    pthread_mutex_unlock(&log_mutex);
}
