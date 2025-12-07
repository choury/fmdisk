#include "log.h"
#include "common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>

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

void log_cleanup(void) {
    pthread_mutex_lock(&log_mutex);

    // Reset FUSE log handler to default
    fuse_set_log_func(NULL);

    if (log_file) {
        fclose(log_file);
        log_file = NULL;
    }

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
    static int duped = 0;
    if (log_file != stderr && !duped) {
        //redirect stdout and stderr to log file
        dup2(fileno(log_file), STDOUT_FILENO);
        dup2(fileno(log_file), STDERR_FILENO);
        duped = 1;
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
