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

int log_init(const char* log_path) {
    pthread_mutex_lock(&log_mutex);
    
    // Close existing log file if any
    if (log_file && log_file != stderr) {
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
    
    if (log_file && log_file != stderr) {
        fclose(log_file);
        log_file = NULL;
    }
    
    pthread_mutex_unlock(&log_mutex);
}

void fuse_log_handler(enum fuse_log_level level, const char *fmt, va_list ap) {
    pthread_mutex_lock(&log_mutex);
    
    if (!log_file) {
        log_file = stderr;
    }
    
    // Get current time
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    char time_str[64];
    strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", tm_info);
    
    // Print timestamp, level, and PID
    fprintf(log_file, "[%s] [%s] [%d] ", 
            time_str, 
            level < 8 ? level_strings[level] : "UNKNOWN",
            getpid());
    
    // Print the actual log message
    vfprintf(log_file, fmt, ap);
    
    if(level <= FUSE_LOG_WARNING) {
        // Ensure immediate write
        fflush(log_file);
    }
    
    pthread_mutex_unlock(&log_mutex);
}
