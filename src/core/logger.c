// src/core/logger.c
#define _POSIX_C_SOURCE 200809L

#include "roole/logger.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <stdarg.h>

// ============================================================================
// CONFIGURATION
// ============================================================================

#define LOG_BUFFER_SIZE 4096
#define FLUSH_INTERVAL_MS 100

// ============================================================================
// THREAD-LOCAL LOG BUFFER
// ============================================================================

typedef struct log_buffer {
    char data[LOG_BUFFER_SIZE];
    size_t used;
    pthread_mutex_t lock;
} log_buffer_t;

static __thread log_buffer_t *tls_log_buffer = NULL;

// ============================================================================
// GLOBAL STATE
// ============================================================================

static log_level_t g_log_level = LOG_LEVEL_INFO;
static log_context_t g_log_context = {0};
static pthread_mutex_t g_context_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_t g_flush_thread;
static volatile int g_shutdown = 0;

#define MAX_COMPONENT_STACK 4
typedef struct {
    char components[MAX_COMPONENT_STACK][32];
    int depth;
} component_stack_t;

static __thread component_stack_t tls_component_stack = {0};

// Ring buffer for cross-thread log aggregation
#define RING_BUFFER_SIZE 128
static log_buffer_t *g_ring_buffers[RING_BUFFER_SIZE];
static size_t g_ring_count = 0;
static pthread_mutex_t g_ring_lock = PTHREAD_MUTEX_INITIALIZER;

// ============================================================================
// INTERNAL FUNCTIONS
// ============================================================================

static log_buffer_t* get_tls_buffer(void) {
    if (!tls_log_buffer) {
        tls_log_buffer = calloc(1, sizeof(log_buffer_t));
        pthread_mutex_init(&tls_log_buffer->lock, NULL);
        
        // Register in global ring for flush thread
        pthread_mutex_lock(&g_ring_lock);
        if (g_ring_count < RING_BUFFER_SIZE) {
            g_ring_buffers[g_ring_count++] = tls_log_buffer;
        }
        pthread_mutex_unlock(&g_ring_lock);
    }
    return tls_log_buffer;
}

static void flush_buffer(log_buffer_t *buf) {
    if (!buf || buf->used == 0) return;
    
    pthread_mutex_lock(&buf->lock);
    if (buf->used > 0) {
        fwrite(buf->data, 1, buf->used, stdout);
        fflush(stdout);
        buf->used = 0;
    }
    pthread_mutex_unlock(&buf->lock);
}

static void* flush_thread_fn(void *arg) {
    (void)arg;
    
    while (!g_shutdown) {
        usleep(FLUSH_INTERVAL_MS * 1000);
        
        pthread_mutex_lock(&g_ring_lock);
        for (size_t i = 0; i < g_ring_count; i++) {
            flush_buffer(g_ring_buffers[i]);
        }
        pthread_mutex_unlock(&g_ring_lock);
    }
    
    return NULL;
}

static const char* level_to_string(log_level_t level) {
    switch (level) {
        case LOG_LEVEL_DEBUG: return "DEBUG";
        case LOG_LEVEL_INFO:  return "INFO ";
        case LOG_LEVEL_WARN:  return "WARN ";
        case LOG_LEVEL_ERROR: return "ERROR";
        default: return "?????";
    }
}

static void get_timestamp(char *buf, size_t size) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    
    struct tm tm;
    localtime_r(&ts.tv_sec, &tm);
    
    snprintf(buf, size, "%04d-%02d-%02dT%02d:%02d:%02d.%03ldZ",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
             tm.tm_hour, tm.tm_min, tm.tm_sec, ts.tv_nsec / 1000000);
}

// ============================================================================
// PUBLIC API
// ============================================================================

void logger_init(void) {
    g_shutdown = 0;
    pthread_create(&g_flush_thread, NULL, flush_thread_fn, NULL);
}

void logger_shutdown(void) {
    g_shutdown = 1;
    pthread_join(g_flush_thread, NULL);
    
    // Final flush
    pthread_mutex_lock(&g_ring_lock);
    for (size_t i = 0; i < g_ring_count; i++) {
        flush_buffer(g_ring_buffers[i]);
        pthread_mutex_destroy(&g_ring_buffers[i]->lock);
        free(g_ring_buffers[i]);
    }
    g_ring_count = 0;
    pthread_mutex_unlock(&g_ring_lock);
}

void logger_set_level(log_level_t level) {
    g_log_level = level;
}

void logger_set_context(uint16_t node_id, const char *cluster_name, const char *node_type) {
    pthread_mutex_lock(&g_context_lock);
    g_log_context.node_id = node_id;
    snprintf(g_log_context.cluster_name, sizeof(g_log_context.cluster_name), 
             "%s", cluster_name ? cluster_name : "unknown");
    snprintf(g_log_context.node_type, sizeof(g_log_context.node_type),
             "%s", node_type ? node_type : "unknown");
    g_log_context.initialized = 1;
    pthread_mutex_unlock(&g_context_lock);
}

void logger_push_component(const char *component_name) {
    if (tls_component_stack.depth < MAX_COMPONENT_STACK && component_name) {
        snprintf(tls_component_stack.components[tls_component_stack.depth], 32,
                "%s", component_name);
        tls_component_stack.depth++;
    }
}

void logger_pop_component(void) {
    if (tls_component_stack.depth > 0) {
        tls_component_stack.depth--;
    }
}

static void get_component_path(char *buf, size_t size) {
    if (tls_component_stack.depth == 0) {
        buf[0] = '\0';
        return;
    }
    
    size_t offset = 0;
    for (int i = 0; i < tls_component_stack.depth && offset < size - 1; i++) {
        int written = snprintf(buf + offset, size - offset, "%s%s",
                              i > 0 ? ":" : "",
                              tls_component_stack.components[i]);
        if (written > 0) offset += written;
    }
}

void logger_log(log_level_t level, const char *file, int line, const char *fmt, ...) {
    if (level < g_log_level) return;
    
    log_buffer_t *buf = get_tls_buffer();
    pthread_mutex_lock(&buf->lock);
    
    // Check if buffer needs emergency flush
    if (buf->used > LOG_BUFFER_SIZE - 512) {
        fwrite(buf->data, 1, buf->used, stdout);
        fflush(stdout);
        buf->used = 0;
    }
    
    char timestamp[32];
    get_timestamp(timestamp, sizeof(timestamp));
    
    // Format: [timestamp][level][node:id][cluster][file:line] message
    int written = 0;
    unsigned long thread_id = (unsigned long)pthread_self();
    char component_path[128];
    get_component_path(component_path, sizeof(component_path));

    pthread_mutex_lock(&g_context_lock);
    if (g_log_context.initialized) {
        if (component_path[0] != '\0') {
            written = snprintf(buf->data + buf->used, LOG_BUFFER_SIZE - buf->used,
                            "[%s][%s][node:%u][%s][%s][tid:%04lx][%s:%d] ",
                            timestamp, level_to_string(level),
                            g_log_context.node_id, g_log_context.cluster_name,
                            component_path, 
                            thread_id % 0xFFFF,
                            file, line);
        } else {
            written = snprintf(buf->data + buf->used, LOG_BUFFER_SIZE - buf->used,
                            "[%s][%s][node:%u][%s]tid:%04lx[%s:%d] ",
                            timestamp, level_to_string(level),
                            g_log_context.node_id, g_log_context.cluster_name,
                            thread_id % 0xFFFF,
                            file, line);
        }
    } else {
        written = snprintf(buf->data + buf->used, LOG_BUFFER_SIZE - buf->used,
                        "[%s][%s]tid:%04lx[%s:%d] ",
                        timestamp, level_to_string(level), thread_id % 0xFFFF,
                        file, line);
    }
    pthread_mutex_unlock(&g_context_lock);
    
    if (written > 0) buf->used += written;
    
    // Append actual message
    va_list args;
    va_start(args, fmt);
    written = vsnprintf(buf->data + buf->used, LOG_BUFFER_SIZE - buf->used, fmt, args);
    va_end(args);
    
    if (written > 0) buf->used += written;
    
    // Add newline
    if (buf->used < LOG_BUFFER_SIZE - 1) {
        buf->data[buf->used++] = '\n';
    }
    
    // Immediate flush for ERROR level
    if (level == LOG_LEVEL_ERROR) {
        fwrite(buf->data, 1, buf->used, stderr);
        fflush(stderr);
        buf->used = 0;
    }
    
    pthread_mutex_unlock(&buf->lock);
}

void logger_flush(void) {
    log_buffer_t *buf = get_tls_buffer();
    flush_buffer(buf);
}