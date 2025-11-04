// src/core/logger.c - Enhanced with structured logging support

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
#define FLUSH_INTERVAL_MS 10
#define CRITICAL_BUFFER_THRESHOLD 3072

// Log format modes
typedef enum {
    LOG_FORMAT_TEXT = 0,    // Traditional text format
    LOG_FORMAT_JSON = 1     // Structured JSON format
} log_format_t;

// ============================================================================
// GLOBAL STATE
// ============================================================================

static log_level_t g_log_level = LOG_LEVEL_INFO;
static log_format_t g_log_format = LOG_FORMAT_TEXT;  // Default to text
static log_context_t g_log_context = {0};
static pthread_mutex_t g_context_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_t g_flush_thread;
static volatile int g_shutdown = 0;

// Thread-local log buffer
typedef struct log_buffer {
    char data[LOG_BUFFER_SIZE];
    size_t used;
    pthread_mutex_t lock;
} log_buffer_t;

static __thread log_buffer_t *tls_log_buffer = NULL;

// Component stack (unchanged from before)
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
// FORMAT DETECTION (from environment variable)
// ============================================================================

static void detect_log_format(void) {
    const char *format_env = getenv("ROOLE_LOG_FORMAT");
    
    if (format_env) {
        if (strcasecmp(format_env, "json") == 0) {
            g_log_format = LOG_FORMAT_JSON;
            fprintf(stderr, "[logger] Using JSON log format\n");
        } else if (strcasecmp(format_env, "text") == 0) {
            g_log_format = LOG_FORMAT_TEXT;
            fprintf(stderr, "[logger] Using text log format\n");
        } else {
            fprintf(stderr, "[logger] Unknown ROOLE_LOG_FORMAT='%s', using text\n", 
                    format_env);
            g_log_format = LOG_FORMAT_TEXT;
        }
    } else {
        g_log_format = LOG_FORMAT_TEXT;  // Default
    }
}

// ============================================================================
// JSON ESCAPING HELPER
// ============================================================================

static void json_escape_string(const char *input, char *output, size_t output_size) {
    size_t in_pos = 0, out_pos = 0;
    
    while (input[in_pos] && out_pos < output_size - 2) {
        char c = input[in_pos];
        
        switch (c) {
            case '"':
            case '\\':
                if (out_pos < output_size - 3) {
                    output[out_pos++] = '\\';
                    output[out_pos++] = c;
                }
                break;
            case '\n':
                if (out_pos < output_size - 3) {
                    output[out_pos++] = '\\';
                    output[out_pos++] = 'n';
                }
                break;
            case '\r':
                if (out_pos < output_size - 3) {
                    output[out_pos++] = '\\';
                    output[out_pos++] = 'r';
                }
                break;
            case '\t':
                if (out_pos < output_size - 3) {
                    output[out_pos++] = '\\';
                    output[out_pos++] = 't';
                }
                break;
            default:
                if (c >= 32 && c <= 126) {  // Printable ASCII
                    output[out_pos++] = c;
                }
                break;
        }
        in_pos++;
    }
    output[out_pos] = '\0';
}

// ============================================================================
// INTERNAL FUNCTIONS (unchanged)
// ============================================================================

static log_buffer_t* get_tls_buffer(void) {
    if (!tls_log_buffer) {
        tls_log_buffer = calloc(1, sizeof(log_buffer_t));
        pthread_mutex_init(&tls_log_buffer->lock, NULL);
        
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
        case LOG_LEVEL_INFO:  return "INFO";
        case LOG_LEVEL_WARN:  return "WARN";
        case LOG_LEVEL_ERROR: return "ERROR";
        default: return "UNKNOWN";
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

// ============================================================================
// TEXT FORMAT LOGGING (Original)
// ============================================================================

static void logger_log_text(log_level_t level, const char *file, int line, 
                           const char *fmt, va_list args) {
    log_buffer_t *buf = get_tls_buffer();
    pthread_mutex_lock(&buf->lock);
    
    if (buf->used > CRITICAL_BUFFER_THRESHOLD) {
        fwrite(buf->data, 1, buf->used, stdout);
        fflush(stdout);
        buf->used = 0;
    }
    
    char timestamp[32];
    get_timestamp(timestamp, sizeof(timestamp));
    
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
                            "[%s][%s][node:%u][%s][tid:%04lx][%s:%d] ",
                            timestamp, level_to_string(level),
                            g_log_context.node_id, g_log_context.cluster_name,
                            thread_id % 0xFFFF,
                            file, line);
        }
    } else {
        written = snprintf(buf->data + buf->used, LOG_BUFFER_SIZE - buf->used,
                        "[%s][%s][tid:%04lx][%s:%d] ",
                        timestamp, level_to_string(level), thread_id % 0xFFFF,
                        file, line);
    }
    pthread_mutex_unlock(&g_context_lock);
    
    if (written > 0) buf->used += written;
    
    written = vsnprintf(buf->data + buf->used, LOG_BUFFER_SIZE - buf->used, fmt, args);
    if (written > 0) buf->used += written;
    
    if (buf->used < LOG_BUFFER_SIZE - 1) {
        buf->data[buf->used++] = '\n';
    }
    
    if (level == LOG_LEVEL_ERROR) {
        fwrite(buf->data, 1, buf->used, stderr);
        fflush(stderr);
        buf->used = 0;
    }
    
    pthread_mutex_unlock(&buf->lock);
}

// ============================================================================
// JSON FORMAT LOGGING (NEW)
// ============================================================================

static void logger_log_json(log_level_t level, const char *file, int line,
                           const char *fmt, va_list args) {
    log_buffer_t *buf = get_tls_buffer();
    pthread_mutex_lock(&buf->lock);
    
    if (buf->used > CRITICAL_BUFFER_THRESHOLD) {
        fwrite(buf->data, 1, buf->used, stdout);
        fflush(stdout);
        buf->used = 0;
    }
    
    char timestamp[32];
    get_timestamp(timestamp, sizeof(timestamp));
    
    char component_path[128];
    get_component_path(component_path, sizeof(component_path));
    
    unsigned long thread_id = (unsigned long)pthread_self();
    
    // Format the actual log message
    char message[1024];
    vsnprintf(message, sizeof(message), fmt, args);
    
    // Escape message for JSON
    char escaped_message[2048];
    json_escape_string(message, escaped_message, sizeof(escaped_message));
    
    // Build JSON log line
    int written = 0;
    
    pthread_mutex_lock(&g_context_lock);
    
    if (g_log_context.initialized) {
        if (component_path[0] != '\0') {
            written = snprintf(buf->data + buf->used, LOG_BUFFER_SIZE - buf->used,
                "{\"timestamp\"%s,\"level\":\"%s\",\"node_id\":%u,"
                "\"cluster\":\"%s\",\"component\":\"%s\",\"thread_id\":%lu,"
                "\"file\":\"%s\",\"line\":%d,\"message\":\"%s\"}\n",
                timestamp, level_to_string(level), g_log_context.node_id,
                g_log_context.cluster_name, component_path, thread_id,
                file, line, escaped_message);
        } else {
            written = snprintf(buf->data + buf->used, LOG_BUFFER_SIZE - buf->used,
                "{\"timestamp\":\"%s\",\"level\":\"%s\",\"node_id\":%u,"
                "\"cluster\":\"%s\",\"thread_id\":%lu,"
                "\"file\":\"%s\",\"line\":%d,\"message\":\"%s\"}\n",
                timestamp, level_to_string(level), g_log_context.node_id,
                g_log_context.cluster_name, thread_id,
                file, line, escaped_message);
        }
    } else {
        written = snprintf(buf->data + buf->used, LOG_BUFFER_SIZE - buf->used,
            "{\"timestamp\":\"%s\",\"level\":\"%s\",\"thread_id\":%lu,"
            "\"file\":\"%s\",\"line\":%d,\"message\":\"%s\"}\n",
            timestamp, level_to_string(level), thread_id,
            file, line, escaped_message);
    }
    
    pthread_mutex_unlock(&g_context_lock);
    
    if (written > 0) buf->used += written;
    
    if (level == LOG_LEVEL_ERROR) {
        fwrite(buf->data, 1, buf->used, stderr);
        fflush(stderr);
        buf->used = 0;
    }
    
    pthread_mutex_unlock(&buf->lock);
}

// ============================================================================
// PUBLIC API
// ============================================================================

void logger_init(void) {
    detect_log_format();  // Check ROOLE_LOG_FORMAT environment variable
    
    g_shutdown = 0;
    pthread_create(&g_flush_thread, NULL, flush_thread_fn, NULL);
}

void logger_shutdown(void) {
    g_shutdown = 1;
    pthread_join(g_flush_thread, NULL);
    
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

void logger_log(log_level_t level, const char *file, int line, const char *fmt, ...) {
    if (level < g_log_level) return;
    
    va_list args;
    va_start(args, fmt);
    
    if (g_log_format == LOG_FORMAT_JSON) {
        logger_log_json(level, file, line, fmt, args);
    } else {
        logger_log_text(level, file, line, fmt, args);
    }
    
    va_end(args);
}

void logger_flush(void) {
    log_buffer_t *buf = get_tls_buffer();
    flush_buffer(buf);
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