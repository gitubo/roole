// src/core/common.c

#define _POSIX_C_SOURCE 200809L

#include "roole/common.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdarg.h>
#include <string.h>

// ============================================================================
// TIMING UTILITIES
// ============================================================================

uint64_t time_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

uint64_t time_now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000 + (uint64_t)ts.tv_nsec / 1000;
}

double time_diff_us(const struct timespec *start, const struct timespec *end) {
    long seconds = end->tv_sec - start->tv_sec;
    long nanoseconds = end->tv_nsec - start->tv_nsec;
    return (double)seconds * 1e6 + (double)nanoseconds / 1e3;
}

void timespec_now(struct timespec *ts) {
    clock_gettime(CLOCK_MONOTONIC, ts);
}

// ============================================================================
// LOGGING
// ============================================================================

static log_level_t g_log_level = LOG_LEVEL_INFO;

void log_set_level(log_level_t level) {
    g_log_level = level;
}

void std_log(log_level_t level, const char *file, int line, 
               const char *fmt, ...) {
    if (level < g_log_level) return;
    
    const char *level_str;
    switch (level) {
        case LOG_LEVEL_DEBUG: level_str = "DEBUG"; break;
        case LOG_LEVEL_INFO:  level_str = "INFO "; break;
        case LOG_LEVEL_WARN:  level_str = "WARN "; break;
        case LOG_LEVEL_ERROR: level_str = "ERROR"; break;
        default: level_str = "?????"; break;
    }
    
    uint64_t now_ms = time_now_ms();
    
    FILE *out = (level >= LOG_LEVEL_ERROR) ? stderr : stdout;
    fprintf(out, "[%lu][%s][%s:%d] ", now_ms, level_str, file, line);
    
    va_list args;
    va_start(args, fmt);
    vfprintf(out, fmt, args);
    va_end(args);
    
    fprintf(out, "\n");
    fflush(out);
}

// ============================================================================
// MEMORY UTILITIES
// ============================================================================

void* safe_malloc(size_t size) {
    void *ptr = malloc(size);
    if (!ptr && size > 0) {
        LOG_ERROR("Failed to allocate %zu bytes", size);
    }
    return ptr;
}

void* safe_calloc(size_t nmemb, size_t size) {
    void *ptr = calloc(nmemb, size);
    if (!ptr && nmemb > 0 && size > 0) {
        LOG_ERROR("Failed to calloc %zu * %zu bytes", nmemb, size);
    }
    return ptr;
}

void* safe_realloc(void *ptr, size_t size) {
    void *new_ptr = realloc(ptr, size);
    if (!new_ptr && size > 0) {
        LOG_ERROR("Failed to realloc %zu bytes", size);
    }
    return new_ptr;
}

void safe_free(void *ptr) {
    free(ptr);
}

// ============================================================================
// STRING UTILITIES
// ============================================================================

char* free_strdup(const char *s) {
    if (!s) return NULL;
    size_t len = strlen(s);
    char *dup = safe_malloc(len + 1);
    if (dup) {
        memcpy(dup, s, len + 1);
    }
    return dup;
}

int safe_strncpy(char *dst, const char *src, size_t dst_size) {
    if (!dst || dst_size == 0) return -1;
    if (!src) {
        dst[0] = '\0';
        return 0;
    }
    
    size_t i;
    for (i = 0; i < dst_size - 1 && src[i] != '\0'; i++) {
        dst[i] = src[i];
    }
    dst[i] = '\0';
    
    return (src[i] == '\0') ? 0 : -1; // -1 if truncated
}

// ============================================================================
// HASH UTILITIES (for hash tables)
// ============================================================================

uint32_t hash_u32(uint32_t x) {
    // MurmurHash3 finalizer
    x ^= x >> 16;
    x *= 0x85ebca6b;
    x ^= x >> 13;
    x *= 0xc2b2ae35;
    x ^= x >> 16;
    return x;
}

uint64_t hash_u64(uint64_t x) {
    // MurmurHash3 64-bit finalizer
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

uint32_t hash_string(const char *str) {
    if (!str) return 0;
    
    // djb2 hash
    uint32_t hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    return hash;
}