// include/roole/common.h

#ifndef ROOLE_COMMON_H
#define ROOLE_COMMON_H

#define _POSIX_C_SOURCE 200809L

#include <stdint.h>
#include <stddef.h>
#include <time.h>
#include "roole/logger.h" 

// Legacy compatibility (redirects to new logger)
#define log_set_level(level) logger_set_level(level)

// ============================================================================
// VERSION & CONSTANTS
// ============================================================================

#define ROOLE_VERSION "0.1.0"
#define MAX_NODE_ID UINT32_MAX

// ============================================================================
// TYPE ALIASES
// ============================================================================

typedef uint16_t node_id_t;
typedef uint64_t execution_id_t;
typedef uint32_t rule_id_t;

// ============================================================================
// ERROR CODES
// ============================================================================

typedef enum {
    RESULT_OK = 0,
    RESULT_ERR_NOMEM = -1,
    RESULT_ERR_INVALID = -2,
    RESULT_ERR_NOTFOUND = -3,
    RESULT_ERR_TIMEOUT = -4,
    RESULT_ERR_NETWORK = -5,
    RESULT_ERR_EXISTS = -6,
    RESULT_ERR_FULL = -7,
    RESULT_ERR_EMPTY = -8
} result_status_t;

// ============================================================================
// LOGGING
// ============================================================================

typedef enum {
    LOG_LEVEL_DEBUG = 0,
    LOG_LEVEL_INFO = 1,
    LOG_LEVEL_WARN = 2,
    LOG_LEVEL_ERROR = 3
} log_level_t;

void log_set_level(log_level_t level);
void std_log(log_level_t level, const char *file, int line, 
               const char *fmt, ...) __attribute__((format(printf, 4, 5)));

#define LOG_DEBUG(...) std_log(LOG_LEVEL_DEBUG, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_INFO(...)  std_log(LOG_LEVEL_INFO,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_WARN(...)  std_log(LOG_LEVEL_WARN,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_ERROR(...) std_log(LOG_LEVEL_ERROR, __FILE__, __LINE__, __VA_ARGS__)

// ============================================================================
// TIMING UTILITIES
// ============================================================================

uint64_t time_now_ms(void);
uint64_t time_now_us(void);
double time_diff_us(const struct timespec *start, const struct timespec *end);
void timespec_now(struct timespec *ts);

// ============================================================================
// MEMORY UTILITIES
// ============================================================================

void* safe_malloc(size_t size);
void* safe_calloc(size_t nmemb, size_t size);
void* safe_realloc(void *ptr, size_t size);
void safe_free(void *ptr);

// ============================================================================
// STRING UTILITIES
// ============================================================================

char* free_strdup(const char *s);
int safe_strncpy(char *dst, const char *src, size_t dst_size);

// ============================================================================
// HASH UTILITIES
// ============================================================================

uint32_t hash_u32(uint32_t x);
uint64_t hash_u64(uint64_t x);
uint32_t hash_string(const char *str);

// ============================================================================
// MACROS
// ============================================================================

#define ROOLE_MIN(a, b) ((a) < (b) ? (a) : (b))
#define ROOLE_MAX(a, b) ((a) > (b) ? (a) : (b))
#define ROOLE_ARRAY_SIZE(arr) (sizeof(arr) / sizeof((arr)[0]))

#endif // ROOLE_COMMON_H