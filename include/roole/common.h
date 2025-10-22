// include/roole/common.h

#ifndef ROOLE_COMMON_H
#define ROOLE_COMMON_H

#define _POSIX_C_SOURCE 200809L

#include <stdint.h>
#include <stddef.h>
#include <time.h>

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
typedef uint32_t dag_id_t;

// ============================================================================
// ERROR CODES
// ============================================================================

typedef enum {
    ROOLE_OK = 0,
    ROOLE_ERR_NOMEM = -1,
    ROOLE_ERR_INVALID = -2,
    ROOLE_ERR_NOTFOUND = -3,
    ROOLE_ERR_TIMEOUT = -4,
    ROOLE_ERR_NETWORK = -5,
    ROOLE_ERR_EXISTS = -6,
    ROOLE_ERR_FULL = -7,
    ROOLE_ERR_EMPTY = -8
} roole_status_t;

// ============================================================================
// LOGGING
// ============================================================================

typedef enum {
    ROOLE_LOG_DEBUG = 0,
    ROOLE_LOG_INFO = 1,
    ROOLE_LOG_WARN = 2,
    ROOLE_LOG_ERROR = 3
} roole_log_level_t;

void roole_log_set_level(roole_log_level_t level);
void roole_log(roole_log_level_t level, const char *file, int line, 
               const char *fmt, ...) __attribute__((format(printf, 4, 5)));

#define ROOLE_LOG_DEBUG(...) roole_log(ROOLE_LOG_DEBUG, __FILE__, __LINE__, __VA_ARGS__)
#define ROOLE_LOG_INFO(...)  roole_log(ROOLE_LOG_INFO,  __FILE__, __LINE__, __VA_ARGS__)
#define ROOLE_LOG_WARN(...)  roole_log(ROOLE_LOG_WARN,  __FILE__, __LINE__, __VA_ARGS__)
#define ROOLE_LOG_ERROR(...) roole_log(ROOLE_LOG_ERROR, __FILE__, __LINE__, __VA_ARGS__)

// ============================================================================
// TIMING UTILITIES
// ============================================================================

uint64_t roole_time_now_ms(void);
uint64_t roole_time_now_us(void);
double roole_time_diff_us(const struct timespec *start, const struct timespec *end);
void roole_timespec_now(struct timespec *ts);

// ============================================================================
// MEMORY UTILITIES
// ============================================================================

void* roole_malloc(size_t size);
void* roole_calloc(size_t nmemb, size_t size);
void* roole_realloc(void *ptr, size_t size);
void roole_free(void *ptr);

// ============================================================================
// STRING UTILITIES
// ============================================================================

char* roole_strdup(const char *s);
int roole_strncpy_safe(char *dst, const char *src, size_t dst_size);

// ============================================================================
// HASH UTILITIES
// ============================================================================

uint32_t roole_hash_u32(uint32_t x);
uint64_t roole_hash_u64(uint64_t x);
uint32_t roole_hash_string(const char *str);

// ============================================================================
// MACROS
// ============================================================================

#define ROOLE_MIN(a, b) ((a) < (b) ? (a) : (b))
#define ROOLE_MAX(a, b) ((a) > (b) ? (a) : (b))
#define ROOLE_ARRAY_SIZE(arr) (sizeof(arr) / sizeof((arr)[0]))

#endif // ROOLE_COMMON_H