// include/roole/common.h

#ifndef ROOLE_COMMON_H
#define ROOLE_COMMON_H

#define _POSIX_C_SOURCE 200809L

#include <stdint.h>
#include <stddef.h>
#include <time.h>
#include "roole/logger.h" 

// ============================================================================
// ENHANCED ERROR HANDLING
// ============================================================================

// Traditional error codes (kept for backward compatibility)
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

// Enhanced result type with rich error context
typedef struct result {
    int code;                      // result_status_t
    char message[256];             // Human-readable error description
    const char *source_file;       // __FILE__ where error originated
    int source_line;               // __LINE__ where error originated
    const char *function;          // Function name
} result_t;

// ============================================================================
// RESULT CONSTRUCTION MACROS
// ============================================================================

// Success result
#define RESULT_SUCCESS() ((result_t){ \
    .code = RESULT_OK, \
    .message = {0}, \
    .source_file = NULL, \
    .source_line = 0, \
    .function = NULL \
})

// Error result with formatted message
#define RESULT_ERROR(error_code, fmt, ...) ((result_t){ \
    .code = (error_code), \
    .message = {0}, \
    .source_file = __FILE__, \
    .source_line = __LINE__, \
    .function = __func__ \
}); \
snprintf(((result_t*)&(result_t){.code=(error_code),.source_file=__FILE__,.source_line=__LINE__,.function=__func__})->message, \
         256, fmt, ##__VA_ARGS__)

// Simple error without message
#define RESULT_ERROR_SIMPLE(error_code) ((result_t){ \
    .code = (error_code), \
    .message = {0}, \
    .source_file = __FILE__, \
    .source_line = __LINE__, \
    .function = __func__ \
})

// ============================================================================
// RESULT CHECKING AND LOGGING
// ============================================================================

// Check if result is successful
static inline int result_is_ok(const result_t *result) {
    return result->code == RESULT_OK;
}

// Check if result is an error
static inline int result_is_error(const result_t *result) {
    return result->code != RESULT_OK;
}

// Log error result
static inline void result_log_error(const result_t *result) {
    if (result_is_error(result)) {
        if (result->message[0] != '\0') {
            LOG_ERROR("[%s:%d in %s()] Error %d: %s",
                     result->source_file ? result->source_file : "unknown",
                     result->source_line,
                     result->function ? result->function : "unknown",
                     result->code,
                     result->message);
        } else {
            LOG_ERROR("[%s:%d in %s()] Error %d",
                     result->source_file ? result->source_file : "unknown",
                     result->source_line,
                     result->function ? result->function : "unknown",
                     result->code);
        }
    }
}

// Get error code from result (for compatibility with old code)
static inline int result_code(const result_t *result) {
    return result->code;
}

// Convert old-style int return to result_t
static inline result_t result_from_int(int code) {
    result_t result = {
        .code = code,
        .message = {0},
        .source_file = NULL,
        .source_line = 0,
        .function = NULL
    };
    return result;
}

// ============================================================================
// CONVENIENCE MACROS FOR COMMON ERROR PATTERNS
// ============================================================================

// Return error if condition is false
#define RETURN_IF_FALSE(cond, error_code, fmt, ...) \
    do { \
        if (!(cond)) { \
            result_t __err = RESULT_ERROR_SIMPLE(error_code); \
            snprintf(__err.message, sizeof(__err.message), fmt, ##__VA_ARGS__); \
            result_log_error(&__err); \
            return __err; \
        } \
    } while(0)

// Return error if pointer is NULL
#define RETURN_IF_NULL(ptr, fmt, ...) \
    RETURN_IF_FALSE((ptr) != NULL, RESULT_ERR_INVALID, fmt, ##__VA_ARGS__)

// Propagate error from called function
#define PROPAGATE_ERROR(result_expr) \
    do { \
        result_t __res = (result_expr); \
        if (result_is_error(&__res)) { \
            result_log_error(&__res); \
            return __res; \
        } \
    } while(0)

// Log and return error on failure
#define CHECK_RESULT(result_expr, error_code, fmt, ...) \
    do { \
        result_t __res = (result_expr); \
        if (result_is_error(&__res)) { \
            result_t __err = RESULT_ERROR_SIMPLE(error_code); \
            snprintf(__err.message, sizeof(__err.message), fmt, ##__VA_ARGS__); \
            result_log_error(&__err); \
            return __err; \
        } \
    } while(0)

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