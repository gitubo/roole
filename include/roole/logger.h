// include/roole/logger.h
#ifndef ROOLE_LOGGER_H
#define ROOLE_LOGGER_H

#include <stdint.h>
#include <stdarg.h>
#include <pthread.h>

// ============================================================================
// LOG LEVELS
// ============================================================================

typedef enum {
    LOG_LEVEL_DEBUG = 0,
    LOG_LEVEL_INFO = 1,
    LOG_LEVEL_WARN = 2,
    LOG_LEVEL_ERROR = 3
} log_level_t;

// ============================================================================
// LOG CONTEXT (Node identity)
// ============================================================================

typedef struct log_context {
    uint16_t node_id;
    char cluster_name[64];
    char node_type[16];  // "router" or "worker"
    int initialized;
} log_context_t;

// ============================================================================
// PUBLIC API
// ============================================================================

void logger_init(void);
void logger_shutdown(void);
void logger_set_level(log_level_t level);
void logger_set_context(uint16_t node_id, const char *cluster_name, const char *node_type);
void logger_log(log_level_t level, const char *file, int line, const char *fmt, ...) 
    __attribute__((format(printf, 4, 5)));
void logger_flush(void);
void logger_push_component(const char *component_name);  // e.g., "gossip", "rpc", "executor"
void logger_pop_component(void);

// ============================================================================
// MACROS (Replace existing LOG_* macros)
// ============================================================================

#define LOG_DEBUG(...) logger_log(LOG_LEVEL_DEBUG, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_INFO(...)  logger_log(LOG_LEVEL_INFO,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_WARN(...)  logger_log(LOG_LEVEL_WARN,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_ERROR(...) logger_log(LOG_LEVEL_ERROR, __FILE__, __LINE__, __VA_ARGS__)

#endif // ROOLE_LOGGER_H