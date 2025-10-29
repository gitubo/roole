// include/roole/metrics.h
// Dependency-free metrics system with Prometheus text format export

#ifndef ROOLE_METRICS_H
#define ROOLE_METRICS_H

#include "roole/common.h"
#include <stdint.h>
#include <pthread.h>

// ============================================================================
// METRIC TYPES
// ============================================================================

typedef enum {
    METRIC_TYPE_COUNTER = 0,
    METRIC_TYPE_GAUGE = 1
} metric_type_t;

// ============================================================================
// METRIC LABEL
// ============================================================================

#define MAX_LABEL_NAME_LEN 64
#define MAX_LABEL_VALUE_LEN 128
#define MAX_LABELS_PER_METRIC 8

typedef struct metric_label {
    char name[MAX_LABEL_NAME_LEN];
    char value[MAX_LABEL_VALUE_LEN];
} metric_label_t;

// ============================================================================
// METRIC STRUCTURE (opaque to users)
// ============================================================================

typedef struct metrics metrics_t;
typedef struct metrics_registry metrics_registry_t;

// ============================================================================
// REGISTRY MANAGEMENT
// ============================================================================

/**
 * Initialize a new metrics registry
 */
metrics_registry_t* metrics_registry_init(void);

/**
 * Destroy metrics registry and free all resources
 */
void metrics_registry_destroy(metrics_registry_t *reg);

// ============================================================================
// METRIC CREATION (get-or-create pattern)
// ============================================================================

/**
 * Get or create a counter metric
 * @param reg Registry
 * @param name Metric name (e.g., "messages_processed_total")
 * @param help Help text
 * @param num_labels Number of labels
 * @param labels Array of labels (can be NULL if num_labels == 0)
 * @return Metric handle, or NULL on error
 */
metrics_t* metrics_get_or_create_counter(metrics_registry_t *reg, 
                                          const char *name, 
                                          const char *help,
                                          size_t num_labels, 
                                          const metric_label_t *labels);

/**
 * Get or create a gauge metric
 */
metrics_t* metrics_get_or_create_gauge(metrics_registry_t *reg,
                                        const char *name,
                                        const char *help,
                                        size_t num_labels,
                                        const metric_label_t *labels);

// ============================================================================
// METRIC OPERATIONS (thread-safe)
// ============================================================================

/**
 * Increment counter by 1
 */
void metrics_counter_inc(metrics_t *metric);

/**
 * Add value to counter
 */
void metrics_counter_add(metrics_t *metric, double val);

/**
 * Set gauge to specific value
 */
void metrics_gauge_set(metrics_t *metric, double val);

/**
 * Increment gauge by 1
 */
void metrics_gauge_inc(metrics_t *metric);

/**
 * Decrement gauge by 1
 */
void metrics_gauge_dec(metrics_t *metric);

// ============================================================================
// PROMETHEUS TEXT FORMAT RENDERING
// ============================================================================

/**
 * Render all metrics in Prometheus text format
 * @param reg Registry
 * @return Dynamically allocated string (caller must free()), or NULL on error
 */
char* metrics_registry_render_prometheus(metrics_registry_t *reg);

#endif // ROOLE_METRICS_H