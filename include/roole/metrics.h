// include/roole/metrics.h
// Dependency-free metrics system with Prometheus text format export

#ifndef ROOLE_METRICS_H
#define ROOLE_METRICS_H

#include "roole/common.h"
#include <stdint.h>
#include <pthread.h>

#define MAX_METRIC_NAME_LEN 128
#define MAX_METRIC_HELP_LEN 256
#define MAX_METRICS_PER_REGISTRY 256
#define MAX_LABEL_NAME_LEN 64
#define MAX_LABEL_VALUE_LEN 128
#define MAX_LABELS_PER_METRIC 8

// ============================================================================
// METRIC TYPES
// ============================================================================

typedef enum {
    METRIC_TYPE_COUNTER = 0,
    METRIC_TYPE_GAUGE = 1
} metric_type_t;

typedef struct metric_label {
    char name[MAX_LABEL_NAME_LEN];
    char value[MAX_LABEL_VALUE_LEN];
} metric_label_t;

// ============================================================================
// INTERNAL STRUCTURES
// ============================================================================

typedef struct metrics {
    char name[MAX_METRIC_NAME_LEN];
    char help[MAX_METRIC_HELP_LEN];
    metric_type_t type;
    
    metric_label_t labels[MAX_LABELS_PER_METRIC];
    size_t num_labels;
    
    double value;
    pthread_mutex_t lock;
    
    int active;
} metrics_t;

typedef struct metrics_registry {
    metrics_t metrics[MAX_METRICS_PER_REGISTRY];
    size_t count;
    pthread_mutex_t lock;
} metrics_registry_t;

// ============================================================================
// HISTOGRAM CONFIGURATION
// ============================================================================

#define HISTOGRAM_MAX_BUCKETS 16

// Predefined bucket configurations
typedef enum {
    HISTOGRAM_BUCKETS_LATENCY_MS,     // 1, 5, 10, 50, 100, 500, 1000, 5000, +Inf
    HISTOGRAM_BUCKETS_LATENCY_US,     // 100, 500, 1000, 5000, 10000, 50000, 100000, +Inf
    HISTOGRAM_BUCKETS_SIZE_BYTES,     // 256, 1K, 4K, 16K, 64K, 256K, 1M, 4M, +Inf
    HISTOGRAM_BUCKETS_CUSTOM
} histogram_buckets_type_t;

typedef struct histogram_buckets {
    double upper_bounds[HISTOGRAM_MAX_BUCKETS];
    size_t count;
} histogram_buckets_t;

// ============================================================================
// HISTOGRAM STRUCTURE
// ============================================================================

typedef struct histogram_metric {
    char name[MAX_METRIC_NAME_LEN];
    char help[MAX_METRIC_HELP_LEN];
    
    metric_label_t labels[MAX_LABELS_PER_METRIC];
    size_t num_labels;
    
    histogram_buckets_t buckets;
    
    // Bucket counters (atomic)
    _Atomic uint64_t bucket_counts[HISTOGRAM_MAX_BUCKETS];
    _Atomic uint64_t count;
    _Atomic double sum;
    
    pthread_mutex_t lock;
    int active;
} histogram_metric_t;

// ============================================================================
// HISTOGRAM API
// ============================================================================

/**
 * Get or create a histogram metric
 */
histogram_metric_t* metrics_get_or_create_histogram(
    metrics_registry_t *reg,
    const char *name,
    const char *help,
    histogram_buckets_type_t buckets_type,
    size_t num_labels,
    const metric_label_t *labels
);

/**
 * Observe a value in the histogram
 * Thread-safe: uses atomic operations
 */
void metrics_histogram_observe(histogram_metric_t *metric, double value);

/**
 * Get predefined bucket configuration
 */
histogram_buckets_t metrics_get_buckets(histogram_buckets_type_t type);

/**
 * Create custom buckets
 */
histogram_buckets_t metrics_custom_buckets(const double *bounds, size_t count);

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