// src/core/metrics.c
// Dependency-free metrics implementation

#define _POSIX_C_SOURCE 200809L

#include "roole/metrics.h"
#include "roole/common.h"
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>


// ============================================================================
// REGISTRY MANAGEMENT
// ============================================================================

metrics_registry_t* metrics_registry_init(void) {
    metrics_registry_t *reg = calloc(1, sizeof(metrics_registry_t));
    if (!reg) {
        LOG_ERROR("Failed to allocate metrics registry");
        return NULL;
    }
    
    if (pthread_mutex_init(&reg->lock, NULL) != 0) {
        LOG_ERROR("Failed to initialize registry mutex");
        free(reg);
        return NULL;
    }
    
    reg->count = 0;
    
    LOG_INFO("Metrics registry initialized");
    return reg;
}

void metrics_registry_destroy(metrics_registry_t *reg) {
    if (!reg) return;
    
    pthread_mutex_lock(&reg->lock);
    
    // Destroy all metric mutexes
    for (size_t i = 0; i < MAX_METRICS_PER_REGISTRY; i++) {
        if (reg->metrics[i].active) {
            pthread_mutex_destroy(&reg->metrics[i].lock);
        }
    }
    
    pthread_mutex_unlock(&reg->lock);
    pthread_mutex_destroy(&reg->lock);
    
    free(reg);
    LOG_INFO("Metrics registry destroyed");
}

// ============================================================================
// HELPER: Generate metric key for lookup
// ============================================================================

static void generate_metric_key(char *key_buf, size_t key_buf_size,
                                const char *name,
                                size_t num_labels,
                                const metric_label_t *labels) {
    snprintf(key_buf, key_buf_size, "%s", name);
    
    // Append sorted labels to create unique key
    for (size_t i = 0; i < num_labels; i++) {
        size_t current_len = strlen(key_buf);
        snprintf(key_buf + current_len, key_buf_size - current_len,
                "_%s=%s", labels[i].name, labels[i].value);
    }
}

// ============================================================================
// METRIC CREATION
// ============================================================================

static metrics_t* find_or_create_metric(metrics_registry_t *reg,
                                        const char *name,
                                        const char *help,
                                        metric_type_t type,
                                        size_t num_labels,
                                        const metric_label_t *labels) {
    if (!reg || !name || !help) return NULL;
    
    if (num_labels > MAX_LABELS_PER_METRIC) {
        LOG_ERROR("Too many labels: %zu (max: %d)", num_labels, MAX_LABELS_PER_METRIC);
        return NULL;
    }
    
    char metric_key[512];
    generate_metric_key(metric_key, sizeof(metric_key), name, num_labels, labels);
    
    pthread_mutex_lock(&reg->lock);
    
    // Search for existing metric
    for (size_t i = 0; i < MAX_METRICS_PER_REGISTRY; i++) {
        if (!reg->metrics[i].active) continue;
        
        // Check if name matches
        if (strcmp(reg->metrics[i].name, name) != 0) continue;
        
        // Check if labels match
        if (reg->metrics[i].num_labels != num_labels) continue;
        
        int labels_match = 1;
        for (size_t j = 0; j < num_labels; j++) {
            if (strcmp(reg->metrics[i].labels[j].name, labels[j].name) != 0 ||
                strcmp(reg->metrics[i].labels[j].value, labels[j].value) != 0) {
                labels_match = 0;
                break;
            }
        }
        
        if (labels_match) {
            pthread_mutex_unlock(&reg->lock);
            return &reg->metrics[i];
        }
    }
    
    // Not found, create new metric
    if (reg->count >= MAX_METRICS_PER_REGISTRY) {
        pthread_mutex_unlock(&reg->lock);
        LOG_ERROR("Metrics registry full");
        return NULL;
    }
    
    // Find free slot
    size_t slot = SIZE_MAX;
    for (size_t i = 0; i < MAX_METRICS_PER_REGISTRY; i++) {
        if (!reg->metrics[i].active) {
            slot = i;
            break;
        }
    }
    
    if (slot == SIZE_MAX) {
        pthread_mutex_unlock(&reg->lock);
        return NULL;
    }
    
    metrics_t *m = &reg->metrics[slot];
    memset(m, 0, sizeof(metrics_t));
    
    safe_strncpy(m->name, name, MAX_METRIC_NAME_LEN);
    safe_strncpy(m->help, help, MAX_METRIC_HELP_LEN);
    m->type = type;
    m->value = 0.0;
    m->num_labels = num_labels;
    
    for (size_t i = 0; i < num_labels; i++) {
        safe_strncpy(m->labels[i].name, labels[i].name, MAX_LABEL_NAME_LEN);
        safe_strncpy(m->labels[i].value, labels[i].value, MAX_LABEL_VALUE_LEN);
    }
    
    if (pthread_mutex_init(&m->lock, NULL) != 0) {
        pthread_mutex_unlock(&reg->lock);
        LOG_ERROR("Failed to initialize metric mutex");
        return NULL;
    }
    
    m->active = 1;
    reg->count++;
    
    pthread_mutex_unlock(&reg->lock);
    
    LOG_DEBUG("Created new %s metric: %s", 
              type == METRIC_TYPE_COUNTER ? "counter" : "gauge", name);
    
    return m;
}

metrics_t* metrics_get_or_create_counter(metrics_registry_t *reg,
                                          const char *name,
                                          const char *help,
                                          size_t num_labels,
                                          const metric_label_t *labels) {
    return find_or_create_metric(reg, name, help, METRIC_TYPE_COUNTER, num_labels, labels);
}

metrics_t* metrics_get_or_create_gauge(metrics_registry_t *reg,
                                        const char *name,
                                        const char *help,
                                        size_t num_labels,
                                        const metric_label_t *labels) {
    return find_or_create_metric(reg, name, help, METRIC_TYPE_GAUGE, num_labels, labels);
}

// ============================================================================
// METRIC OPERATIONS
// ============================================================================

void metrics_counter_inc(metrics_t *metric) {
    if (!metric) return;
    pthread_mutex_lock(&metric->lock);
    metric->value += 1.0;
    pthread_mutex_unlock(&metric->lock);
}

void metrics_counter_add(metrics_t *metric, double val) {
    if (!metric || val < 0.0) return;
    pthread_mutex_lock(&metric->lock);
    metric->value += val;
    pthread_mutex_unlock(&metric->lock);
}

void metrics_gauge_set(metrics_t *metric, double val) {
    if (!metric) return;
    pthread_mutex_lock(&metric->lock);
    metric->value = val;
    pthread_mutex_unlock(&metric->lock);
}

void metrics_gauge_inc(metrics_t *metric) {
    if (!metric) return;
    pthread_mutex_lock(&metric->lock);
    metric->value += 1.0;
    pthread_mutex_unlock(&metric->lock);
}

void metrics_gauge_dec(metrics_t *metric) {
    if (!metric) return;
    pthread_mutex_lock(&metric->lock);
    metric->value -= 1.0;
    pthread_mutex_unlock(&metric->lock);
}

// ============================================================================
// PROMETHEUS TEXT FORMAT RENDERING
// ============================================================================

char* metrics_registry_render_prometheus(metrics_registry_t *reg) {
    if (!reg) return NULL;
    
    // Allocate large buffer for output
    size_t buffer_size = 65536;  // 64KB should be enough
    char *buffer = malloc(buffer_size);
    if (!buffer) {
        LOG_ERROR("Failed to allocate render buffer");
        return NULL;
    }
    
    buffer[0] = '\0';
    size_t offset = 0;
    
    pthread_mutex_lock(&reg->lock);
    
    // ========================================================================
    // RENDER COUNTER/GAUGE METRICS
    // ========================================================================
    
    for (size_t i = 0; i < MAX_METRICS_PER_REGISTRY; i++) {
        if (!reg->metrics[i].active) continue;
        
        metrics_t *m = &reg->metrics[i];
        pthread_mutex_lock(&m->lock);
        
        if (offset + 512 >= buffer_size) {
            LOG_WARN("Metrics buffer full, truncating output");
            pthread_mutex_unlock(&m->lock);
            break;
        }
        
        // HELP
        int written = snprintf(buffer + offset, buffer_size - offset,
                              "# HELP %s %s\n", m->name, m->help);
        if (written < 0 || (size_t)written >= buffer_size - offset) {
            pthread_mutex_unlock(&m->lock);
            break;
        }
        offset += written;
        
        // TYPE
        written = snprintf(buffer + offset, buffer_size - offset,
                          "# TYPE %s %s\n",
                          m->name,
                          m->type == METRIC_TYPE_COUNTER ? "counter" : "gauge");
        if (written < 0 || (size_t)written >= buffer_size - offset) {
            pthread_mutex_unlock(&m->lock);
            break;
        }
        offset += written;
        
        // Metric name
        written = snprintf(buffer + offset, buffer_size - offset, "%s", m->name);
        if (written < 0 || (size_t)written >= buffer_size - offset) {
            pthread_mutex_unlock(&m->lock);
            break;
        }
        offset += written;
        
        // Labels
        if (m->num_labels > 0) {
            written = snprintf(buffer + offset, buffer_size - offset, "{");
            if (written < 0 || (size_t)written >= buffer_size - offset) {
                pthread_mutex_unlock(&m->lock);
                break;
            }
            offset += written;
            
            for (size_t j = 0; j < m->num_labels; j++) {
                written = snprintf(buffer + offset, buffer_size - offset,
                                  "%s%s=\"%s\"",
                                  j > 0 ? "," : "",
                                  m->labels[j].name,
                                  m->labels[j].value);
                if (written < 0 || (size_t)written >= buffer_size - offset) {
                    pthread_mutex_unlock(&m->lock);
                    goto done;
                }
                offset += written;
            }
            
            written = snprintf(buffer + offset, buffer_size - offset, "}");
            if (written < 0 || (size_t)written >= buffer_size - offset) {
                pthread_mutex_unlock(&m->lock);
                break;
            }
            offset += written;
        }
        
        // Value
        written = snprintf(buffer + offset, buffer_size - offset,
                          " %.0f\n", m->value);
        if (written < 0 || (size_t)written >= buffer_size - offset) {
            pthread_mutex_unlock(&m->lock);
            break;
        }
        offset += written;
        
        pthread_mutex_unlock(&m->lock);
    }
    
    // ========================================================================
    // RENDER HISTOGRAM METRICS (FIXED)
    // ========================================================================
    
    LOG_DEBUG("Rendering %zu histograms", reg->histogram_count);
    
    for (size_t i = 0; i < reg->histogram_count; i++) {
        histogram_metric_t *h = &reg->histograms[i];
        
        if (!h->active) {
            LOG_DEBUG("  Histogram %zu inactive, skipping", i);
            continue;
        }
        
        pthread_mutex_lock(&h->lock);
        
        if (offset + 2048 >= buffer_size) {
            LOG_WARN("Buffer full, cannot render histogram %s", h->name);
            pthread_mutex_unlock(&h->lock);
            break;
        }
        
        LOG_DEBUG("Rendering histogram: %s (buckets=%zu)", h->name, h->buckets.count);
        
        // HELP
        int written = snprintf(buffer + offset, buffer_size - offset,
                              "# HELP %s %s\n", h->name, h->help);
        offset += written;
        
        // TYPE
        written = snprintf(buffer + offset, buffer_size - offset,
                          "# TYPE %s histogram\n", h->name);
        offset += written;
        
        // Build label string (reusable for all histogram lines)
        char label_str[512] = {0};
        if (h->num_labels > 0) {
            size_t label_offset = 0;
            for (size_t j = 0; j < h->num_labels; j++) {
                label_offset += snprintf(label_str + label_offset, 
                                        sizeof(label_str) - label_offset,
                                        "%s%s=\"%s\"",
                                        j > 0 ? "," : "",
                                        h->labels[j].name,
                                        h->labels[j].value);
            }
        }
        
        // Render buckets
        for (size_t j = 0; j < h->buckets.count; j++) {
            if (offset + 256 >= buffer_size) break;
            
            // Format: metric_name_bucket{labels,le="upper_bound"} count
            written = snprintf(buffer + offset, buffer_size - offset,
                              "%s_bucket{%s%sle=\"",
                              h->name,
                              label_str,
                              h->num_labels > 0 ? "," : "");
            offset += written;
            
            // Format upper bound (+Inf for last bucket)
            if (isinf(h->buckets.upper_bounds[j])) {
                written = snprintf(buffer + offset, buffer_size - offset, "+Inf");
            } else {
                written = snprintf(buffer + offset, buffer_size - offset,
                                  "%.2f", h->buckets.upper_bounds[j]);
            }
            offset += written;
            
            // ✅ FIX: Use atomic load for bucket counts
            uint64_t bucket_count = __atomic_load_n(&h->bucket_counts[j], __ATOMIC_RELAXED);
            
            written = snprintf(buffer + offset, buffer_size - offset,
                              "\"} %lu\n", bucket_count);
            offset += written;
        }
        
        // ✅ FIX: Use atomic loads for sum and count
        uint64_t total_count = __atomic_load_n(&h->count, __ATOMIC_RELAXED);
        uint64_t total_sum = __atomic_load_n(&h->sum, __ATOMIC_RELAXED);
        
        // Render _sum
        written = snprintf(buffer + offset, buffer_size - offset,
                          "%s_sum{%s} %lu\n",
                          h->name, label_str, total_sum);
        offset += written;
        
        // Render _count
        written = snprintf(buffer + offset, buffer_size - offset,
                          "%s_count{%s} %lu\n",
                          h->name, label_str, total_count);
        offset += written;
        
        pthread_mutex_unlock(&h->lock);
    }
    
done:
    pthread_mutex_unlock(&reg->lock);
    
    LOG_DEBUG("Rendered %zu bytes of metrics (histograms included)", offset);
    return buffer;
}
histogram_metric_t* metrics_get_or_create_histogram(
    metrics_registry_t *reg,
    const char *name,
    const char *help,
    histogram_buckets_type_t buckets_type,
    size_t num_labels,
    const metric_label_t *labels) {
    
    if (!reg || !name) return NULL;
    
    char metric_key[512];
    generate_metric_key(metric_key, sizeof(metric_key), name, num_labels, labels);
    
    pthread_mutex_lock(&reg->lock);
    
    // Search existing
    for (size_t i = 0; i < reg->histogram_count; i++) {
        if (!reg->histograms[i].active) continue;
        if (strcmp(reg->histograms[i].name, name) == 0) {
            // Match labels
            int match = 1;
            for (size_t j = 0; j < num_labels; j++) {
                if (strcmp(reg->histograms[i].labels[j].name, labels[j].name) != 0 ||
                    strcmp(reg->histograms[i].labels[j].value, labels[j].value) != 0) {
                    match = 0;
                    break;
                }
            }
            if (match) {
                pthread_mutex_unlock(&reg->lock);
                return &reg->histograms[i];
            }
        }
    }
    
    // Create new
    if (reg->histogram_count >= MAX_METRICS_PER_REGISTRY) {
        pthread_mutex_unlock(&reg->lock);
        return NULL;
    }
    
    histogram_metric_t *h = &reg->histograms[reg->histogram_count];
    memset(h, 0, sizeof(histogram_metric_t));
    
    safe_strncpy(h->name, name, MAX_METRIC_NAME_LEN);
    safe_strncpy(h->help, help, MAX_METRIC_HELP_LEN);
    h->num_labels = num_labels;
    for (size_t i = 0; i < num_labels; i++) {
        h->labels[i] = labels[i];
    }
    
    h->buckets = metrics_get_buckets(buckets_type);
    h->count = 0;
    h->sum = 0;
    for (size_t i = 0; i < h->buckets.count; i++) {
        h->bucket_counts[i] = 0;
    }
    
    pthread_mutex_init(&h->lock, NULL);
    h->active = 1;
    reg->histogram_count++;
    
    pthread_mutex_unlock(&reg->lock);
    return h;
}

histogram_buckets_t metrics_get_buckets(histogram_buckets_type_t type) {
    histogram_buckets_t buckets = {0};
    
    switch (type) {
        case HISTOGRAM_BUCKETS_LATENCY_MS:
            buckets.count = 9;
            buckets.upper_bounds[0] = 1;
            buckets.upper_bounds[1] = 5;
            buckets.upper_bounds[2] = 10;
            buckets.upper_bounds[3] = 50;
            buckets.upper_bounds[4] = 100;
            buckets.upper_bounds[5] = 500;
            buckets.upper_bounds[6] = 1000;
            buckets.upper_bounds[7] = 5000;
            buckets.upper_bounds[8] = INFINITY;
            break;
            
        case HISTOGRAM_BUCKETS_LATENCY_US:
            buckets.count = 8;
            buckets.upper_bounds[0] = 100;
            buckets.upper_bounds[1] = 500;
            buckets.upper_bounds[2] = 1000;
            buckets.upper_bounds[3] = 5000;
            buckets.upper_bounds[4] = 10000;
            buckets.upper_bounds[5] = 50000;
            buckets.upper_bounds[6] = 100000;
            buckets.upper_bounds[7] = INFINITY;
            break;
            
        case HISTOGRAM_BUCKETS_SIZE_BYTES:
            buckets.count = 9;
            buckets.upper_bounds[0] = 256;
            buckets.upper_bounds[1] = 1024;
            buckets.upper_bounds[2] = 4096;
            buckets.upper_bounds[3] = 16384;
            buckets.upper_bounds[4] = 65536;
            buckets.upper_bounds[5] = 262144;
            buckets.upper_bounds[6] = 1048576;
            buckets.upper_bounds[7] = 4194304;
            buckets.upper_bounds[8] = INFINITY;
            break;
            
        case HISTOGRAM_BUCKETS_CUSTOM:
            // User must set manually
            buckets.count = 0;
            break;
    }
    return buckets;
}

void metrics_histogram_observe(histogram_metric_t *metric, int value) {
    if (!metric) return;
    
    __sync_fetch_and_add(&metric->count, 1);
    __sync_fetch_and_add(&metric->sum, value);  
    
    for (size_t i = 0; i < metric->buckets.count; i++) {
        if (value <= metric->buckets.upper_bounds[i]) {
            __sync_fetch_and_add(&metric->bucket_counts[i], 1);
            break;
        }
    }
}