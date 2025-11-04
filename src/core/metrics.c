// src/core/metrics.c
// Dependency-free metrics implementation

#define _POSIX_C_SOURCE 200809L

#include "roole/metrics.h"
#include "roole/common.h"
#include <stdlib.h>
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
    
    // Render each metric
    for (size_t i = 0; i < MAX_METRICS_PER_REGISTRY; i++) {
        if (!reg->metrics[i].active) continue;
        
        metrics_t *m = &reg->metrics[i];
        
        pthread_mutex_lock(&m->lock);
        
        // Format: # HELP metric_name help text
        int written = snprintf(buffer + offset, buffer_size - offset,
                              "# HELP %s %s\n", m->name, m->help);
        if (written < 0 || (size_t)written >= buffer_size - offset) {
            pthread_mutex_unlock(&m->lock);
            break;
        }
        offset += written;
        
        // Format: # TYPE metric_name counter|gauge
        written = snprintf(buffer + offset, buffer_size - offset,
                          "# TYPE %s %s\n",
                          m->name,
                          m->type == METRIC_TYPE_COUNTER ? "counter" : "gauge");
        if (written < 0 || (size_t)written >= buffer_size - offset) {
            pthread_mutex_unlock(&m->lock);
            break;
        }
        offset += written;
        
        // Format: metric_name{label1="value1",label2="value2"} value
        written = snprintf(buffer + offset, buffer_size - offset, "%s", m->name);
        if (written < 0 || (size_t)written >= buffer_size - offset) {
            pthread_mutex_unlock(&m->lock);
            break;
        }
        offset += written;
        
        // Add labels if present
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
        
        // Add value
        written = snprintf(buffer + offset, buffer_size - offset,
                          " %.0f\n", m->value);
        if (written < 0 || (size_t)written >= buffer_size - offset) {
            pthread_mutex_unlock(&m->lock);
            break;
        }
        offset += written;
        
        pthread_mutex_unlock(&m->lock);
    }
    
done:
    pthread_mutex_unlock(&reg->lock);
    
    return buffer;
}