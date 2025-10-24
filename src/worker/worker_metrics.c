// src/worker/worker_metrics.c

#define _POSIX_C_SOURCE 200809L

#include "roole/worker_metrics.h"
#include "roole/common.h"

#ifdef ENABLE_METRICS

#include <prom.h>
#include <promhttp.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// ============================================================================
// METRICS STRUCTURE
// ============================================================================

struct worker_metrics {
    node_id_t worker_id;
    uint16_t metrics_port;
    uint64_t start_time_ms;
    
    // Prometheus registry
    prom_registry_t *registry;
    
    // HTTP server thread
    pthread_t http_thread;
    int shutdown_flag;
    
    // COUNTERS
    prom_counter_t *messages_processed_total;
    prom_counter_t *messages_failed_total;
    prom_counter_t *heartbeats_sent_total;
    
    // GAUGES
    prom_gauge_t *queue_size;
    prom_gauge_t *active_executions;
    prom_gauge_t *uptime_seconds;
    
    // HISTOGRAMS
    prom_histogram_t *processing_time_us;
    prom_histogram_t *queue_wait_time_us;
};

// ============================================================================
// HTTP SERVER THREAD (serves /metrics endpoint)
// ============================================================================

static void* metrics_http_server_thread(void *arg) {
    worker_metrics_t *metrics = (worker_metrics_t*)arg;
    
    LOF_INFO("WORKER", "Metrics HTTP server starting on port %u", metrics->metrics_port);
    
    // Start Prometheus HTTP server (blocking)
    // This serves GET /metrics automatically
    int result = promhttp_start_daemon(metrics->metrics_port, NULL);
    
    if (result != 0) {
        LOG_ERROR_C("WORKER", "Failed to start metrics HTTP server on port %u", 
                    metrics->metrics_port);
    }
    
    return NULL;
}

// ============================================================================
// INITIALIZATION
// ============================================================================

worker_metrics_t* worker_metrics_init(node_id_t worker_id, uint16_t metrics_port) {
    worker_metrics_t *m = calloc(1, sizeof(worker_metrics_t));
    if (!m) {
        LOG_ERROR_C("WORKER", "Failed to allocate metrics structure");
        return NULL;
    }
    
    m->worker_id = worker_id;
    m->metrics_port = metrics_port;
    m->start_time_ms = time_now_ms();
    m->shutdown_flag = 0;
    
    // Initialize Prometheus collector registry
    m->registry = prom_collector_registry_default();
    if (!m->registry) {
        LOG_ERROR_C("WORKER", "Failed to get Prometheus registry");
        free(m);
        return NULL;
    }
    
    // Create worker_id label
    char worker_id_str[16];
    snprintf(worker_id_str, sizeof(worker_id_str), "%u", worker_id);
    const char *label_names[] = {"worker_id"};
    const char *label_values[] = {worker_id_str};
    
    // ========================================================================
    // COUNTERS
    // ========================================================================
    
    m->messages_processed_total = prom_counter_new(
        "roole_worker_messages_processed_total",
        "Total number of messages successfully processed",
        1, label_names
    );
    if (m->messages_processed_total) {
        prom_collector_registry_must_register_metric(m->messages_processed_total);
    }
    
    m->messages_failed_total = prom_counter_new(
        "roole_worker_messages_failed_total",
        "Total number of messages that failed processing",
        1, label_names
    );
    if (m->messages_failed_total) {
        prom_collector_registry_must_register_metric(m->messages_failed_total);
    }
    
    m->heartbeats_sent_total = prom_counter_new(
        "roole_worker_heartbeats_sent_total",
        "Total number of heartbeats sent to router",
        1, label_names
    );
    if (m->heartbeats_sent_total) {
        prom_collector_registry_must_register_metric(m->heartbeats_sent_total);
    }
    
    // ========================================================================
    // GAUGES
    // ========================================================================
    
    m->queue_size = prom_gauge_new(
        "roole_worker_queue_size",
        "Current number of messages in worker queue",
        1, label_names
    );
    if (m->queue_size) {
        prom_collector_registry_must_register_metric(m->queue_size);
    }
    
    m->active_executions = prom_gauge_new(
        "roole_worker_active_executions",
        "Current number of messages being processed",
        1, label_names
    );
    if (m->active_executions) {
        prom_collector_registry_must_register_metric(m->active_executions);
    }
    
    m->uptime_seconds = prom_gauge_new(
        "roole_worker_uptime_seconds",
        "Worker uptime in seconds",
        1, label_names
    );
    if (m->uptime_seconds) {
        prom_collector_registry_must_register_metric(m->uptime_seconds);
    }
    
    // ========================================================================
    // HISTOGRAMS (processing time distribution)
    // ========================================================================
    
    // Buckets: 100us, 500us, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s
    double processing_buckets[] = {100, 500, 1000, 5000, 10000, 50000, 
                                   100000, 500000, 1000000, 5000000};
    
    m->processing_time_us = prom_histogram_new(
        "roole_worker_processing_time_microseconds",
        "Message processing time in microseconds",
        processing_buckets, 10, 1, label_names
    );
    if (m->processing_time_us) {
        prom_collector_registry_must_register_metric(m->processing_time_us);
    }
    
    m->queue_wait_time_us = prom_histogram_new(
        "roole_worker_queue_wait_time_microseconds",
        "Time message spent waiting in queue (microseconds)",
        processing_buckets, 10, 1, label_names
    );
    if (m->queue_wait_time_us) {
        prom_collector_registry_must_register_metric(m->queue_wait_time_us);
    }
    
    // ========================================================================
    // Start HTTP server thread
    // ========================================================================
    
    if (pthread_create(&m->http_thread, NULL, metrics_http_server_thread, m) != 0) {
        LOG_ERROR_C("WORKER", "Failed to start metrics HTTP server thread");
        // Continue anyway - metrics collection still works, just no HTTP endpoint
    } else {
        pthread_detach(m->http_thread);
    }
    
    LOF_INFO("WORKER", "Metrics initialized (worker_id=%u, port=%u)", 
               worker_id, metrics_port);
    LOF_INFO("WORKER", "Metrics available at http://0.0.0.0:%u/metrics", metrics_port);
    
    return m;
}

void worker_metrics_shutdown(worker_metrics_t *metrics) {
    if (!metrics) return;
    
    metrics->shutdown_flag = 1;
    
    // Note: promhttp doesn't provide clean shutdown, so we just detach the thread
    // In production, you'd want to add proper shutdown handling
    
    LOF_INFO("WORKER", "Metrics subsystem shutdown");
    free(metrics);
}

// ============================================================================
// COUNTER OPERATIONS
// ============================================================================

void worker_metrics_inc_messages_processed(worker_metrics_t *metrics) {
    if (!metrics || !metrics->messages_processed_total) return;
    
    char worker_id_str[16];
    snprintf(worker_id_str, sizeof(worker_id_str), "%u", metrics->worker_id);
    const char *label_values[] = {worker_id_str};
    
    prom_counter_inc(metrics->messages_processed_total, label_values);
}

void worker_metrics_inc_messages_failed(worker_metrics_t *metrics) {
    if (!metrics || !metrics->messages_failed_total) return;
    
    char worker_id_str[16];
    snprintf(worker_id_str, sizeof(worker_id_str), "%u", metrics->worker_id);
    const char *label_values[] = {worker_id_str};
    
    prom_counter_inc(metrics->messages_failed_total, label_values);
}

void worker_metrics_inc_heartbeats_sent(worker_metrics_t *metrics) {
    if (!metrics || !metrics->heartbeats_sent_total) return;
    
    char worker_id_str[16];
    snprintf(worker_id_str, sizeof(worker_id_str), "%u", metrics->worker_id);
    const char *label_values[] = {worker_id_str};
    
    prom_counter_inc(metrics->heartbeats_sent_total, label_values);
}

// ============================================================================
// GAUGE OPERATIONS
// ============================================================================

void worker_metrics_set_queue_size(worker_metrics_t *metrics, size_t size) {
    if (!metrics || !metrics->queue_size) return;
    
    char worker_id_str[16];
    snprintf(worker_id_str, sizeof(worker_id_str), "%u", metrics->worker_id);
    const char *label_values[] = {worker_id_str};
    
    prom_gauge_set(metrics->queue_size, (double)size, label_values);
}

void worker_metrics_set_active_executions(worker_metrics_t *metrics, uint32_t count) {
    if (!metrics || !metrics->active_executions) return;
    
    char worker_id_str[16];
    snprintf(worker_id_str, sizeof(worker_id_str), "%u", metrics->worker_id);
    const char *label_values[] = {worker_id_str};
    
    prom_gauge_set(metrics->active_executions, (double)count, label_values);
}

void worker_metrics_set_uptime(worker_metrics_t *metrics, uint64_t uptime_seconds) {
    if (!metrics || !metrics->uptime_seconds) return;
    
    char worker_id_str[16];
    snprintf(worker_id_str, sizeof(worker_id_str), "%u", metrics->worker_id);
    const char *label_values[] = {worker_id_str};
    
    prom_gauge_set(metrics->uptime_seconds, (double)uptime_seconds, label_values);
}

// ============================================================================
// HISTOGRAM OPERATIONS
// ============================================================================

void worker_metrics_observe_processing_time(worker_metrics_t *metrics, double duration_us) {
    if (!metrics || !metrics->processing_time_us) return;
    
    char worker_id_str[16];
    snprintf(worker_id_str, sizeof(worker_id_str), "%u", metrics->worker_id);
    const char *label_values[] = {worker_id_str};
    
    prom_histogram_observe(metrics->processing_time_us, duration_us, label_values);
}

void worker_metrics_observe_queue_wait_time(worker_metrics_t *metrics, double duration_us) {
    if (!metrics || !metrics->queue_wait_time_us) return;
    
    char worker_id_str[16];
    snprintf(worker_id_str, sizeof(worker_id_str), "%u", metrics->worker_id);
    const char *label_values[] = {worker_id_str};
    
    prom_histogram_observe(metrics->queue_wait_time_us, duration_us, label_values);
}

#endif // ENABLE_METRICS