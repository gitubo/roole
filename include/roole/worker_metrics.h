// include/roole/worker_metrics.h

#ifndef ROOLE_WORKER_METRICS_H
#define ROOLE_WORKER_METRICS_H

#include "roole/common.h"
#include <stdint.h>

#ifdef ENABLE_METRICS

// Forward declaration
typedef struct worker_metrics worker_metrics_t;

// ============================================================================
// METRICS INITIALIZATION
// ============================================================================

/**
 * Initialize worker metrics subsystem
 * @param worker_id Worker identifier for labeling
 * @param metrics_port HTTP port for /metrics endpoint (e.g., 9090)
 * @return Metrics handle, or NULL on failure
 */
worker_metrics_t* worker_metrics_init(node_id_t worker_id, uint16_t metrics_port);

/**
 * Shutdown metrics subsystem
 */
void worker_metrics_shutdown(worker_metrics_t *metrics);

// ============================================================================
// COUNTER METRICS
// ============================================================================

/**
 * Increment messages processed counter
 */
void worker_metrics_inc_messages_processed(worker_metrics_t *metrics);

/**
 * Increment messages failed counter
 */
void worker_metrics_inc_messages_failed(worker_metrics_t *metrics);

/**
 * Increment heartbeats sent counter
 */
void worker_metrics_inc_heartbeats_sent(worker_metrics_t *metrics);

// ============================================================================
// GAUGE METRICS
// ============================================================================

/**
 * Set current queue size
 */
void worker_metrics_set_queue_size(worker_metrics_t *metrics, size_t size);

/**
 * Set active executions count
 */
void worker_metrics_set_active_executions(worker_metrics_t *metrics, uint32_t count);

/**
 * Set worker uptime in seconds
 */
void worker_metrics_set_uptime(worker_metrics_t *metrics, uint64_t uptime_seconds);

// ============================================================================
// HISTOGRAM METRICS
// ============================================================================

/**
 * Observe message processing time in microseconds
 */
void worker_metrics_observe_processing_time(worker_metrics_t *metrics, double duration_us);

/**
 * Observe queue wait time in microseconds
 */
void worker_metrics_observe_queue_wait_time(worker_metrics_t *metrics, double duration_us);

#else // ENABLE_METRICS not defined

// No-op stubs when metrics disabled
typedef struct worker_metrics worker_metrics_t;

static inline worker_metrics_t* worker_metrics_init(node_id_t worker_id, uint16_t metrics_port) {
    (void)worker_id; (void)metrics_port;
    return NULL;
}

static inline void worker_metrics_shutdown(worker_metrics_t *metrics) { (void)metrics; }
static inline void worker_metrics_inc_messages_processed(worker_metrics_t *m) { (void)m; }
static inline void worker_metrics_inc_messages_failed(worker_metrics_t *m) { (void)m; }
static inline void worker_metrics_inc_heartbeats_sent(worker_metrics_t *m) { (void)m; }
static inline void worker_metrics_set_queue_size(worker_metrics_t *m, size_t s) { (void)m; (void)s; }
static inline void worker_metrics_set_active_executions(worker_metrics_t *m, uint32_t c) { (void)m; (void)c; }
static inline void worker_metrics_set_uptime(worker_metrics_t *m, uint64_t u) { (void)m; (void)u; }
static inline void worker_metrics_observe_processing_time(worker_metrics_t *m, double d) { (void)m; (void)d; }
static inline void worker_metrics_observe_queue_wait_time(worker_metrics_t *m, double d) { (void)m; (void)d; }

#endif // ENABLE_METRICS

#endif // ROOLE_WORKER_METRICS_H