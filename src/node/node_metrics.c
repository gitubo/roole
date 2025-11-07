#define _POSIX_C_SOURCE 200809L

#include "roole/node_state.h"
#include "roole/node.h"
#include "roole/config.h"
#include "roole/common.h"
#include "roole/event_bus.h"
#include "roole/service_registry.h"
#include "roole/metrics.h"
#include <stdio.h>
#include <string.h>

// ============================================================================
// UNIFIED METRICS INITIALIZATION
// ============================================================================

int node_metrics_init_ex(node_state_t *state, const char *metrics_addr) {
    if (!state) return RESULT_ERR_INVALID;
    
    // If no metrics address, skip metrics
    if (!metrics_addr || strlen(metrics_addr) == 0) {
        LOG_INFO("Metrics disabled: no metrics_addr configured");
        state->metrics_registry = NULL;
        state->metrics_server = NULL;
        return RESULT_OK;
    }
    
    // Parse metrics address
    char metrics_ip[16];
    uint16_t metrics_port;
    config_parse_address(metrics_addr, metrics_ip, &metrics_port);
    
    if (metrics_port == 0) {
        LOG_WARN("Metrics disabled: invalid port in config");
        state->metrics_registry = NULL;
        state->metrics_server = NULL;
        return RESULT_OK;
    }
    
    LOG_INFO("Initializing metrics system on %s:%u...", metrics_ip, metrics_port);
    
    // Create registry
    state->metrics_registry = metrics_registry_init();
    if (!state->metrics_registry) {
        LOG_WARN("Failed to initialize metrics registry");
        return RESULT_ERR_NOMEM;
    }
    
    // Get identity
    const node_identity_t *id = node_state_get_identity(state);
    const node_capabilities_t *caps = node_state_get_capabilities(state);
    
    // Build standard labels
    char node_id_str[32];
    snprintf(node_id_str, sizeof(node_id_str), "%u", id->node_id);
    
    const char *node_type_label = caps->has_ingress ? "router" : "worker";
    
    // Create standard labels
    metric_label_t labels[3];
    safe_strncpy(labels[0].name, "cluster_name", MAX_LABEL_NAME_LEN);
    safe_strncpy(labels[0].value, id->cluster_name, MAX_LABEL_VALUE_LEN);
    safe_strncpy(labels[1].name, "node_id", MAX_LABEL_NAME_LEN);
    safe_strncpy(labels[1].value, node_id_str, MAX_LABEL_VALUE_LEN);
    safe_strncpy(labels[2].name, "node_type", MAX_LABEL_NAME_LEN);
    safe_strncpy(labels[2].value, node_type_label, MAX_LABEL_VALUE_LEN);
    
    // Create counter metrics
    state->metric_messages_processed = metrics_get_or_create_counter(
        state->metrics_registry,
        "messages_processed_total",
        "Total number of messages successfully processed",
        3, labels
    );
    
    state->metric_messages_failed = metrics_get_or_create_counter(
        state->metrics_registry,
        "messages_failed_total",
        "Total number of messages that failed processing",
        3, labels
    );
    
    state->metric_messages_routed = metrics_get_or_create_counter(
        state->metrics_registry,
        "messages_routed_total",
        "Total number of messages routed to other nodes",
        3, labels
    );
    
    // Create gauge metrics
    state->metric_queue_size = metrics_get_or_create_gauge(
        state->metrics_registry,
        "messages_queue_size",
        "Current number of messages in processing queue",
        3, labels
    );
    
    state->metric_active_executions = metrics_get_or_create_gauge(
        state->metrics_registry,
        "active_executions",
        "Number of currently executing messages",
        3, labels
    );
    
    state->metric_uptime_seconds = metrics_get_or_create_gauge(
        state->metrics_registry,
        "uptime_seconds",
        "Node uptime in seconds",
        3, labels
    );
    
    // Cluster metrics
    state->metric_cluster_members_total = metrics_get_or_create_gauge(
        state->metrics_registry,
        "cluster_members_total",
        "Total number of cluster members known to this node",
        3, labels
    );
    
    state->metric_cluster_members_active = metrics_get_or_create_gauge(
        state->metrics_registry,
        "cluster_members_active",
        "Number of active cluster members",
        3, labels
    );
    
    state->metric_cluster_members_suspect = metrics_get_or_create_gauge(
        state->metrics_registry,
        "cluster_members_suspect",
        "Number of suspected cluster members",
        3, labels
    );
    
    state->metric_cluster_members_dead = metrics_get_or_create_gauge(
        state->metrics_registry,
        "cluster_members_dead",
        "Number of dead cluster members",
        3, labels
    );

    // Create histogram metrics
    /**** TODO HISTOGRAM ****/
    /*
    state->histogram_exec_duration = metrics_get_or_create_histogram(
        state->metrics_registry,
        "execution_duration_ms",
        "Histogram of message execution duration in milliseconds",
        HISTOGRAM_BUCKETS_LATENCY_MS,
        3, labels
    );
    
    state->histogram_queue_wait = metrics_get_or_create_histogram(
        state->metrics_registry,
        "message_queue_wait_ms",
        "Histogram of time messages spend in queue before processing",
        HISTOGRAM_BUCKETS_LATENCY_MS,
        3, labels
    );
    
    state->histogram_message_size = metrics_get_or_create_histogram(
        state->metrics_registry,
        "message_size_bytes",
        "Histogram of message sizes in bytes",
        HISTOGRAM_BUCKETS_SIZE_BYTES,
        3, labels
    );
    
    state->histogram_gossip_rtt = metrics_get_or_create_histogram(
        state->metrics_registry,
        "gossip_rtt_us",
        "Histogram of gossip PING/ACK round-trip time in microseconds",
        HISTOGRAM_BUCKETS_LATENCY_US,
        3, labels
    );
    */
    LOG_INFO("All metrics created with standard labels (cluster_name, node_id, node_type)");
    
    // Start HTTP server
    state->metrics_server = metrics_server_start(
        state->metrics_registry,
        metrics_ip,
        metrics_port
    );
    
    if (!state->metrics_server) {
        LOG_ERROR("Failed to start metrics HTTP server on %s:%u", 
                 metrics_ip, metrics_port);
        LOG_WARN("Continuing without metrics endpoint");
        return RESULT_ERR_NETWORK;
    }
    
    LOG_INFO("Metrics HTTP server started on http://%s:%u/metrics", 
             metrics_ip, metrics_port);
    
    return RESULT_OK;
}

void node_metrics_shutdown_ex(node_state_t *state) {
    if (!state) return;
    
    if (state->metrics_server) {
        metrics_server_shutdown(state->metrics_server);
        state->metrics_server = NULL;
    }
    
    if (state->metrics_registry) {
        metrics_registry_destroy(state->metrics_registry);
        state->metrics_registry = NULL;
    }
    
    LOG_INFO("Metrics system shutdown complete");
}

void node_metrics_update_periodic_ex(node_state_t *state) {
    if (!state || !state->metrics_registry) return;
    
    // Get subsystems
    message_queue_t *queue = node_state_get_message_queue(state);
    cluster_view_t *view = node_state_get_cluster_view(state);
    
    // Update uptime
    if (state->metric_uptime_seconds) {
        uint64_t uptime_seconds = (time_now_ms() - state->start_time_ms) / 1000;
        metrics_gauge_set(state->metric_uptime_seconds, (double)uptime_seconds);
    }
    
    // Update queue size
    if (state->metric_queue_size && queue) {
        size_t queue_size = message_queue_size(queue);
        metrics_gauge_set(state->metric_queue_size, (double)queue_size);
    }
    
    // Update active executions
    if (state->metric_active_executions) {
        metrics_gauge_set(state->metric_active_executions, 
                         (double)state->active_executions);
    }
    
    // Update cluster metrics
    node_metrics_update_cluster_ex(state);
    
    // Update event bus metrics
    service_registry_t *registry = service_registry_global();
    if (registry) {
        event_bus_t *event_bus = (event_bus_t*)service_registry_get(registry,
                                                                     SERVICE_TYPE_EVENT_BUS,
                                                                     "main");
        if (event_bus) {
            event_bus_stats_t stats;
            event_bus_get_stats(event_bus, &stats);
            
            static uint64_t last_log = 0;
            uint64_t now = time_now_ms();
            if (now - last_log > 60000) {  // Every 60 seconds
                LOG_INFO("Event bus stats: published=%lu dispatched=%lu dropped=%lu queue=%lu subs=%lu",
                        stats.events_published, stats.events_dispatched, 
                        stats.events_dropped, stats.queue_size, stats.subscribers_total);
                last_log = now;
            }
        }
    }
}

void node_metrics_update_cluster_ex(node_state_t *state) {
    if (!state) return;
    
    cluster_view_t *view = node_state_get_cluster_view(state);
    if (!view || !view->members) return;
    
    pthread_rwlock_rdlock(&view->lock);
    
    size_t total = view->count;
    size_t active = 0;
    size_t suspect = 0;
    size_t dead = 0;
    
    for (size_t i = 0; i < total; i++) {
        cluster_member_t *member = &view->members[i];
        
        switch (member->status) {
            case NODE_STATUS_ALIVE:
                active++;
                break;
            case NODE_STATUS_SUSPECT:
                suspect++;
                break;
            case NODE_STATUS_DEAD:
                dead++;
                break;
            default:
                break;
        }
    }
    
    pthread_rwlock_unlock(&view->lock);
    
    // Update metrics
    if (state->metric_cluster_members_total) {
        metrics_gauge_set(state->metric_cluster_members_total, (double)total);
    }
    if (state->metric_cluster_members_active) {
        metrics_gauge_set(state->metric_cluster_members_active, (double)active);
    }
    if (state->metric_cluster_members_suspect) {
        metrics_gauge_set(state->metric_cluster_members_suspect, (double)suspect);
    }
    if (state->metric_cluster_members_dead) {
        metrics_gauge_set(state->metric_cluster_members_dead, (double)dead);
    }
}