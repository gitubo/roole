// src/node/node_metrics.c

#define _POSIX_C_SOURCE 200809L

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

int node_metrics_init(unified_node_t *node, const char *metrics_addr) {
    if (!node) return RESULT_ERR_INVALID;
    
    // If no metrics address, skip metrics
    if (!metrics_addr || strlen(metrics_addr) == 0) {
        LOG_INFO("Metrics disabled: no metrics_addr configured");
        node->metrics_registry = NULL;
        node->metrics_server = NULL;
        return RESULT_OK;
    }
    
    // Parse metrics address
    char metrics_ip[16];
    uint16_t metrics_port;
    config_parse_address(metrics_addr, metrics_ip, &metrics_port);
    
    if (metrics_port == 0) {
        LOG_WARN("Metrics disabled: invalid port in config");
        node->metrics_registry = NULL;
        node->metrics_server = NULL;
        return RESULT_OK;
    }
    
    LOG_INFO("Initializing metrics system on %s:%u...", metrics_ip, metrics_port);
    
    // Create registry
    node->metrics_registry = metrics_registry_init();
    if (!node->metrics_registry) {
        LOG_WARN("Failed to initialize metrics registry");
        return RESULT_ERR_NOMEM;
    }
    
    // Build standard labels (cluster_name, node_id, node_type)
    char node_id_str[32];
    snprintf(node_id_str, sizeof(node_id_str), "%u", node->node_id);
    
    // Determine node_type label based on capabilities
    const char *node_type_label = node->capabilities.has_ingress ? "router" : "worker";
    
    // Create standard labels (reuse from existing code)
    metric_label_t labels[3];
    safe_strncpy(labels[0].name, "cluster_name", MAX_LABEL_NAME_LEN);
    safe_strncpy(labels[0].value, node->cluster_name, MAX_LABEL_VALUE_LEN);
    safe_strncpy(labels[1].name, "node_id", MAX_LABEL_NAME_LEN);
    safe_strncpy(labels[1].value, node_id_str, MAX_LABEL_VALUE_LEN);
    safe_strncpy(labels[2].name, "node_type", MAX_LABEL_NAME_LEN);
    safe_strncpy(labels[2].value, node_type_label, MAX_LABEL_VALUE_LEN);
    
    // Create counter metrics (all with 3 labels)
    node->metric_messages_processed = metrics_get_or_create_counter(
        node->metrics_registry,
        "messages_processed_total",
        "Total number of messages successfully processed",
        3, labels
    );
    
    node->metric_messages_failed = metrics_get_or_create_counter(
        node->metrics_registry,
        "messages_failed_total",
        "Total number of messages that failed processing",
        3, labels
    );
    
    node->metric_messages_routed = metrics_get_or_create_counter(
        node->metrics_registry,
        "messages_routed_total",
        "Total number of messages routed to other nodes",
        3, labels
    );
    
    // Create gauge metrics (all with 3 labels)
    node->metric_queue_size = metrics_get_or_create_gauge(
        node->metrics_registry,
        "messages_queue_size",
        "Current number of messages in processing queue",
        3, labels
    );
    
    node->metric_active_executions = metrics_get_or_create_gauge(
        node->metrics_registry,
        "active_executions",
        "Number of currently executing messages",
        3, labels
    );
    
    node->metric_uptime_seconds = metrics_get_or_create_gauge(
        node->metrics_registry,
        "uptime_seconds",
        "Node uptime in seconds",
        3, labels
    );
    
    // Cluster metrics (all with 3 labels)
    node->metric_cluster_members_total = metrics_get_or_create_gauge(
        node->metrics_registry,
        "cluster_members_total",
        "Total number of cluster members known to this node",
        3, labels
    );
    
    node->metric_cluster_members_active = metrics_get_or_create_gauge(
        node->metrics_registry,
        "cluster_members_active",
        "Number of active cluster members",
        3, labels
    );
    
    node->metric_cluster_members_suspect = metrics_get_or_create_gauge(
        node->metrics_registry,
        "cluster_members_suspect",
        "Number of suspected cluster members",
        3, labels
    );
    
    node->metric_cluster_members_dead = metrics_get_or_create_gauge(
        node->metrics_registry,
        "cluster_members_dead",
        "Number of dead cluster members",
        3, labels
    );

    // CREATE HISTOGRAM METRICS (NEW)
    
    // Execution duration (milliseconds)
    node->histogram_exec_duration = metrics_get_or_create_histogram(
        node->metrics_registry,
        "execution_duration_ms",
        "Histogram of message execution duration in milliseconds",
        HISTOGRAM_BUCKETS_LATENCY_MS,
        3, labels
    );
    
    // Queue wait time (milliseconds)
    node->histogram_queue_wait = metrics_get_or_create_histogram(
        node->metrics_registry,
        "message_queue_wait_ms",
        "Histogram of time messages spend in queue before processing",
        HISTOGRAM_BUCKETS_LATENCY_MS,
        3, labels
    );
    
    // Message size (bytes)
    node->histogram_message_size = metrics_get_or_create_histogram(
        node->metrics_registry,
        "message_size_bytes",
        "Histogram of message sizes in bytes",
        HISTOGRAM_BUCKETS_SIZE_BYTES,
        3, labels
    );
    
    // Gossip round-trip time (microseconds)
    node->histogram_gossip_rtt = metrics_get_or_create_histogram(
        node->metrics_registry,
        "gossip_rtt_us",
        "Histogram of gossip PING/ACK round-trip time in microseconds",
        HISTOGRAM_BUCKETS_LATENCY_US,
        3, labels
    );
    
   
    LOG_INFO("All metrics created with standard labels (cluster_name, node_id, node_type)");
    
    // Start HTTP server
    node->metrics_server = metrics_server_start(
        node->metrics_registry,
        metrics_ip,
        metrics_port
    );
    
    if (!node->metrics_server) {
        LOG_ERROR("Failed to start metrics HTTP server on %s:%u", 
                 metrics_ip, metrics_port);
        LOG_WARN("Continuing without metrics endpoint");
        return RESULT_ERR_NETWORK;
    }
    
    LOG_INFO("Metrics HTTP server started on http://%s:%u/metrics", 
             metrics_ip, metrics_port);
    
    return RESULT_OK;
}

void node_metrics_shutdown(unified_node_t *node) {
    if (!node) return;
    
    if (node->metrics_server) {
        metrics_server_shutdown(node->metrics_server);
        node->metrics_server = NULL;
    }
    
    if (node->metrics_registry) {
        metrics_registry_destroy(node->metrics_registry);
        node->metrics_registry = NULL;
    }
    
    LOG_INFO("Metrics system shutdown complete");
}

void node_metrics_update_periodic(unified_node_t *node) {
    if (!node || !node->metrics_registry) return;
    
    // Update uptime
    if (node->metric_uptime_seconds) {
        uint64_t uptime_seconds = (time_now_ms() - node->start_time_ms) / 1000;
        metrics_gauge_set(node->metric_uptime_seconds, (double)uptime_seconds);
    }
    
    // Update queue size
    if (node->metric_queue_size) {
        size_t queue_size = message_queue_size(&node->message_queue);
        metrics_gauge_set(node->metric_queue_size, (double)queue_size);
    }
    
    // Update active executions
    if (node->metric_active_executions) {
        metrics_gauge_set(node->metric_active_executions, 
                         (double)node->active_executions);
    }
    
    // Update cluster metrics
    node_metrics_update_cluster(node);

        // Update cluster metrics
    node_metrics_update_cluster(node);
    
    // UPDATE EVENT BUS METRICS
    service_registry_t *registry = service_registry_global();
    if (registry) {
        event_bus_t *event_bus = (event_bus_t*)service_registry_get(registry,
                                                                     SERVICE_TYPE_EVENT_BUS,
                                                                     "main");
        if (event_bus) {
            event_bus_stats_t stats;
            event_bus_get_stats(event_bus, &stats);
            
            // TODO: Add event bus metrics to registry if needed
            // For now, just log periodically
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

void node_metrics_update_cluster(unified_node_t *node) {
    if (!node || !node->cluster_view.members) return;
    
    pthread_rwlock_rdlock(&node->cluster_view.lock);
    
    size_t total = node->cluster_view.count;
    size_t active = 0;
    size_t suspect = 0;
    size_t dead = 0;
    
    for (size_t i = 0; i < total; i++) {
        cluster_member_t *member = &node->cluster_view.members[i];
        
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
    
    pthread_rwlock_unlock(&node->cluster_view.lock);
    
    // Update metrics
    if (node->metric_cluster_members_total) {
        metrics_gauge_set(node->metric_cluster_members_total, (double)total);
    }
    if (node->metric_cluster_members_active) {
        metrics_gauge_set(node->metric_cluster_members_active, (double)active);
    }
    if (node->metric_cluster_members_suspect) {
        metrics_gauge_set(node->metric_cluster_members_suspect, (double)suspect);
    }
    if (node->metric_cluster_members_dead) {
        metrics_gauge_set(node->metric_cluster_members_dead, (double)dead);
    }
}