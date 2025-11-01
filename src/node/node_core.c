// src/node/node_core.c

#define _POSIX_C_SOURCE 200809L

#include "roole/node.h"
#include "roole/config.h"
#include "roole/common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

// ============================================================================
// MEMBER EVENT CALLBACK
// ============================================================================

static void on_member_event(node_id_t node_id, node_type_t type,
                           const char *ip, uint16_t data_port,
                           const char *event_type, void *user_data) {
    (void)type;
    unified_node_t *node = (unified_node_t*)user_data;

    // Handle peer join/leave/failure
    if (strcmp(event_type, MEMBER_EVENT_JOIN) == 0) {
        LOG_INFO("Peer %u joined cluster (%s:%u)", node_id, ip, data_port);
        
        // Add to peer pool
        peer_pool_add(&node->peer_pool, node_id, ip, 0, data_port);
        
        // Establish DATA channel connection
        peer_info_t *peer = peer_pool_get(&node->peer_pool, node_id);
        if (peer) {
            if (!peer->data_channel) {
                peer->data_channel = safe_malloc(sizeof(rpc_channel_t));
                if (peer->data_channel) {
                    if (rpc_client_connect(peer->data_channel, ip, data_port,
                                          RPC_CHANNEL_DATA, 4096) == 0) {
                        LOG_INFO("DATA channel established to peer %u", node_id);
                    } else {
                        LOG_ERROR("Failed to connect DATA channel to peer %u", node_id);
                        safe_free(peer->data_channel);
                        peer->data_channel = NULL;
                    }
                }
            }
            peer_pool_release(&node->peer_pool);
        }
    }
    else if (strcmp(event_type, MEMBER_EVENT_FAILED) == 0 ||
             strcmp(event_type, MEMBER_EVENT_LEAVE) == 0) {
        LOG_WARN("Peer %u left/failed", node_id);
        
        peer_pool_update_status(&node->peer_pool, node_id, NODE_STATUS_DEAD);
        
        // TODO: Re-schedule executions assigned to this peer
    }
    else if (strcmp(event_type, MEMBER_EVENT_UPDATE) == 0) {
        LOG_DEBUG("Peer %u updated", node_id);
    }
}

// ============================================================================
// CLEANUP THREAD
// ============================================================================

static void* node_cleanup_thread_fn(void *arg) {
    unified_node_t *node = (unified_node_t*)arg;
    
    LOG_INFO("Node cleanup thread started");
    
    while (!node->shutdown_flag) {
        usleep(60 * 1000 * 1000);  // 60 seconds
        
        // Cleanup completed executions
        size_t cleaned = execution_tracker_cleanup_completed(&node->exec_tracker);
        if (cleaned > 0) {
            LOG_DEBUG("Cleaned up %zu completed executions", cleaned);
        }
    }
    
    LOG_INFO("Node cleanup thread stopped");
    return NULL;
}

// ============================================================================
// UNIFIED NODE INITIALIZATION
// ============================================================================

int node_init(unified_node_t *node, const roole_config_t *config, 
              size_t num_executor_threads) {
    if (!node || !config) return RESULT_ERR_INVALID;
    
    memset(node, 0, sizeof(unified_node_t));
    
    // Parse addresses
    char gossip_ip[16], data_ip[16], ingress_ip[16], metrics_ip[16];
    uint16_t gossip_port, data_port, ingress_port = 0, metrics_port = 0;
    
    config_parse_address(config->ports.gossip_addr, gossip_ip, &gossip_port);
    config_parse_address(config->ports.data_addr, data_ip, &data_port);
    
    if (config->ports.ingress_addr[0] != '\0') {
        config_parse_address(config->ports.ingress_addr, ingress_ip, &ingress_port);
    }
    
    if (config->ports.metrics_addr[0] != '\0') {
        config_parse_address(config->ports.metrics_addr, metrics_ip, &metrics_port);
    }
    
    // Set basic fields
    node->node_id = config->node_id;
    safe_strncpy(node->cluster_name, config->cluster_name, MAX_CONFIG_STRING);
    safe_strncpy(node->bind_addr, gossip_ip, MAX_IP_LEN);
    node->gossip_port = gossip_port;
    node->data_port = data_port;
    node->ingress_port = ingress_port;
    node->metrics_port = metrics_port;
    node->shutdown_flag = 0;
    node->active_executions = 0;
    node->catalog_version = 0;
    node->start_time_ms = time_now_ms();
    node->num_executor_threads = num_executor_threads;
    node->executor_threads = NULL;
    
    // Detect capabilities
    node_detect_capabilities(node, config);
    node_print_capabilities(node);
    
    // Initialize DAG catalog
    if (dag_catalog_init(&node->dag_catalog, MAX_DAGS) != RESULT_OK) {
        LOG_ERROR("Failed to initialize DAG catalog");
        return RESULT_ERR_INVALID;
    }
    
    // Initialize peer pool
    if (peer_pool_init(&node->peer_pool, MAX_PEERS) != RESULT_OK) {
        LOG_ERROR("Failed to initialize peer pool");
        dag_catalog_destroy(&node->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    // Initialize execution tracker
    if (execution_tracker_init(&node->exec_tracker, MAX_PENDING_EXECUTIONS) != RESULT_OK) {
        LOG_ERROR("Failed to initialize execution tracker");
        peer_pool_destroy(&node->peer_pool);
        dag_catalog_destroy(&node->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    // Initialize message queue (if can execute)
    if (node->capabilities.can_execute) {
        if (message_queue_init(&node->message_queue, MAX_NODE_QUEUE_SIZE) != RESULT_OK) {
            LOG_ERROR("Failed to initialize message queue");
            execution_tracker_destroy(&node->exec_tracker);
            peer_pool_destroy(&node->peer_pool);
            dag_catalog_destroy(&node->dag_catalog);
            return RESULT_ERR_INVALID;
        }
    }
    
    // Initialize cluster view
    if (cluster_view_init(&node->cluster_view, MAX_CLUSTER_NODES) != RESULT_OK) {
        LOG_ERROR("Failed to initialize cluster view");
        if (node->capabilities.can_execute) {
            message_queue_destroy(&node->message_queue);
        }
        execution_tracker_destroy(&node->exec_tracker);
        peer_pool_destroy(&node->peer_pool);
        dag_catalog_destroy(&node->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    // Initialize membership (gossip protocol)
    if (membership_init(&node->membership, node->node_id, NODE_TYPE_WORKER,
                       node->bind_addr, node->gossip_port, node->data_port) != RESULT_OK) {
        LOG_ERROR("Failed to initialize membership");
        cluster_view_destroy(&node->cluster_view);
        if (node->capabilities.can_execute) {
            message_queue_destroy(&node->message_queue);
        }
        execution_tracker_destroy(&node->exec_tracker);
        peer_pool_destroy(&node->peer_pool);
        dag_catalog_destroy(&node->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    membership_set_callback(node->membership, on_member_event, node);
    node->gossip_engine = ((struct membership_handle*)node->membership)->gossip_engine;
    
    // Initialize metrics
    if (config->ports.metrics_addr[0] != '\0') {
        node_metrics_init(node, config->ports.metrics_addr);
    } else {
        node->metrics_registry = NULL;
        node->metrics_server = NULL;
    }
    
    LOG_INFO("========================================");
    LOG_INFO("Unified Node Initialized:");
    LOG_INFO("  Node ID: %u", node->node_id);
    LOG_INFO("  Cluster: %s", node->cluster_name);
    LOG_INFO("  Gossip: %s:%u", gossip_ip, gossip_port);
    LOG_INFO("  Data: %s:%u", data_ip, data_port);
    
    if (node->capabilities.has_ingress) {
        LOG_INFO("  Ingress: %s:%u (CLIENT FACING)", ingress_ip, ingress_port);
    } else {
        LOG_INFO("  Ingress: DISABLED");
    }
    
    if (node->metrics_port > 0) {
        LOG_INFO("  Metrics: http://%s:%u/metrics", metrics_ip, metrics_port);
    } else {
        LOG_INFO("  Metrics: DISABLED");
    }
    
    LOG_INFO("========================================");
    
    return RESULT_OK;
}

// ============================================================================
// UNIFIED NODE START
// ============================================================================

int node_start(unified_node_t *node) {
    if (!node) return RESULT_ERR_INVALID;
    
    LOG_INFO("Starting unified node %u...", node->node_id);
    
    // Start cleanup thread
    if (pthread_create(&node->cleanup_thread, NULL, 
                      node_cleanup_thread_fn, node) != 0) {
        LOG_ERROR("Failed to start cleanup thread");
        return RESULT_ERR_INVALID;
    }
    
    LOG_INFO("Cleanup thread started");
    
    // Start executor threads (if can execute)
    if (node->capabilities.can_execute) {
        if (node_start_executors(node) != RESULT_OK) {
            LOG_ERROR("Failed to start executor threads");
            node->shutdown_flag = 1;
            pthread_join(node->cleanup_thread, NULL);
            return RESULT_ERR_INVALID;
        }
        LOG_INFO("Started %zu executor threads", node->num_executor_threads);
    } else {
        LOG_INFO("Execution capability disabled, no executor threads");
    }
    
    LOG_INFO("Node %u started successfully", node->node_id);
    return RESULT_OK;
}

// ============================================================================
// UNIFIED NODE SHUTDOWN
// ============================================================================

void node_shutdown(unified_node_t *node) {
    if (!node) return;
    
    LOG_INFO("========================================");
    LOG_INFO("Shutting down node %u", node->node_id);
    LOG_INFO("========================================");
    
    // Set shutdown flag
    node->shutdown_flag = 1;
    
    // Stop executor threads
    if (node->executor_threads && node->capabilities.can_execute) {
        LOG_INFO("Stopping executor threads...");
        node_stop_executors(node);
    }
    
    // Wait for cleanup thread
    LOG_INFO("Stopping cleanup thread...");
    pthread_join(node->cleanup_thread, NULL);
    
    // Gracefully leave cluster
    if (node->membership) {
        LOG_INFO("Leaving cluster...");
        membership_leave(node->membership);
        sleep(1);  // Give time for LEAVE message to propagate
        
        membership_shutdown(node->membership);
        node->membership = NULL;
    }
    
    // Shutdown metrics
    if (node->metrics_server || node->metrics_registry) {
        LOG_INFO("Shutting down metrics...");
        node_metrics_shutdown(node);
    }
    
    // Cleanup resources
    LOG_INFO("Cleaning up resources...");
    
    cluster_view_destroy(&node->cluster_view);
    
    if (node->capabilities.can_execute) {
        message_queue_destroy(&node->message_queue);
    }
    
    execution_tracker_destroy(&node->exec_tracker);
    peer_pool_destroy(&node->peer_pool);
    dag_catalog_destroy(&node->dag_catalog);
    
    LOG_INFO("========================================");
    LOG_INFO("Node %u shutdown complete", node->node_id);
    LOG_INFO("========================================");
}

// ============================================================================
// BOOTSTRAP FROM CONFIG
// ============================================================================
/*
int node_bootstrap(unified_node_t *node, const roole_config_t *config) {
    if (!node || !config || config->router_count == 0) {
        LOG_WARN("No seed routers configured for bootstrap");
        return RESULT_OK;
    }
    
    // Select random seed router
    size_t router_idx = rand() % config->router_count;
    const char *router_addr = config->routers[router_idx];
    
    char router_ip[16];
    uint16_t router_gossip_port;
    config_parse_address(router_addr, router_ip, &router_gossip_port);
    
    LOG_INFO("Bootstrapping from seed router %s:%u", router_ip, router_gossip_port);
    
    // Add seed to gossip engine
    if (gossip_engine_add_seed(node->gossip_engine, router_ip, router_gossip_port) != 0) {
        LOG_ERROR("Failed to add seed router");
        return RESULT_ERR_NETWORK;
    }
    
    // Announce join
    gossip_engine_announce_join(node->gossip_engine);
    
    LOG_INFO("Bootstrap complete, waiting for cluster membership...");
    sleep(2);  // Give time for gossip to propagate
    
    return RESULT_OK;
}
*/