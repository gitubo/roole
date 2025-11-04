// src/node/node_core.c

#define _POSIX_C_SOURCE 200809L

#include "roole/node.h"
#include "roole/config.h"
#include "roole/common.h"
#include "roole/logger.h"
#include "roole/service_registry.h"
#include "roole/event_bus.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

// ============================================================================
// MEMBER EVENT CALLBACK (Publishes to Event Bus)
// ============================================================================

static void on_member_event(node_id_t node_id, node_type_t type,
                           const char *ip, uint16_t data_port,
                           const char *event_type_str, void *user_data) {
    (void)type;
    unified_node_t *node = (unified_node_t*)user_data;
    
    // Get event bus from registry
    service_registry_t *registry = service_registry_global();
    event_bus_t *event_bus = NULL;
    if (registry) {
        event_bus = (event_bus_t*)service_registry_get(registry, 
                                                       SERVICE_TYPE_EVENT_BUS, 
                                                       "main");
    }
    
    // Determine event type
    event_type_t ev_type;
    if (strcmp(event_type_str, MEMBER_EVENT_JOIN) == 0) {
        ev_type = EVENT_TYPE_PEER_JOINED;
        LOG_INFO("Peer %u joined cluster (%s:%u)", node_id, ip, data_port);
    } else if (strcmp(event_type_str, MEMBER_EVENT_FAILED) == 0) {
        ev_type = EVENT_TYPE_PEER_FAILED;
        LOG_WARN("Peer %u failed", node_id);
    } else if (strcmp(event_type_str, MEMBER_EVENT_LEAVE) == 0) {
        ev_type = EVENT_TYPE_PEER_LEFT;
        LOG_WARN("Peer %u left", node_id);
    } else if (strcmp(event_type_str, MEMBER_EVENT_UPDATE) == 0) {
        ev_type = EVENT_TYPE_PEER_UPDATED;
        LOG_DEBUG("Peer %u updated", node_id);
    } else {
        return;  // Unknown event
    }
    
    // Publish event to bus
    if (event_bus) {
        event_t event = {
            .type = ev_type,
            .timestamp_ms = time_now_ms(),
            .source_node_id = node->node_id,
            .data.peer = {
                .node_id = node_id,
                .node_type = type,
                .data_port = data_port,
                .status = NODE_STATUS_ALIVE
            }
        };
        safe_strncpy(event.data.peer.ip_address, ip, MAX_IP_LEN);
        
        event_bus_publish(event_bus, &event);
    }
    
    // Original logic (will be moved to event subscribers in next step)
    if (strcmp(event_type_str, MEMBER_EVENT_JOIN) == 0) {
        peer_pool_add(&node->peer_pool, node_id, ip, 0, data_port);
        
        peer_info_t *peer = peer_pool_get(&node->peer_pool, node_id);
        if (peer) {
            if (!peer->data_channel) {
                peer->data_channel = safe_malloc(sizeof(rpc_channel_t));
                if (peer->data_channel) {

                    // Retry connection a few times (worker might still be starting)
                    int connected = 0;
                    for (int retry = 0; retry < 3; retry++) {
                        if (retry > 0) {
                            LOG_DEBUG("Retrying DATA channel connection to peer %u (attempt %d/3)", 
                                     node_id, retry + 1);
                            usleep(500000);  // Wait 500ms between retries
                        }
                        
                        if (rpc_client_connect(peer->data_channel, ip, data_port,
                                              RPC_CHANNEL_DATA, 4096) == 0) {
                            LOG_INFO("DATA channel established to peer %u", node_id);
                            connected = 1;
                            break;
                        }
                    }
                    
                    if (!connected) {
                        LOG_WARN("Failed to connect DATA channel to peer %u after 3 attempts (will retry later)", 
                                node_id);
                        safe_free(peer->data_channel);
                        peer->data_channel = NULL;
                    }

                }
            }
            peer_pool_release(&node->peer_pool);
        }
    }
    else if (strcmp(event_type_str, MEMBER_EVENT_FAILED) == 0 ||
             strcmp(event_type_str, MEMBER_EVENT_LEAVE) == 0) {
        peer_pool_update_status(&node->peer_pool, node_id, NODE_STATUS_DEAD);
    }
}

// ============================================================================
// EVENT BUS SUBSCRIBERS
// ============================================================================

static void handle_peer_events(const event_t *event, void *user_data) {
    unified_node_t *node = (unified_node_t*)user_data;
    if (!node) return;
    
    const event_peer_t *peer_data = &event->data.peer;
    
    switch (event->type) {
        case EVENT_TYPE_PEER_JOINED:
            // Already handled in on_member_event (will refactor later)
            break;
            
        case EVENT_TYPE_PEER_FAILED:
        case EVENT_TYPE_PEER_LEFT:
            peer_pool_update_status(&node->peer_pool, 
                                   peer_data->node_id, 
                                   NODE_STATUS_DEAD);
            LOG_DEBUG("Event subscriber: Peer %u marked as DEAD", peer_data->node_id);
            break;
            
        case EVENT_TYPE_PEER_SUSPECT:
            peer_pool_update_status(&node->peer_pool,
                                   peer_data->node_id,
                                   NODE_STATUS_SUSPECT);
            LOG_DEBUG("Event subscriber: Peer %u marked as SUSPECT", peer_data->node_id);
            break;
            
        case EVENT_TYPE_PEER_UPDATED:
            LOG_DEBUG("Event subscriber: Peer %u updated", peer_data->node_id);
            break;
            
        default:
            break;
    }
}

// ============================================================================
// CLEANUP THREAD
// ============================================================================

static void* node_cleanup_thread_fn(void *arg) {
    unified_node_t *node = (unified_node_t*)arg;
    logger_push_component("cleanup");
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
    logger_pop_component();
    return NULL;
}


static void* node_metrics_update_thread_fn(void *arg) {
    unified_node_t *node = (unified_node_t*)arg;
    logger_push_component("metrics:updater");
    LOG_INFO("Node metrics update thread started");
    
    while (!node->shutdown_flag) {
        // Update metrics every 10 seconds
        sleep(10);
        
        if (node->shutdown_flag) break;
        
        // Update all periodic metrics
        node_metrics_update_periodic(node);
        
        LOG_DEBUG("Periodic metrics updated");
    }
    
    LOG_INFO("Node metrics update thread stopped");
    logger_pop_component();
    return NULL;
}

// ============================================================================
// UNIFIED NODE INITIALIZATION
// ============================================================================

int node_init(unified_node_t *node, const roole_config_t *config, 
              size_t num_executor_threads) {
    if (!node || !config) return RESULT_ERR_INVALID;
    
    memset(node, 0, sizeof(unified_node_t));
    
    // SET LOGGER CONTEXT FIRST (before any LOG calls)
    const char *node_type_str = (config->node_type == NODE_TYPE_ROUTER) ? "router" : "worker";
    logger_set_context(config->node_id, config->cluster_name, node_type_str);
    
    // CREATE AND SET GLOBAL SERVICE REGISTRY
    service_registry_t *registry = service_registry_create();
    if (!registry) {
        LOG_ERROR("Failed to create service registry");
        return RESULT_ERR_NOMEM;
    }
    service_registry_set_global(registry);

    // CREATE EVENT BUS
    event_bus_t *event_bus = event_bus_create();
    if (!event_bus) {
        LOG_ERROR("Failed to create event bus");
        service_registry_destroy(registry);
        return RESULT_ERR_NOMEM;
    }
    service_registry_register(registry, SERVICE_TYPE_EVENT_BUS, "main", event_bus);
    
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
    node->node_type = config->node_type;
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
    if (membership_init(&node->membership, node->node_id, node->node_type,
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
    
    // SUBSCRIBE TO PEER EVENTS ON EVENT BUS
    /*
    event_bus_t *event_bus = (event_bus_t*)service_registry_get(registry,
                                                                SERVICE_TYPE_EVENT_BUS,
                                                                "main");
    */
   
    if (event_bus) {
        event_bus_subscribe(event_bus, EVENT_TYPE_PEER_JOINED, handle_peer_events, node);
        event_bus_subscribe(event_bus, EVENT_TYPE_PEER_LEFT, handle_peer_events, node);
        event_bus_subscribe(event_bus, EVENT_TYPE_PEER_FAILED, handle_peer_events, node);
        event_bus_subscribe(event_bus, EVENT_TYPE_PEER_SUSPECT, handle_peer_events, node);
        event_bus_subscribe(event_bus, EVENT_TYPE_PEER_UPDATED, handle_peer_events, node);
        LOG_INFO("Subscribed to peer events on event bus");
    }

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
    
    // START METRICS UPDATE THREAD (NEW)
    if (node->metrics_registry) {
        if (pthread_create(&node->metrics_update_thread, NULL,
                          node_metrics_update_thread_fn, node) != 0) {
            LOG_ERROR("Failed to start metrics update thread");
            node->shutdown_flag = 1;
            pthread_join(node->cleanup_thread, NULL);
            return RESULT_ERR_INVALID;
        }
        LOG_INFO("Metrics update thread started");
    }

    // Start executor threads (if can execute)
    if (node->capabilities.can_execute) {
        if (node_start_executors(node) != RESULT_OK) {
            LOG_ERROR("Failed to start executor threads");
            node->shutdown_flag = 1;
            pthread_join(node->cleanup_thread, NULL);
            if (node->metrics_registry) {
                pthread_join(node->metrics_update_thread, NULL);
            }
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
    LOG_INFO("Initiating graceful shutdown for node %u", node->node_id);
    LOG_INFO("========================================");
    
    // PHASE 1: Stop accepting new work
    LOG_INFO("[Shutdown Phase 1/6] Stopping RPC servers...");
    // RPC server will see shutdown_flag and stop accepting connections
    node->shutdown_flag = 1;
    
    // Unregister from service registry immediately
    service_registry_t *registry = service_registry_global();
    if (registry) {
        service_registry_unregister(registry, SERVICE_TYPE_NODE_STATE, "unified_node");
    }
    
    // PHASE 2: Drain message queue (if can execute)
    if (node->capabilities.can_execute) {
        LOG_INFO("[Shutdown Phase 2/6] Draining message queue...");
        
        int drain_attempts = 0;
        const int MAX_DRAIN_SECONDS = 10;
        
        while (drain_attempts < MAX_DRAIN_SECONDS) {
            size_t queue_size = message_queue_size(&node->message_queue);
            
            if (queue_size == 0) {
                LOG_INFO("Message queue drained successfully");
                break;
            }
            
            LOG_INFO("Waiting for queue to drain: %zu messages remaining", queue_size);
            sleep(1);
            drain_attempts++;
        }
        
        size_t final_queue_size = message_queue_size(&node->message_queue);
        if (final_queue_size > 0) {
            LOG_WARN("Message queue not fully drained: %zu messages lost", 
                     final_queue_size);
        }
    } else {
        LOG_INFO("[Shutdown Phase 2/6] No message queue (skipped)");
    }
    
    // PHASE 3: Wait for active executions to complete
    LOG_INFO("[Shutdown Phase 3/6] Waiting for active executions to complete...");
    
    int exec_wait_attempts = 0;
    const int MAX_EXEC_WAIT_SECONDS = 30;
    
    while (exec_wait_attempts < MAX_EXEC_WAIT_SECONDS) {
        uint32_t active = node->active_executions;
        
        if (active == 0) {
            LOG_INFO("All executions completed");
            break;
        }
        
        LOG_INFO("Waiting for executions: %u active", active);
        sleep(1);
        exec_wait_attempts++;
    }
    
    if (node->active_executions > 0) {
        LOG_WARN("Shutdown timeout: %u executions still active (will be terminated)",
                 node->active_executions);
    }
    
    // PHASE 4: Stop worker threads
    LOG_INFO("[Shutdown Phase 4/6] Stopping worker threads...");
    
    if (node->executor_threads && node->capabilities.can_execute) {
        LOG_DEBUG("Stopping executor threads...");
        node_stop_executors(node);
        LOG_INFO("Executor threads stopped");
    }
    
    if (node->metrics_registry) {
        LOG_DEBUG("Stopping metrics update thread...");
        pthread_join(node->metrics_update_thread, NULL);
        LOG_INFO("Metrics update thread stopped");
    }
    
    LOG_DEBUG("Stopping cleanup thread...");
    pthread_join(node->cleanup_thread, NULL);
    LOG_INFO("Cleanup thread stopped");
    
    // PHASE 5: Gracefully leave cluster
    LOG_INFO("[Shutdown Phase 5/6] Leaving cluster...");
    
    if (node->membership) {
        // Send LEAVE message to all peers
        membership_leave(node->membership);
        
        // Wait for LEAVE propagation
        LOG_DEBUG("Waiting 2 seconds for LEAVE propagation...");
        sleep(2);
        
        // Shutdown membership/gossip
        membership_shutdown(node->membership);
        node->membership = NULL;
        LOG_INFO("Left cluster gracefully");
    }
    
    // PHASE 6: Cleanup resources and flush logs/metrics
    LOG_INFO("[Shutdown Phase 6/6] Cleaning up resources...");
    
    // Final metrics update before shutdown
    if (node->metrics_registry) {
        LOG_DEBUG("Final metrics update...");
        node_metrics_update_periodic(node);
    }
    
    // Shutdown metrics (flushes final metrics)
    if (node->metrics_server || node->metrics_registry) {
        LOG_DEBUG("Shutting down metrics server...");
        node_metrics_shutdown(node);
        LOG_INFO("Metrics server stopped");
    }
    
    // Cleanup data structures
    LOG_DEBUG("Destroying cluster view...");
    cluster_view_destroy(&node->cluster_view);
    
    if (node->capabilities.can_execute) {
        LOG_DEBUG("Destroying message queue...");
        message_queue_destroy(&node->message_queue);
    }
    
    LOG_DEBUG("Destroying execution tracker...");
    execution_tracker_destroy(&node->exec_tracker);
    
    LOG_DEBUG("Destroying peer pool...");
    peer_pool_destroy(&node->peer_pool);
    
    LOG_DEBUG("Destroying DAG catalog...");
    dag_catalog_destroy(&node->dag_catalog);
    
    // Destroy event bus
    event_bus_t *event_bus = (event_bus_t*)service_registry_get(registry, 
                                                                SERVICE_TYPE_EVENT_BUS, 
                                                                "main");
    if (event_bus) {
        LOG_DEBUG("Destroying event bus...");
        event_bus_destroy(event_bus);
        service_registry_unregister(registry, SERVICE_TYPE_EVENT_BUS, "main");
    }

    // Destroy global registry
    if (registry) {
        service_registry_set_global(NULL);
        service_registry_destroy(registry);
    }

    LOG_INFO("========================================");
    LOG_INFO("Node %u shutdown complete", node->node_id);
    LOG_INFO("========================================");
    
    // Flush all logs
    logger_flush();
}