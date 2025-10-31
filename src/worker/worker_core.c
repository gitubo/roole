// src/worker/worker_core.c
// UPDATED: Integrated dependency-free metrics system

#define _POSIX_C_SOURCE 200809L

#include "roole/worker.h"
#include "roole/config.h"
#include "roole/common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <errno.h>

// ============================================================================
// WORKER CALLBACKS
// ============================================================================

static void on_member_event(node_id_t node_id, node_type_t type,
                           const char *ip, uint16_t data_port,
                           const char *event_type, void *user_data) {
    worker_state_t *worker = (worker_state_t*)user_data;

    if (type == NODE_TYPE_ROUTER) {
        if (strcmp(event_type, MEMBER_EVENT_JOIN) == 0) {
            worker_add_router(worker, node_id, ip, data_port);
        }
        else if (strcmp(event_type, MEMBER_EVENT_FAILED) == 0 ||
                 strcmp(event_type, MEMBER_EVENT_LEAVE) == 0) {
            worker_remove_router(worker, node_id);
        }
    }
}

// ============================================================================
// EXECUTOR THREAD
// ============================================================================
/*
void* worker_executor_thread_fn(void *arg) {
    worker_state_t *worker = (worker_state_t*)arg;
    
    LOG_INFO("Worker executor thread started");
    
    while (!worker->shutdown_flag) {
        message_t message;
        
        int ret = message_queue_pop(&worker->message_queue, &message, 1000);
        
        if (ret == RESULT_ERR_TIMEOUT || ret == RESULT_ERR_EMPTY) {
            // Update queue size metric
            if (worker->metric_queue_size) {
                size_t queue_size = message_queue_size(&worker->message_queue);
                metrics_gauge_set(worker->metric_queue_size, (double)queue_size);
            }
            continue;
        }
        
        if (ret != RESULT_OK) {
            LOG_ERROR("Failed to pop message from queue");
            continue;
        }
        
        uint64_t now_ms = time_now_ms();
        uint64_t wait_time_us = (now_ms - message.received_at_ms) * 1000;
        
        LOG_INFO("Processing message %lu (DAG %u, waited %lu us)", 
                 message.exec_id, message.dag_id, wait_time_us);
        
        __sync_fetch_and_add(&worker->active_executions, 1);
        
        // Update active executions metric
        if (worker->metric_active_executions) {
            metrics_gauge_set(worker->metric_active_executions, 
                            (double)worker->active_executions);
        }
        
        struct timespec start_time;
        timespec_now(&start_time);
        
        // Simulate processing (replace with actual DAG execution)
        usleep(500000);  // 500ms
        
        struct timespec end_time;
        timespec_now(&end_time);
        double processing_time_us = time_diff_us(&start_time, &end_time);
        
        execution_status_t status = EXEC_STATUS_COMPLETED;
        
        // Send status update to router
        pthread_mutex_lock(&worker->routers_lock);
        router_connection_t *router_conn = NULL;
        for (size_t i = 0; i < MAX_ROUTER_CONNECTIONS; i++) {
            if (worker->routers[i].active) {
                router_conn = &worker->routers[i];
                break;
            }
        }
        
        if (router_conn && router_conn->data_channel) {
            uint8_t payload[9];
            memcpy(payload, &message.exec_id, 8);
            payload[8] = (uint8_t)status;
            
            size_t msg_len = rpc_pack_message(
                router_conn->data_channel->tx_buffer,
                worker->worker_id,
                message.exec_id,
                RPC_TYPE_REQUEST,
                RPC_STATUS_UNKNOWN,
                FUNC_ID_EXECUTION_UPDATE,
                payload,
                9
            );
            
            send(router_conn->data_channel->socket_fd,
                router_conn->data_channel->tx_buffer, msg_len, 0);
            
            LOG_INFO("Message %lu completed (%.2fms processing)", 
                     message.exec_id, processing_time_us / 1000.0);
            
            // Update success metric
            if (worker->metric_messages_processed) {
                metrics_counter_inc(worker->metric_messages_processed);
            }
        } else {
            LOG_ERROR("No router connection available for status update");
            
            // Update failure metric
            if (worker->metric_messages_failed) {
                metrics_counter_inc(worker->metric_messages_failed);
            }
        }
        pthread_mutex_unlock(&worker->routers_lock);
        
        __sync_fetch_and_sub(&worker->active_executions, 1);
        
        // Update active executions metric
        if (worker->metric_active_executions) {
            metrics_gauge_set(worker->metric_active_executions, 
                            (double)worker->active_executions);
        }
    }
    
    LOG_INFO("Worker executor thread stopped");
    return NULL;
}
*/

// ============================================================================
// WORKER INITIALIZATION
// ============================================================================

int worker_init(worker_state_t *worker, node_id_t worker_id,
               uint16_t gossip_port, uint16_t data_port,
               const char *bind_addr, size_t num_executor_threads, 
               const char *metrics_addr, const char *cluster_name) {
    if (!worker || num_executor_threads == 0 || num_executor_threads > 16) {
        return RESULT_ERR_INVALID;
    }

    memset(worker, 0, sizeof(worker_state_t));
    worker->worker_id = worker_id;
    worker->gossip_port = gossip_port;
    worker->data_port = data_port;
    worker->num_executor_threads = num_executor_threads;
    worker->shutdown_flag = 0;
    worker->active_executions = 0;
    worker->catalog_version = 0;
    worker->start_time_ms = time_now_ms();
    safe_strncpy(worker->cluster_name, cluster_name, MAX_CONFIG_STRING);
    safe_strncpy(worker->bind_addr, bind_addr ? bind_addr : "0.0.0.0", MAX_IP_LEN);

    // Detect capabilities from config
    roole_config_t temp_config;
    memset(&temp_config, 0, sizeof(temp_config));
    safe_strncpy(temp_config.cluster_name, cluster_name, MAX_CONFIG_STRING);
    
    // Workers typically don't have ingress_addr configured
    temp_config.ports.ingress_addr[0] = '\0';
    
    // Create temp unified node for capability detection
    unified_node_t temp_node;
    temp_node.node_id = worker_id;
    safe_strncpy(temp_node.cluster_name, cluster_name, MAX_CONFIG_STRING);
    
    node_detect_capabilities(&temp_node, &temp_config);
    
    LOG_INFO("Worker capabilities detected:");
    LOG_INFO("  has_ingress: %d (workers typically don't have ingress)", 
             temp_node.capabilities.has_ingress);
    LOG_INFO("  can_execute: %d", temp_node.capabilities.can_execute);
    LOG_INFO("  can_route: %d", temp_node.capabilities.can_route);
    
    // Initialize DAG catalog
    if (dag_catalog_init(&worker->dag_catalog, MAX_DAGS) != RESULT_OK) {
        LOG_ERROR("Failed to initialize DAG catalog");
        return RESULT_ERR_INVALID;
    }
    
    // Initialize message queue
    if (message_queue_init(&worker->message_queue, MAX_NODE_QUEUE_SIZE) != RESULT_OK) {
        LOG_ERROR("Failed to initialize message queue");
        dag_catalog_destroy(&worker->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    // Initialize cluster view
    if (cluster_view_init(&worker->cluster_view, MAX_CLUSTER_NODES) != RESULT_OK) {
        LOG_ERROR("Failed to initialize cluster view");
        message_queue_destroy(&worker->message_queue);
        dag_catalog_destroy(&worker->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    // Initialize routers lock
    if (pthread_mutex_init(&worker->routers_lock, NULL) != 0) {
        LOG_ERROR("Failed to initialize routers lock");
        cluster_view_destroy(&worker->cluster_view);
        message_queue_destroy(&worker->message_queue);
        dag_catalog_destroy(&worker->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    // Initialize membership (gossip protocol)
    if (membership_init(&worker->membership, worker_id, NODE_TYPE_WORKER,
                       worker->bind_addr, worker->gossip_port, worker->data_port) != RESULT_OK) {
        LOG_ERROR("Failed to initialize membership");
        pthread_mutex_destroy(&worker->routers_lock);
        cluster_view_destroy(&worker->cluster_view);
        message_queue_destroy(&worker->message_queue);
        dag_catalog_destroy(&worker->dag_catalog);
        return RESULT_ERR_INVALID;
    }

    membership_set_callback(worker->membership, on_member_event, worker);
    worker->gossip_engine = ((struct membership_handle*)worker->membership)->gossip_engine;

    // Initialize metrics system if metrics_addr provided
    worker->start_time_ms = time_now_ms();
    
    if (metrics_addr && strlen(metrics_addr) > 0) {
        // Use unified metrics system (temporary bridge until full migration)
        unified_node_t temp_node;
        temp_node.node_id = worker->worker_id;
        safe_strncpy(temp_node.cluster_name, worker->cluster_name, MAX_CONFIG_STRING);
        temp_node.start_time_ms = worker->start_time_ms;
        temp_node.cluster_view = worker->cluster_view;
        
        if (node_metrics_init(&temp_node, metrics_addr) == RESULT_OK) {
            // Copy metrics handles back to worker
            worker->metrics_registry = temp_node.metrics_registry;
            worker->metrics_server = temp_node.metrics_server;
            worker->metric_messages_processed = temp_node.metric_messages_processed;
            worker->metric_messages_failed = temp_node.metric_messages_failed;
            worker->metric_queue_size = temp_node.metric_queue_size;
            worker->metric_uptime_seconds = temp_node.metric_uptime_seconds;
            worker->metric_cluster_members_total = temp_node.metric_cluster_members_total;
            worker->metric_cluster_members_active = temp_node.metric_cluster_members_active;
            worker->metric_cluster_members_suspect = temp_node.metric_cluster_members_suspect;
            worker->metric_cluster_members_dead = temp_node.metric_cluster_members_dead;
        }
    } else {
        LOG_INFO("Metrics disabled for worker");
        worker->metrics_registry = NULL;
        worker->metrics_server = NULL;
    }

    LOG_INFO("Worker %u initialized (GOSSIP:%u, DATA:%u, %zu executor threads)",
             worker_id, gossip_port, data_port, num_executor_threads);
    return RESULT_OK;
}

int worker_start(worker_state_t *worker) {
    if (!worker) return RESULT_ERR_INVALID;
    
    // Start executor threads
    /*
    for (size_t i = 0; i < worker->num_executor_threads; i++) {
        if (pthread_create(&worker->executor_threads[i], NULL, 
                          worker_executor_thread_fn, worker) != 0) {
            LOG_ERROR("Failed to start executor thread %zu", i);
            
            // Stop already started threads
            worker->shutdown_flag = 1;
            for (size_t j = 0; j < i; j++) {
                pthread_join(worker->executor_threads[j], NULL);
            }
            return RESULT_ERR_INVALID;
        }
    }
    */

    // Start executor threads using unified executor
    unified_node_t temp_node;
    temp_node.node_id = worker->worker_id;
    temp_node.shutdown_flag = 0;
    temp_node.active_executions = 0;
    temp_node.num_executor_threads = worker->num_executor_threads;
    temp_node.message_queue = worker->message_queue;
    temp_node.dag_catalog = worker->dag_catalog;
    temp_node.peer_pool.peers = NULL;  // Not needed for execution
    temp_node.capabilities.can_execute = 1;
    
    // Copy metrics handles
    temp_node.metric_messages_processed = worker->metric_messages_processed;
    temp_node.metric_messages_failed = worker->metric_messages_failed;
    temp_node.metric_queue_size = worker->metric_queue_size;
    temp_node.metric_active_executions = worker->metric_active_executions;
    
    if (node_start_executors(&temp_node) != RESULT_OK) {
        LOG_ERROR("Failed to start executor threads");
        return RESULT_ERR_INVALID;
    }
    
    // Copy back thread handles for cleanup
    worker->num_executor_threads = temp_node.num_executor_threads;
    // Copy thread handles (array to array)
    for (size_t i = 0; i < worker->num_executor_threads && i < 16; i++) {
        worker->executor_threads[i] = temp_node.executor_threads[i];
    }

    free(temp_node.executor_threads);
    temp_node.executor_threads = NULL;
    
    LOG_INFO("Worker %u started (%zu executor threads)", 
             worker->worker_id, worker->num_executor_threads);
    return RESULT_OK;
}

void worker_shutdown(worker_state_t *worker) {
    if (!worker) return;
    
    LOG_INFO("Shutting down worker %u", worker->worker_id);
    
    // Set shutdown flag
    worker->shutdown_flag = 1;
    
    // Wait for executor threads to complete using unified shutdown
    if (worker->num_executor_threads > 0) {
        LOG_INFO("Stopping %zu executor threads...", worker->num_executor_threads);
        
        unified_node_t temp_node;
        temp_node.shutdown_flag = 1;
        temp_node.num_executor_threads = worker->num_executor_threads;
        temp_node.executor_threads = worker->executor_threads;  // Direct array assignment OK
        
        node_stop_executors(&temp_node);
        
        // Clear thread count (array itself is part of struct, not freed)
        worker->num_executor_threads = 0;
        
        LOG_INFO("Executor threads stopped");
    }
    
    // Shutdown membership (gossip protocol)
    if (worker->membership) {
        LOG_INFO("Shutting down membership/gossip...");
        membership_shutdown(worker->membership);
        worker->membership = NULL;
    }
    
    // Shutdown metrics system
    if (worker->metrics_server) {
        LOG_INFO("Shutting down metrics server...");
        metrics_server_shutdown(worker->metrics_server);
        worker->metrics_server = NULL;
    }
    
    if (worker->metrics_registry) {
        LOG_INFO("Destroying metrics registry...");
        metrics_registry_destroy(worker->metrics_registry);
        worker->metrics_registry = NULL;
    }
    
    // Cleanup router connections
    LOG_INFO("Cleaning up router connections...");
    pthread_mutex_lock(&worker->routers_lock);
    
    for (size_t i = 0; i < MAX_ROUTER_CONNECTIONS; i++) {
        if (worker->routers[i].active) {
            if (worker->routers[i].data_channel) {
                rpc_channel_destroy(worker->routers[i].data_channel);
                safe_free(worker->routers[i].data_channel);
                worker->routers[i].data_channel = NULL;
            }
            worker->routers[i].active = 0;
        }
    }
    
    pthread_mutex_unlock(&worker->routers_lock);
    pthread_mutex_destroy(&worker->routers_lock);
    
    // Cleanup resources
    LOG_INFO("Destroying worker resources...");
    cluster_view_destroy(&worker->cluster_view);
    message_queue_destroy(&worker->message_queue);
    dag_catalog_destroy(&worker->dag_catalog);

    LOG_INFO("Worker %u shutdown complete", worker->worker_id);
}

int worker_enqueue_message(worker_state_t *worker, execution_id_t exec_id,
                          rule_id_t dag_id, node_id_t sender_id,
                          const uint8_t *message_data, size_t message_len) {
    if (!worker || !message_data || message_len == 0 || message_len > MAX_MESSAGE_SIZE) {
        return RESULT_ERR_INVALID;
    }
    
    message_t message;
    memset(&message, 0, sizeof(message));
    
    message.exec_id = exec_id;
    message.dag_id = dag_id;
    message.sender_id = sender_id;
    message.received_at_ms = time_now_ms();
    
    memcpy(message.message_data, message_data, message_len);
    message.message_len = message_len;
    
    return message_queue_push(&worker->message_queue, &message);
}

// ============================================================================
// WORKER BOOTSTRAP FROM CONFIG
// ============================================================================
/*
int worker_bootstrap_from_config(worker_state_t *worker, const roole_config_t *config) {
    if (!worker || !config || config->router_count == 0) {
        LOG_ERROR("Invalid bootstrap configuration");
        return RESULT_ERR_INVALID;
    }
    
    // Select random seed router
    size_t router_idx = rand() % config->router_count;
    const char *router_addr = config->routers[router_idx];
    
    char router_ip[16];
    uint16_t router_gossip_port;
    config_parse_address(router_addr, router_ip, &router_gossip_port);
    
    LOG_INFO("Bootstrapping from router %s:%u", router_ip, router_gossip_port);
    
    // Build WORKER_JOIN message
    gossip_message_t join_msg = {
        .version = 1,
        .msg_type = GOSSIP_MSG_WORKER_JOIN,
        .sender_id = worker->worker_id,
        .sequence_num = 1,
        .num_updates = 1
    };
    
    join_msg.updates[0].node_id = worker->worker_id;
    join_msg.updates[0].node_type = NODE_TYPE_WORKER;
    join_msg.updates[0].status = NODE_STATUS_ALIVE;
    join_msg.updates[0].incarnation = 0;
    safe_strncpy(join_msg.updates[0].ip_address, worker->bind_addr, MAX_IP_LEN);
    join_msg.updates[0].gossip_port = worker->gossip_port;
    join_msg.updates[0].data_port = worker->data_port;
    
    // Create UDP socket for bootstrap
    int bootstrap_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (bootstrap_sock < 0) {
        LOG_ERROR("Cannot create bootstrap socket: %s", strerror(errno));
        return RESULT_ERR_NETWORK;
    }
    
    // Serialize message
    uint8_t buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    ssize_t msg_size = gossip_message_serialize(&join_msg, buffer, sizeof(buffer));
    
    if (msg_size < 0) {
        LOG_ERROR("Failed to serialize WORKER_JOIN message");
        close(bootstrap_sock);
        return RESULT_ERR_INVALID;
    }
    
    LOG_DEBUG("WORKER_JOIN serialized: %zd bytes (num_updates=%u)", 
             msg_size, join_msg.num_updates);

    // Send to seed router
    struct sockaddr_in router_addr_in;
    memset(&router_addr_in, 0, sizeof(router_addr_in));
    router_addr_in.sin_family = AF_INET;
    router_addr_in.sin_port = htons(router_gossip_port);
    
    if (inet_pton(AF_INET, router_ip, &router_addr_in.sin_addr) <= 0) {
        LOG_ERROR("Invalid router IP address: %s", router_ip);
        close(bootstrap_sock);
        return RESULT_ERR_INVALID;
    }
    
    if (sendto(bootstrap_sock, buffer, msg_size, 0,
              (struct sockaddr*)&router_addr_in, sizeof(router_addr_in)) < 0) {
        LOG_ERROR("Failed to send WORKER_JOIN: %s", strerror(errno));
        close(bootstrap_sock);
        return RESULT_ERR_NETWORK;
    }
    
    LOG_INFO("WORKER_JOIN sent to %s:%u, waiting for response...", 
             router_ip, router_gossip_port);
    
    // Set receive timeout (5 seconds)
    struct timeval tv = {.tv_sec = 5, .tv_usec = 0};
    if (setsockopt(bootstrap_sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        LOG_WARN("Failed to set socket timeout: %s", strerror(errno));
    }
    
    // Wait for JOIN_RESPONSE
    uint8_t recv_buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    ssize_t received = recvfrom(bootstrap_sock, recv_buffer, sizeof(recv_buffer), 
                               0, NULL, NULL);
    
    close(bootstrap_sock);
    
    if (received < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            LOG_ERROR("Timeout waiting for JOIN_RESPONSE (5 seconds)");
            return RESULT_ERR_TIMEOUT;
        }
        LOG_ERROR("Failed to receive JOIN_RESPONSE: %s", strerror(errno));
        return RESULT_ERR_NETWORK;
    }
    
    LOG_DEBUG("Received response: %zd bytes", received);
    
    // Deserialize response
    gossip_message_t response;
    if (gossip_message_deserialize(recv_buffer, received, &response) != 0) {
        LOG_ERROR("Invalid JOIN_RESPONSE (deserialization failed)");
        return RESULT_ERR_INVALID;
    }
    
    if (response.msg_type != GOSSIP_MSG_JOIN_RESPONSE) {
        LOG_ERROR("Unexpected message type: %u (expected %u)", 
                  response.msg_type, GOSSIP_MSG_JOIN_RESPONSE);
        return RESULT_ERR_INVALID;
    }
    
    LOG_INFO("Received JOIN_RESPONSE from router %u", response.sender_id);
    
    // Parse bootstrap data (list of routers)
    gossip_bootstrap_response_t bootstrap_data;
    size_t header_size = 16 + response.num_updates * 44;
    
    if ((size_t)received < header_size) {
        LOG_ERROR("JOIN_RESPONSE too short: %zd bytes (expected at least %zu)", 
                  received, header_size);
        return RESULT_ERR_INVALID;
    }
    
    if (gossip_deserialize_bootstrap_response(recv_buffer + header_size,
                                              received - header_size,
                                              &bootstrap_data) != 0) {
        LOG_ERROR("Failed to deserialize bootstrap data");
        return RESULT_ERR_INVALID;
    }
    
    LOG_INFO("Received %u router addresses in bootstrap response", 
             bootstrap_data.num_routers);
    
    // Connect to all routers in bootstrap response
    for (uint8_t i = 0; i < bootstrap_data.num_routers; i++) {
        char router_data_ip[16];
        uint16_t router_data_port;
        
        config_parse_address(bootstrap_data.routers[i].data_addr, 
                           router_data_ip, &router_data_port);
        
        LOG_INFO("Connecting to router %u at %s (DATA port %u)", 
                 bootstrap_data.routers[i].node_id,
                 bootstrap_data.routers[i].data_addr,
                 router_data_port);
        
        int result = worker_add_router(worker, 
                                      bootstrap_data.routers[i].node_id,
                                      router_data_ip, 
                                      router_data_port);
        
        if (result == RESULT_OK) {
            LOG_INFO("Successfully connected to router %u", 
                     bootstrap_data.routers[i].node_id);
        } else {
            LOG_WARN("Failed to connect to router %u (error: %d)", 
                     bootstrap_data.routers[i].node_id, result);
        }
    }
    
    LOG_INFO("Worker bootstrap completed successfully");
    return RESULT_OK;
}
*/
// ============================================================================
// ROUTER MANAGEMENT
// ============================================================================

int worker_add_router(worker_state_t *worker, node_id_t router_id,
                     const char *ip, uint16_t data_port) {
    if (!worker || !ip) return RESULT_ERR_INVALID;

    pthread_mutex_lock(&worker->routers_lock);

    // Check if router already exists
    for (size_t i = 0; i < worker->router_count; i++) {
        if (worker->routers[i].active && worker->routers[i].router_id == router_id) {
            pthread_mutex_unlock(&worker->routers_lock);
            LOG_DEBUG("Router %u already connected", router_id);
            return RESULT_OK;
        }
    }

    // Find free slot
    size_t slot = SIZE_MAX;
    for (size_t i = 0; i < MAX_ROUTER_CONNECTIONS; i++) {
        if (!worker->routers[i].active) {
            slot = i;
            break;
        }
    }

    if (slot == SIZE_MAX) {
        pthread_mutex_unlock(&worker->routers_lock);
        LOG_ERROR("Maximum router connections reached (%d)", MAX_ROUTER_CONNECTIONS);
        return RESULT_ERR_FULL;
    }

    // Initialize router connection
    router_connection_t *conn = &worker->routers[slot];
    memset(conn, 0, sizeof(router_connection_t));

    conn->router_id = router_id;
    safe_strncpy(conn->ip, ip, MAX_IP_LEN);
    conn->data_port = data_port;
    conn->last_sync_ms = time_now_ms();
    conn->active = 1;

    // Allocate DATA channel
    conn->data_channel = safe_malloc(sizeof(rpc_channel_t));
    if (!conn->data_channel) {
        LOG_ERROR("Failed to allocate RPC data channel");
        conn->active = 0;
        pthread_mutex_unlock(&worker->routers_lock);
        return RESULT_ERR_NOMEM;
    }

    // Connect DATA channel
    if (rpc_client_connect(conn->data_channel, ip, data_port,
                          RPC_CHANNEL_DATA, 4096) != 0) {
        LOG_ERROR("Failed to connect DATA channel to router %u at %s:%u", 
                  router_id, ip, data_port);
        safe_free(conn->data_channel);
        conn->data_channel = NULL;
        conn->active = 0;
        pthread_mutex_unlock(&worker->routers_lock);
        return RESULT_ERR_NETWORK;
    }

    // Update router count
    if (slot >= worker->router_count) {
        worker->router_count = slot + 1;
    }

    pthread_mutex_unlock(&worker->routers_lock);

    LOG_INFO("Connected to router %u (%s DATA:%u)", router_id, ip, data_port);
    return RESULT_OK;
}

int worker_remove_router(worker_state_t *worker, node_id_t router_id) {
    if (!worker) return RESULT_ERR_INVALID;

    pthread_mutex_lock(&worker->routers_lock);

    for (size_t i = 0; i < MAX_ROUTER_CONNECTIONS; i++) {
        if (worker->routers[i].active && worker->routers[i].router_id == router_id) {
            // Close and free DATA channel
            if (worker->routers[i].data_channel) {
                rpc_channel_destroy(worker->routers[i].data_channel);
                safe_free(worker->routers[i].data_channel);
            }

            worker->routers[i].active = 0;

            pthread_mutex_unlock(&worker->routers_lock);
            LOG_INFO("Disconnected from router %u", router_id);
            return RESULT_OK;
        }
    }

    pthread_mutex_unlock(&worker->routers_lock);
    LOG_WARN("Router %u not found (cannot remove)", router_id);
    return RESULT_ERR_NOTFOUND;
}

void worker_update_cluster_metrics(worker_state_t *worker) {
    if (!worker) return;
    
    // Bridge to unified metrics (temporary)
    unified_node_t temp_node;
    temp_node.cluster_view = worker->cluster_view;
    temp_node.metric_cluster_members_total = worker->metric_cluster_members_total;
    temp_node.metric_cluster_members_active = worker->metric_cluster_members_active;
    temp_node.metric_cluster_members_suspect = worker->metric_cluster_members_suspect;
    temp_node.metric_cluster_members_dead = worker->metric_cluster_members_dead;
    
    node_metrics_update_cluster(&temp_node);
}