// src/worker/worker_core.c

#define _POSIX_C_SOURCE 200809L

#include "roole/worker.h"
#include "roole/common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

// ============================================================================
// WORKER CALLBACKS
// ============================================================================

static void on_member_event(node_id_t node_id, node_type_t type,
                           const char *ip, uint16_t port,
                           const char *event_type, void *user_data) {
    worker_state_t *worker = (worker_state_t*)user_data;

    if (type == NODE_TYPE_ROUTER) {
        if (strcmp(event_type, MEMBER_EVENT_JOIN) == 0) {
            // For multi-channel: assume router uses consecutive ports
            // port = service_port, port+1 = data_port
            uint16_t service_port = port;
            uint16_t data_port = port + 1;
            worker_add_router(worker, node_id, ip, service_port, data_port);
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

void* worker_executor_thread_fn(void *arg) {
    worker_state_t *worker = (worker_state_t*)arg;
    
    LOG_INFO("Worker executor thread started");
    
    while (!worker->shutdown_flag) {
        message_t message;
        
        // Pop message from queue (blocking, 1 second timeout)
        int ret = message_queue_pop(&worker->message_queue, &message, 1000);
        
        if (ret == RESULT_ERR_TIMEOUT || ret == RESULT_ERR_EMPTY) {
            // ADD: Update queue size metric
            worker_metrics_set_queue_size(worker->metrics, 
                                         message_queue_size(&worker->message_queue));
            continue;
        }
        
        if (ret != RESULT_OK) {
            LOG_ERROR("Failed to pop message from queue");
            continue;
        }
        
        // ADD: Calculate queue wait time
        uint64_t now_ms = time_now_ms();
        uint64_t wait_time_us = (now_ms - message.received_at_ms) * 1000;
        worker_metrics_observe_queue_wait_time(worker->metrics, (double)wait_time_us);
        
        LOG_INFO("Processing message %lu (DAG %u)", message.exec_id, message.dag_id);
        
        __sync_fetch_and_add(&worker->active_executions, 1);
        
        // ADD: Update active executions metric
        worker_metrics_set_active_executions(worker->metrics, worker->active_executions);
        
        // ADD: Track processing start time
        struct timespec start_time;
        timespec_now(&start_time);
        
        // COMMENT: Simple 500ms processing simulation
        usleep(500000);  // 500ms
        
        // ADD: Calculate processing time
        struct timespec end_time;
        timespec_now(&end_time);
        double processing_time_us = time_diff_us(&start_time, &end_time);
        
        // COMMENT: Send success status back to router
        execution_status_t status = EXEC_STATUS_COMPLETED;
        
        // Find router connection (use first active router)
        pthread_mutex_lock(&worker->routers_lock);
        router_connection_t *router_conn = NULL;
        for (size_t i = 0; i < MAX_ROUTER_CONNECTIONS; i++) {
            if (worker->routers[i].active) {
                router_conn = &worker->routers[i];
                break;
            }
        }
        
        if (router_conn && router_conn->data_channel) {
            // Build status update payload: [exec_id: 8][status: 1]
            uint8_t payload[9];
            memcpy(payload, &message.exec_id, 8);
            payload[8] = (uint8_t)status;
            
            // Pack and send via DATA channel
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
            
            // ADD: Record metrics
            worker_metrics_observe_processing_time(worker->metrics, processing_time_us);
            worker_metrics_inc_messages_processed(worker->metrics);
        } else {
            LOG_ERROR("No router connection available for status update");
            // ADD: Record failure
            worker_metrics_inc_messages_failed(worker->metrics);
        }
        pthread_mutex_unlock(&worker->routers_lock);
        
        __sync_fetch_and_sub(&worker->active_executions, 1);
        
        // ADD: Update active executions metric
        worker_metrics_set_active_executions(worker->metrics, worker->active_executions);
    }
    
    LOG_INFO("Worker executor thread stopped");
    return NULL;
}

// ============================================================================
// HEARTBEAT THREAD
// ============================================================================

static void* worker_heartbeat_thread_fn(void *arg) {
    worker_state_t *worker = (worker_state_t*)arg;
    
    LOG_INFO("Worker heartbeat thread started");
    
    while (!worker->shutdown_flag) {
        usleep(DEFAULT_HEARTBEAT_INTERVAL_MS * 1000);
        
        worker_send_heartbeat(worker);
    }
    
    LOG_INFO("Worker heartbeat thread stopped");
    return NULL;
}

// ============================================================================
// WORKER INITIALIZATION
// ============================================================================

int worker_init(worker_state_t *worker, node_id_t worker_id,
               uint16_t service_port, uint16_t data_port,
               size_t num_executor_threads, uint16_t metrics_port) {
    if (!worker || num_executor_threads == 0 || num_executor_threads > 16) {
        return RESULT_ERR_INVALID;
    }

    memset(worker, 0, sizeof(worker_state_t));
    worker->worker_id = worker_id;
    worker->service_port = service_port;
    worker->data_port = data_port;
    worker->num_executor_threads = num_executor_threads;
    worker->shutdown_flag = 0;
    worker->active_executions = 0;
    worker->catalog_version = 0;
    
    // Initialize DAG catalog
    if (dag_catalog_init(&worker->dag_catalog, MAX_DAGS) != RESULT_OK) {
        LOG_ERROR("Failed to initialize DAG catalog");
        return RESULT_ERR_INVALID;
    }
    
    // Initialize message queue
    if (message_queue_init(&worker->message_queue, MAX_WORKER_QUEUE_SIZE) != RESULT_OK) {
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
    
    // Initialize membership
    char bind_addr[32];
    snprintf(bind_addr, sizeof(bind_addr), "0.0.0.0");

    if (membership_init(&worker->membership, worker_id, NODE_TYPE_WORKER,
                       bind_addr, service_port) != RESULT_OK) {
        LOG_ERROR("Failed to initialize membership");
        pthread_mutex_destroy(&worker->routers_lock);
        cluster_view_destroy(&worker->cluster_view);
        message_queue_destroy(&worker->message_queue);
        dag_catalog_destroy(&worker->dag_catalog);
        return RESULT_ERR_INVALID;
    }

    membership_set_callback(worker->membership, on_member_event, worker);

    // ADD THIS BLOCK:
    // Initialize metrics (optional)
    if (metrics_port > 0) {
        worker->metrics = worker_metrics_init(worker_id, metrics_port);
        if (!worker->metrics) {
            LOG_WARN("WORKER", "Failed to initialize metrics (continuing without metrics)");
        }
    } else {
        worker->metrics = NULL;
        LOG_INFO("WORKER", "Metrics disabled (no metrics_port specified)");
    }

    LOG_INFO("Worker %u initialized (SERVICE:%u, DATA:%u, %zu executor threads)",
                   worker_id, service_port, data_port, num_executor_threads);
    return RESULT_OK;
}

int worker_start(worker_state_t *worker) {
    if (!worker) return RESULT_ERR_INVALID;
    
    // Start executor threads
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
    
    // Start heartbeat thread
    if (pthread_create(&worker->heartbeat_thread, NULL, 
                      worker_heartbeat_thread_fn, worker) != 0) {
        LOG_ERROR("Failed to start heartbeat thread");
        
        worker->shutdown_flag = 1;
        for (size_t i = 0; i < worker->num_executor_threads; i++) {
            pthread_join(worker->executor_threads[i], NULL);
        }
        return RESULT_ERR_INVALID;
    }
    
    // TODO: Start RPC worker for receiving messages from routers
    // rpc_worker_run(worker->port, worker_rpc_service_table);
    
    LOG_INFO("Worker %u started", worker->worker_id);
    return RESULT_OK;
}

void worker_shutdown(worker_state_t *worker) {
    if (!worker) return;
    
    LOG_INFO("Shutting down worker %u", worker->worker_id);
    
    worker->shutdown_flag = 1;
    
    // Wait for all executor threads
    for (size_t i = 0; i < worker->num_executor_threads; i++) {
        pthread_join(worker->executor_threads[i], NULL);
    }
    
    // Wait for heartbeat thread
    pthread_join(worker->heartbeat_thread, NULL);
    
    // Cleanup
    if (worker->membership) {
        membership_shutdown(worker->membership);
    }
    
    pthread_mutex_destroy(&worker->routers_lock);
    cluster_view_destroy(&worker->cluster_view);
    message_queue_destroy(&worker->message_queue);
    dag_catalog_destroy(&worker->dag_catalog);
    
    if (worker->metrics) {
        worker_metrics_shutdown(worker->metrics);
        worker->metrics = NULL;
    }

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
// message MANAGEMENT
// ============================================================================
/*
int worker_enqueue_task(worker_state_t *worker, execution_id_t exec_id,
                       rule_id_t dag_id, node_id_t router_id,
                       const uint8_t *message, size_t message_len) {
    if (!worker || !message || message_len == 0 || message_len > MAX_MESSAGE_SIZE) {
        return RESULT_ERR_INVALID;
    }
    
    task_t task;
    memset(&task, 0, sizeof(task));
    
    task.exec_id = exec_id;
    task.dag_id = dag_id;
    task.router_id = router_id;
    task.received_at_ms = time_now_ms();
    
    memcpy(task.message_data, message, message_len);
    task.message_len = message_len;
    
    return task_queue_push(&worker->task_queue, &task);
}
*/
// ============================================================================
// ROUTER MANAGEMENT
// ============================================================================

int worker_add_router(worker_state_t *worker, node_id_t router_id,
                     const char *ip, uint16_t service_port, uint16_t data_port) {
    if (!worker || !ip) return RESULT_ERR_INVALID;

    pthread_mutex_lock(&worker->routers_lock);

    // Check if already exists
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
        LOG_ERROR("Maximum router connections reached");
        return RESULT_ERR_FULL;
    }

    router_connection_t *conn = &worker->routers[slot];
    memset(conn, 0, sizeof(router_connection_t));

    conn->router_id = router_id;
    safe_strncpy(conn->ip, ip, MAX_IP_LEN);
    conn->service_port = service_port;
    conn->data_port = data_port;
    conn->last_sync_ms = time_now_ms();
    conn->active = 1;

    // Initialize RPC channels to router (SERVICE and DATA)
    conn->service_channel = safe_malloc(sizeof(rpc_channel_t));
    conn->data_channel = safe_malloc(sizeof(rpc_channel_t));

    if (!conn->service_channel || !conn->data_channel) {
        LOG_ERROR("Failed to allocate RPC channels");
        if (conn->service_channel) safe_free(conn->service_channel);
        if (conn->data_channel) safe_free(conn->data_channel);
        pthread_mutex_unlock(&worker->routers_lock);
        return RESULT_ERR_NOMEM;
    }

    // Connect SERVICE channel
    if (rpc_client_connect(conn->service_channel, ip, service_port,
                          RPC_CHANNEL_SERVICE, 4096) != 0) {
        LOG_ERROR("Failed to connect SERVICE channel to router %u", router_id);
        safe_free(conn->service_channel);
        safe_free(conn->data_channel);
        conn->service_channel = NULL;
        conn->data_channel = NULL;
        conn->active = 0;
        pthread_mutex_unlock(&worker->routers_lock);
        return RESULT_ERR_NETWORK;
    }

    // Connect DATA channel
    if (rpc_client_connect(conn->data_channel, ip, data_port,
                          RPC_CHANNEL_DATA, 4096) != 0) {
        LOG_ERROR("Failed to connect DATA channel to router %u", router_id);
        rpc_channel_destroy(conn->service_channel);
        safe_free(conn->service_channel);
        safe_free(conn->data_channel);
        conn->service_channel = NULL;
        conn->data_channel = NULL;
        conn->active = 0;
        pthread_mutex_unlock(&worker->routers_lock);
        return RESULT_ERR_NETWORK;
    }

    if (slot >= worker->router_count) {
        worker->router_count = slot + 1;
    }

    pthread_mutex_unlock(&worker->routers_lock);

    LOG_INFO("Connected to router %u (%s SERVICE:%u DATA:%u)",
                   router_id, ip, service_port, data_port);

    return RESULT_OK;
}

int worker_remove_router(worker_state_t *worker, node_id_t router_id) {
    if (!worker) return RESULT_ERR_INVALID;

    pthread_mutex_lock(&worker->routers_lock);

    for (size_t i = 0; i < MAX_ROUTER_CONNECTIONS; i++) {
        if (worker->routers[i].active && worker->routers[i].router_id == router_id) {
            // Close RPC channels (both service and data)
            if (worker->routers[i].service_channel) {
                rpc_channel_destroy(worker->routers[i].service_channel);
                safe_free(worker->routers[i].service_channel);
            }
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
    return RESULT_ERR_NOTFOUND;
}

// ============================================================================
// COMMUNICATION WITH ROUTERS
// ============================================================================

int worker_send_heartbeat(worker_state_t *worker) {
    if (!worker) return RESULT_ERR_INVALID;
    
    pthread_mutex_lock(&worker->routers_lock);
    
    for (size_t i = 0; i < MAX_ROUTER_CONNECTIONS; i++) {
        if (worker->routers[i].active) {
            node_id_t router_id = worker->routers[i].router_id;
            
            // TODO: Send heartbeat via RPC
            // Payload: worker_id, active_executions, load_score
            
            uint32_t active = worker->active_executions;
            float load = (float)active / (float)worker->num_executor_threads;
            
            LOG_DEBUG("Heartbeat to router %u (active=%u, load=%.2f)", 
                           router_id, active, load);

            uint8_t payload[16];
            memcpy(payload, &worker->worker_id, sizeof(node_id_t));
            memcpy(payload + sizeof(node_id_t), &active, sizeof(uint32_t));
            memcpy(payload + sizeof(node_id_t) + 4, &load, sizeof(float));

            // Pack and send heartbeat message via SERVICE channel
            size_t msg_len = rpc_pack_message(
                worker->routers[i].service_channel->tx_buffer,
                worker->worker_id,
                0,
                RPC_TYPE_REQUEST,
                RPC_STATUS_UNKNOWN,
                FUNC_ID_WORKER_HEARTBEAT,
                payload,
                16
            );

            if (send(worker->routers[i].service_channel->socket_fd,
                    worker->routers[i].service_channel->tx_buffer, msg_len, 0) <= 0) {
                LOG_ERROR("Failed to send heartbeat");
                return RESULT_ERR_NETWORK;
            }
        }
    }
    
    pthread_mutex_unlock(&worker->routers_lock);
    
    return RESULT_OK;
}

int worker_register_with_router(worker_state_t *worker, const char *router_ip,
                                uint16_t service_port, uint16_t data_port) {
    if (!worker || !router_ip) return RESULT_ERR_INVALID;

    LOG_INFO("Registering with router at %s (SERVICE:%u DATA:%u)",
                   router_ip, service_port, data_port);

    // Connect to router's SERVICE channel for registration
    rpc_channel_t channel;
    if (rpc_client_connect(&channel, router_ip, service_port,
                          RPC_CHANNEL_SERVICE, 4096) != 0) {
        LOG_ERROR("Failed to connect to router SERVICE channel for registration");
        return RESULT_ERR_NETWORK;
    }

    // Build registration payload: [worker_id][service_port][data_port]
    uint8_t payload[6];
    memcpy(payload, &worker->worker_id, sizeof(node_id_t));
    memcpy(payload + sizeof(node_id_t), &worker->service_port, sizeof(uint16_t));
    memcpy(payload + sizeof(node_id_t) + 2, &worker->data_port, sizeof(uint16_t));
    
    // Pack and send registration message
    size_t msg_len = rpc_pack_message(
        channel.tx_buffer,
        worker->worker_id,
        1,  // request_id
        RPC_TYPE_REQUEST,
        RPC_STATUS_UNKNOWN,
        FUNC_ID_WORKER_REGISTRATION,
        payload,
        6
    );
    
    if (send(channel.socket_fd, channel.tx_buffer, msg_len, 0) <= 0) {
        LOG_ERROR("Failed to send registration");
        rpc_channel_destroy(&channel);
        return RESULT_ERR_NETWORK;
    }
    
    // Wait for ACK
    rpc_header_t resp_header;
    if (recv(channel.socket_fd, channel.rx_buffer, RPC_HEADER_SIZE, 0) <= 0) {
        LOG_ERROR("Failed to receive registration ACK");
        rpc_channel_destroy(&channel);
        return RESULT_ERR_NETWORK;
    }
    
    rpc_unpack_header(channel.rx_buffer, &resp_header);
    
    if (resp_header.type_and_status.fields.status == RPC_STATUS_SUCCESS) {
        LOG_INFO("Successfully registered with router");

        // Add router to connections (using the same ports we just connected to)
        worker_add_router(worker, 1, router_ip, service_port, data_port);

        rpc_channel_destroy(&channel);
        return RESULT_OK;
    }
    
    LOG_ERROR("Registration failed");
    rpc_channel_destroy(&channel);
    return RESULT_ERR_INVALID;
}

int worker_send_execution_update(worker_state_t *worker, node_id_t router_id,
                                execution_id_t exec_id, execution_status_t status) {
    if (!worker || exec_id == 0) return RESULT_ERR_INVALID;
    
    pthread_mutex_lock(&worker->routers_lock);
    
    router_connection_t *conn = NULL;
    for (size_t i = 0; i < MAX_ROUTER_CONNECTIONS; i++) {
        if (worker->routers[i].active && worker->routers[i].router_id == router_id) {
            conn = &worker->routers[i];
            break;
        }
    }
    
    pthread_mutex_unlock(&worker->routers_lock);
    
    if (!conn) {
        LOG_WARN("Router %u not found for execution update", router_id);
        return RESULT_ERR_NOTFOUND;
    }
    
    // TODO: Send execution status via RPC
    // Payload: exec_id, status
    
    LOG_DEBUG("Execution update to router %u: exec %lu -> status %d", 
                   router_id, exec_id, status);
    
    // TODO: Actual RPC call
    // rpc_pack_message(..., FUNC_ID_EXECUTION_UPDATE, ...);
    
    return RESULT_OK;
}

int worker_sync_catalog_from_router(worker_state_t *worker, node_id_t router_id) {
    if (!worker) return RESULT_ERR_INVALID;
    
    // TODO: Request full DAG catalog from router via RPC
    // This should be done periodically or when worker detects catalog is stale
    
    LOG_INFO("Syncing DAG catalog from router %u", router_id);
    
    // TODO: Actual RPC call to fetch catalog
    // rpc_pack_message(..., FUNC_ID_SYNC_CATALOG, ...);
    
    return RESULT_OK;
}