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
            worker_add_router(worker, node_id, ip, port);
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
    
    ROOLE_LOG_INFO("Worker executor thread started");
    
    while (!worker->shutdown_flag) {
        task_t task;
        
        // Pop task from queue (blocking, 1 second timeout)
        int ret = task_queue_pop(&worker->task_queue, &task, 1000);
        
        if (ret == ROOLE_ERR_TIMEOUT || ret == ROOLE_ERR_EMPTY) {
            continue;  // No task available, retry
        }
        
        if (ret != ROOLE_OK) {
            ROOLE_LOG_ERROR("Failed to pop task from queue");
            continue;
        }
        
        ROOLE_LOG_INFO("Executing task %lu (DAG %u)", task.exec_id, task.dag_id);
        
        // Increment active executions
        __sync_fetch_and_add(&worker->active_executions, 1);
        
        // Get DAG from catalog
        dag_t *dag = dag_catalog_get(&worker->dag_catalog, task.dag_id);
        if (!dag) {
            ROOLE_LOG_ERROR("DAG %u not found in worker catalog", task.dag_id);
            worker_send_execution_update(worker, task.router_id, 
                                        task.exec_id, EXEC_STATUS_FAILED);
            __sync_fetch_and_sub(&worker->active_executions, 1);
            continue;
        }
        
        // Execute DAG
        dag_execution_context_t ctx;
        memset(&ctx, 0, sizeof(ctx));
        
        ctx.exec_id = task.exec_id;
        ctx.dag = dag;
        ctx.input_data = task.message_data;
        ctx.input_len = task.message_len;
        
        // Allocate output buffer
        ctx.output_capacity = MAX_MESSAGE_SIZE;
        ctx.output_data = roole_malloc(ctx.output_capacity);
        
        struct timespec start_time;
        roole_timespec_now(&start_time);
        
        int exec_result = dag_execute(&ctx);
        
        struct timespec end_time;
        roole_timespec_now(&end_time);
        double exec_time_us = roole_time_diff_us(&start_time, &end_time);
        
        dag_catalog_release(&worker->dag_catalog);
        
        // Send result to router
        execution_status_t status = (exec_result == ROOLE_OK) ? 
                                    EXEC_STATUS_COMPLETED : EXEC_STATUS_FAILED;
        
        worker_send_execution_update(worker, task.router_id, task.exec_id, status);
        
        if (status == EXEC_STATUS_COMPLETED) {
            ROOLE_LOG_INFO("Task %lu completed in %.2f us", task.exec_id, exec_time_us);
        } else {
            ROOLE_LOG_ERROR("Task %lu failed", task.exec_id);
        }
        
        // Cleanup
        if (ctx.output_data) {
            roole_free(ctx.output_data);
        }
        
        // Decrement active executions
        __sync_fetch_and_sub(&worker->active_executions, 1);
    }
    
    ROOLE_LOG_INFO("Worker executor thread stopped");
    return NULL;
}

// ============================================================================
// HEARTBEAT THREAD
// ============================================================================

static void* worker_heartbeat_thread_fn(void *arg) {
    worker_state_t *worker = (worker_state_t*)arg;
    
    ROOLE_LOG_INFO("Worker heartbeat thread started");
    
    while (!worker->shutdown_flag) {
        usleep(DEFAULT_HEARTBEAT_INTERVAL_MS * 1000);
        
        worker_send_heartbeat(worker);
    }
    
    ROOLE_LOG_INFO("Worker heartbeat thread stopped");
    return NULL;
}

// ============================================================================
// WORKER INITIALIZATION
// ============================================================================

int worker_init(worker_state_t *worker, node_id_t worker_id, uint16_t port, 
               size_t num_executor_threads) {
    if (!worker || num_executor_threads == 0 || num_executor_threads > 16) {
        return ROOLE_ERR_INVALID;
    }
    
    memset(worker, 0, sizeof(worker_state_t));
    worker->worker_id = worker_id;
    worker->port = port;
    worker->num_executor_threads = num_executor_threads;
    worker->shutdown_flag = 0;
    worker->active_executions = 0;
    worker->catalog_version = 0;
    
    // Initialize DAG catalog
    if (dag_catalog_init(&worker->dag_catalog, MAX_DAGS) != ROOLE_OK) {
        ROOLE_LOG_ERROR("Failed to initialize DAG catalog");
        return ROOLE_ERR_INVALID;
    }
    
    // Initialize task queue
    if (task_queue_init(&worker->task_queue, MAX_WORKER_QUEUE_SIZE) != ROOLE_OK) {
        ROOLE_LOG_ERROR("Failed to initialize task queue");
        dag_catalog_destroy(&worker->dag_catalog);
        return ROOLE_ERR_INVALID;
    }
    
    // Initialize cluster view
    if (cluster_view_init(&worker->cluster_view, MAX_CLUSTER_NODES) != ROOLE_OK) {
        ROOLE_LOG_ERROR("Failed to initialize cluster view");
        task_queue_destroy(&worker->task_queue);
        dag_catalog_destroy(&worker->dag_catalog);
        return ROOLE_ERR_INVALID;
    }
    
    // Initialize routers lock
    if (pthread_mutex_init(&worker->routers_lock, NULL) != 0) {
        ROOLE_LOG_ERROR("Failed to initialize routers lock");
        cluster_view_destroy(&worker->cluster_view);
        task_queue_destroy(&worker->task_queue);
        dag_catalog_destroy(&worker->dag_catalog);
        return ROOLE_ERR_INVALID;
    }
    
    // Initialize membership
    char bind_addr[32];
    snprintf(bind_addr, sizeof(bind_addr), "0.0.0.0");
    
    if (membership_init(&worker->membership, worker_id, NODE_TYPE_WORKER, 
                       bind_addr, port + 1000) != ROOLE_OK) {
        ROOLE_LOG_ERROR("Failed to initialize membership");
        pthread_mutex_destroy(&worker->routers_lock);
        cluster_view_destroy(&worker->cluster_view);
        task_queue_destroy(&worker->task_queue);
        dag_catalog_destroy(&worker->dag_catalog);
        return ROOLE_ERR_INVALID;
    }
    
    membership_set_callback(worker->membership, on_member_event, worker);
    
    ROOLE_LOG_INFO("Worker %u initialized on port %u (%zu executor threads)", 
                   worker_id, port, num_executor_threads);
    return ROOLE_OK;
}

int worker_start(worker_state_t *worker) {
    if (!worker) return ROOLE_ERR_INVALID;
    
    // Start executor threads
    for (size_t i = 0; i < worker->num_executor_threads; i++) {
        if (pthread_create(&worker->executor_threads[i], NULL, 
                          worker_executor_thread_fn, worker) != 0) {
            ROOLE_LOG_ERROR("Failed to start executor thread %zu", i);
            
            // Stop already started threads
            worker->shutdown_flag = 1;
            for (size_t j = 0; j < i; j++) {
                pthread_join(worker->executor_threads[j], NULL);
            }
            return ROOLE_ERR_INVALID;
        }
    }
    
    // Start heartbeat thread
    if (pthread_create(&worker->heartbeat_thread, NULL, 
                      worker_heartbeat_thread_fn, worker) != 0) {
        ROOLE_LOG_ERROR("Failed to start heartbeat thread");
        
        worker->shutdown_flag = 1;
        for (size_t i = 0; i < worker->num_executor_threads; i++) {
            pthread_join(worker->executor_threads[i], NULL);
        }
        return ROOLE_ERR_INVALID;
    }
    
    // TODO: Start RPC worker for receiving tasks from routers
    // rpc_worker_run(worker->port, worker_rpc_service_table);
    
    ROOLE_LOG_INFO("Worker %u started", worker->worker_id);
    return ROOLE_OK;
}

void worker_shutdown(worker_state_t *worker) {
    if (!worker) return;
    
    ROOLE_LOG_INFO("Shutting down worker %u", worker->worker_id);
    
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
    task_queue_destroy(&worker->task_queue);
    dag_catalog_destroy(&worker->dag_catalog);
    
    ROOLE_LOG_INFO("Worker %u shutdown complete", worker->worker_id);
}

// ============================================================================
// TASK MANAGEMENT
// ============================================================================

int worker_enqueue_task(worker_state_t *worker, execution_id_t exec_id,
                       dag_id_t dag_id, node_id_t router_id,
                       const uint8_t *message, size_t message_len) {
    if (!worker || !message || message_len == 0 || message_len > MAX_MESSAGE_SIZE) {
        return ROOLE_ERR_INVALID;
    }
    
    task_t task;
    memset(&task, 0, sizeof(task));
    
    task.exec_id = exec_id;
    task.dag_id = dag_id;
    task.router_id = router_id;
    task.received_at_ms = roole_time_now_ms();
    
    memcpy(task.message_data, message, message_len);
    task.message_len = message_len;
    
    return task_queue_push(&worker->task_queue, &task);
}

// ============================================================================
// ROUTER MANAGEMENT
// ============================================================================

int worker_add_router(worker_state_t *worker, node_id_t router_id,
                     const char *ip, uint16_t port) {
    if (!worker || !ip) return ROOLE_ERR_INVALID;
    
    pthread_mutex_lock(&worker->routers_lock);
    
    // Check if already exists
    for (size_t i = 0; i < worker->router_count; i++) {
        if (worker->routers[i].active && worker->routers[i].router_id == router_id) {
            pthread_mutex_unlock(&worker->routers_lock);
            ROOLE_LOG_DEBUG("Router %u already connected", router_id);
            return ROOLE_OK;
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
        ROOLE_LOG_ERROR("Maximum router connections reached");
        return ROOLE_ERR_FULL;
    }
    
    router_connection_t *conn = &worker->routers[slot];
    memset(conn, 0, sizeof(router_connection_t));
    
    conn->router_id = router_id;
    roole_strncpy_safe(conn->ip, ip, MAX_IP_LEN);
    conn->port = port;
    conn->last_sync_ms = roole_time_now_ms();
    conn->active = 1;
    
    // Initialize RPC channel to router
    conn->rpc_channel = roole_malloc(sizeof(rpc_channel_t));
    if (!conn->rpc_channel) {
        ROOLE_LOG_ERROR("Failed to allocate RPC channel");
        pthread_mutex_unlock(&worker->routers_lock);
        return ROOLE_ERR_NOMEM;
    }
    
    if (rpc_router_init(conn->rpc_channel, ip, port, 4096) != 0) {
        ROOLE_LOG_ERROR("Failed to initialize RPC channel to router %u", router_id);
        roole_free(conn->rpc_channel);
        conn->rpc_channel = NULL;
        conn->active = 0;
        pthread_mutex_unlock(&worker->routers_lock);
        return ROOLE_ERR_NETWORK;
    }
    
    if (slot >= worker->router_count) {
        worker->router_count = slot + 1;
    }
    
    pthread_mutex_unlock(&worker->routers_lock);
    
    ROOLE_LOG_INFO("Connected to router %u (%s:%u)", router_id, ip, port);
    
    return ROOLE_OK;
}

int worker_remove_router(worker_state_t *worker, node_id_t router_id) {
    if (!worker) return ROOLE_ERR_INVALID;
    
    pthread_mutex_lock(&worker->routers_lock);
    
    for (size_t i = 0; i < MAX_ROUTER_CONNECTIONS; i++) {
        if (worker->routers[i].active && worker->routers[i].router_id == router_id) {
            // Close RPC channel
            if (worker->routers[i].rpc_channel) {
                rpc_channel_destroy(worker->routers[i].rpc_channel);
                roole_free(worker->routers[i].rpc_channel);
            }
            
            worker->routers[i].active = 0;
            
            pthread_mutex_unlock(&worker->routers_lock);
            ROOLE_LOG_INFO("Disconnected from router %u", router_id);
            return ROOLE_OK;
        }
    }
    
    pthread_mutex_unlock(&worker->routers_lock);
    return ROOLE_ERR_NOTFOUND;
}

// ============================================================================
// COMMUNICATION WITH ROUTERS
// ============================================================================

int worker_send_heartbeat(worker_state_t *worker) {
    if (!worker) return ROOLE_ERR_INVALID;
    
    pthread_mutex_lock(&worker->routers_lock);
    
    for (size_t i = 0; i < MAX_ROUTER_CONNECTIONS; i++) {
        if (worker->routers[i].active) {
            node_id_t router_id = worker->routers[i].router_id;
            
            // TODO: Send heartbeat via RPC
            // Payload: worker_id, active_executions, load_score
            
            uint32_t active = worker->active_executions;
            float load = (float)active / (float)worker->num_executor_threads;
            
            ROOLE_LOG_DEBUG("Heartbeat to router %u (active=%u, load=%.2f)", 
                           router_id, active, load);

            uint8_t payload[16];
            memcpy(payload, &worker->worker_id, sizeof(node_id_t));
            memcpy(payload + sizeof(node_id_t), &active, sizeof(uint32_t));
            memcpy(payload + sizeof(node_id_t) + 4, &load, sizeof(float));
            
            // Pack and send registration message
            size_t msg_len = rpc_pack_message(
                worker->routers[i].rpc_channel->tx_buffer,
                worker->worker_id,
                0,
                RPC_TYPE_REQUEST,
                RPC_STATUS_UNKNOWN,
                FUNC_ID_WORKER_HEARTBEAT,
                payload,
                16
            );
            
            if (send(worker->routers[i].rpc_channel->socket_fd, worker->routers[i].rpc_channel->tx_buffer, msg_len, 0) <= 0) {
                ROOLE_LOG_ERROR("Failed to send heartbeat");
                return ROOLE_ERR_NETWORK;
            }
        }
    }
    
    pthread_mutex_unlock(&worker->routers_lock);
    
    return ROOLE_OK;
}

int worker_register_with_router(worker_state_t *worker, const char *router_ip, uint16_t router_port) {
    if (!worker || !router_ip) return ROOLE_ERR_INVALID;
    
    ROOLE_LOG_INFO("Registering with router at %s:%u", router_ip, router_port);
    
    // Connect to router
    rpc_channel_t channel;
    if (rpc_router_init(&channel, router_ip, router_port, 4096) != 0) {
        ROOLE_LOG_ERROR("Failed to connect to router for registration");
        return ROOLE_ERR_NETWORK;
    }
    
    // Build registration payload: [worker_id][worker_port]
    uint8_t payload[4];
    memcpy(payload, &worker->worker_id, sizeof(node_id_t));
    memcpy(payload + sizeof(node_id_t), &worker->port, sizeof(uint16_t));
    
    // Pack and send registration message
    size_t msg_len = rpc_pack_message(
        channel.tx_buffer,
        worker->worker_id,
        1,  // request_id
        RPC_TYPE_REQUEST,
        RPC_STATUS_UNKNOWN, 
        FUNC_ID_WORKER_REGISTRATION,
        payload,
        4
    );
    
    if (send(channel.socket_fd, channel.tx_buffer, msg_len, 0) <= 0) {
        ROOLE_LOG_ERROR("Failed to send registration");
        rpc_channel_destroy(&channel);
        return ROOLE_ERR_NETWORK;
    }
    
    // Wait for ACK
    rpc_header_t resp_header;
    if (recv(channel.socket_fd, channel.rx_buffer, RPC_HEADER_SIZE, 0) <= 0) {
        ROOLE_LOG_ERROR("Failed to receive registration ACK");
        rpc_channel_destroy(&channel);
        return ROOLE_ERR_NETWORK;
    }
    
    rpc_unpack_header(channel.rx_buffer, &resp_header);
    
    if (resp_header.type_and_status.fields.status == RPC_STATUS_SUCCESS) {
        ROOLE_LOG_INFO("Successfully registered with router");
        
        // Add router to connections
        worker_add_router(worker, 1, router_ip, router_port);
        
        rpc_channel_destroy(&channel);
        return ROOLE_OK;
    }
    
    ROOLE_LOG_ERROR("Registration failed");
    rpc_channel_destroy(&channel);
    return ROOLE_ERR_INVALID;
}

int worker_send_execution_update(worker_state_t *worker, node_id_t router_id,
                                execution_id_t exec_id, execution_status_t status) {
    if (!worker || exec_id == 0) return ROOLE_ERR_INVALID;
    
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
        ROOLE_LOG_WARN("Router %u not found for execution update", router_id);
        return ROOLE_ERR_NOTFOUND;
    }
    
    // TODO: Send execution status via RPC
    // Payload: exec_id, status
    
    ROOLE_LOG_DEBUG("Execution update to router %u: exec %lu -> status %d", 
                   router_id, exec_id, status);
    
    // TODO: Actual RPC call
    // rpc_pack_message(..., FUNC_ID_EXECUTION_UPDATE, ...);
    
    return ROOLE_OK;
}

int worker_sync_catalog_from_router(worker_state_t *worker, node_id_t router_id) {
    if (!worker) return ROOLE_ERR_INVALID;
    
    // TODO: Request full DAG catalog from router via RPC
    // This should be done periodically or when worker detects catalog is stale
    
    ROOLE_LOG_INFO("Syncing DAG catalog from router %u", router_id);
    
    // TODO: Actual RPC call to fetch catalog
    // rpc_pack_message(..., FUNC_ID_SYNC_CATALOG, ...);
    
    return ROOLE_OK;
}