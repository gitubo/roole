// src/worker/worker_core.c

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

// ============================================================================
// WORKER CALLBACKS
// ============================================================================

static void on_member_event(node_id_t node_id, node_type_t type,
                           const char *ip, uint16_t port,
                           const char *event_type, void *user_data) {
    worker_state_t *worker = (worker_state_t*)user_data;

    if (type == NODE_TYPE_ROUTER) {
        if (strcmp(event_type, MEMBER_EVENT_JOIN) == 0) {
            uint16_t data_port = port + 1;
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

void* worker_executor_thread_fn(void *arg) {
    worker_state_t *worker = (worker_state_t*)arg;
    
    LOG_INFO("Worker executor thread started");
    
    while (!worker->shutdown_flag) {
        message_t message;
        
        int ret = message_queue_pop(&worker->message_queue, &message, 1000);
        
        if (ret == RESULT_ERR_TIMEOUT || ret == RESULT_ERR_EMPTY) {
            worker_metrics_set_queue_size(worker->metrics, 
                                         message_queue_size(&worker->message_queue));
            continue;
        }
        
        if (ret != RESULT_OK) {
            LOG_ERROR("Failed to pop message from queue");
            continue;
        }
        
        uint64_t now_ms = time_now_ms();
        uint64_t wait_time_us = (now_ms - message.received_at_ms) * 1000;
        worker_metrics_observe_queue_wait_time(worker->metrics, (double)wait_time_us);
        
        LOG_INFO("Processing message %lu (DAG %u)", message.exec_id, message.dag_id);
        
        __sync_fetch_and_add(&worker->active_executions, 1);
        worker_metrics_set_active_executions(worker->metrics, worker->active_executions);
        
        struct timespec start_time;
        timespec_now(&start_time);
        
        usleep(500000);
        
        struct timespec end_time;
        timespec_now(&end_time);
        double processing_time_us = time_diff_us(&start_time, &end_time);
        
        execution_status_t status = EXEC_STATUS_COMPLETED;
        
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
            
            worker_metrics_observe_processing_time(worker->metrics, processing_time_us);
            worker_metrics_inc_messages_processed(worker->metrics);
        } else {
            LOG_ERROR("No router connection available for status update");
            worker_metrics_inc_messages_failed(worker->metrics);
        }
        pthread_mutex_unlock(&worker->routers_lock);
        
        __sync_fetch_and_sub(&worker->active_executions, 1);
        worker_metrics_set_active_executions(worker->metrics, worker->active_executions);
    }
    
    LOG_INFO("Worker executor thread stopped");
    return NULL;
}

// ============================================================================
// WORKER INITIALIZATION
// ============================================================================

int worker_init(worker_state_t *worker, node_id_t worker_id,
               uint16_t gossip_port, uint16_t data_port,
               const char *bind_addr, size_t num_executor_threads, 
               uint16_t metrics_port) {
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
    
    safe_strncpy(worker->bind_addr, bind_addr ? bind_addr : "0.0.0.0", MAX_IP_LEN);
    
    if (dag_catalog_init(&worker->dag_catalog, MAX_DAGS) != RESULT_OK) {
        LOG_ERROR("Failed to initialize DAG catalog");
        return RESULT_ERR_INVALID;
    }
    
    if (message_queue_init(&worker->message_queue, MAX_WORKER_QUEUE_SIZE) != RESULT_OK) {
        LOG_ERROR("Failed to initialize message queue");
        dag_catalog_destroy(&worker->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    if (cluster_view_init(&worker->cluster_view, MAX_CLUSTER_NODES) != RESULT_OK) {
        LOG_ERROR("Failed to initialize cluster view");
        message_queue_destroy(&worker->message_queue);
        dag_catalog_destroy(&worker->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    if (pthread_mutex_init(&worker->routers_lock, NULL) != 0) {
        LOG_ERROR("Failed to initialize routers lock");
        cluster_view_destroy(&worker->cluster_view);
        message_queue_destroy(&worker->message_queue);
        dag_catalog_destroy(&worker->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    if (membership_init(&worker->membership, worker_id, NODE_TYPE_WORKER,
                       worker->bind_addr, gossip_port) != RESULT_OK) {
        LOG_ERROR("Failed to initialize membership");
        pthread_mutex_destroy(&worker->routers_lock);
        cluster_view_destroy(&worker->cluster_view);
        message_queue_destroy(&worker->message_queue);
        dag_catalog_destroy(&worker->dag_catalog);
        return RESULT_ERR_INVALID;
    }

    membership_set_callback(worker->membership, on_member_event, worker);
    
    // Get gossip engine handle for direct access
    worker->gossip_engine = ((struct membership_handle*)worker->membership)->gossip_engine;

    if (metrics_port > 0) {
        worker->metrics = worker_metrics_init(worker_id, metrics_port);
        if (!worker->metrics) {
            LOG_WARN("Failed to initialize metrics (continuing without metrics)");
        }
    } else {
        worker->metrics = NULL;
        LOG_INFO("Metrics disabled (no metrics_port specified)");
    }

    LOG_INFO("Worker %u initialized (GOSSIP:%u, DATA:%u, %zu executor threads)",
             worker_id, gossip_port, data_port, num_executor_threads);
    return RESULT_OK;
}

int worker_start(worker_state_t *worker) {
    if (!worker) return RESULT_ERR_INVALID;
    
    for (size_t i = 0; i < worker->num_executor_threads; i++) {
        if (pthread_create(&worker->executor_threads[i], NULL, 
                          worker_executor_thread_fn, worker) != 0) {
            LOG_ERROR("Failed to start executor thread %zu", i);
            
            worker->shutdown_flag = 1;
            for (size_t j = 0; j < i; j++) {
                pthread_join(worker->executor_threads[j], NULL);
            }
            return RESULT_ERR_INVALID;
        }
    }
    
    LOG_INFO("Worker %u started", worker->worker_id);
    return RESULT_OK;
}

void worker_shutdown(worker_state_t *worker) {
    if (!worker) return;
    
    LOG_INFO("Shutting down worker %u", worker->worker_id);
    
    worker->shutdown_flag = 1;
    
    for (size_t i = 0; i < worker->num_executor_threads; i++) {
        pthread_join(worker->executor_threads[i], NULL);
    }
    
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
// WORKER BOOTSTRAP FROM CONFIG
// ============================================================================

int worker_bootstrap_from_config(worker_state_t *worker, const roole_config_t *config) {
    if (!worker || !config || config->router_count == 0) {
        LOG_ERROR("Invalid bootstrap configuration");
        return RESULT_ERR_INVALID;
    }
    
    size_t router_idx = rand() % config->router_count;
    const char *router_addr = config->routers[router_idx];
    
    char router_ip[16];
    uint16_t router_gossip_port;
    config_parse_address(router_addr, router_ip, &router_gossip_port);
    
    LOG_INFO("Bootstrapping from router %s:%u", router_ip, router_gossip_port);
    
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
    join_msg.updates[0].service_port = worker->data_port;
    
    int bootstrap_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (bootstrap_sock < 0) {
        LOG_ERROR("Cannot create bootstrap socket");
        return RESULT_ERR_NETWORK;
    }
    
    uint8_t buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    ssize_t msg_size = gossip_message_serialize(&join_msg, buffer, sizeof(buffer));
    
    struct sockaddr_in router_addr_in;
    memset(&router_addr_in, 0, sizeof(router_addr_in));
    router_addr_in.sin_family = AF_INET;
    router_addr_in.sin_port = htons(router_gossip_port);
    inet_pton(AF_INET, router_ip, &router_addr_in.sin_addr);
    
    if (sendto(bootstrap_sock, buffer, msg_size, 0,
              (struct sockaddr*)&router_addr_in, sizeof(router_addr_in)) < 0) {
        LOG_ERROR("Failed to send WORKER_JOIN");
        close(bootstrap_sock);
        return RESULT_ERR_NETWORK;
    }
    
    LOG_INFO("WORKER_JOIN sent, waiting for response...");
    
    struct timeval tv = {.tv_sec = 5, .tv_usec = 0};
    setsockopt(bootstrap_sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    
    uint8_t recv_buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    ssize_t received = recvfrom(bootstrap_sock, recv_buffer, sizeof(recv_buffer), 0, NULL, NULL);
    close(bootstrap_sock);
    
    if (received < 0) {
        LOG_ERROR("Timeout waiting for JOIN_RESPONSE");
        return RESULT_ERR_TIMEOUT;
    }
    
    gossip_message_t response;
    if (gossip_message_deserialize(recv_buffer, received, &response) != 0) {
        LOG_ERROR("Invalid JOIN_RESPONSE");
        return RESULT_ERR_INVALID;
    }
    
    if (response.msg_type != GOSSIP_MSG_JOIN_RESPONSE) {
        LOG_ERROR("Unexpected message type: %u", response.msg_type);
        return RESULT_ERR_INVALID;
    }
    
    gossip_bootstrap_response_t bootstrap_data;
    size_t header_size = 16 + response.num_updates * 44;
    
    if (gossip_deserialize_bootstrap_response(recv_buffer + header_size,
                                              received - header_size,
                                              &bootstrap_data) != 0) {
        LOG_ERROR("Failed to deserialize bootstrap data");
        return RESULT_ERR_INVALID;
    }
    
    LOG_INFO("Received bootstrap data with %u routers", bootstrap_data.num_routers);
    
    for (uint8_t i = 0; i < bootstrap_data.num_routers; i++) {
        char router_data_ip[16];
        uint16_t router_data_port;
        
        config_parse_address(bootstrap_data.routers[i].data_addr, 
                           router_data_ip, &router_data_port);
        
        worker_add_router(worker, bootstrap_data.routers[i].node_id,
                         router_data_ip, router_data_port);
        
        LOG_INFO("Connected to router %u (%s)", 
                 bootstrap_data.routers[i].node_id, 
                 bootstrap_data.routers[i].data_addr);
    }
    
    return RESULT_OK;
}

// ============================================================================
// ROUTER MANAGEMENT
// ============================================================================

int worker_add_router(worker_state_t *worker, node_id_t router_id,
                     const char *ip, uint16_t data_port) {
    if (!worker || !ip) return RESULT_ERR_INVALID;

    pthread_mutex_lock(&worker->routers_lock);

    for (size_t i = 0; i < worker->router_count; i++) {
        if (worker->routers[i].active && worker->routers[i].router_id == router_id) {
            pthread_mutex_unlock(&worker->routers_lock);
            LOG_DEBUG("Router %u already connected", router_id);
            return RESULT_OK;
        }
    }

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
    conn->data_port = data_port;
    conn->last_sync_ms = time_now_ms();
    conn->active = 1;

    conn->data_channel = safe_malloc(sizeof(rpc_channel_t));

    if (!conn->data_channel) {
        LOG_ERROR("Failed to allocate RPC channel");
        pthread_mutex_unlock(&worker->routers_lock);
        return RESULT_ERR_NOMEM;
    }

    if (rpc_client_connect(conn->data_channel, ip, data_port,
                          RPC_CHANNEL_DATA, 4096) != 0) {
        LOG_ERROR("Failed to connect DATA channel to router %u", router_id);
        safe_free(conn->data_channel);
        conn->data_channel = NULL;
        conn->active = 0;
        pthread_mutex_unlock(&worker->routers_lock);
        return RESULT_ERR_NETWORK;
    }

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