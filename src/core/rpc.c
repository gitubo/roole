// src/core/rpc.c

#define _POSIX_C_SOURCE 200809L

#include "roole/rpc.h"
#include "roole/common.h"
#include "roole/logger.h"
#include "roole/service_registry.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <math.h> 
#include <inttypes.h>

// ============================================================================
// CONFIGURATION
// ============================================================================

#define MAX_EVENTS 64
#define BACKLOG 128
#define MAX_BUFFER_SIZE 4096 

// ============================================================================
// GLOBAL STATE (Single-threaded access in worker event loop)
// ============================================================================

//static rpc_service_entry_t *g_service_table = NULL;
static rpc_async_context_t *g_pending_contexts = NULL; 
static uint64_t g_rpc_request_counter = 0;

static rpc_service_entry_t* get_service_table(void) {
    service_registry_t *registry = service_registry_global();
    if (!registry) return NULL;
    
    return (rpc_service_entry_t*)service_registry_get(registry,
                                                      SERVICE_TYPE_RPC_SERVER,
                                                      "service_table");
}

static void set_service_table(rpc_service_entry_t *table) {
    service_registry_t *registry = service_registry_global();
    if (!registry) return;
    
    service_registry_register(registry, SERVICE_TYPE_RPC_SERVER,
                             "service_table", table);
}

// ============================================================================
// TIMING UTILITIES
// ============================================================================
/*
static double time_diff_us(const struct timespec *start, const struct timespec *end) {
    long seconds = end->tv_sec - start->tv_sec;
    long nanoseconds = end->tv_nsec - start->tv_nsec;
    return (double)seconds * 1e6 + (double)nanoseconds / 1e3;
}
*/

// ============================================================================
// CHANNEL TYPE MAPPING
// ============================================================================
/*
rpc_channel_type_t rpc_get_channel_for_func(uint8_t func_id) {
    // SERVICE channel: heartbeat, registration, catalog sync, cluster management
    if (func_id == FUNC_ID_WORKER_HEARTBEAT ||
        func_id == FUNC_ID_WORKER_REGISTRATION ||
        func_id == FUNC_ID_SYNC_CATALOG ||
        func_id == FUNC_ID_JOIN_CLUSTER ||
        func_id == FUNC_ID_NODE_LIST ||
        func_id == FUNC_ID_HEARTBEAT ||
        func_id == FUNC_ID_GOSSIP ||
        func_id == FUNC_ID_LEAVE_CLUSTER) {
        return RPC_CHANNEL_SERVICE;
    }

    // DATA channel: message processing, execution updates
    if (func_id == FUNC_ID_EXECUTE_DAG ||
        func_id == FUNC_ID_EXECUTION_UPDATE) {
        return RPC_CHANNEL_DATA;
    }

    // INGRESS channel: DAG management from external clients
    if (func_id == FUNC_ID_ADD_DAG ||
        func_id == FUNC_ID_UPDATE_DAG ||
        func_id == FUNC_ID_REMOVE_DAG ||
        func_id == FUNC_ID_GET_DAG ||
        func_id == FUNC_ID_LIST_DAGS) {
        return RPC_CHANNEL_INGRESS;
    }

    // Default to SERVICE for unknown functions
    return RPC_CHANNEL_SERVICE;
}
*/
// ============================================================================
// CHANNEL MANAGEMENT
// ============================================================================

int rpc_channel_init(rpc_channel_t *channel, int fd, rpc_channel_type_t type, size_t buffer_size) {
    if (buffer_size == 0) return -1;

    channel->socket_fd = fd;
    channel->channel_type = type;
    channel->rx_buffer_size = buffer_size;
    channel->tx_buffer_size = buffer_size;
    channel->rx_data_len = 0;

    channel->rx_buffer = (uint8_t *)malloc(buffer_size);
    channel->tx_buffer = (uint8_t *)malloc(buffer_size);

    if (!channel->rx_buffer || !channel->tx_buffer) {
        if (channel->rx_buffer) free(channel->rx_buffer);
        if (channel->tx_buffer) free(channel->tx_buffer);
        return -1;
    }

    return 0;
}

void rpc_channel_destroy(rpc_channel_t *channel) {
    if (channel) {
        if (channel->socket_fd != -1) {
            close(channel->socket_fd);
            channel->socket_fd = -1;
        }
        if (channel->rx_buffer) free(channel->rx_buffer);
        if (channel->tx_buffer) free(channel->tx_buffer);
    }
}

// ============================================================================
// MESSAGE SERIALIZATION
// ============================================================================

size_t rpc_pack_message(uint8_t *buffer, node_id_t node_id, uint32_t request_id, 
                         uint8_t type, uint8_t status, uint8_t func_id, 
                         const uint8_t *payload, size_t payload_len) {
    
    uint32_t total_len = (uint32_t)(RPC_HEADER_SIZE + payload_len);
    uint32_t net_total_len = htonl(total_len);
    uint32_t net_request_id = htonl(request_id);

    memcpy(buffer, &net_total_len, 4);
    memcpy(buffer + 4, &net_request_id, 4);
    memcpy(buffer + 8, &node_id, sizeof(node_id_t));

    rpc_type_status_t type_and_status;
    type_and_status.fields.type = type;
    type_and_status.fields.status = status;
    buffer[10] = type_and_status.byte;

    buffer[11] = func_id;

    if (payload_len > 0 && payload != NULL) {
        memcpy(buffer + RPC_HEADER_SIZE, payload, payload_len);
    }

    return total_len;
}

int rpc_unpack_header(const uint8_t *buffer, rpc_header_t *header) {
    uint32_t net_total_len;
    uint32_t net_request_id;

    if (!buffer || !header) return -1;

    memcpy(&net_total_len, buffer, 4);
    memcpy(&net_request_id, buffer + 4, 4);

    header->total_len = ntohl(net_total_len);
    header->request_id = ntohl(net_request_id);
    
    // Extract sender_id (uint16_t at offset 8-9)
    memcpy(&header->sender_id, buffer + 8, sizeof(node_id_t));
    
    // Extract type_and_status at offset 10
    header->type_and_status.byte = buffer[10];
    
    // Extract func_id at offset 11
    header->func_id = buffer[11];

    if (header->total_len < RPC_HEADER_SIZE) return -1;

/*    LOG_INFO("header:\n\tlength: %d\n\trequest_id: %d\n\tsender_id: %d\n\ttype: %d\n\tstatus: %d\n\tfunc_id: %d", 
                    header->total_len,
                    header->request_id,
                    header->sender_id,
                    header->type_and_status,
                    header->type_and_status,
                    header->func_id
                    );
  */  
    return 0;
}

// ============================================================================
// CLIENT CONNECTION (replaces old rpc_router_init)
// ============================================================================

int rpc_client_connect(rpc_channel_t *channel, const char *ip, uint16_t port,
                       rpc_channel_type_t channel_type, size_t buffer_size) {
    int client_fd;
    struct sockaddr_in server_addr;

    client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd == -1) {
        LOG_ERROR("rpc_client_connect: socket failed");
        return -1;
    }

    if (rpc_channel_init(channel, client_fd, channel_type, buffer_size) != 0) {
        LOG_ERROR("rpc_client_connect: Failed to initialize RPC channel buffers");
        close(client_fd);
        return -1;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, ip, &server_addr.sin_addr) <= 0) {
        LOG_ERROR("rpc_client_connect: inet_pton failed");
        rpc_channel_destroy(channel);
        return -1;
    }

    if (connect(client_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        LOG_ERROR("rpc_client_connect: connect to %s:%u failed", ip, port);
        rpc_channel_destroy(channel);
        return -1;
    }

    const char *channel_name = (channel_type == RPC_CHANNEL_DATA) ? "DATA" : "INGRESS";
    LOG_INFO("RPC client connected to %s:%u (channel: %s)", ip, port, channel_name);
    return 0;
}

// ============================================================================
// MULTI-CHANNEL LISTENER
// ============================================================================

static int make_socket_non_blocking(int fd);  // Forward declaration

int rpc_multi_listener_init(rpc_multi_channel_listener_t *listener) {
    if (!listener) return -1;

    listener->count = 0;
    listener->epoll_fd = epoll_create1(0);
    if (listener->epoll_fd == -1) {
        LOG_ERROR("rpc_multi_listener_init: epoll_create1 failed");
        return -1;
    }

    for (size_t i = 0; i < MAX_CHANNEL_TYPES; i++) {
        listener->listeners[i].listener_fd = -1;
        listener->listeners[i].port = 0;
    }

    return 0;
}

int rpc_multi_listener_add(rpc_multi_channel_listener_t *listener,
                           rpc_channel_type_t type, uint16_t port) {
    if (!listener || listener->count >= MAX_CHANNEL_TYPES) return -1;

    int listener_fd;
    struct sockaddr_in server_addr;
    struct epoll_event event;

    // Create socket
    listener_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listener_fd == -1) {
        LOG_ERROR("rpc_multi_listener_add: socket failed for port %u", port);
        return -1;
    }

    // Set SO_REUSEADDR
    int opt = 1;
    if (setsockopt(listener_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        LOG_ERROR("rpc_multi_listener_add: setsockopt failed");
        close(listener_fd);
        return -1;
    }

    // Set non-blocking
    if (make_socket_non_blocking(listener_fd) == -1) {
        LOG_ERROR("rpc_multi_listener_add: make_socket_non_blocking failed");
        close(listener_fd);
        return -1;
    }

    // Bind
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(port);

    if (bind(listener_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        LOG_ERROR("rpc_multi_listener_add: bind failed for port %u", port);
        close(listener_fd);
        return -1;
    }

    // Listen
    if (listen(listener_fd, BACKLOG) == -1) {
        LOG_ERROR("rpc_multi_listener_add: listen failed");
        close(listener_fd);
        return -1;
    }

    // Add to epoll
    event.events = EPOLLIN | EPOLLET;
    event.data.fd = listener_fd;
    if (epoll_ctl(listener->epoll_fd, EPOLL_CTL_ADD, listener_fd, &event) == -1) {
        LOG_ERROR("rpc_multi_listener_add: epoll_ctl failed");
        close(listener_fd);
        return -1;
    }

    // Store listener info
    listener->listeners[listener->count].listener_fd = listener_fd;
    listener->listeners[listener->count].channel_type = type;
    listener->listeners[listener->count].port = port;
    listener->count++;

    const char *channel_name = (type == RPC_CHANNEL_DATA) ? "DATA" : "INGRESS";
    LOG_INFO("[RPC] Listening on port %u for %s channel", port, channel_name);

    return 0;
}

void rpc_multi_listener_destroy(rpc_multi_channel_listener_t *listener) {
    if (!listener) return;

    for (size_t i = 0; i < listener->count; i++) {
        if (listener->listeners[i].listener_fd != -1) {
            close(listener->listeners[i].listener_fd);
            listener->listeners[i].listener_fd = -1;
        }
    }

    if (listener->epoll_fd != -1) {
        close(listener->epoll_fd);
        listener->epoll_fd = -1;
    }
}

// ============================================================================
// WORKER INTERNALS (Async Reactor Pattern)
// ============================================================================

static int make_socket_non_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    flags |= O_NONBLOCK;
    return (fcntl(fd, F_SETFL, flags) == -1) ? -1 : 0;
}

static int send_response(rpc_channel_t *channel, const uint8_t *buffer, size_t length) {
    size_t total_sent = 0;
    ssize_t bytes_sent;

    while (total_sent < length) {
        bytes_sent = send(channel->socket_fd, buffer + total_sent, length - total_sent, 0);
        if (bytes_sent <= 0) {
            if (bytes_sent < 0 && errno == EINTR) continue; 
            return -1;
        }
        total_sent += bytes_sent;
    }
    return 0;
}

void rpc_remove_context(rpc_async_context_t *context) {
    rpc_async_context_t **indirect = &g_pending_contexts;
    while (*indirect) {
        if (*indirect == context) {
            *indirect = context->next;
            break;
        }
        indirect = &(*indirect)->next;
    }
}

int rpc_send_async_response(rpc_async_context_t *context, uint8_t status, 
                            const uint8_t *response_payload, size_t response_len) {
    
    if (context == NULL || context->channel == NULL) return -1;

    struct timespec end_time;
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    double execution_time_us = time_diff_us(&context->start_time, &end_time);
    
    LOG_DEBUG("[RPC] ReqID %u, FuncID %u, Execution: %.2f us", 
                    context->request_id, context->func_id, execution_time_us);

    // Pack response
    size_t response_msg_len = rpc_pack_message(
        context->channel->tx_buffer, 
        context->sender_id,
        context->request_id, 
        RPC_TYPE_RESPONSE, 
        status, 
        context->func_id, 
        response_payload, 
        response_len
    );

    // Send response
    int send_result = send_response(context->channel, context->channel->tx_buffer, response_msg_len);
    if (send_result != 0) {
        LOG_ERROR("[RPC] Failed to send response on FD %d (Request %u)", 
                        context->channel->socket_fd, context->request_id);
    }
    
    // Cleanup: remove and free context
    rpc_remove_context(context);
    free(context); 
    
    return (send_result != 0) ? -1 : 0;
}

// ============================================================================
// REQUEST PROCESSING
// ============================================================================

static void process_buffered_data(rpc_channel_t *channel) {
    rpc_header_t header;
    size_t processed_bytes = 0;
    
    rpc_service_entry_t *g_service_table = get_service_table();
    if (g_service_table == NULL) {
        LOG_ERROR("[RPC] Service table not initialized. Dropping requests");
        channel->rx_data_len = 0;
        return;
    }

    while (channel->rx_data_len - processed_bytes >= RPC_HEADER_SIZE) {
        
        if (rpc_unpack_header(channel->rx_buffer + processed_bytes, &header) != 0) {
            LOG_ERROR("[RPC] Invalid header on FD %d. Dropping buffer", channel->socket_fd);
            channel->rx_data_len = 0;
            return;
        }

        if (channel->rx_data_len - processed_bytes < header.total_len) {
            break; // Partial message, wait for more data
        }
        
        // Full RPC message received
        g_rpc_request_counter++;
        
        uint8_t func_id = header.func_id;
        uint32_t req_id = header.request_id;
        size_t payload_len = header.total_len - RPC_HEADER_SIZE;
        uint8_t *in_payload = channel->rx_buffer + processed_bytes + RPC_HEADER_SIZE;
        
        // Create async context
        rpc_async_context_t *context = (rpc_async_context_t *)malloc(sizeof(rpc_async_context_t));
        if (!context) {
            LOG_ERROR("[RPC] Out of memory for async context");
            return; 
        }
        
        context->channel = channel;
        context->request_id = req_id;
        context->func_id = func_id;
        clock_gettime(CLOCK_MONOTONIC, &context->start_time);
        
        // Find handler
        rpc_async_handler_fn handler = NULL;
        
        for (int i = 0; g_service_table[i].handler != NULL; i++) {
            if (g_service_table[i].func_id == func_id) {
                handler = g_service_table[i].handler;
                break;
            }
        }
        
        if (handler != NULL) {
            // Add to pending contexts
            context->next = g_pending_contexts;
            g_pending_contexts = context;
            
            // Execute async handler
            if (handler(context, in_payload, payload_len) == 0) {
                LOG_DEBUG("[RPC] Recv (Total: %" PRIu64 ", ID: %u, Func: %u). Async started", 
                                g_rpc_request_counter, req_id, func_id);
            } else {
                // Handler failed to start
                rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0); 
            }
        } else {
            // Function not found
            LOG_WARN("[RPC] Function %u not found", func_id);
            rpc_send_async_response(context, RPC_STATUS_FUNC_NOT_FOUND, NULL, 0); 
        }
        
        processed_bytes += header.total_len;
    }

    // Compact buffer
    if (processed_bytes > 0) {
        channel->rx_data_len -= processed_bytes;
        if (channel->rx_data_len > 0) {
            memmove(channel->rx_buffer, channel->rx_buffer + processed_bytes, channel->rx_data_len);
        }
    }
}

// ============================================================================
// MULTI-CHANNEL EVENT LOOP (Generic - used by both worker and router)
// ============================================================================

static int rpc_multi_channel_event_loop(rpc_multi_channel_listener_t *listener,
                                        rpc_service_entry_t *service_table) {
    logger_push_component("rpc:server");
    struct epoll_event event, events[MAX_EVENTS];

    //g_service_table = service_table;
    set_service_table(service_table);

    // Main event loop
    while (1) {
        int n = epoll_wait(listener->epoll_fd, events, MAX_EVENTS, -1);
        if (n == -1) {
            if (errno == EINTR) continue;
            LOG_ERROR("epoll_wait failed");
            break;
        }

        for (int i = 0; i < n; i++) {
            int is_listener = 0;
            rpc_channel_type_t listener_type = RPC_CHANNEL_DATA;

            // Check if this is a listener socket
            for (size_t j = 0; j < listener->count; j++) {
                if (events[i].data.fd == listener->listeners[j].listener_fd) {
                    is_listener = 1;
                    listener_type = listener->listeners[j].channel_type;
                    break;
                }
            }

            if (is_listener) {
                // New connection on one of the listener sockets
                int listener_fd = events[i].data.fd;
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                int client_fd;

                while ((client_fd = accept(listener_fd, (struct sockaddr *)&client_addr, &client_len)) != -1) {

                    if (make_socket_non_blocking(client_fd) == -1) {
                        close(client_fd);
                        continue;
                    }

                    rpc_channel_t *channel = (rpc_channel_t *)malloc(sizeof(rpc_channel_t));
                    if (!channel || rpc_channel_init(channel, client_fd, listener_type, MAX_BUFFER_SIZE) != 0) {
                        if (channel) free(channel);
                        close(client_fd);
                        continue;
                    }

                    event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
                    event.data.ptr = channel;
                    if (epoll_ctl(listener->epoll_fd, EPOLL_CTL_ADD, client_fd, &event) == -1) {
                        rpc_channel_destroy(channel);
                        free(channel);
                        continue;
                    }

                    const char *channel_name = (listener_type == RPC_CHANNEL_DATA) ? "DATA" : "INGRESS";
                    LOG_DEBUG("[RPC] New connection on %s channel (FD %d)", channel_name, client_fd);
                }

                if (client_fd == -1 && errno != EWOULDBLOCK && errno != EAGAIN) {
                    LOG_ERROR("accept error");
                }
            } else {
                // Data on existing connection
                rpc_channel_t *channel = (rpc_channel_t *)events[i].data.ptr;
                int client_fd = channel->socket_fd;
                int close_connection = 0;

                if (events[i].events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
                    LOG_DEBUG("[RPC] Connection closed on FD %d", client_fd);
                    close_connection = 1;
                }

                if (events[i].events & EPOLLIN) {
                    ssize_t bytes_read;

                    // Read until EAGAIN/EWOULDBLOCK
                    while (channel->rx_data_len < channel->rx_buffer_size) {
                        bytes_read = read(client_fd, channel->rx_buffer + channel->rx_data_len,
                                        channel->rx_buffer_size - channel->rx_data_len);

                        if (bytes_read > 0) {
                            channel->rx_data_len += bytes_read;
                        }
                        else if (bytes_read == 0) {
                            LOG_DEBUG("[RPC] Client FD %d closed gracefully", client_fd);
                            close_connection = 1;
                            break;
                        }
                        else if (bytes_read == -1 && (errno == EWOULDBLOCK || errno == EAGAIN)) {
                            break;
                        }
                        else {
                            close_connection = 1;
                            break;
                        }
                    }

                    if (!close_connection && channel->rx_data_len > 0) {
                        process_buffered_data(channel);
                    }

                    if (!close_connection && channel->rx_data_len == channel->rx_buffer_size) {
                        LOG_ERROR("[RPC] Receive buffer full on FD %d", client_fd);
                        close_connection = 1;
                    }
                }

                if (close_connection) {
                    epoll_ctl(listener->epoll_fd, EPOLL_CTL_DEL, client_fd, NULL);
                    // TODO: Close all pending contexts for this channel
                    rpc_channel_destroy(channel);
                    free(channel);
                }
            }
        }
    }

    return -1;
}

// ============================================================================
// WORKER EVENT LOOP (Two channels: SERVICE + DATA)
// ============================================================================

int rpc_worker_run(uint16_t data_port, rpc_service_entry_t *service_table) {
    rpc_multi_channel_listener_t listener;

    LOG_INFO("[RPC] Worker starting with DATA port %u", data_port);

    if (rpc_multi_listener_init(&listener) != 0) {
        LOG_ERROR("Failed to initialize multi-channel listener");
        return -1;
    }

    if (rpc_multi_listener_add(&listener, RPC_CHANNEL_DATA, data_port) != 0) {
        LOG_ERROR("Failed to add DATA listener");
        rpc_multi_listener_destroy(&listener);
        return -1;
    }

    int result = rpc_multi_channel_event_loop(&listener, service_table);

    rpc_multi_listener_destroy(&listener);
    return result;
}

// ============================================================================
// ROUTER EVENT LOOP (Three channels: SERVICE + DATA + INGRESS)
// ============================================================================

int rpc_router_run(uint16_t data_port, uint16_t ingress_port,
                   rpc_service_entry_t *service_table) {
    rpc_multi_channel_listener_t listener;

    LOG_INFO("[RPC] Router starting with DATA port %u, INGRESS port %u",
                   data_port, ingress_port);

    if (rpc_multi_listener_init(&listener) != 0) {
        LOG_ERROR("Failed to initialize multi-channel listener");
        return -1;
    }

    if (rpc_multi_listener_add(&listener, RPC_CHANNEL_DATA, data_port) != 0) {
        LOG_ERROR("Failed to add DATA listener");
        rpc_multi_listener_destroy(&listener);
        return -1;
    }

    if (rpc_multi_listener_add(&listener, RPC_CHANNEL_INGRESS, ingress_port) != 0) {
        LOG_ERROR("Failed to add INGRESS listener");
        rpc_multi_listener_destroy(&listener);
        return -1;
    }

    int result = rpc_multi_channel_event_loop(&listener, service_table);

    rpc_multi_listener_destroy(&listener);
    return result;
}