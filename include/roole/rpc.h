// include/roole/rpc.h

#ifndef ROOLE_RPC_H
#define ROOLE_RPC_H

#define _POSIX_C_SOURCE 200809L

#include "roole/common.h"
#include <stdint.h>
#include <stddef.h>
#include <unistd.h>
#include <time.h>

// ============================================================================
// RPC MESSAGE HEADER DEFINITIONS
// ============================================================================

#define RPC_HEADER_SIZE 12
#define RPC_TYPE_STATUS 0x00
#define RPC_TYPE_REQUEST 0x01
#define RPC_TYPE_RESPONSE 0x02

// Status Codes
#define RPC_STATUS_SUCCESS 0x00
#define RPC_STATUS_BAD_ARGUMENT 0x01
#define RPC_STATUS_FUNC_NOT_FOUND 0x02
#define RPC_STATUS_INTERNAL_ERROR 0x03
#define RPC_STATUS_UNKNOWN 0xFF

// Function IDs (extended for cluster operations)
#define FUNC_ID_ADD 0x01  // Example: simple add function

// Cluster Management Functions
#define FUNC_ID_JOIN_CLUSTER 0x10
#define FUNC_ID_NODE_LIST 0x11
#define FUNC_ID_HEARTBEAT 0x12
#define FUNC_ID_GOSSIP 0x13
#define FUNC_ID_LEAVE_CLUSTER 0x14

// Router <-> Worker Functions
#define FUNC_ID_EXECUTE_DAG 0x20
#define FUNC_ID_WORKER_HEARTBEAT 0x21
#define FUNC_ID_EXECUTION_UPDATE 0x22
#define FUNC_ID_SYNC_CATALOG 0x23
#define FUNC_ID_WORKER_REGISTRATION 0x24

// DAG Management (Router operations)
#define FUNC_ID_ADD_DAG 0x30
#define FUNC_ID_UPDATE_DAG 0x31
#define FUNC_ID_REMOVE_DAG 0x32
#define FUNC_ID_GET_DAG 0x33
#define FUNC_ID_LIST_DAGS 0x34

typedef union rpc_type_status {
    uint8_t byte;
    struct {
        uint8_t status : 4; 
        uint8_t type : 4;   
    } fields;
} rpc_type_status_t;

typedef struct rpc_header {
    uint32_t total_len;
    uint32_t request_id;
    node_id_t sender_id;
    rpc_type_status_t type_and_status;
    uint8_t func_id;
} rpc_header_t;

// ============================================================================
// RPC CHANNEL (I/O Context)
// ============================================================================

typedef struct rpc_channel {
    int socket_fd;
    uint8_t *rx_buffer;
    size_t rx_buffer_size;
    size_t rx_data_len;
    uint8_t *tx_buffer;
    size_t tx_buffer_size;
} rpc_channel_t;

// ============================================================================
// ASYNCHRONOUS CONTEXT AND TIMING
// ============================================================================

typedef struct rpc_async_context {
    rpc_channel_t *channel;
    uint32_t request_id;
    uint8_t func_id;
    node_id_t sender_id;
    
    // Timing / Metrics
    struct timespec start_time;
    
    // Linked list for pending contexts
    struct rpc_async_context *next; 
} rpc_async_context_t;

/**
 * @brief Send async response back to client.
 * Must be called exactly once per async handler invocation.
 */
int rpc_send_async_response(rpc_async_context_t *context, uint8_t status, 
                            const uint8_t *response_payload, size_t response_len);

/**
 * @brief Remove context from pending list (called internally by rpc_send_async_response)
 */
void rpc_remove_context(rpc_async_context_t *context);

// ============================================================================
// RPC HANDLER (Async Service Definition)
// ============================================================================

/**
 * @brief Async RPC handler signature.
 * Handler must start async operation and return immediately.
 * Call rpc_send_async_response when work is complete.
 * 
 * @param context Context for async response
 * @param in_data Request payload
 * @param in_len Payload length
 * @return 0 if async operation started successfully, -1 otherwise
 */
typedef int (*rpc_async_handler_fn)(rpc_async_context_t *context, 
                                    const uint8_t *in_data, size_t in_len);

typedef struct rpc_service_entry {
    uint8_t func_id;
    rpc_async_handler_fn handler;
    size_t max_response_len;
} rpc_service_entry_t;

// ============================================================================
// CORE RPC FUNCTIONS (Serialization/Channel Management)
// ============================================================================

int rpc_channel_init(rpc_channel_t *channel, int fd, size_t buffer_size);
void rpc_channel_destroy(rpc_channel_t *channel);

size_t rpc_pack_message(uint8_t *buffer, node_id_t node_id, uint32_t request_id, 
                         uint8_t type, uint8_t status, uint8_t func_id, 
                         const uint8_t *payload, size_t payload_len);

int rpc_unpack_header(const uint8_t *buffer, rpc_header_t *header);

// ============================================================================
// ARCHITECTURAL ABSTRACTIONS (Worker/Router)
// ============================================================================

/**
 * @brief Initialize and connect RPC channel to remote worker/router (used by client)
 */
int rpc_router_init(rpc_channel_t *channel, const char *ip, uint16_t port, size_t buffer_size);

/**
 * @brief Start RPC worker event loop (epoll-based)
 * Handles all incoming connections and dispatches to service handlers.
 */
int rpc_worker_run(uint16_t port, rpc_service_entry_t *service_table);

#endif // ROOLE_RPC_H