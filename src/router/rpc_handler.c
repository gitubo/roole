// src/router/rpc_handlers.c

#define _POSIX_C_SOURCE 200809L

#include "roole/router.h"
#include "roole/rpc.h"
#include "roole/common.h"
#include <string.h>
#include <stdlib.h>



// Global router state (set by router_start_rpc_server)
router_state_t *g_router_state = NULL;

int handle_submit_message(rpc_async_context_t *context, 
                          const uint8_t *in_data, size_t in_len);


void router_set_rpc_state(router_state_t *router) {
    g_router_state = router;
}

// ============================================================================
// HANDLER: Get Execution Status (Client -> Router)
// ============================================================================

/**
 * Request payload:
 *   [exec_id: 8 bytes]
 * 
 * Response payload:
 *   [status: 1 byte]
 */
static int handle_get_execution_status(rpc_async_context_t *context,
                                       const uint8_t *in_data, size_t in_len) {
    if (!g_router_state || in_len != sizeof(execution_id_t)) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    execution_id_t exec_id;
    memcpy(&exec_id, in_data, sizeof(execution_id_t));
    
    execution_status_t status;
    int result = router_get_execution_status(g_router_state, exec_id, &status);
    
    if (result != RESULT_OK) {
        return rpc_send_async_response(context, RPC_STATUS_FUNC_NOT_FOUND, NULL, 0);
    }
    
    uint8_t response = (uint8_t)status;
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, &response, 1);
}

// ============================================================================
// HANDLER: Execution Status Update (Worker -> Router)
// ============================================================================

/**
 * Request payload:
 *   [exec_id: 8 bytes][status: 1 byte]
 * 
 * Response: ACK
 */
static int handle_execution_update(rpc_async_context_t *context,
                                   const uint8_t *in_data, size_t in_len) {
    if (!g_router_state || in_len != 9) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    execution_id_t exec_id;
    uint8_t status_byte;
    
    memcpy(&exec_id, in_data, sizeof(execution_id_t));
    memcpy(&status_byte, in_data + 8, 1);
    
    execution_status_t status = (execution_status_t)status_byte;
    
    router_on_execution_update(g_router_state, exec_id, status);
    
    uint8_t ack = 1;
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, &ack, 1);
}

// ============================================================================
// HANDLER: List DAGs (Client -> Router)
// ============================================================================

/**
 * Response payload:
 *   [count: 4 bytes][dag_id_1: 4 bytes][dag_id_2: 4 bytes]...
 */
static int handle_list_dags(rpc_async_context_t *context,
                            const uint8_t *in_data, size_t in_len) {
    (void)in_data;
    (void)in_len;
    
    if (!g_router_state) {
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    rule_id_t dag_ids[MAX_DAGS];
    size_t count = dag_catalog_list(&g_router_state->dag_catalog, dag_ids, MAX_DAGS);
    
    // Build response: [count][dag_id_1][dag_id_2]...
    size_t response_len = sizeof(uint32_t) + count * sizeof(rule_id_t);
    uint8_t *response = malloc(response_len);
    if (!response) {
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    uint32_t count_u32 = (uint32_t)count;
    memcpy(response, &count_u32, sizeof(uint32_t));
    memcpy(response + sizeof(uint32_t), dag_ids, count * sizeof(rule_id_t));
    
    int result = rpc_send_async_response(context, RPC_STATUS_SUCCESS, response, response_len);
    free(response);
    
    return result;
}

// ============================================================================
// HANDLER: Worker Registration (Worker -> Router)
// ============================================================================

/**
 * Request payload:
 *   [worker_id: 4 bytes][service_port: 2 bytes][data_port: 2 bytes]
 *
 * Response: ACK
 */
static int handle_worker_register(rpc_async_context_t *context,
                                  const uint8_t *in_data, size_t in_len) {
    if (!g_router_state || in_len != 6) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }

    node_id_t worker_id;
    uint16_t service_port;
    uint16_t data_port;

    memcpy(&worker_id, in_data, sizeof(node_id_t));
    memcpy(&service_port, in_data + sizeof(node_id_t), sizeof(uint16_t));
    memcpy(&data_port, in_data + sizeof(node_id_t) + 2, sizeof(uint16_t));

    // Extract IP from connection (simplified - get peer address)
    char worker_ip[16] = "127.0.0.1"; // TODO: Extract from socket

    LOG_INFO("[RPC] Worker registration: ID=%u, SERVICE:%u, DATA:%u",
                   worker_id, service_port, data_port);

    router_on_worker_join(g_router_state, worker_id, worker_ip, data_port);

    uint8_t ack = 1;
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, &ack, 1);
}

// ============================================================================
// RPC SERVICE TABLE (Router)
// ============================================================================

rpc_service_entry_t router_rpc_service_table[] = {
    // Client operations
    { FUNC_ID_SUBMIT_MESSAGE, handle_submit_message, 8192 },  // NEW: Message-based API
    { FUNC_ID_GET_STATUS, handle_get_execution_status, 16 },
    { FUNC_ID_LIST_DAGS, handle_list_dags, 4096 },
    
    // Worker operations
    { FUNC_ID_WORKER_REGISTRATION, handle_worker_register, 16 },         // FUNC_ID_WORKER_REGISTER
    { FUNC_ID_EXECUTION_UPDATE, handle_execution_update, 16 },
    
    // Sentinel
    { 0, NULL, 0 }
};