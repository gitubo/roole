// COMMENT: NEW FILE - Message handling logic for router
// This replaces the task submission approach with message-based approach

#define _POSIX_C_SOURCE 200809L

#include "roole/node.h"
#include "roole/rpc.h"
#include "roole/common.h"
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>

extern router_state_t *g_router_state;  // Defined in rpc_handler.c

// ============================================================================
// HANDLER: Submit Message (Client -> Router)
// FUNC_ID_SUBMIT_MESSAGE (0x25)
// ============================================================================

/**
 * Request payload:
 *   [dag_id: 4 bytes][message: variable]
 * 
 * Response payload:
 *   [exec_id: 8 bytes][status: 1 byte]
 */
int handle_submit_message(rpc_async_context_t *context, 
                          const uint8_t *in_data, size_t in_len) {
    if (!g_router_state || in_len < sizeof(rule_id_t)) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    // Deserialize request
    rule_id_t dag_id;
    memcpy(&dag_id, in_data, sizeof(rule_id_t));
    
    const uint8_t *message = in_data + sizeof(rule_id_t);
    size_t message_len = in_len - sizeof(rule_id_t);
    
    LOG_INFO("[RPC] Received message submission (DAG %u, %zu bytes)", dag_id, message_len);
    
    // 1. Verify DAG exists
    dag_t *dag = dag_catalog_get(&g_router_state->dag_catalog, dag_id);
    if (!dag) {
        LOG_ERROR("DAG %u not found", dag_id);
        return rpc_send_async_response(context, RPC_STATUS_FUNC_NOT_FOUND, NULL, 0);
    }
    dag_catalog_release(&g_router_state->dag_catalog);
    
    // 2. Select worker using load balancing
    node_id_t worker_id = router_select_worker(g_router_state, LOAD_BALANCE_LEAST_LOADED);
    if (worker_id == 0) {
        LOG_ERROR("No available workers");
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    // 3. Create execution record for tracking
    execution_id_t exec_id = execution_tracker_add(&g_router_state->exec_tracker, 
                                                   dag_id, worker_id, 
                                                   message, message_len, 3);
    if (exec_id == 0) {
        LOG_ERROR("Failed to create execution record");
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    // 4. Get worker info and send message to worker
    peer_info_t *worker = peer_pool_get(&g_router_state->worker_pool, worker_id);
    if (!worker || !worker->data_channel) {
        LOG_ERROR("Worker %u not found or not connected", worker_id);
        execution_tracker_update_status(&g_router_state->exec_tracker, exec_id, EXEC_STATUS_FAILED);
        peer_pool_release(&g_router_state->worker_pool);
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    // Build payload for worker: [exec_id: 8][dag_id: 4][sender_id: 2][message: variable]
    size_t worker_payload_len = 8 + 4 + 2 + message_len;
    uint8_t *worker_payload = malloc(worker_payload_len);
    if (!worker_payload) {
        peer_pool_release(&g_router_state->worker_pool);
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    memcpy(worker_payload, &exec_id, 8);
    memcpy(worker_payload + 8, &dag_id, 4);
    memcpy(worker_payload + 12, &context->sender_id, 2);
    memcpy(worker_payload + 14, message, message_len);
    
    // Send FUNC_ID_PROCESS_MESSAGE to worker via DATA channel
    size_t rpc_msg_len = rpc_pack_message(
        worker->data_channel->tx_buffer,
        g_router_state->router_id,
        exec_id,  // Use exec_id as request_id for tracking
        RPC_TYPE_REQUEST,
        RPC_STATUS_UNKNOWN,
        FUNC_ID_PROCESS_MESSAGE,
        worker_payload,
        worker_payload_len
    );
    
    ssize_t sent = send(worker->data_channel->socket_fd, 
                       worker->data_channel->tx_buffer, rpc_msg_len, 0);
    
    free(worker_payload);
    peer_pool_release(&g_router_state->worker_pool);
    
    if (sent <= 0) {
        LOG_ERROR("Failed to send message to worker %u", worker_id);
        execution_tracker_update_status(&g_router_state->exec_tracker, exec_id, EXEC_STATUS_FAILED);
        return rpc_send_async_response(context, RPC_STATUS_NETWORK, NULL, 0);
    }
    
    // Mark as running
    execution_tracker_update_status(&g_router_state->exec_tracker, exec_id, EXEC_STATUS_RUNNING);
    
    // Build response: [exec_id][status]
    uint8_t response[9];
    memcpy(response, &exec_id, sizeof(execution_id_t));
    response[8] = (uint8_t)EXEC_STATUS_RUNNING;
    
    LOG_INFO("[RPC] Message routed to worker %u (exec_id: %lu)", worker_id, exec_id);
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, response, 9);
}