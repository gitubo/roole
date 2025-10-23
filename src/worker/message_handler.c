// COMMENT: NEW FILE - Message processing logic for worker

#define _POSIX_C_SOURCE 200809L

#include "roole/worker.h"
#include "roole/rpc.h"
#include "roole/common.h"
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

extern worker_state_t *g_worker_state;  // Defined in rpc_handler.c

// ============================================================================
// HANDLER: Process Message (Router -> Worker)
// FUNC_ID_PROCESS_MESSAGE (0x26)
// ============================================================================

/**
 * Request payload:
 *   [exec_id: 8 bytes][dag_id: 4 bytes][sender_id: 2 bytes][message: variable]
 * 
 * Response: ACK (actual result sent later via FUNC_ID_EXECUTION_UPDATE)
 */
int handle_process_message(rpc_async_context_t *context,
                           const uint8_t *in_data, size_t in_len) {
    if (!g_worker_state || in_len < 14) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    execution_id_t exec_id;
    rule_id_t dag_id;
    node_id_t sender_id;
    
    memcpy(&exec_id, in_data, 8);
    memcpy(&dag_id, in_data + 8, 4);
    memcpy(&sender_id, in_data + 12, 2);
    
    const uint8_t *message = in_data + 14;
    size_t message_len = in_len - 14;
    
    LOG_INFO("[RPC] Received message for processing (exec_id: %lu, DAG: %u, %zu bytes)",
                   exec_id, dag_id, message_len);
    
    // Enqueue message for processing
    int result = worker_enqueue_message(g_worker_state, exec_id, dag_id, sender_id,
                                       message, message_len);
    
    if (result != RESULT_OK) {
        LOG_ERROR("[RPC] Failed to enqueue message");
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    // Send ACK
    uint8_t ack = 1;
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, &ack, 1);
}