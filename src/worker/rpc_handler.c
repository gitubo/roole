// src/worker/rpc_handlers.c

#define _POSIX_C_SOURCE 200809L

#include "roole/node.h"
#include "roole/rpc.h"
#include "roole/common.h"
#include <string.h>
#include <stdlib.h>

// Global worker state
worker_state_t *g_worker_state = NULL;

int handle_process_message(rpc_async_context_t *context,
                           const uint8_t *in_data, size_t in_len);

void worker_set_rpc_state(worker_state_t *worker) {
    g_worker_state = worker;
}


// ============================================================================
// HANDLER: Sync DAG Catalog (Router -> Worker)
// ============================================================================

/**
 * Request payload:
 *   [dag: serialized DAG structure]
 * 
 * Response: ACK
 */
static int handle_sync_catalog(rpc_async_context_t *context,
                               const uint8_t *in_data, size_t in_len) {
    if (!g_worker_state || in_len < sizeof(dag_t)) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    dag_t dag;
    if (dag_deserialize(in_data, in_len, &dag) != RESULT_OK) {
        LOG_ERROR("[RPC] Failed to deserialize DAG");
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    // Add/update DAG in worker's catalog
    int result = dag_catalog_add(&g_worker_state->dag_catalog, &dag);
    if (result == RESULT_ERR_EXISTS) {
        result = dag_catalog_update(&g_worker_state->dag_catalog, &dag);
    }
    
    if (result == RESULT_OK) {
        LOG_INFO("[RPC] Synced DAG %u to worker catalog", dag.dag_id);
    }
    
    uint8_t ack = 1;
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, &ack, 1);
}

// ============================================================================
// RPC SERVICE TABLE (Worker)
// ============================================================================

rpc_service_entry_t worker_rpc_service_table[] = {
    { FUNC_ID_PROCESS_MESSAGE, handle_process_message, 8192 }, 
    { FUNC_ID_SYNC_CATALOG, handle_sync_catalog, 8192 },
    
    // Sentinel
    { 0, NULL, 0 }
};