// src/worker/rpc_handlers.c

#define _POSIX_C_SOURCE 200809L

#include "roole/worker.h"
#include "roole/rpc.h"
#include "roole/common.h"
#include <string.h>
#include <stdlib.h>

// Global worker state
static worker_state_t *g_worker_state = NULL;

void worker_set_rpc_state(worker_state_t *worker) {
    g_worker_state = worker;
}

// ============================================================================
// HANDLER: Execute DAG (Router -> Worker)
// ============================================================================

/**
 * Request payload:
 *   [exec_id: 8 bytes][dag_id: 4 bytes][router_id: 4 bytes][message: variable]
 * 
 * Response: ACK (actual result sent later via FUNC_ID_EXECUTION_UPDATE)
 */
static int handle_execute_dag(rpc_async_context_t *context,
                              const uint8_t *in_data, size_t in_len) {
    if (!g_worker_state || in_len < 16) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    execution_id_t exec_id;
    dag_id_t dag_id;
    node_id_t router_id;
    
    memcpy(&exec_id, in_data, sizeof(execution_id_t));
    memcpy(&dag_id, in_data + 8, sizeof(dag_id_t));
    memcpy(&router_id, in_data + 12, sizeof(node_id_t));
    
    const uint8_t *message = in_data + 16;
    size_t message_len = in_len - 16;
    
    ROOLE_LOG_INFO("[RPC] Received DAG execution request (exec_id: %lu, DAG: %u, %zu bytes)",
                   exec_id, dag_id, message_len);
    
    // Enqueue task
    int result = worker_enqueue_task(g_worker_state, exec_id, dag_id, router_id, 
                                    message, message_len);
    
    if (result != ROOLE_OK) {
        ROOLE_LOG_ERROR("[RPC] Failed to enqueue task");
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    // Send ACK
    uint8_t ack = 1;
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, &ack, 1);
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
    if (dag_deserialize(in_data, in_len, &dag) != ROOLE_OK) {
        ROOLE_LOG_ERROR("[RPC] Failed to deserialize DAG");
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    // Add/update DAG in worker's catalog
    int result = dag_catalog_add(&g_worker_state->dag_catalog, &dag);
    if (result == ROOLE_ERR_EXISTS) {
        result = dag_catalog_update(&g_worker_state->dag_catalog, &dag);
    }
    
    if (result == ROOLE_OK) {
        ROOLE_LOG_INFO("[RPC] Synced DAG %u to worker catalog", dag.dag_id);
    }
    
    uint8_t ack = 1;
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, &ack, 1);
}

// ============================================================================
// RPC SERVICE TABLE (Worker)
// ============================================================================

rpc_service_entry_t worker_rpc_service_table[] = {
    { FUNC_ID_EXECUTE_DAG, handle_execute_dag, 8192 },
    { FUNC_ID_SYNC_CATALOG, handle_sync_catalog, 8192 },
    
    // Sentinel
    { 0, NULL, 0 }
};