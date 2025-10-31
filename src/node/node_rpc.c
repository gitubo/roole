// src/node/node_rpc.c

#define _POSIX_C_SOURCE 200809L

#include "roole/node.h"
#include "roole/rpc.h"
#include "roole/common.h"
#include <string.h>
#include <stdlib.h>

// ============================================================================
// GLOBAL STATE (Set by node initialization)
// ============================================================================

static unified_node_t *g_node_state = NULL;

void node_set_rpc_state(unified_node_t *node) {
    g_node_state = node;
}

unified_node_t* node_get_rpc_state(void) {
    return g_node_state;
}

// ============================================================================
// FORWARD DECLARATIONS (Handlers defined in separate files)
// ============================================================================

// Client-facing handlers (INGRESS channel)
int handle_submit_message(rpc_async_context_t *context, 
                          const uint8_t *in_data, size_t in_len);
int handle_get_execution_status(rpc_async_context_t *context,
                                const uint8_t *in_data, size_t in_len);
int handle_list_dags(rpc_async_context_t *context,
                     const uint8_t *in_data, size_t in_len);

// Peer-to-peer handlers (DATA channel)
int handle_process_message(rpc_async_context_t *context,
                           const uint8_t *in_data, size_t in_len);
int handle_execution_update(rpc_async_context_t *context,
                            const uint8_t *in_data, size_t in_len);
int handle_sync_catalog(rpc_async_context_t *context,
                        const uint8_t *in_data, size_t in_len);

// ============================================================================
// UNIFIED RPC SERVICE TABLE (All possible handlers)
// ============================================================================

static rpc_service_entry_t g_all_handlers[] = {
    // Client operations (INGRESS only)
    { FUNC_ID_SUBMIT_MESSAGE, handle_submit_message, 8192 },
    { FUNC_ID_GET_STATUS, handle_get_execution_status, 16 },
    { FUNC_ID_LIST_DAGS, handle_list_dags, 4096 },
    
    // Peer operations (DATA channel)
    { FUNC_ID_PROCESS_MESSAGE, handle_process_message, 8192 },
    { FUNC_ID_EXECUTION_UPDATE, handle_execution_update, 16 },
    { FUNC_ID_SYNC_CATALOG, handle_sync_catalog, 8192 },
    
    // Sentinel
    { 0, NULL, 0 }
};

// ============================================================================
// DYNAMIC SERVICE TABLE BUILDER
// ============================================================================

rpc_service_entry_t* node_build_rpc_service_table(const unified_node_t *node) {
    if (!node) return NULL;
    
    // Count handlers needed
    size_t handler_count = 0;
    
    for (size_t i = 0; g_all_handlers[i].handler != NULL; i++) {
        uint8_t func_id = g_all_handlers[i].func_id;
        int include = 0;
        
        // INGRESS handlers (only if node has ingress capability)
        if (func_id == FUNC_ID_SUBMIT_MESSAGE ||
            func_id == FUNC_ID_GET_STATUS ||
            func_id == FUNC_ID_LIST_DAGS) {
            include = node->capabilities.has_ingress;
        }
        // DATA handlers (always included for peer communication)
        else if (func_id == FUNC_ID_PROCESS_MESSAGE ||
                 func_id == FUNC_ID_EXECUTION_UPDATE ||
                 func_id == FUNC_ID_SYNC_CATALOG) {
            include = 1;
        }
        
        if (include) handler_count++;
    }
    
    // Allocate service table (+1 for sentinel)
    rpc_service_entry_t *table = calloc(handler_count + 1, sizeof(rpc_service_entry_t));
    if (!table) {
        LOG_ERROR("Failed to allocate RPC service table");
        return NULL;
    }
    
    // Build filtered table
    size_t table_idx = 0;
    for (size_t i = 0; g_all_handlers[i].handler != NULL; i++) {
        uint8_t func_id = g_all_handlers[i].func_id;
        int include = 0;
        
        // Apply same filtering logic
        if (func_id == FUNC_ID_SUBMIT_MESSAGE ||
            func_id == FUNC_ID_GET_STATUS ||
            func_id == FUNC_ID_LIST_DAGS) {
            include = node->capabilities.has_ingress;
        }
        else if (func_id == FUNC_ID_PROCESS_MESSAGE ||
                 func_id == FUNC_ID_EXECUTION_UPDATE ||
                 func_id == FUNC_ID_SYNC_CATALOG) {
            include = 1;
        }
        
        if (include) {
            table[table_idx++] = g_all_handlers[i];
        }
    }
    
    // Add sentinel
    table[table_idx].func_id = 0;
    table[table_idx].handler = NULL;
    table[table_idx].max_response_len = 0;
    
    LOG_INFO("Built RPC service table with %zu handlers (ingress: %d, execute: %d, route: %d)",
             handler_count, 
             node->capabilities.has_ingress,
             node->capabilities.can_execute,
             node->capabilities.can_route);
    
    return table;
}

void node_free_rpc_service_table(rpc_service_entry_t *table) {
    if (table) {
        free(table);
    }
}

// ============================================================================
// RPC SERVER STARTUP (Capability-driven)
// ============================================================================

int node_start_rpc_servers(unified_node_t *node, rpc_service_entry_t *service_table) {
    if (!node || !service_table) return RESULT_ERR_INVALID;
    
    // Set global state for handlers
    node_set_rpc_state(node);
    
    LOG_INFO("========================================");
    LOG_INFO("Starting RPC servers:");
    LOG_INFO("  DATA channel: port %u (peer communication)", node->data_port);
    
    if (node->capabilities.has_ingress) {
        LOG_INFO("  INGRESS channel: port %u (client requests)", node->ingress_port);
        LOG_INFO("  Starting in ROUTER mode (DATA + INGRESS)");
        LOG_INFO("========================================");
        
        // Start router mode (DATA + INGRESS)
        return rpc_router_run(node->data_port, node->ingress_port, service_table);
    } else {
        LOG_INFO("  INGRESS channel: DISABLED (no ingress capability)");
        LOG_INFO("  Starting in WORKER mode (DATA only)");
        LOG_INFO("========================================");
        
        // Start worker mode (DATA only)
        return rpc_worker_run(node->data_port, service_table);
    }
}