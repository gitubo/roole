// src/node/node_rpc.c

#define _POSIX_C_SOURCE 200809L

#include "roole/node.h"
#include "roole/rpc.h"
#include "roole/common.h"
#include "roole/service_registry.h"
#include <string.h>
#include <stdlib.h>

// ============================================================================
// GLOBAL STATE (Set by node initialization)
// ============================================================================

//static unified_node_t *g_node_state = NULL;

void node_set_rpc_state(unified_node_t *node) {
    service_registry_t *registry = service_registry_global();
    if (!registry) {
        LOG_ERROR("Global service registry not initialized");
        return;
    }
    
    service_registry_register(registry, SERVICE_TYPE_NODE_STATE, 
                             "unified_node", node);
}

unified_node_t* node_get_rpc_state(void) {
    service_registry_t *registry = service_registry_global();
    if (!registry) {
        LOG_ERROR("Global service registry not initialized");
        return NULL;
    }
    
    return (unified_node_t*)service_registry_get(registry, 
                                                 SERVICE_TYPE_NODE_STATE,
                                                 "unified_node");
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
    
    // Count handlers needed based on capabilities
    size_t handler_count = 0;
    
    // INGRESS handlers (only if has_ingress capability)
    if (node->capabilities.has_ingress) {
        handler_count += 3;  // SUBMIT_MESSAGE, GET_STATUS, LIST_DAGS
    }
    
    // DATA handlers (always included for peer communication)
    handler_count += 3;  // PROCESS_MESSAGE, EXECUTION_UPDATE, SYNC_CATALOG
    
    // Allocate service table (+1 for sentinel)
    rpc_service_entry_t *table = calloc(handler_count + 1, sizeof(rpc_service_entry_t));
    if (!table) {
        LOG_ERROR("Failed to allocate RPC service table");
        return NULL;
    }
    
    size_t idx = 0;
    
    // INGRESS handlers (client-facing)
    if (node->capabilities.has_ingress) {
        table[idx++] = (rpc_service_entry_t){
            FUNC_ID_SUBMIT_MESSAGE, handle_submit_message, 8192
        };
        table[idx++] = (rpc_service_entry_t){
            FUNC_ID_GET_STATUS, handle_get_execution_status, 16
        };
        table[idx++] = (rpc_service_entry_t){
            FUNC_ID_LIST_DAGS, handle_list_dags, 4096
        };
        LOG_DEBUG("Registered INGRESS handlers (client-facing)");
    }
    
    // DATA handlers (peer-to-peer) - always included
    table[idx++] = (rpc_service_entry_t){
        FUNC_ID_PROCESS_MESSAGE, handle_process_message, 8192
    };
    table[idx++] = (rpc_service_entry_t){
        FUNC_ID_EXECUTION_UPDATE, handle_execution_update, 16
    };
    table[idx++] = (rpc_service_entry_t){
        FUNC_ID_SYNC_CATALOG, handle_sync_catalog, 8192
    };
    LOG_DEBUG("Registered DATA handlers (peer communication)");
    
    // Sentinel
    table[idx] = (rpc_service_entry_t){0, NULL, 0};
    
    LOG_INFO("RPC service table built: %zu handlers (ingress=%d, data=always)",
             handler_count, node->capabilities.has_ingress);
    
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
    
    service_registry_t *registry = service_registry_global();
    if (registry) {
        service_registry_register(registry, SERVICE_TYPE_RPC_SERVER,
                                 "service_table", service_table);
    }

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