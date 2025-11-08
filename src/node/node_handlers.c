// src/node/node_handlers.c - PHASE 2: Updated to use node_state_t

#define _POSIX_C_SOURCE 200809L

#include "roole/node_state.h"
#include "roole/node.h"
#include "roole/rpc.h"
#include "roole/common.h"
#include "roole/event_bus.h"
#include "roole/service_registry.h"
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <arpa/inet.h>

static inline node_state_t* get_node_state(void) {
    service_registry_t *registry = service_registry_global();
    if (!registry) {
        LOG_ERROR("Global service registry not available");
        return NULL;
    }
    
    node_state_t *state = (node_state_t*)service_registry_get(
        registry, SERVICE_TYPE_NODE_STATE, "main"
    );
    
    if (!state) {
        LOG_ERROR("Node state not registered in service registry");
    }
    
    return state;
}

// ============================================================================
// HANDLER: Submit Message (Client -> Node with ingress)
// ============================================================================

int handle_submit_message(rpc_async_context_t *context, 
                          const uint8_t *in_data, size_t in_len) {
    node_state_t *state = get_node_state();
    
    if (!state || in_len < sizeof(rule_id_t)) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    // Deserialize request
    rule_id_t dag_id;
    memcpy(&dag_id, in_data, sizeof(rule_id_t));
    
    const uint8_t *message = in_data + sizeof(rule_id_t);
    size_t message_len = in_len - sizeof(rule_id_t);
    
    LOG_INFO("[RPC] Received message submission (DAG %u, %zu bytes)", dag_id, message_len);
    
    // Get subsystems using accessors
    dag_catalog_t *catalog = node_state_get_dag_catalog(state);
    peer_pool_t *pool = node_state_get_peer_pool(state);
    execution_tracker_t *tracker = node_state_get_exec_tracker(state);
    message_queue_t *queue = node_state_get_message_queue(state);
    const node_capabilities_t *caps = node_state_get_capabilities(state);
    const node_identity_t *identity = node_state_get_identity(state);
    
    // Verify DAG exists
    dag_t *dag = dag_catalog_get(catalog, dag_id);
    if (!dag) {
        LOG_ERROR("DAG %u not found", dag_id);
        return rpc_send_async_response(context, RPC_STATUS_FUNC_NOT_FOUND, NULL, 0);
    }
    dag_catalog_release(catalog);
    
    // Select peer for execution
    node_id_t target_peer = 0;
    
    if (caps->can_execute) {
        target_peer = identity->node_id;
    } else if (caps->can_route) {
        target_peer = peer_pool_select_least_loaded(pool);
        if (target_peer == 0) {
            LOG_ERROR("No available execution peers");
            return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
        }
    } else {
        LOG_ERROR("Node cannot execute or route");
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    // Create execution record
    execution_id_t exec_id = execution_tracker_add(tracker, dag_id, target_peer, 
                                                   message, message_len, 3);
    if (exec_id == 0) {
        LOG_ERROR("Failed to create execution record");
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    // Route message
    if (target_peer == identity->node_id) {
        // Process locally
        message_t msg = {
            .exec_id = exec_id,
            .dag_id = dag_id,
            .sender_id = context->sender_id,
            .received_at_ms = time_now_ms(),
            .message_len = message_len
        };
        memcpy(msg.message_data, message, message_len);
        
        if (message_queue_push(queue, &msg) != RESULT_OK) {
            LOG_ERROR("Failed to enqueue message locally");
            execution_tracker_update_status(tracker, exec_id, EXEC_STATUS_FAILED);
            return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
        }
        
        LOG_INFO("[RPC] Message enqueued locally (exec_id: %lu)", exec_id);
    } else {
        // Send to remote peer
        peer_info_t *peer = peer_pool_get(pool, target_peer);
        if (!peer || !peer->data_channel) {
            LOG_ERROR("Peer %u not available", target_peer);
            execution_tracker_update_status(tracker, exec_id, EXEC_STATUS_FAILED);
            peer_pool_release(pool);
            return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
        }
        
        // Build payload: [exec_id][dag_id][sender_id][message]
        size_t payload_len = 8 + 4 + 2 + message_len;
        uint8_t *payload = malloc(payload_len);
        if (!payload) {
            peer_pool_release(pool);
            return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
        }
        
        memcpy(payload, &exec_id, 8);
        memcpy(payload + 8, &dag_id, 4);
        memcpy(payload + 12, &context->sender_id, 2);
        memcpy(payload + 14, message, message_len);
        
        // Send via DATA channel
        size_t rpc_msg_len = rpc_pack_message(
            peer->data_channel->tx_buffer,
            identity->node_id,
            exec_id,
            RPC_TYPE_REQUEST,
            RPC_STATUS_UNKNOWN,
            FUNC_ID_PROCESS_MESSAGE,
            payload,
            payload_len
        );
        
        ssize_t sent = send(peer->data_channel->socket_fd, 
                           peer->data_channel->tx_buffer, rpc_msg_len, 0);
        
        free(payload);
        peer_pool_release(pool);
        
        if (sent <= 0) {
            LOG_ERROR("Failed to send message to peer %u", target_peer);
            execution_tracker_update_status(tracker, exec_id, EXEC_STATUS_FAILED);
            return rpc_send_async_response(context, RPC_STATUS_NETWORK, NULL, 0);
        }
        
        LOG_INFO("[RPC] Message routed to peer %u (exec_id: %lu)", target_peer, exec_id);
    }
    
    // Mark as running
    execution_tracker_update_status(tracker, exec_id, EXEC_STATUS_RUNNING);
    
    // Build response: [exec_id][status]
    uint8_t response[9];
    memcpy(response, &exec_id, sizeof(execution_id_t));
    response[8] = (uint8_t)EXEC_STATUS_RUNNING;
    
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, response, 9);
}
// ============================================================================
// HANDLER: Get Execution Status
// ============================================================================

int handle_get_execution_status(rpc_async_context_t *context,
                                const uint8_t *in_data, size_t in_len) {
    node_state_t *state = get_node_state();
    
    if (!state || in_len != sizeof(execution_id_t)) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    execution_id_t exec_id;
    memcpy(&exec_id, in_data, sizeof(execution_id_t));
    
    execution_tracker_t *tracker = node_state_get_exec_tracker(state);
    execution_record_t *rec = execution_tracker_get(tracker, exec_id);
    if (!rec) {
        return rpc_send_async_response(context, RPC_STATUS_FUNC_NOT_FOUND, NULL, 0);
    }
    
    uint8_t status = (uint8_t)rec->status;
    execution_tracker_release(tracker);
    
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, &status, 1);
}

// ============================================================================
// HANDLER: List DAGs
// ============================================================================

int handle_list_dags(rpc_async_context_t *context,
                     const uint8_t *in_data, size_t in_len) {
    (void)in_data;
    (void)in_len;
    
    node_state_t *state = get_node_state();
    if (!state) {
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    dag_catalog_t *catalog = node_state_get_dag_catalog(state);
    
    rule_id_t dag_ids[MAX_DAGS];
    size_t count = dag_catalog_list(catalog, dag_ids, MAX_DAGS);
    
    // Build response: [count: 4 bytes][dag_id_1: 4 bytes]...
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
// HANDLER: Process Message (Peer -> Executor)
// ============================================================================

int handle_process_message(rpc_async_context_t *context,
                           const uint8_t *in_data, size_t in_len) {
    node_state_t *state = get_node_state();
    
    if (!state || in_len < 14) {
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
    
    LOG_INFO("[RPC] Received message for processing (exec_id: %lu, DAG: %u)",
             exec_id, dag_id);
    
    message_queue_t *queue = node_state_get_message_queue(state);
    
    message_t msg = {
        .exec_id = exec_id,
        .dag_id = dag_id,
        .sender_id = sender_id,
        .received_at_ms = time_now_ms(),
        .message_len = message_len
    };
    memcpy(msg.message_data, message, message_len);
    
    int result = message_queue_push(queue, &msg);
    if (result != RESULT_OK) {
        LOG_ERROR("[RPC] Failed to enqueue message");
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    uint8_t ack = 1;
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, &ack, 1);
}

// ============================================================================
// HANDLER: Execution Update (Peer -> Coordinator)
// ============================================================================

int handle_execution_update(rpc_async_context_t *context,
                            const uint8_t *in_data, size_t in_len) {
    node_state_t *state = get_node_state();
    
    if (!state || in_len != 9) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    execution_id_t exec_id;
    uint8_t status_byte;
    
    memcpy(&exec_id, in_data, 8);
    memcpy(&status_byte, in_data + 8, 1);
    
    execution_status_t status = (execution_status_t)status_byte;
    
    execution_tracker_t *tracker = node_state_get_exec_tracker(state);
    execution_tracker_update_status(tracker, exec_id, status);
    
    LOG_DEBUG("[RPC] Execution %lu status updated to %d", exec_id, status);
    
    uint8_t ack = 1;
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, &ack, 1);
}

// ============================================================================
// HANDLER: Sync Catalog
// ============================================================================

int handle_sync_catalog(rpc_async_context_t *context,
                       const uint8_t *in_data, size_t in_len) {
    node_state_t *state = get_node_state();
    
    if (!state || in_len < sizeof(dag_t)) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    dag_t dag;
    if (dag_deserialize(in_data, in_len, &dag) != RESULT_OK) {
        LOG_ERROR("[RPC] Failed to deserialize DAG");
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    dag_catalog_t *catalog = node_state_get_dag_catalog(state);
    
    int result = dag_catalog_add(catalog, &dag);
    if (result == RESULT_ERR_EXISTS) {
        result = dag_catalog_update(catalog, &dag);
    }
    
    if (result == RESULT_OK) {
        LOG_INFO("[RPC] Synced DAG %u to catalog", dag.dag_id);
    }
    
    uint8_t ack = 1;
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, &ack, 1);
}

// ============================================================================
// HANDLER: Add DAG
// ============================================================================

int handle_add_dag(rpc_async_context_t *context,
                   const uint8_t *in_data, size_t in_len) {
    
    node_state_t *state = get_node_state();
    if (!state) {
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    // Parse payload (unchanged)
    const uint8_t *ptr = in_data;
    size_t remaining = in_len;
    
    // Extract DAG ID
    if (remaining < sizeof(uint32_t)) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    uint32_t dag_id = ntohl(*(uint32_t*)ptr);
    ptr += sizeof(uint32_t);
    remaining -= sizeof(uint32_t);
    
    // Extract name length
    if (remaining < sizeof(uint32_t)) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    uint32_t name_len = ntohl(*(uint32_t*)ptr);
    ptr += sizeof(uint32_t);
    remaining -= sizeof(uint32_t);
    
    // Extract name
    if (remaining < name_len || name_len >= MAX_DAG_NAME) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    char dag_name[MAX_DAG_NAME];
    memcpy(dag_name, ptr, name_len);
    dag_name[name_len] = '\0';
    ptr += name_len;
    remaining -= name_len;
    
    // Extract num_stages
    if (remaining < sizeof(uint32_t)) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    uint32_t num_stages = ntohl(*(uint32_t*)ptr);
    ptr += sizeof(uint32_t);
    remaining -= sizeof(uint32_t);
    
    // Build DAG structure
    dag_t dag;
    memset(&dag, 0, sizeof(dag_t));
    dag.dag_id = dag_id;
    safe_strncpy(dag.name, dag_name, MAX_DAG_NAME);
    dag.version = 1;
    dag.step_count = num_stages;
    dag.created_at_ms = time_now_ms();
    dag.updated_at_ms = dag.created_at_ms;
    
    // Parse stages
    for (uint32_t i = 0; i < num_stages && i < MAX_DAG_STEPS; i++) {
        // Stage ID
        if (remaining < sizeof(uint32_t)) {
            return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
        }
        uint32_t stage_id = ntohl(*(uint32_t*)ptr);
        ptr += sizeof(uint32_t);
        remaining -= sizeof(uint32_t);
        
        dag.steps[i].step_id = stage_id;
        snprintf(dag.steps[i].name, MAX_STEP_NAME, "stage_%u", stage_id);
        snprintf(dag.steps[i].function_name, MAX_STEP_NAME, "process_stage_%u", stage_id);
        
        // Number of dependencies
        if (remaining < sizeof(uint32_t)) {
            return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
        }
        uint32_t num_deps = ntohl(*(uint32_t*)ptr);
        ptr += sizeof(uint32_t);
        remaining -= sizeof(uint32_t);
        
        dag.steps[i].dependency_count = num_deps;
        
        // Parse dependencies
        for (uint32_t j = 0; j < num_deps && j < MAX_STEP_DEPENDENCIES; j++) {
            if (remaining < sizeof(uint32_t)) {
                return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
            }
            uint32_t dep_id = ntohl(*(uint32_t*)ptr);
            ptr += sizeof(uint32_t);
            remaining -= sizeof(uint32_t);
            
            dag.steps[i].dependencies[j] = dep_id;
        }
        
        dag.steps[i].timeout_ms = 30000;
        dag.steps[i].max_retries = 3;
    }
    
    // Validate DAG
    if (dag_validate(&dag) != RESULT_OK) {
        LOG_ERROR("DAG validation failed for DAG %u", dag_id);
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    // Add to local catalog
    dag_catalog_t *catalog = node_state_get_dag_catalog(state);
    int result = dag_catalog_add(catalog, &dag);
    
    if (result != RESULT_OK) {
        LOG_ERROR("Failed to add DAG %u to catalog", dag_id);
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    LOG_INFO("DAG %u '%s' registered successfully (%u stages)", 
             dag_id, dag_name, num_stages);
    
    // ✅ NEW: PROPAGATE DAG TO ALL WORKERS
    LOG_INFO("Propagating DAG %u to all workers...", dag_id);
    
    peer_pool_t *pool = node_state_get_peer_pool(state);
    const node_identity_t *identity = node_state_get_identity(state);
    
    // Get list of all alive workers
    node_id_t worker_ids[MAX_PEERS];
    size_t worker_count = peer_pool_list_by_capability(pool, 1, worker_ids, MAX_PEERS);
    
    LOG_INFO("Found %zu workers to sync DAG catalog", worker_count);
    
    // Serialize DAG
    uint8_t dag_buffer[8192];
    size_t serialized_size = dag_serialize(&dag, dag_buffer, sizeof(dag_buffer));
    
    if (serialized_size == 0) {
        LOG_WARN("Failed to serialize DAG for propagation");
    } else {
        // Send to each worker
        for (size_t i = 0; i < worker_count; i++) {
            peer_info_t *peer = peer_pool_get(pool, worker_ids[i]);
            
            if (!peer || !peer->data_channel) {
                LOG_WARN("Worker %u not available for DAG sync", worker_ids[i]);
                peer_pool_release(pool);
                continue;
            }
            
            // Build SYNC_CATALOG RPC message
            size_t rpc_msg_len = rpc_pack_message(
                peer->data_channel->tx_buffer,
                identity->node_id,
                dag_id,  // Use DAG ID as request ID
                RPC_TYPE_REQUEST,
                RPC_STATUS_UNKNOWN,
                FUNC_ID_SYNC_CATALOG,
                dag_buffer,
                serialized_size
            );
            
            // Send to worker
            ssize_t sent = send(peer->data_channel->socket_fd,
                               peer->data_channel->tx_buffer, rpc_msg_len, 0);
            
            peer_pool_release(pool);
            
            if (sent > 0) {
                LOG_INFO("DAG %u synced to worker %u", dag_id, worker_ids[i]);
            } else {
                LOG_WARN("Failed to sync DAG %u to worker %u", dag_id, worker_ids[i]);
            }
        }
    }
    
    // ✅ NEW: Publish event for DAG addition
    service_registry_t *registry = service_registry_global();
    if (registry) {
        event_bus_t *event_bus = (event_bus_t*)service_registry_get(registry,
                                                                     SERVICE_TYPE_EVENT_BUS,
                                                                     "main");
        if (event_bus) {
            event_t event = {
                .type = EVENT_TYPE_CATALOG_UPDATED,
                .timestamp_ms = time_now_ms(),
                .source_node_id = identity->node_id,
                .data.catalog = {
                    .dag_id = dag_id,
                    .version = dag.version
                }
            };
            safe_strncpy(event.data.catalog.dag_name, dag_name, 64);
            event_bus_publish(event_bus, &event);
        }
    }
    
    uint8_t ack = 1;
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, &ack, 1);
}