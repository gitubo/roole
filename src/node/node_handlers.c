// src/node/node_handlers.c

#define _POSIX_C_SOURCE 200809L

#include "roole/node.h"
#include "roole/rpc.h"
#include "roole/common.h"
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>

// Forward declarations for all RPC handlers
int handle_submit_message(rpc_async_context_t *context, 
                          const uint8_t *in_data, size_t in_len);
int handle_get_execution_status(rpc_async_context_t *context,
                                const uint8_t *in_data, size_t in_len);
int handle_list_dags(rpc_async_context_t *context,
                     const uint8_t *in_data, size_t in_len);
int handle_process_message(rpc_async_context_t *context,
                           const uint8_t *in_data, size_t in_len);
int handle_execution_update(rpc_async_context_t *context,
                            const uint8_t *in_data, size_t in_len);
int handle_sync_catalog(rpc_async_context_t *context,
                        const uint8_t *in_data, size_t in_len);

// External reference to global node state
extern unified_node_t* node_get_rpc_state(void);

// ============================================================================
// HANDLER: Submit Message (Client -> Node with ingress)
// ============================================================================

int handle_submit_message(rpc_async_context_t *context, 
                          const uint8_t *in_data, size_t in_len) {
    unified_node_t *node = node_get_rpc_state();
    
    if (!node || in_len < sizeof(rule_id_t)) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    // Deserialize request
    rule_id_t dag_id;
    memcpy(&dag_id, in_data, sizeof(rule_id_t));
    
    const uint8_t *message = in_data + sizeof(rule_id_t);
    size_t message_len = in_len - sizeof(rule_id_t);
    
    LOG_INFO("[RPC] Received message submission (DAG %u, %zu bytes)", dag_id, message_len);
    
    // Verify DAG exists
    dag_t *dag = dag_catalog_get(&node->dag_catalog, dag_id);
    if (!dag) {
        LOG_ERROR("DAG %u not found", dag_id);
        return rpc_send_async_response(context, RPC_STATUS_FUNC_NOT_FOUND, NULL, 0);
    }
    dag_catalog_release(&node->dag_catalog);
    
    // Select peer for execution (or self if can_execute)
    node_id_t target_peer = 0;
    
    if (node->capabilities.can_execute) {
        // Can process locally
        target_peer = node->node_id;
    } else if (node->capabilities.can_route) {
        // Route to capable peer
        target_peer = peer_pool_select_least_loaded(&node->peer_pool);
        if (target_peer == 0) {
            LOG_ERROR("No available execution peers");
            return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
        }
    } else {
        LOG_ERROR("Node cannot execute or route");
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    // Create execution record
    execution_id_t exec_id = execution_tracker_add(&node->exec_tracker, 
                                                   dag_id, target_peer, 
                                                   message, message_len, 3);
    if (exec_id == 0) {
        LOG_ERROR("Failed to create execution record");
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    // Route message
    if (target_peer == node->node_id) {
        // Process locally - enqueue
        message_t msg;
        msg.exec_id = exec_id;
        msg.dag_id = dag_id;
        msg.sender_id = context->sender_id;
        msg.received_at_ms = time_now_ms();
        memcpy(msg.message_data, message, message_len);
        msg.message_len = message_len;
        
        if (message_queue_push(&node->message_queue, &msg) != RESULT_OK) {
            LOG_ERROR("Failed to enqueue message locally");
            execution_tracker_update_status(&node->exec_tracker, exec_id, EXEC_STATUS_FAILED);
            return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
        }
        
        LOG_INFO("[RPC] Message enqueued locally (exec_id: %lu)", exec_id);
    } else {
        // Send to remote peer
        peer_info_t *peer = peer_pool_get(&node->peer_pool, target_peer);
        if (!peer || !peer->data_channel) {
            LOG_ERROR("Peer %u not available", target_peer);
            execution_tracker_update_status(&node->exec_tracker, exec_id, EXEC_STATUS_FAILED);
            peer_pool_release(&node->peer_pool);
            return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
        }
        
        // Build payload: [exec_id][dag_id][sender_id][message]
        size_t payload_len = 8 + 4 + 2 + message_len;
        uint8_t *payload = malloc(payload_len);
        if (!payload) {
            peer_pool_release(&node->peer_pool);
            return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
        }
        
        memcpy(payload, &exec_id, 8);
        memcpy(payload + 8, &dag_id, 4);
        memcpy(payload + 12, &context->sender_id, 2);
        memcpy(payload + 14, message, message_len);
        
        // Send via DATA channel
        size_t rpc_msg_len = rpc_pack_message(
            peer->data_channel->tx_buffer,
            node->node_id,
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
        peer_pool_release(&node->peer_pool);
        
        if (sent <= 0) {
            LOG_ERROR("Failed to send message to peer %u", target_peer);
            execution_tracker_update_status(&node->exec_tracker, exec_id, EXEC_STATUS_FAILED);
            return rpc_send_async_response(context, RPC_STATUS_NETWORK, NULL, 0);
        }
        
        LOG_INFO("[RPC] Message routed to peer %u (exec_id: %lu)", target_peer, exec_id);
        
        // Update metrics
        if (node->metric_messages_routed) {
            metrics_counter_inc(node->metric_messages_routed);
        }
    }
    
    // Mark as running
    execution_tracker_update_status(&node->exec_tracker, exec_id, EXEC_STATUS_RUNNING);
    
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
    unified_node_t *node = node_get_rpc_state();
    
    if (!node || in_len != sizeof(execution_id_t)) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    execution_id_t exec_id;
    memcpy(&exec_id, in_data, sizeof(execution_id_t));
    
    execution_record_t *rec = execution_tracker_get(&node->exec_tracker, exec_id);
    if (!rec) {
        return rpc_send_async_response(context, RPC_STATUS_FUNC_NOT_FOUND, NULL, 0);
    }
    
    uint8_t status = (uint8_t)rec->status;
    execution_tracker_release(&node->exec_tracker);
    
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, &status, 1);
}

// ============================================================================
// HANDLER: List DAGs
// ============================================================================


int handle_list_dags(rpc_async_context_t *context,
                     const uint8_t *in_data, size_t in_len) {
    (void)in_data;
    (void)in_len;
    
    unified_node_t *node = node_get_rpc_state();
    if (!node) {
        return rpc_send_async_response(context, RPC_STATUS_INTERNAL_ERROR, NULL, 0);
    }
    
    rule_id_t dag_ids[MAX_DAGS];
    size_t count = dag_catalog_list(&node->dag_catalog, dag_ids, MAX_DAGS);
    
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
// HANDLER: Process Message (Peer -> Node with execution capability)
// ============================================================================

int handle_process_message(rpc_async_context_t *context,
                           const uint8_t *in_data, size_t in_len) {
    unified_node_t *node = node_get_rpc_state();
    
    if (!node || in_len < 14) {
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
    
    // Enqueue for execution
    message_t msg;
    msg.exec_id = exec_id;
    msg.dag_id = dag_id;
    msg.sender_id = sender_id;
    msg.received_at_ms = time_now_ms();
    memcpy(msg.message_data, message, message_len);
    msg.message_len = message_len;
    
    int result = message_queue_push(&node->message_queue, &msg);
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
    unified_node_t *node = node_get_rpc_state();
    
    if (!node || in_len != 9) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    execution_id_t exec_id;
    uint8_t status_byte;
    
    memcpy(&exec_id, in_data, 8);
    memcpy(&status_byte, in_data + 8, 1);
    
    execution_status_t status = (execution_status_t)status_byte;
    
    execution_tracker_update_status(&node->exec_tracker, exec_id, status);
    
    LOG_DEBUG("[RPC] Execution %lu status updated to %d", exec_id, status);
    
    uint8_t ack = 1;
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, &ack, 1);
}

// ============================================================================
// HANDLER: Sync Catalog
// ============================================================================

int handle_sync_catalog(rpc_async_context_t *context,
                       const uint8_t *in_data, size_t in_len) {
    unified_node_t *node = node_get_rpc_state();
    
    if (!node || in_len < sizeof(dag_t)) {
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    dag_t dag;
    if (dag_deserialize(in_data, in_len, &dag) != RESULT_OK) {
        LOG_ERROR("[RPC] Failed to deserialize DAG");
        return rpc_send_async_response(context, RPC_STATUS_BAD_ARGUMENT, NULL, 0);
    }
    
    int result = dag_catalog_add(&node->dag_catalog, &dag);
    if (result == RESULT_ERR_EXISTS) {
        result = dag_catalog_update(&node->dag_catalog, &dag);
    }
    
    if (result == RESULT_OK) {
        LOG_INFO("[RPC] Synced DAG %u to catalog", dag.dag_id);
    }
    
    uint8_t ack = 1;
    return rpc_send_async_response(context, RPC_STATUS_SUCCESS, &ack, 1);
}