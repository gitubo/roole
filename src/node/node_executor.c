// src/node/node_executor.c

#define _POSIX_C_SOURCE 200809L

#include "roole/node.h"
#include "roole/dag.h"
#include "roole/common.h"
#include "roole/event_bus.h"
#include "roole/service_registry.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

// ============================================================================
// EXECUTOR THREAD (Unified for all nodes)
// ============================================================================

void* node_executor_thread_fn(void *arg) {
    // OLD: unified_node_t *node = (unified_node_t*)arg;
    // NEW:
    node_state_t *state = (node_state_t*)arg;
    
    logger_push_component("executor");
    LOG_INFO("Node executor thread started");
    
    // Get subsystems via accessors
    message_queue_t *queue = node_state_get_message_queue(state);
    dag_catalog_t *catalog = node_state_get_dag_catalog(state);
    execution_tracker_t *tracker = node_state_get_exec_tracker(state);
    peer_pool_t *pool = node_state_get_peer_pool(state);
    const node_identity_t *id = node_state_get_identity(state);
    
    // Get event bus for publishing events
    service_registry_t *registry = service_registry_global();
    event_bus_t *event_bus = NULL;
    if (registry) {
        event_bus = (event_bus_t*)service_registry_get(registry,
                                                       SERVICE_TYPE_EVENT_BUS,
                                                       "main");
    }
    
    while (!state->shutdown_flag) {
        message_t message;
        
        struct timespec queue_pop_time;
        timespec_now(&queue_pop_time);
        
        // OLD: message_queue_pop(&node->message_queue, &message, 1000);
        // NEW:
        int ret = message_queue_pop(queue, &message, 1000);
        
        if (ret == RESULT_ERR_TIMEOUT || ret == RESULT_ERR_EMPTY) {
            // Update metrics during idle
            if (state->metric_queue_size) {
                size_t queue_size = message_queue_size(queue);
                metrics_gauge_set(state->metric_queue_size, (double)queue_size);
            }
            continue;
        }
        
        if (ret != RESULT_OK) {
            LOG_ERROR("Failed to pop message from queue");
            continue;
        }
        
        uint64_t now_ms = time_now_ms();
        uint64_t wait_time_ms = now_ms - message.received_at_ms;
        
        LOG_INFO("Processing message %lu (DAG %u, waited %lu ms)",
                 message.exec_id, message.dag_id, wait_time_ms);
        
        // Publish EXECUTION_STARTED event
        if (event_bus) {
            event_t event = {
                .type = EVENT_TYPE_EXECUTION_STARTED,
                .timestamp_ms = time_now_ms(),
                .source_node_id = id->node_id,
                .data.execution = {
                    .exec_id = message.exec_id,
                    .dag_id = message.dag_id,
                    .assigned_peer = id->node_id,
                    .timestamp_ms = time_now_ms(),
                    .status_code = 0
                }
            };
            event_bus_publish(event_bus, &event);
        }
        
        // Record metrics
        /**** TODO HISTOGRAM */
        /*
        if (state->histogram_queue_wait) {
            metrics_histogram_observe(state->histogram_queue_wait, (double)wait_time_ms);
        }
        
        if (state->histogram_message_size) {
            metrics_histogram_observe(state->histogram_message_size,
                                     (double)message.message_len);
        }
        */
        
        // Increment active executions
        __sync_fetch_and_add(&state->active_executions, 1);
        
        struct timespec exec_start_time;
        timespec_now(&exec_start_time);
        
        if (state->metric_active_executions) {
            metrics_gauge_set(state->metric_active_executions,
                            (double)state->active_executions);
        }
        
        // Get DAG from catalog
        // OLD: dag_t *dag = dag_catalog_get(&node->dag_catalog, message.dag_id);
        // NEW:
        dag_t *dag = dag_catalog_get(catalog, message.dag_id);
        
        if (!dag) {
            LOG_ERROR("DAG %u not found in catalog", message.dag_id);
            __sync_fetch_and_sub(&state->active_executions, 1);
            
            if (state->metric_messages_failed) {
                metrics_counter_inc(state->metric_messages_failed);
            }
            continue;
        }
        
        // Setup execution context
        dag_execution_context_t exec_ctx;
        memset(&exec_ctx, 0, sizeof(exec_ctx));
        
        exec_ctx.exec_id = message.exec_id;
        exec_ctx.dag = dag;
        exec_ctx.input_data = message.message_data;
        exec_ctx.input_len = message.message_len;
        
        size_t output_capacity = 4096;
        exec_ctx.output_data = malloc(output_capacity);
        exec_ctx.output_capacity = output_capacity;
        exec_ctx.output_len = 0;
        
        execution_status_t status = EXEC_STATUS_FAILED;
        
        if (exec_ctx.output_data) {
            int exec_result = dag_execute(&exec_ctx);
            
            struct timespec exec_end_time;
            timespec_now(&exec_end_time);
            double exec_duration_us = time_diff_us(&exec_start_time, &exec_end_time);
            
            /***** TODO HISTOGRAM */
            /*
            if (state->histogram_exec_duration) {
                double exec_duration_ms = exec_duration_us / 1000.0;
                metrics_histogram_observe(state->histogram_exec_duration, exec_duration_ms);
            }
            */
            
            if (exec_result == RESULT_OK) {
                status = EXEC_STATUS_COMPLETED;
                LOG_INFO("Message %lu completed successfully", message.exec_id);
                
                if (event_bus) {
                    event_t event = {
                        .type = EVENT_TYPE_EXECUTION_COMPLETED,
                        .timestamp_ms = time_now_ms(),
                        .source_node_id = id->node_id,
                        .data.execution = {
                            .exec_id = message.exec_id,
                            .dag_id = message.dag_id,
                            .assigned_peer = id->node_id,
                            .timestamp_ms = time_now_ms(),
                            .status_code = 0
                        }
                    };
                    event_bus_publish(event_bus, &event);
                }
                
                if (state->metric_messages_processed) {
                    metrics_counter_inc(state->metric_messages_processed);
                }
            } else {
                status = EXEC_STATUS_FAILED;
                LOG_ERROR("Message %lu execution failed", message.exec_id);
                
                if (event_bus) {
                    event_t event = {
                        .type = EVENT_TYPE_EXECUTION_FAILED,
                        .timestamp_ms = time_now_ms(),
                        .source_node_id = id->node_id,
                        .data.execution = {
                            .exec_id = message.exec_id,
                            .dag_id = message.dag_id,
                            .assigned_peer = id->node_id,
                            .timestamp_ms = time_now_ms(),
                            .status_code = exec_result
                        }
                    };
                    event_bus_publish(event_bus, &event);
                }
                
                if (state->metric_messages_failed) {
                    metrics_counter_inc(state->metric_messages_failed);
                }
            }
        } else {
            LOG_ERROR("Failed to allocate output buffer");
            if (state->metric_messages_failed) {
                metrics_counter_inc(state->metric_messages_failed);
            }
        }
        
        // OLD: dag_catalog_release(&node->dag_catalog);
        // NEW:
        dag_catalog_release(catalog);
        
        if (exec_ctx.output_data) {
            free(exec_ctx.output_data);
        }
        
        // Send status update (if routed from another node)
        if (message.sender_id != id->node_id) {
            // OLD: peer_info_t *sender = peer_pool_get(&node->peer_pool, message.sender_id);
            // NEW:
            peer_info_t *sender = peer_pool_get(pool, message.sender_id);
            
            if (sender && sender->data_channel) {
                uint8_t payload[9];
                memcpy(payload, &message.exec_id, 8);
                payload[8] = (uint8_t)status;
                
                size_t msg_len = rpc_pack_message(
                    sender->data_channel->tx_buffer,
                    id->node_id,
                    message.exec_id,
                    RPC_TYPE_REQUEST,
                    RPC_STATUS_UNKNOWN,
                    FUNC_ID_EXECUTION_UPDATE,
                    payload,
                    9
                );
                
                ssize_t sent = send(sender->data_channel->socket_fd,
                                   sender->data_channel->tx_buffer, msg_len, 0);
                
                if (sent > 0) {
                    LOG_DEBUG("Sent status update to peer %u", message.sender_id);
                } else {
                    LOG_WARN("Failed to send status update to peer %u", message.sender_id);
                }
            }
            
            // OLD: peer_pool_release(&node->peer_pool);
            // NEW:
            peer_pool_release(pool);
        } else {
            // OLD: execution_tracker_update_status(&node->exec_tracker, ...);
            // NEW:
            execution_tracker_update_status(tracker, message.exec_id, status);
        }
        
        __sync_fetch_and_sub(&state->active_executions, 1);
        
        if (state->metric_active_executions) {
            metrics_gauge_set(state->metric_active_executions,
                            (double)state->active_executions);
        }
    }
    
    LOG_INFO("Node executor thread stopped");
    logger_pop_component();
    return NULL;
}

// ============================================================================
// EXECUTOR MANAGEMENT
// ============================================================================

int node_start_executors(unified_node_t *node) {
    if (!node || !node->capabilities.can_execute) {
        LOG_INFO("Execution capability disabled, no executor threads started");
        return RESULT_OK;
    }
    
    if (node->num_executor_threads == 0) {
        LOG_WARN("num_executor_threads is 0, defaulting to 1");
        node->num_executor_threads = 1;
    }
    
    // Allocate thread handles
    node->executor_threads = calloc(node->num_executor_threads, sizeof(pthread_t));
    if (!node->executor_threads) {
        LOG_ERROR("Failed to allocate executor thread handles");
        return RESULT_ERR_NOMEM;
    }
    
    // Start threads
    for (size_t i = 0; i < node->num_executor_threads; i++) {
        if (pthread_create(&node->executor_threads[i], NULL, 
                          node_executor_thread_fn, node) != 0) {
            LOG_ERROR("Failed to start executor thread %zu", i);
            
            // Stop already started threads
            node->shutdown_flag = 1;
            for (size_t j = 0; j < i; j++) {
                pthread_join(node->executor_threads[j], NULL);
            }
            
            free(node->executor_threads);
            node->executor_threads = NULL;
            return RESULT_ERR_INVALID;
        }
    }
    
    LOG_INFO("Started %zu executor threads", node->num_executor_threads);
    return RESULT_OK;
}

void node_stop_executors(unified_node_t *node) {
    if (!node || !node->executor_threads) return;
    
    LOG_INFO("Stopping executor threads...");
    
    node->shutdown_flag = 1;
    
    // Wait for all threads to finish
    for (size_t i = 0; i < node->num_executor_threads; i++) {
        pthread_join(node->executor_threads[i], NULL);
    }
    
    free(node->executor_threads);
    node->executor_threads = NULL;
    
    LOG_INFO("All executor threads stopped");
}

int node_start_executors_ex(node_state_t *state, size_t num_threads) {
    if (!state) return RESULT_ERR_INVALID;
    
    const node_capabilities_t *caps = node_state_get_capabilities(state);
    
    if (!caps->can_execute) {
        LOG_INFO("Execution capability disabled, no executor threads started");
        return RESULT_OK;
    }
    
    if (num_threads == 0) {
        LOG_WARN("num_threads is 0, defaulting to 1");
        num_threads = 1;
    }
    
    state->executor_threads = calloc(num_threads, sizeof(pthread_t));
    if (!state->executor_threads) {
        LOG_ERROR("Failed to allocate executor thread handles");
        return RESULT_ERR_NOMEM;
    }
    
    for (size_t i = 0; i < num_threads; i++) {
        if (pthread_create(&state->executor_threads[i], NULL,
                          node_executor_thread_fn, state) != 0) {
            LOG_ERROR("Failed to start executor thread %zu", i);
            
            state->shutdown_flag = 1;
            for (size_t j = 0; j < i; j++) {
                pthread_join(state->executor_threads[j], NULL);
            }
            
            free(state->executor_threads);
            state->executor_threads = NULL;
            return RESULT_ERR_INVALID;
        }
    }
    
    LOG_INFO("Started %zu executor threads", num_threads);
    return RESULT_OK;
}

void node_stop_executors_ex(node_state_t *state) {
    if (!state || !state->executor_threads) return;
    
    LOG_INFO("Stopping executor threads...");
    
    state->shutdown_flag = 1;
    
    for (size_t i = 0; i < state->num_executor_threads; i++) {
        pthread_join(state->executor_threads[i], NULL);
    }
    
    free(state->executor_threads);
    state->executor_threads = NULL;
    
    LOG_INFO("All executor threads stopped");
}