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
    unified_node_t *node = (unified_node_t*)arg;
    
    logger_push_component("executor");
    LOG_INFO("Node executor thread started");
    
    while (!node->shutdown_flag) {
        message_t message;

        // Record queue pop time
        struct timespec queue_pop_time;
        timespec_now(&queue_pop_time);
        
        // Pop message from queue (1 second timeout)
        int ret = message_queue_pop(&node->message_queue, &message, 1000);
        
        if (ret == RESULT_ERR_TIMEOUT || ret == RESULT_ERR_EMPTY) {
            // Update metrics during idle time
            if (node->metric_queue_size) {
                size_t queue_size = message_queue_size(&node->message_queue);
                metrics_gauge_set(node->metric_queue_size, (double)queue_size);
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
        
        // PUBLISH EXECUTION_STARTED EVENT
        service_registry_t *registry = service_registry_global();
        event_bus_t *event_bus = NULL;
        if (registry) {
            event_bus = (event_bus_t*)service_registry_get(registry,
                                                           SERVICE_TYPE_EVENT_BUS,
                                                           "main");
        }
        
        if (event_bus) {
            event_t event = {
                .type = EVENT_TYPE_EXECUTION_STARTED,
                .timestamp_ms = time_now_ms(),
                .source_node_id = node->node_id,
                .data.execution = {
                    .exec_id = message.exec_id,
                    .dag_id = message.dag_id,
                    .assigned_peer = node->node_id,
                    .timestamp_ms = time_now_ms(),
                    .status_code = 0
                }
            };
            event_bus_publish(event_bus, &event);
        }

        // Calculate queue wait time
        if (node->histogram_queue_wait) {
            uint64_t wait_ms = time_now_ms() - message.received_at_ms;
            metrics_histogram_observe(node->histogram_queue_wait, (double)wait_ms);
        }
        
        // Record message size
        if (node->histogram_message_size) {
            metrics_histogram_observe(node->histogram_message_size, 
                                     (double)message.message_len);
        }

        // Increment active executions
        __sync_fetch_and_add(&node->active_executions, 1);
        
        // Record execution start time
        struct timespec exec_start_time;
        timespec_now(&exec_start_time);

        if (node->metric_active_executions) {
            metrics_gauge_set(node->metric_active_executions, 
                            (double)node->active_executions);
        }
        
        struct timespec start_time;
        timespec_now(&start_time);
        
        // Get DAG from catalog
        dag_t *dag = dag_catalog_get(&node->dag_catalog, message.dag_id);
        if (!dag) {
            LOG_ERROR("DAG %u not found in catalog", message.dag_id);
            
            __sync_fetch_and_sub(&node->active_executions, 1);
            
            if (node->metric_messages_failed) {
                metrics_counter_inc(node->metric_messages_failed);
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
        
        // Allocate output buffer
        size_t output_capacity = 4096;
        exec_ctx.output_data = malloc(output_capacity);
        exec_ctx.output_capacity = output_capacity;
        exec_ctx.output_len = 0;
        
        // Execute DAG
        execution_status_t status = EXEC_STATUS_FAILED;
        
        if (exec_ctx.output_data) {
            int exec_result = dag_execute(&exec_ctx);

            // Calculate execution duration
            struct timespec exec_end_time;
            timespec_now(&exec_end_time);
            double exec_duration_us = time_diff_us(&exec_start_time, &exec_end_time);
            
            // Record execution duration in histogram
            if (node->histogram_exec_duration) {
                double exec_duration_ms = exec_duration_us / 1000.0;
                metrics_histogram_observe(node->histogram_exec_duration, exec_duration_ms);
            }
            
            if (exec_result == RESULT_OK) {
                status = EXEC_STATUS_COMPLETED;
                LOG_INFO("Message %lu completed successfully", message.exec_id);
                
                // PUBLISH EXECUTION_COMPLETED EVENT
                if (event_bus) {
                    event_t event = {
                        .type = EVENT_TYPE_EXECUTION_COMPLETED,
                        .timestamp_ms = time_now_ms(),
                        .source_node_id = node->node_id,
                        .data.execution = {
                            .exec_id = message.exec_id,
                            .dag_id = message.dag_id,
                            .assigned_peer = node->node_id,
                            .timestamp_ms = time_now_ms(),
                            .status_code = 0
                        }
                    };
                    event_bus_publish(event_bus, &event);
                }
                
                if (node->metric_messages_processed) {
                    metrics_counter_inc(node->metric_messages_processed);
                }
            } else {
                status = EXEC_STATUS_FAILED;
                LOG_ERROR("Message %lu execution failed", message.exec_id);
                
                // PUBLISH EXECUTION_FAILED EVENT
                if (event_bus) {
                    event_t event = {
                        .type = EVENT_TYPE_EXECUTION_FAILED,
                        .timestamp_ms = time_now_ms(),
                        .source_node_id = node->node_id,
                        .data.execution = {
                            .exec_id = message.exec_id,
                            .dag_id = message.dag_id,
                            .assigned_peer = node->node_id,
                            .timestamp_ms = time_now_ms(),
                            .status_code = exec_result
                        }
                    };
                    event_bus_publish(event_bus, &event);
                }
                
                if (node->metric_messages_failed) {
                    metrics_counter_inc(node->metric_messages_failed);
                }
            }
        } else {
            LOG_ERROR("Failed to allocate output buffer");
            if (node->metric_messages_failed) {
                metrics_counter_inc(node->metric_messages_failed);
            }
        }
        
        // Release DAG catalog lock
        dag_catalog_release(&node->dag_catalog);
        
        // Free output buffer
        if (exec_ctx.output_data) {
            free(exec_ctx.output_data);
        }
        
        struct timespec end_time;
        timespec_now(&end_time);
        double processing_time_us = time_diff_us(&start_time, &end_time);
        
        LOG_DEBUG("Message %lu processing took %.2f ms", 
                 message.exec_id, processing_time_us / 1000.0);
        
        // Send execution status update back to originating node
        // Find peer that routed this message (if not self)
        if (message.sender_id != node->node_id) {
            peer_info_t *sender_peer = peer_pool_get(&node->peer_pool, message.sender_id);
            
            if (sender_peer && sender_peer->data_channel) {
                // Build status update payload: [exec_id][status]
                uint8_t payload[9];
                memcpy(payload, &message.exec_id, 8);
                payload[8] = (uint8_t)status;
                
                size_t msg_len = rpc_pack_message(
                    sender_peer->data_channel->tx_buffer,
                    node->node_id,
                    message.exec_id,
                    RPC_TYPE_REQUEST,
                    RPC_STATUS_UNKNOWN,
                    FUNC_ID_EXECUTION_UPDATE,
                    payload,
                    9
                );
                
                ssize_t sent = send(sender_peer->data_channel->socket_fd,
                                   sender_peer->data_channel->tx_buffer, msg_len, 0);
                
                if (sent > 0) {
                    LOG_DEBUG("Sent status update to peer %u", message.sender_id);
                } else {
                    LOG_WARN("Failed to send status update to peer %u", message.sender_id);
                }
            } else {
                LOG_WARN("Sender peer %u not available for status update", message.sender_id);
            }
            
            peer_pool_release(&node->peer_pool);
        } else {
            // Update local execution tracker
            execution_tracker_update_status(&node->exec_tracker, message.exec_id, status);
        }
        
        // Decrement active executions
        __sync_fetch_and_sub(&node->active_executions, 1);
        
        if (node->metric_active_executions) {
            metrics_gauge_set(node->metric_active_executions, 
                            (double)node->active_executions);
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