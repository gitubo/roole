// src/router/router_core.c

#define _POSIX_C_SOURCE 200809L

#include "roole/router.h"
#include "roole/common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// ============================================================================
// ROUTER CALLBACKS
// ============================================================================

static void on_member_event(node_id_t node_id, node_type_t type,
                           const char *ip, uint16_t port,
                           const char *event_type, void *user_data) {
    router_state_t *router = (router_state_t*)user_data;

    if (type == NODE_TYPE_WORKER) {
        if (strcmp(event_type, MEMBER_EVENT_JOIN) == 0) {
            // For multi-channel: assume worker uses consecutive ports
            // port = service_port, port+1 = data_port
            uint16_t service_port = port;
            uint16_t data_port = port + 1;
            router_on_worker_join(router, node_id, ip, service_port, data_port);
        }
        else if (strcmp(event_type, MEMBER_EVENT_FAILED) == 0 ||
                 strcmp(event_type, MEMBER_EVENT_LEAVE) == 0) {
            router_on_worker_failed(router, node_id);
        }
    }
}

static void on_heartbeat_timeout(node_id_t node_id, node_status_t new_status, void *user_data) {
    router_state_t *router = (router_state_t*)user_data;
    
    LOG_WARN("Worker %u heartbeat timeout (status: %d)", node_id, new_status);
    
    if (new_status == NODE_STATUS_DEAD) {
        router_on_worker_failed(router, node_id);
    } else {
        worker_pool_update_status(&router->worker_pool, node_id, new_status);
    }
}

// ============================================================================
// BACKGROUND THREADS
// ============================================================================

static void* router_heartbeat_thread_fn(void *arg) {
    router_state_t *router = (router_state_t*)arg;
    
    LOG_INFO("Router heartbeat checker thread started");
    
    while (!router->shutdown_flag) {
        usleep(1000 * 1000);  // Check every second
        
        // Check for heartbeat timeouts
        heartbeat_tracker_check_timeouts(router->heartbeat_tracker, 
                                        on_heartbeat_timeout, router);
    }
    
    LOG_INFO("Router heartbeat checker thread stopped");
    return NULL;
}

static void* router_cleanup_thread_fn(void *arg) {
    router_state_t *router = (router_state_t*)arg;
    
    LOG_INFO("Router cleanup thread started");
    
    while (!router->shutdown_flag) {
        usleep(60 * 1000 * 1000);  // Run every minute
        
        // Cleanup old completed executions
        execution_tracker_cleanup_completed(&router->exec_tracker);
    }
    
    LOG_INFO("Router cleanup thread stopped");
    return NULL;
}

// ============================================================================
// ROUTER INITIALIZATION
// ============================================================================

int router_init(router_state_t *router, node_id_t router_id,
               uint16_t service_port, uint16_t data_port, uint16_t ingress_port) {
    if (!router) return RESULT_ERR_INVALID;

    memset(router, 0, sizeof(router_state_t));
    router->router_id = router_id;
    router->service_port = service_port;
    router->data_port = data_port;
    router->ingress_port = ingress_port;
    router->shutdown_flag = 0;
    
    // Initialize DAG catalog
    if (dag_catalog_init(&router->dag_catalog, MAX_DAGS) != RESULT_OK) {
        LOG_ERROR("Failed to initialize DAG catalog");
        return RESULT_ERR_INVALID;
    }
    
    // Initialize worker pool
    if (worker_pool_init(&router->worker_pool, MAX_WORKERS) != RESULT_OK) {
        LOG_ERROR("Failed to initialize worker pool");
        dag_catalog_destroy(&router->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    // Initialize execution tracker
    if (execution_tracker_init(&router->exec_tracker, MAX_PENDING_EXECUTIONS) != RESULT_OK) {
        LOG_ERROR("Failed to initialize execution tracker");
        worker_pool_destroy(&router->worker_pool);
        dag_catalog_destroy(&router->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    // Initialize cluster view
    if (cluster_view_init(&router->cluster_view, MAX_CLUSTER_NODES) != RESULT_OK) {
        LOG_ERROR("Failed to initialize cluster view");
        execution_tracker_destroy(&router->exec_tracker);
        worker_pool_destroy(&router->worker_pool);
        dag_catalog_destroy(&router->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    // Initialize heartbeat tracker
    heartbeat_config_t hb_config = {
        .interval_ms = DEFAULT_HEARTBEAT_INTERVAL_MS,
        .timeout_ms = DEFAULT_HEARTBEAT_TIMEOUT_MS,
        .dead_timeout_ms = DEFAULT_HEARTBEAT_TIMEOUT_MS * 3
    };
    
    if (heartbeat_tracker_init(&router->heartbeat_tracker, &hb_config) != RESULT_OK) {
        LOG_ERROR("Failed to initialize heartbeat tracker");
        cluster_view_destroy(&router->cluster_view);
        execution_tracker_destroy(&router->exec_tracker);
        worker_pool_destroy(&router->worker_pool);
        dag_catalog_destroy(&router->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    // Initialize membership (gossip)
    char bind_addr[32];
    snprintf(bind_addr, sizeof(bind_addr), "0.0.0.0");

    if (membership_init(&router->membership, router_id, NODE_TYPE_ROUTER,
                       bind_addr, service_port) != RESULT_OK) {
        LOG_ERROR("Failed to initialize membership");
        heartbeat_tracker_destroy(router->heartbeat_tracker);
        cluster_view_destroy(&router->cluster_view);
        execution_tracker_destroy(&router->exec_tracker);
        worker_pool_destroy(&router->worker_pool);
        dag_catalog_destroy(&router->dag_catalog);
        return RESULT_ERR_INVALID;
    }

    membership_set_callback(router->membership, on_member_event, router);

    LOG_INFO("Router %u initialized (SERVICE:%u, DATA:%u, INGRESS:%u)",
                   router_id, service_port, data_port, ingress_port);
    return RESULT_OK;
}

int router_start(router_state_t *router) {
    if (!router) return RESULT_ERR_INVALID;
    
    // Start background threads
    if (pthread_create(&router->heartbeat_thread, NULL, 
                      router_heartbeat_thread_fn, router) != 0) {
        LOG_ERROR("Failed to start heartbeat thread");
        return RESULT_ERR_INVALID;
    }
    
    if (pthread_create(&router->cleanup_thread, NULL, 
                      router_cleanup_thread_fn, router) != 0) {
        LOG_ERROR("Failed to start cleanup thread");
        router->shutdown_flag = 1;
        pthread_join(router->heartbeat_thread, NULL);
        return RESULT_ERR_INVALID;
    }
    
    // TODO: Start RPC worker for receiving requests from clients
    // rpc_worker_run(router->port, router_rpc_service_table);
    
    LOG_INFO("Router %u started", router->router_id);
    return RESULT_OK;
}

void router_shutdown(router_state_t *router) {
    if (!router) return;
    
    LOG_INFO("Shutting down router %u", router->router_id);
    
    router->shutdown_flag = 1;
    
    // Wait for threads
    pthread_join(router->heartbeat_thread, NULL);
    pthread_join(router->cleanup_thread, NULL);
    
    // Cleanup
    if (router->membership) {
        membership_shutdown(router->membership);
    }
    
    heartbeat_tracker_destroy(router->heartbeat_tracker);
    cluster_view_destroy(&router->cluster_view);
    execution_tracker_destroy(&router->exec_tracker);
    worker_pool_destroy(&router->worker_pool);
    dag_catalog_destroy(&router->dag_catalog);
    
    LOG_INFO("Router %u shutdown complete", router->router_id);
}

// ============================================================================
// DAG MANAGEMENT
// ============================================================================

int router_add_dag(router_state_t *router, const dag_t *dag) {
    if (!router || !dag) return RESULT_ERR_INVALID;
    
    // Validate DAG
    if (dag_validate(dag) != RESULT_OK) {
        LOG_ERROR("DAG validation failed");
        return RESULT_ERR_INVALID;
    }
    
    // TODO: Use Raft consensus to replicate across routers
    // For now, just add locally
    int result = dag_catalog_add(&router->dag_catalog, dag);
    
    if (result == RESULT_OK) {
        LOG_INFO("Router %u: Added DAG %u '%s'", 
                      router->router_id, dag->dag_id, dag->name);
    }
    
    return result;
}

int router_update_dag(router_state_t *router, const dag_t *dag) {
    if (!router || !dag) return RESULT_ERR_INVALID;
    
    if (dag_validate(dag) != RESULT_OK) {
        LOG_ERROR("DAG validation failed");
        return RESULT_ERR_INVALID;
    }
    
    // TODO: Use Raft consensus
    int result = dag_catalog_update(&router->dag_catalog, dag);
    
    if (result == RESULT_OK) {
        LOG_INFO("Router %u: Updated DAG %u", router->router_id, dag->dag_id);
    }
    
    return result;
}

int router_remove_dag(router_state_t *router, rule_id_t dag_id) {
    if (!router) return RESULT_ERR_INVALID;
    
    // TODO: Use Raft consensus
    int result = dag_catalog_remove(&router->dag_catalog, dag_id);
    
    if (result == RESULT_OK) {
        LOG_INFO("Router %u: Removed DAG %u", router->router_id, dag_id);
    }
    
    return result;
}

dag_t* router_get_dag(router_state_t *router, rule_id_t dag_id) {
    if (!router) return NULL;
    
    return dag_catalog_get(&router->dag_catalog, dag_id);
}

// ============================================================================
// MESSAGE SUBMISSION
// ============================================================================

int router_submit_message(router_state_t *router, rule_id_t dag_id,
                         const uint8_t *message, size_t message_len,
                         execution_id_t *out_exec_id) {
    if (!router || !message || message_len == 0) return RESULT_ERR_INVALID;
    
    // 1. Verify DAG exists
    dag_t *dag = dag_catalog_get(&router->dag_catalog, dag_id);
    if (!dag) {
        LOG_ERROR("DAG %u not found", dag_id);
        return RESULT_ERR_NOTFOUND;
    }
    dag_catalog_release(&router->dag_catalog);
    
    // 2. Select worker
    node_id_t worker_id = router_select_worker(router, LOAD_BALANCE_LEAST_LOADED);
    if (worker_id == 0) {
        LOG_ERROR("No available workers");
        return RESULT_ERR_NOTFOUND;
    }
    
    // 3. Create execution record
    execution_id_t exec_id = execution_tracker_add(&router->exec_tracker, 
                                                   dag_id, worker_id, 
                                                   message, message_len, 3);
    if (exec_id == 0) {
        LOG_ERROR("Failed to create execution record");
        return RESULT_ERR_INVALID;
    }
    
    // 4. Send to worker via RPC
    // TODO: Implement actual RPC send
    LOG_INFO("Submitted execution %lu (DAG %u) to worker %u", 
                   exec_id, dag_id, worker_id);
    
    // Mark as running
    execution_tracker_update_status(&router->exec_tracker, exec_id, EXEC_STATUS_RUNNING);
    
    if (out_exec_id) {
        *out_exec_id = exec_id;
    }
    
    return RESULT_OK;
}

int router_get_execution_status(router_state_t *router, execution_id_t exec_id,
                               execution_status_t *out_status) {
    if (!router || exec_id == 0 || !out_status) return RESULT_ERR_INVALID;
    
    execution_record_t *rec = execution_tracker_get(&router->exec_tracker, exec_id);
    if (!rec) {
        return RESULT_ERR_NOTFOUND;
    }
    
    *out_status = rec->status;
    execution_tracker_release(&router->exec_tracker);
    
    return RESULT_OK;
}

// ============================================================================
// WORKER MANAGEMENT
// ============================================================================

// COMMENT: Complete replacement for router_on_worker_join in src/router/router_core.c
// Replace entire function (around line 340):

int router_on_worker_join(router_state_t *router, node_id_t worker_id,
                         const char *ip, uint16_t service_port, uint16_t data_port) {
    if (!router || !ip) return RESULT_ERR_INVALID;

    LOG_INFO("Worker %u joined (%s SERVICE:%u DATA:%u)",
                   worker_id, ip, service_port, data_port);

    // Add to worker pool
    int result = worker_pool_add(&router->worker_pool, worker_id, ip, service_port, data_port);
    if (result != RESULT_OK && result != RESULT_ERR_EXISTS) {
        return result;
    }

    // Start tracking heartbeats
    heartbeat_tracker_add_node(router->heartbeat_tracker, worker_id);

    // Establish DATA channel connection
    worker_info_t *worker = worker_pool_get(&router->worker_pool, worker_id);
    if (worker) {
        if (!worker->data_channel) {
            worker->data_channel = safe_malloc(sizeof(rpc_channel_t));
            if (worker->data_channel) {
                if (rpc_client_connect(worker->data_channel, ip, data_port,
                                      RPC_CHANNEL_DATA, 4096) == 0) {
                    LOG_INFO("DATA channel established to worker %u", worker_id);
                } else {
                    LOG_ERROR("Failed to connect DATA channel to worker %u", worker_id);
                    safe_free(worker->data_channel);
                    worker->data_channel = NULL;
                }
            } else {
                LOG_ERROR("Failed to allocate DATA channel for worker %u", worker_id);
            }
        }
        
        worker_pool_release(&router->worker_pool);
    }
    
    return RESULT_OK;
}

int router_on_worker_failed(router_state_t *router, node_id_t worker_id) {
    if (!router) return RESULT_ERR_INVALID;
    
    LOG_ERROR("Worker %u failed - initiating recovery", worker_id);
    
    // Mark worker as dead
    worker_pool_update_status(&router->worker_pool, worker_id, NODE_STATUS_DEAD);
    
    // Get all pending executions on this worker
    execution_id_t pending_execs[MAX_PENDING_EXECUTIONS];
    size_t count = execution_tracker_get_by_worker(&router->exec_tracker, 
                                                   worker_id, pending_execs, 
                                                   MAX_PENDING_EXECUTIONS);
    
    LOG_INFO("Found %zu pending executions on failed worker %u", count, worker_id);
    
    // Re-schedule each execution
    for (size_t i = 0; i < count; i++) {
        execution_record_t *rec = execution_tracker_get(&router->exec_tracker, 
                                                        pending_execs[i]);
        if (rec) {
            LOG_INFO("Re-scheduling execution %lu", rec->exec_id);
            
            // Re-submit
            execution_id_t new_exec_id;
            router_submit_message(router, rec->dag_id, 
                                rec->message_data, rec->message_len, 
                                &new_exec_id);
            
            // Mark old execution as failed
            execution_tracker_update_status(&router->exec_tracker, 
                                          rec->exec_id, EXEC_STATUS_FAILED);
            
            execution_tracker_release(&router->exec_tracker);
        }
    }
    
    // Stop tracking heartbeats
    heartbeat_tracker_remove_node(router->heartbeat_tracker, worker_id);
    
    // Remove from pool (after some delay, or keep as dead for monitoring)
    // worker_pool_remove(&router->worker_pool, worker_id);
    
    return RESULT_OK;
}

int router_on_worker_heartbeat(router_state_t *router, node_id_t worker_id,
                              uint32_t active_execs, float load) {
    if (!router) return RESULT_ERR_INVALID;
    
    // Update heartbeat tracker
    heartbeat_tracker_update(router->heartbeat_tracker, worker_id);
    
    // Update worker pool load
    worker_pool_update_load(&router->worker_pool, worker_id, active_execs, load);
    
    return RESULT_OK;
}

int router_on_execution_update(router_state_t *router, execution_id_t exec_id,
                              execution_status_t status) {
    if (!router || exec_id == 0) return RESULT_ERR_INVALID;
    
    execution_tracker_update_status(&router->exec_tracker, exec_id, status);
    
    if (status == EXEC_STATUS_COMPLETED) {
        LOG_INFO("Execution %lu completed successfully", exec_id);
    } else if (status == EXEC_STATUS_FAILED) {
        LOG_ERROR("Execution %lu failed", exec_id);
    }
    
    return RESULT_OK;
}