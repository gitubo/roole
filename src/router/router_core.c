// src/router/router_core.c

#define _POSIX_C_SOURCE 200809L

#include "roole/router.h"
#include "roole/common.h"
#include "roole/config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// ============================================================================
// ROUTER CALLBACKS
// ============================================================================

static void on_member_event(node_id_t node_id, node_type_t type,
                           const char *ip, uint16_t data_port,
                           const char *event_type, void *user_data) {
    router_state_t *router = (router_state_t*)user_data;

    if (type == NODE_TYPE_WORKER) {
        if (strcmp(event_type, MEMBER_EVENT_JOIN) == 0) {
            router_on_worker_join(router, node_id, ip, data_port);
        }
        else if (strcmp(event_type, MEMBER_EVENT_FAILED) == 0 ||
                 strcmp(event_type, MEMBER_EVENT_LEAVE) == 0) {
            router_on_worker_failed(router, node_id);
        }
    }
}

// ============================================================================
// BACKGROUND THREADS
// ============================================================================

static void* router_cleanup_thread_fn(void *arg) {
    router_state_t *router = (router_state_t*)arg;
    
    LOG_INFO("Router cleanup thread started");
    
    while (!router->shutdown_flag) {
        usleep(60 * 1000 * 1000);
        
        execution_tracker_cleanup_completed(&router->exec_tracker);
    }
    
    LOG_INFO("Router cleanup thread stopped");
    return NULL;
}

// ============================================================================
// ROUTER INITIALIZATION
// ============================================================================

int router_init(router_state_t *router, node_id_t router_id,
               uint16_t gossip_port, uint16_t data_port, uint16_t ingress_port,
               const char *bind_addr, const char *metrics_addr) {
    if (!router) return RESULT_ERR_INVALID;

    memset(router, 0, sizeof(router_state_t));
    router->router_id = router_id;
    router->gossip_port = gossip_port;
    router->data_port = data_port;
    router->ingress_port = ingress_port;
    router->shutdown_flag = 0;
    
    safe_strncpy(router->bind_addr, bind_addr ? bind_addr : "0.0.0.0", MAX_IP_LEN);
    
    if (dag_catalog_init(&router->dag_catalog, MAX_DAGS) != RESULT_OK) {
        LOG_ERROR("Failed to initialize DAG catalog");
        return RESULT_ERR_INVALID;
    }
    
    if (worker_pool_init(&router->worker_pool, MAX_WORKERS) != RESULT_OK) {
        LOG_ERROR("Failed to initialize worker pool");
        dag_catalog_destroy(&router->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    if (execution_tracker_init(&router->exec_tracker, MAX_PENDING_EXECUTIONS) != RESULT_OK) {
        LOG_ERROR("Failed to initialize execution tracker");
        worker_pool_destroy(&router->worker_pool);
        dag_catalog_destroy(&router->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    if (cluster_view_init(&router->cluster_view, MAX_CLUSTER_NODES) != RESULT_OK) {
        LOG_ERROR("Failed to initialize cluster view");
        execution_tracker_destroy(&router->exec_tracker);
        worker_pool_destroy(&router->worker_pool);
        dag_catalog_destroy(&router->dag_catalog);
        return RESULT_ERR_INVALID;
    }
    
    if (membership_init(&router->membership, router_id, NODE_TYPE_ROUTER,
                       router->bind_addr, router->gossip_port, router->data_port) != RESULT_OK) {
        LOG_ERROR("Failed to initialize membership");
        cluster_view_destroy(&router->cluster_view);
        execution_tracker_destroy(&router->exec_tracker);
        worker_pool_destroy(&router->worker_pool);
        dag_catalog_destroy(&router->dag_catalog);
        return RESULT_ERR_INVALID;
    }

    membership_set_callback(router->membership, on_member_event, router);
    
    // Get gossip engine handle for direct access
    router->gossip_engine = ((struct membership_handle*)router->membership)->gossip_engine;

// ========================================================================
    // METRICS INITIALIZATION
    // ========================================================================
    
    if (metrics_addr && strlen(metrics_addr) > 0) {
        char metrics_ip[16];
        uint16_t metrics_port;
        config_parse_address(metrics_addr, metrics_ip, &metrics_port);
        
        LOG_INFO("Metrics configuration: addr='%s', parsed ip='%s', port=%u", 
                 metrics_addr, metrics_ip, metrics_port);
        
        if (metrics_port > 0) {
            LOG_INFO("Initializing router metrics system...");
            
            router->metrics_registry = metrics_registry_init();
            if (!router->metrics_registry) {
                LOG_WARN("Failed to initialize metrics registry - continuing without metrics");
            } else {
                LOG_INFO("Metrics registry initialized successfully");
                
                // Create metric labels: node_id and node_type
                char router_id_str[32];
                snprintf(router_id_str, sizeof(router_id_str), "%u", router_id);
                
                metric_label_t labels[2];
                safe_strncpy(labels[0].name, "node_id", MAX_LABEL_NAME_LEN);
                safe_strncpy(labels[0].value, router_id_str, MAX_LABEL_VALUE_LEN);
                safe_strncpy(labels[1].name, "node_type", MAX_LABEL_NAME_LEN);
                safe_strncpy(labels[1].value, "router", MAX_LABEL_VALUE_LEN);
                
                LOG_DEBUG("Creating router metrics with labels: node_id=%s, node_type=router", 
                         router_id_str);
                
                // Create counter metrics
                router->metric_messages_routed_total = metrics_get_or_create_counter(
                    router->metrics_registry,
                    "router_messages_routed_total",
                    "Total number of messages successfully routed to workers",
                    2, labels
                );
                
                router->metric_messages_routed_failed = metrics_get_or_create_counter(
                    router->metrics_registry,
                    "router_messages_routed_failed",
                    "Total number of messages that failed to route",
                    2, labels
                );
                
                // Create gauge metrics
                router->metric_uptime_seconds = metrics_get_or_create_gauge(
                    router->metrics_registry,
                    "uptime_seconds",
                    "Node uptime in seconds",
                    2, labels
                );
                
                // Cluster metrics
                router->metric_cluster_members_total = metrics_get_or_create_gauge(
                    router->metrics_registry,
                    "cluster_members_total",
                    "Total number of cluster members known to this node",
                    2, labels
                );
                
                router->metric_cluster_members_active = metrics_get_or_create_gauge(
                    router->metrics_registry,
                    "cluster_members_active",
                    "Number of active cluster members",
                    2, labels
                );
                
                router->metric_cluster_members_suspect = metrics_get_or_create_gauge(
                    router->metrics_registry,
                    "cluster_members_suspect",
                    "Number of suspected cluster members",
                    2, labels
                );
                
                router->metric_cluster_members_dead = metrics_get_or_create_gauge(
                    router->metrics_registry,
                    "cluster_members_dead",
                    "Number of dead cluster members",
                    2, labels
                );
                
                LOG_INFO("Metrics created successfully, starting HTTP server...");
                
                // Start metrics HTTP server
                router->metrics_server = metrics_server_start(
                    router->metrics_registry,
                    metrics_ip,
                    metrics_port
                );
                
                if (!router->metrics_server) {
                    LOG_ERROR("Failed to start metrics HTTP server on %s:%u", 
                            metrics_ip, metrics_port);
                    LOG_WARN("Continuing without metrics endpoint");
                } else {
                    LOG_INFO("Metrics HTTP server started successfully on http://%s:%u/metrics", 
                            metrics_ip, metrics_port);
                }
            }
        } else {
            LOG_WARN("Metrics disabled: invalid port=%u in config", metrics_port);
        }
    } else {
        LOG_INFO("Metrics disabled: no metrics_addr configured");
        router->metrics_registry = NULL;
        router->metrics_server = NULL;
    }

    LOG_INFO("Router %u initialized (GOSSIP:%u, DATA:%u, INGRESS:%u)",
             router_id, gossip_port, data_port, ingress_port);
    return RESULT_OK;
}

int router_start(router_state_t *router) {
    if (!router) return RESULT_ERR_INVALID;
    
    if (pthread_create(&router->cleanup_thread, NULL, 
                      router_cleanup_thread_fn, router) != 0) {
        LOG_ERROR("Failed to start cleanup thread");
        return RESULT_ERR_INVALID;
    }
    
    LOG_INFO("Router %u started", router->router_id);
    return RESULT_OK;
}

void router_shutdown(router_state_t *router) {
    if (!router) return;
    
    LOG_INFO("Shutting down router %u", router->router_id);
    
    router->shutdown_flag = 1;
    
    pthread_join(router->cleanup_thread, NULL);
    
    if (router->membership) {
        membership_shutdown(router->membership);
    }
    
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
    
    if (dag_validate(dag) != RESULT_OK) {
        LOG_ERROR("DAG validation failed");
        return RESULT_ERR_INVALID;
    }
    
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
    
    int result = dag_catalog_update(&router->dag_catalog, dag);
    
    if (result == RESULT_OK) {
        LOG_INFO("Router %u: Updated DAG %u", router->router_id, dag->dag_id);
    }
    
    return result;
}

int router_remove_dag(router_state_t *router, rule_id_t dag_id) {
    if (!router) return RESULT_ERR_INVALID;
    
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
    
    dag_t *dag = dag_catalog_get(&router->dag_catalog, dag_id);
    if (!dag) {
        LOG_ERROR("DAG %u not found", dag_id);
        return RESULT_ERR_NOTFOUND;
    }
    dag_catalog_release(&router->dag_catalog);
    
    node_id_t worker_id = router_select_worker(router, LOAD_BALANCE_LEAST_LOADED);
    if (worker_id == 0) {
        LOG_ERROR("No available workers");
        return RESULT_ERR_NOTFOUND;
    }
    
    execution_id_t exec_id = execution_tracker_add(&router->exec_tracker, 
                                                   dag_id, worker_id, 
                                                   message, message_len, 3);
    if (exec_id == 0) {
        LOG_ERROR("Failed to create execution record");
        return RESULT_ERR_INVALID;
    }
    
    LOG_INFO("Submitted execution %lu (DAG %u) to worker %u", 
             exec_id, dag_id, worker_id);
    
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

int router_on_worker_join(router_state_t *router, node_id_t worker_id,
                         const char *ip, uint16_t data_port) {
    if (!router || !ip) return RESULT_ERR_INVALID;

    LOG_INFO("Worker %u joined (%s DATA:%u)", worker_id, ip, data_port);// Add to worker pool

    int result = worker_pool_add(&router->worker_pool, worker_id, ip, data_port);
    if (result != RESULT_OK && result != RESULT_ERR_EXISTS) {
        return result;
    }// Establish DATA channel connection
    worker_info_t *worker = worker_pool_get(&router->worker_pool, worker_id);
    if (worker) {
        if (!worker->data_channel) {
            worker->data_channel = safe_malloc(sizeof(rpc_channel_t));
            if (worker->data_channel) {
                if (rpc_client_connect(worker->data_channel, ip, data_port,
                                    RPC_CHANNEL_DATA, 4096) == 0) {
                    LOG_INFO("DATA channel established to worker %u", worker_id);
                } else {
                    LOG_ERROR("Failed to connect DATA channel to worker %u on port %u", worker_id, data_port);
                    safe_free(worker->data_channel);
                    worker->data_channel = NULL;
                }
            } else {
                LOG_ERROR("Failed to allocate DATA channel for worker %u", worker_id);
            }
        }    worker_pool_release(&router->worker_pool);
    }
    return RESULT_OK;
}

int router_on_worker_failed(router_state_t *router, node_id_t worker_id) {
    if (!router) return RESULT_ERR_INVALID;
    LOG_ERROR("Worker %u failed - initiating recovery", worker_id);
    worker_pool_update_status(&router->worker_pool, worker_id, NODE_STATUS_DEAD);
    execution_id_t pending_execs[MAX_PENDING_EXECUTIONS];
    size_t count = execution_tracker_get_by_worker(&router->exec_tracker, 
                                               worker_id, pending_execs, 
                                               MAX_PENDING_EXECUTIONS);
    LOG_INFO("Found %zu pending executions on failed worker %u", count, worker_id);
    for (size_t i = 0; i < count; i++) {
        execution_record_t *rec = execution_tracker_get(&router->exec_tracker, 
                                                        pending_execs[i]);
        if (rec) {
            LOG_INFO("Re-scheduling execution %lu", rec->exec_id);        
            execution_id_t new_exec_id;
            router_submit_message(router, rec->dag_id, 
                                rec->message_data, rec->message_len, 
                                &new_exec_id);        
            execution_tracker_update_status(&router->exec_tracker, 
                                        rec->exec_id, EXEC_STATUS_FAILED);        
            execution_tracker_release(&router->exec_tracker);
        }
    }
    return RESULT_OK;
}

int router_on_execution_update(router_state_t *router, execution_id_t exec_id, execution_status_t status) {
    if (!router || exec_id == 0) 
        return RESULT_ERR_INVALID;
    execution_tracker_update_status(&router->exec_tracker, exec_id, status);
    if (status == EXEC_STATUS_COMPLETED) {
        LOG_INFO("Execution %lu completed successfully", exec_id);
    } else if (status == EXEC_STATUS_FAILED) {
        LOG_ERROR("Execution %lu failed", exec_id);
    }return RESULT_OK;
}


void router_update_cluster_metrics(router_state_t *router) {
    if (!router || !router->cluster_view.members) return;
    
    pthread_rwlock_rdlock(&router->cluster_view.lock);
    
    size_t total = router->cluster_view.count;
    size_t active = 0;
    size_t suspect = 0;
    size_t dead = 0;
    
    for (size_t i = 0; i < total; i++) {
        cluster_member_t *member = &router->cluster_view.members[i];
        
        switch (member->status) {
            case NODE_STATUS_ALIVE:
                active++;
                break;
            case NODE_STATUS_SUSPECT:
                suspect++;
                break;
            case NODE_STATUS_DEAD:
                dead++;
                break;
            default:
                break;
        }
    }
    
    pthread_rwlock_unlock(&router->cluster_view.lock);
    
    // Update metrics
    if (router->metric_cluster_members_total) {
        metrics_gauge_set(router->metric_cluster_members_total, (double)total);
    }
    if (router->metric_cluster_members_active) {
        metrics_gauge_set(router->metric_cluster_members_active, (double)active);
    }
    if (router->metric_cluster_members_suspect) {
        metrics_gauge_set(router->metric_cluster_members_suspect, (double)suspect);
    }
    if (router->metric_cluster_members_dead) {
        metrics_gauge_set(router->metric_cluster_members_dead, (double)dead);
    }
}