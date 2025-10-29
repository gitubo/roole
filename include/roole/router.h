// include/roole/router.h

#ifndef ROOLE_ROUTER_H
#define ROOLE_ROUTER_H

#include "roole/common.h"
#include "roole/dag.h"
#include "roole/cluster.h"
#include "roole/rpc.h"
#include "roole/gossip.h"
#include "roole/metrics.h"
#include "roole/metrics_server.h"
#include <pthread.h>

// ============================================================================
// WORKER POOL (Router-side worker management)
// ============================================================================

typedef struct router_state router_state_t;

#define MAX_WORKERS 256

typedef struct worker_info {
    node_id_t worker_id;
    char ip[MAX_IP_LEN];
    uint16_t data_port;
    node_status_t status;
    uint32_t active_executions;
    float load_score;
    uint64_t last_seen_ms;

    rpc_channel_t *data_channel;
} worker_info_t;

typedef struct worker_pool {
    worker_info_t *workers;
    size_t count;
    size_t capacity;
    pthread_mutex_t lock;
} worker_pool_t;

int worker_pool_init(worker_pool_t *pool, size_t capacity);
void worker_pool_destroy(worker_pool_t *pool);

int worker_pool_add(worker_pool_t *pool, node_id_t worker_id, const char *ip,
                    uint16_t data_port);
int worker_pool_remove(worker_pool_t *pool, node_id_t worker_id);
int worker_pool_update_status(worker_pool_t *pool, node_id_t worker_id, node_status_t status);
int worker_pool_update_load(worker_pool_t *pool, node_id_t worker_id, 
                            uint32_t active_execs, float load_score);

worker_info_t* worker_pool_get(worker_pool_t *pool, node_id_t worker_id);
void worker_pool_release(worker_pool_t *pool);

node_id_t worker_pool_select_least_loaded(worker_pool_t *pool);
node_id_t worker_pool_select_round_robin(worker_pool_t *pool);

size_t worker_pool_list_alive(worker_pool_t *pool, node_id_t *out_worker_ids, size_t max_count);

// ============================================================================
// RPC HANDLERS
// ============================================================================

void router_set_rpc_state(router_state_t *router);

extern rpc_service_entry_t router_rpc_service_table[];

// ============================================================================
// EXECUTION TRACKER
// ============================================================================

#define MAX_PENDING_EXECUTIONS 10000
#define MAX_MESSAGE_SIZE 4096

typedef enum {
    EXEC_STATUS_PENDING = 0,
    EXEC_STATUS_RUNNING = 1,
    EXEC_STATUS_COMPLETED = 2,
    EXEC_STATUS_FAILED = 3,
    EXEC_STATUS_RETRYING = 4,
    EXEC_STATUS_TIMEOUT = 5
} execution_status_t;

typedef struct execution_record {
    execution_id_t exec_id;
    rule_id_t dag_id;
    node_id_t assigned_worker;
    execution_status_t status;
    
    uint64_t submit_time_ms;
    uint64_t start_time_ms;
    uint64_t complete_time_ms;
    
    uint8_t retry_count;
    uint8_t max_retries;
    
    uint8_t message_data[MAX_MESSAGE_SIZE];
    size_t message_len;
    
    int active;
} execution_record_t;

typedef struct message {
    execution_id_t exec_id;
    rule_id_t dag_id;
    uint8_t message_data[MAX_MESSAGE_SIZE];
    size_t message_len;
    uint64_t received_at_ms;
    node_id_t sender_id;
} message_t;

typedef struct execution_tracker {
    execution_record_t *records;
    size_t capacity;
    execution_id_t next_exec_id;
    pthread_rwlock_t lock;
} execution_tracker_t;

int execution_tracker_init(execution_tracker_t *tracker, size_t capacity);
void execution_tracker_destroy(execution_tracker_t *tracker);

execution_id_t execution_tracker_add(execution_tracker_t *tracker, rule_id_t dag_id,
                                    node_id_t worker_id, const uint8_t *message, 
                                    size_t message_len, uint8_t max_retries);

int execution_tracker_update_status(execution_tracker_t *tracker, execution_id_t exec_id,
                                   execution_status_t status);

execution_record_t* execution_tracker_get(execution_tracker_t *tracker, execution_id_t exec_id);
void execution_tracker_release(execution_tracker_t *tracker);

size_t execution_tracker_get_by_worker(execution_tracker_t *tracker, node_id_t worker_id,
                                      execution_id_t *out_exec_ids, size_t max_count);

int execution_tracker_remove(execution_tracker_t *tracker, execution_id_t exec_id);
size_t execution_tracker_cleanup_completed(execution_tracker_t *tracker);

// ============================================================================
// ROUTER STATE
// ============================================================================

typedef struct router_state {
    node_id_t router_id;
    uint16_t gossip_port;
    uint16_t data_port;
    uint16_t ingress_port;
    char bind_addr[MAX_IP_LEN];

    dag_catalog_t dag_catalog;
    worker_pool_t worker_pool;
    execution_tracker_t exec_tracker;

    cluster_view_t cluster_view;
    membership_handle_t *membership;
    gossip_engine_t *gossip_engine;

    pthread_t cleanup_thread;

    // NEW: Metrics
    metrics_registry_t *metrics_registry;
    metrics_server_t *metrics_server;
    
    metrics_t *metric_messages_routed_total;
    metrics_t *metric_messages_routed_failed;
    metrics_t *metric_uptime_seconds;
    metrics_t *metric_cluster_members_total;
    metrics_t *metric_cluster_members_active;
    metrics_t *metric_cluster_members_suspect;
    metrics_t *metric_cluster_members_dead;
    
    uint64_t start_time_ms;

    int shutdown_flag;
} router_state_t;

// ============================================================================
// ROUTER API
// ============================================================================

int router_init(router_state_t *router, node_id_t router_id,
               uint16_t gossip_port, uint16_t data_port, uint16_t ingress_port,
               const char *bind_addr);
int router_start(router_state_t *router);
void router_shutdown(router_state_t *router);

int router_add_dag(router_state_t *router, const dag_t *dag);
int router_update_dag(router_state_t *router, const dag_t *dag);
int router_remove_dag(router_state_t *router, rule_id_t dag_id);
dag_t* router_get_dag(router_state_t *router, rule_id_t dag_id);

int router_submit_message(router_state_t *router, rule_id_t dag_id,
                         const uint8_t *message, size_t message_len,
                         execution_id_t *out_exec_id);

int router_get_execution_status(router_state_t *router, execution_id_t exec_id,
                               execution_status_t *out_status);

int router_on_worker_join(router_state_t *router, node_id_t worker_id,
                         const char *ip, uint16_t data_port);
int router_on_worker_failed(router_state_t *router, node_id_t worker_id);

int router_on_execution_update(router_state_t *router, execution_id_t exec_id,
                              execution_status_t status);

// ============================================================================
// METRICS HELPERS
// ============================================================================

void router_update_cluster_metrics(router_state_t *router);

// ============================================================================
// LOAD BALANCING STRATEGIES
// ============================================================================

typedef enum {
    LOAD_BALANCE_ROUND_ROBIN = 0,
    LOAD_BALANCE_LEAST_LOADED = 1,
    LOAD_BALANCE_RANDOM = 2
} load_balance_strategy_t;

node_id_t router_select_worker(router_state_t *router, load_balance_strategy_t strategy);

#endif // ROOLE_ROUTER_H