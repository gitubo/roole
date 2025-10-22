// include/roole/router.h

#ifndef ROOLE_ROUTER_H
#define ROOLE_ROUTER_H

#include "roole/common.h"
#include "roole/dag.h"
#include "roole/cluster.h"
#include "roole/rpc.h"
#include <pthread.h>

// ============================================================================
// WORKER POOL (Router-side worker management)
// ============================================================================

typedef struct router_state router_state_t;

#define MAX_WORKERS 256

typedef struct worker_info {
    node_id_t worker_id;
    char ip[MAX_IP_LEN];
    uint16_t port;
    node_status_t status;
    uint32_t active_executions;
    float load_score;  // For load balancing (0.0 = idle, 1.0 = fully loaded)
    uint64_t last_heartbeat_ms;
    rpc_channel_t *rpc_channel;
} worker_info_t;

typedef struct worker_pool {
    worker_info_t *workers;
    size_t count;
    size_t capacity;
    pthread_mutex_t lock;
} worker_pool_t;

int worker_pool_init(worker_pool_t *pool, size_t capacity);
void worker_pool_destroy(worker_pool_t *pool);

int worker_pool_add(worker_pool_t *pool, node_id_t worker_id, const char *ip, uint16_t port);
int worker_pool_remove(worker_pool_t *pool, node_id_t worker_id);
int worker_pool_update_status(worker_pool_t *pool, node_id_t worker_id, node_status_t status);
int worker_pool_update_load(worker_pool_t *pool, node_id_t worker_id, 
                            uint32_t active_execs, float load_score);

worker_info_t* worker_pool_get(worker_pool_t *pool, node_id_t worker_id);
void worker_pool_release(worker_pool_t *pool);

// Load balancing
node_id_t worker_pool_select_least_loaded(worker_pool_t *pool);
node_id_t worker_pool_select_round_robin(worker_pool_t *pool);

// Query
size_t worker_pool_list_alive(worker_pool_t *pool, node_id_t *out_worker_ids, size_t max_count);

// ============================================================================
// RPC HANDLERS
// ============================================================================

// Set global router state for RPC handlers
void router_set_rpc_state(router_state_t *router);

// RPC service table for router
extern rpc_service_entry_t router_rpc_service_table[];

// ============================================================================
// EXECUTION TRACKER (Track active executions on workers)
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
    dag_id_t dag_id;
    node_id_t assigned_worker;
    execution_status_t status;
    
    uint64_t submit_time_ms;
    uint64_t start_time_ms;
    uint64_t complete_time_ms;
    
    uint8_t retry_count;
    uint8_t max_retries;
    
    // Original message for retry
    uint8_t message_data[MAX_MESSAGE_SIZE];
    size_t message_len;
    
    int active;  // 1 if tracking, 0 if slot free
} execution_record_t;

typedef struct execution_tracker {
    execution_record_t *records;
    size_t capacity;
    execution_id_t next_exec_id;
    pthread_rwlock_t lock;
} execution_tracker_t;

int execution_tracker_init(execution_tracker_t *tracker, size_t capacity);
void execution_tracker_destroy(execution_tracker_t *tracker);

// Add new execution
execution_id_t execution_tracker_add(execution_tracker_t *tracker, dag_id_t dag_id,
                                    node_id_t worker_id, const uint8_t *message, 
                                    size_t message_len, uint8_t max_retries);

// Update execution status
int execution_tracker_update_status(execution_tracker_t *tracker, execution_id_t exec_id,
                                   execution_status_t status);

// Query
execution_record_t* execution_tracker_get(execution_tracker_t *tracker, execution_id_t exec_id);
void execution_tracker_release(execution_tracker_t *tracker);

// Get all executions on a specific worker
size_t execution_tracker_get_by_worker(execution_tracker_t *tracker, node_id_t worker_id,
                                      execution_id_t *out_exec_ids, size_t max_count);

// Remove completed/failed executions (cleanup)
int execution_tracker_remove(execution_tracker_t *tracker, execution_id_t exec_id);
size_t execution_tracker_cleanup_completed(execution_tracker_t *tracker);

// ============================================================================
// ROUTER STATE
// ============================================================================

typedef struct router_state {
    node_id_t router_id;
    uint16_t port;
    
    // DAG catalog (replicated via Raft consensus)
    dag_catalog_t dag_catalog;
    
    // Worker management
    worker_pool_t worker_pool;
    execution_tracker_t exec_tracker;
    
    // Cluster membership
    cluster_view_t cluster_view;
    membership_handle_t *membership;
    
    // Heartbeat tracking for workers
    heartbeat_tracker_t *heartbeat_tracker;
    
    // Consensus (for DAG catalog sync - optional, placeholder for Raft)
    // raft_handle_t *raft;
    
    // Background threads
    pthread_t heartbeat_thread;
    pthread_t recovery_thread;
    pthread_t cleanup_thread;
    
    int shutdown_flag;
} router_state_t;

// ============================================================================
// ROUTER API
// ============================================================================

// Initialization
int router_init(router_state_t *router, node_id_t router_id, uint16_t port);
int router_start(router_state_t *router);
void router_shutdown(router_state_t *router);

// DAG management (triggers consensus among routers)
int router_add_dag(router_state_t *router, const dag_t *dag);
int router_update_dag(router_state_t *router, const dag_t *dag);
int router_remove_dag(router_state_t *router, dag_id_t dag_id);
dag_t* router_get_dag(router_state_t *router, dag_id_t dag_id);

// Message submission & execution
int router_submit_message(router_state_t *router, dag_id_t dag_id,
                         const uint8_t *message, size_t message_len,
                         execution_id_t *out_exec_id);

int router_get_execution_status(router_state_t *router, execution_id_t exec_id,
                               execution_status_t *out_status);

// Worker management (called by membership callbacks)
int router_on_worker_join(router_state_t *router, node_id_t worker_id, 
                         const char *ip, uint16_t port);
int router_on_worker_failed(router_state_t *router, node_id_t worker_id);

// Worker heartbeat/status update (called by RPC handlers)
int router_on_worker_heartbeat(router_state_t *router, node_id_t worker_id,
                              uint32_t active_execs, float load);

// Execution status update (called by RPC handlers)
int router_on_execution_update(router_state_t *router, execution_id_t exec_id,
                              execution_status_t status);

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