// include/roole/node.h

#ifndef ROOLE_NODE_H
#define ROOLE_NODE_H

#include "roole/common.h"
#include "roole/config.h"
#include "roole/dag.h"
#include "roole/cluster.h"
#include "roole/rpc.h"
#include "roole/gossip.h"
#include "roole/metrics.h"
#include "roole/metrics_server.h"
#include "roole/node_state.h"
#include "roole/event_bus.h"
#include <pthread.h>

#define MAX_WORKERS 256  // Legacy compatibility
#define MAX_WORKER_QUEUE_SIZE 1000  // Legacy compatibility

// ============================================================================
// NODE CAPABILITIES (Configuration-driven flags)
// ============================================================================

typedef struct node_capabilities {
    int has_ingress;        // Accepts external client requests
    int can_execute;        // Processes messages (runs executor threads)
    int can_route;          // Routes messages to other nodes
} node_capabilities_t;

// ============================================================================
// PEER INFO (Replaces worker_info_t - tracks all cluster nodes)
// ============================================================================

#define MAX_PEERS 512

typedef struct peer_info {
    node_id_t node_id;
    node_type_t node_type;  // Legacy - will be deprecated
    char ip[MAX_IP_LEN];
    uint16_t gossip_port;
    uint16_t data_port;
    node_status_t status;
    
    uint32_t active_executions;
    float load_score;
    uint64_t last_seen_ms;
    
    rpc_channel_t *data_channel;
    
    node_capabilities_t capabilities;  // Peer's capabilities
} peer_info_t;

typedef struct peer_pool {
    peer_info_t *peers;
    size_t count;
    size_t capacity;
    pthread_mutex_t lock;
} peer_pool_t;

// ============================================================================
// MESSAGE QUEUE (Unified for all nodes)
// ============================================================================

#define MAX_NODE_QUEUE_SIZE 1000
#define MAX_MESSAGE_SIZE 4096

typedef struct message {
    execution_id_t exec_id;
    rule_id_t dag_id;
    uint8_t message_data[MAX_MESSAGE_SIZE];
    size_t message_len;
    uint64_t received_at_ms;
    node_id_t sender_id;
} message_t;

typedef struct message_queue {
    message_t *messages;
    size_t head;
    size_t tail;
    size_t capacity;
    size_t count;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} message_queue_t;

// ============================================================================
// EXECUTION TRACKER (All nodes track executions)
// ============================================================================

#define MAX_PENDING_EXECUTIONS 10000

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
    node_id_t assigned_peer;
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

typedef struct execution_tracker {
    execution_record_t *records;
    size_t capacity;
    execution_id_t next_exec_id;
    pthread_rwlock_t lock;
} execution_tracker_t;

// ============================================================================
// UNIFIED NODE STATE
// ============================================================================

typedef struct unified_node {
    // Identity
    node_id_t node_id;
    node_type_t node_type;
    char cluster_name[MAX_CONFIG_STRING];
    char bind_addr[MAX_IP_LEN];
    
    // Ports
    uint16_t gossip_port;
    uint16_t data_port;
    uint16_t ingress_port;  // 0 if not enabled
    uint16_t metrics_port;  // 0 if not enabled
    
    // Capabilities
    node_capabilities_t capabilities;
    
    // DAG catalog (all nodes maintain)
    dag_catalog_t dag_catalog;
    uint64_t catalog_version;
    
    // Execution tracking
    execution_tracker_t exec_tracker;
    
    // Message processing
    message_queue_t message_queue;
    uint32_t active_executions;
    
    // Peer management
    peer_pool_t peer_pool;
    
    // Cluster membership
    cluster_view_t cluster_view;
    membership_handle_t *membership;
    gossip_engine_t *gossip_engine;
    
    // Metrics
    metrics_registry_t *metrics_registry;
    metrics_server_t *metrics_server;
    
    metrics_t *metric_messages_processed;
    metrics_t *metric_messages_failed;
    metrics_t *metric_messages_routed;
    metrics_t *metric_queue_size;
    metrics_t *metric_active_executions;
    metrics_t *metric_uptime_seconds;
    
    metrics_t *metric_cluster_members_total;
    metrics_t *metric_cluster_members_active;
    metrics_t *metric_cluster_members_suspect;
    metrics_t *metric_cluster_members_dead;

    histogram_metric_t *histogram_exec_duration;
    histogram_metric_t *histogram_queue_wait;
    histogram_metric_t *histogram_message_size;
    histogram_metric_t *histogram_gossip_rtt;

    pthread_t metrics_update_thread;
    
    // Executor threads
    pthread_t *executor_threads;
    size_t num_executor_threads;
    
    // Background threads
    pthread_t cleanup_thread;
    
    uint64_t start_time_ms;
    int shutdown_flag;
    
} unified_node_t;

// ============================================================================
// PEER POOL API
// ============================================================================

int peer_pool_init(peer_pool_t *pool, size_t capacity);
void peer_pool_destroy(peer_pool_t *pool);
int peer_pool_add(peer_pool_t *pool, node_id_t node_id, const char *ip,
                  uint16_t gossip_port, uint16_t data_port);
int peer_pool_remove(peer_pool_t *pool, node_id_t node_id);
int peer_pool_update_status(peer_pool_t *pool, node_id_t node_id, node_status_t status);
peer_info_t* peer_pool_get(peer_pool_t *pool, node_id_t node_id);
void peer_pool_release(peer_pool_t *pool);
node_id_t peer_pool_select_least_loaded(peer_pool_t *pool);
int peer_pool_update_load(peer_pool_t *pool, node_id_t node_id, 
                          uint32_t active_execs, float load_score);
int peer_pool_update_capabilities(peer_pool_t *pool, node_id_t node_id,
                                  const node_capabilities_t *caps);
size_t peer_pool_list_alive(peer_pool_t *pool, node_id_t *out_peer_ids, size_t max_count);
size_t peer_pool_list_by_capability(peer_pool_t *pool, int can_execute,
                                    node_id_t *out_peer_ids, size_t max_count);
node_id_t peer_pool_select_round_robin(peer_pool_t *pool);

// ============================================================================
// MESSAGE QUEUE API
// ============================================================================

int message_queue_init(message_queue_t *queue, size_t capacity);
void message_queue_destroy(message_queue_t *queue);
int message_queue_push(message_queue_t *queue, const message_t *message);
int message_queue_pop(message_queue_t *queue, message_t *out_message, int timeout_ms);
size_t message_queue_size(message_queue_t *queue);
int message_queue_is_empty(message_queue_t *queue);

// ============================================================================
// EXECUTION TRACKER API
// ============================================================================

int execution_tracker_init(execution_tracker_t *tracker, size_t capacity);
void execution_tracker_destroy(execution_tracker_t *tracker);
execution_id_t execution_tracker_add(execution_tracker_t *tracker, rule_id_t dag_id,
                                    node_id_t peer_id, const uint8_t *message, 
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
// METRICS API
// ============================================================================

int node_metrics_init(unified_node_t *node, const char *metrics_addr);
void node_metrics_shutdown(unified_node_t *node);
void node_metrics_update_periodic(unified_node_t *node);
void node_metrics_update_cluster(unified_node_t *node);

// ============================================================================
// RPC MANAGEMENT API
// ============================================================================

void node_set_rpc_state(unified_node_t *node);
unified_node_t* node_get_rpc_state(void);

rpc_service_entry_t* node_build_rpc_service_table(const unified_node_t *node);
void node_free_rpc_service_table(rpc_service_entry_t *table);

int node_start_rpc_servers(unified_node_t *node, rpc_service_entry_t *service_table);

// ============================================================================
// EXECUTOR API
// ============================================================================

void* node_executor_thread_fn(void *arg);
int node_start_executors(unified_node_t *node);
void node_stop_executors(unified_node_t *node);

// ============================================================================
// CAPABILITY DETECTION API
// ============================================================================

void node_detect_capabilities(unified_node_t *node, const roole_config_t *config);
void node_print_capabilities(const unified_node_t *node);

// ============================================================================
// UNIFIED NODE LIFECYCLE API
// ============================================================================

int node_init(unified_node_t *node, const roole_config_t *config, 
              size_t num_executor_threads);
int node_start(unified_node_t *node);
void node_shutdown(unified_node_t *node);

// ============================================================================
// BOOTSTRAP API
// ============================================================================

int node_bootstrap_from_config(unified_node_t *node, const roole_config_t *config);
int node_bootstrap_with_retry(unified_node_t *node, const roole_config_t *config, 
                              int max_retries);

// ============================================================================
// NEW API: node_state_t-based functions (ADD TO END OF include/roole/node.h)
// ============================================================================

// From node_capabilities.c
void node_detect_capabilities_ex(const roole_config_t *config,
                                 node_capabilities_t *caps,
                                 node_identity_t *identity);

void node_print_capabilities_ex(const node_capabilities_t *caps,
                                const node_identity_t *identity);

// From node_bootstrap.c (forward declare node_state_t)
struct node_state;
int node_bootstrap_from_config_ex(struct node_state *state, 
                                  const roole_config_t *config);
int node_bootstrap_with_retry_ex(struct node_state *state,
                                 const roole_config_t *config,
                                 int max_retries);

// From node_rpc.c
rpc_service_entry_t* node_build_rpc_service_table_ex(const struct node_state *state);
int node_start_rpc_servers_ex(struct node_state *state, 
                              rpc_service_entry_t *service_table);

// From node_executor.c
int node_start_executors_ex(struct node_state *state, size_t num_threads);
void node_stop_executors_ex(struct node_state *state);

// From node_metrics.c
int node_metrics_init_ex(struct node_state *state, const char *metrics_addr);
void node_metrics_shutdown_ex(struct node_state *state);
void node_metrics_update_periodic_ex(struct node_state *state);
void node_metrics_update_cluster_ex(struct node_state *state);

#endif // ROOLE_NODE_H