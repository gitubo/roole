// include/roole/node.h - CLEANED UP VERSION
// Remove all unified_node_t definitions and old API functions

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

// ============================================================================
// FORWARD DECLARATIONS (for node_state.h)
// ============================================================================

typedef struct node_capabilities node_capabilities_t;
typedef struct peer_pool peer_pool_t;
typedef struct execution_tracker execution_tracker_t;
typedef struct message_queue message_queue_t;
typedef struct membership_handle membership_handle_t;

// ============================================================================
// PEER INFO
// ============================================================================

#define MAX_PEERS 512

typedef struct peer_info {
    node_id_t node_id;
    node_type_t node_type;
    char ip[MAX_IP_LEN];
    uint16_t gossip_port;
    uint16_t data_port;
    node_status_t status;
    
    uint32_t active_executions;
    float load_score;
    uint64_t last_seen_ms;
    
    rpc_channel_t *data_channel;
    
    node_capabilities_t capabilities;
} peer_info_t;

struct peer_pool {
    peer_info_t *peers;
    size_t count;
    size_t capacity;
    pthread_mutex_t lock;
};

// ============================================================================
// MESSAGE QUEUE
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

struct message_queue {
    message_t *messages;
    size_t head;
    size_t tail;
    size_t capacity;
    size_t count;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
};

// ============================================================================
// EXECUTION TRACKER
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

struct execution_tracker {
    execution_record_t *records;
    size_t capacity;
    execution_id_t next_exec_id;
    pthread_rwlock_t lock;
};

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
// METRICS API (node_state_t based)
// ============================================================================

int node_metrics_init_ex(node_state_t *state, const char *metrics_addr);
void node_metrics_shutdown_ex(node_state_t *state);
void node_metrics_update_periodic_ex(node_state_t *state);
void node_metrics_update_cluster_ex(node_state_t *state);

// ============================================================================
// RPC MANAGEMENT API (node_state_t based)
// ============================================================================

rpc_service_entry_t* node_build_rpc_service_table_ex(const node_state_t *state);
void node_free_rpc_service_table(rpc_service_entry_t *table);
int node_start_rpc_servers_ex(node_state_t *state, rpc_service_entry_t *service_table);

// ============================================================================
// EXECUTOR API (node_state_t based)
// ============================================================================

void* node_executor_thread_fn(void *arg);
int node_start_executors_ex(node_state_t *state, size_t num_threads);
void node_stop_executors_ex(node_state_t *state);

// ============================================================================
// CAPABILITY DETECTION API (node_state_t based)
// ============================================================================

void node_detect_capabilities_ex(const roole_config_t *config,
                                 node_capabilities_t *caps,
                                 node_identity_t *identity);

void node_print_capabilities_ex(const node_capabilities_t *caps,
                                const node_identity_t *identity);

// ============================================================================
// BOOTSTRAP API (node_state_t based)
// ============================================================================

int node_bootstrap_from_config_ex(node_state_t *state, 
                                  const roole_config_t *config);
int node_bootstrap_with_retry_ex(node_state_t *state,
                                 const roole_config_t *config,
                                 int max_retries);

#endif // ROOLE_NODE_H