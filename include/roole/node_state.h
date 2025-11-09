// include/roole/node_state.h - COMPLETE with metrics support

#ifndef ROOLE_NODE_STATE_H
#define ROOLE_NODE_STATE_H

#include "roole/common.h"
#include "roole/config.h"
#include "roole/cluster.h"
#include "roole/dag.h"
#include "roole/metrics_server.h"
#include "roole/metrics.h"
#include "roole/event_bus.h"
//#include "roole/node.h"
#include <pthread.h>

// ============================================================================
// FORWARD DECLARATIONS (to break circular dependency)
// ============================================================================

typedef struct node_capabilities node_capabilities_t;
typedef struct peer_pool peer_pool_t;
typedef struct execution_tracker execution_tracker_t;
typedef struct message_queue message_queue_t;
typedef struct membership_handle membership_handle_t;

// ============================================================================
// NODE IDENTITY (Immutable after initialization)
// ============================================================================

typedef struct node_identity {
    node_id_t node_id;
    node_type_t node_type;
    char cluster_name[64];
    char bind_addr[MAX_IP_LEN];
    uint16_t gossip_port;
    uint16_t data_port;
    uint16_t ingress_port;
    uint16_t metrics_port;
} node_identity_t;

typedef struct node_capabilities {
    int has_ingress;        // Accepts external client requests
    int can_execute;        // Processes messages (runs executor threads)
    int can_route;          // Routes messages to other nodes
} node_capabilities_t;

// ============================================================================
// NODE STATE (Single source of truth)
// ============================================================================

typedef struct node_state {
    // Identity (immutable)
    node_identity_t identity;
    node_capabilities_t capabilities;
    
    // Owned subsystems (node_state has exclusive ownership)
    dag_catalog_t *dag_catalog;
    peer_pool_t *peer_pool;
    execution_tracker_t *exec_tracker;
    message_queue_t *message_queue;
    
    // Cluster membership (owns the view)
    cluster_view_t *cluster_view;
    membership_handle_t *membership;
    
    // Observability
    metrics_registry_t *metrics_registry;
    metrics_server_t *metrics_server;
    event_bus_t *event_bus;
    
    // Metrics references (for fast access)
    metrics_t *metric_cluster_members_total;
    metrics_t *metric_cluster_members_active;
    metrics_t *metric_cluster_members_suspect;
    metrics_t *metric_cluster_members_dead;
    metrics_t *metric_messages_processed;
    metrics_t *metric_messages_failed;
    metrics_t *metric_messages_routed;
    metrics_t *metric_queue_size;
    metrics_t *metric_active_executions;
    metrics_t *metric_uptime_seconds;
    metrics_t *metric_dag_catalog_size;
    
    histogram_metric_t *histogram_exec_duration;
    histogram_metric_t *histogram_queue_wait;
    histogram_metric_t *histogram_message_size;
    histogram_metric_t *histogram_gossip_rtt;

    // Lifecycle
    uint64_t start_time_ms;
    volatile int shutdown_flag;
    
    // Worker threads
    pthread_t *executor_threads;
    size_t num_executor_threads;
    pthread_t cleanup_thread;
    pthread_t metrics_update_thread;
    
    // Statistics (atomic counters)
    _Atomic uint32_t active_executions;
    _Atomic uint64_t messages_processed;
    _Atomic uint64_t messages_failed;
    _Atomic uint64_t messages_routed;

    // Synchronization for startup
    volatile int rpc_server_ready;
    pthread_mutex_t rpc_ready_lock;
    pthread_cond_t rpc_ready_cond;
    
} node_state_t;

// ============================================================================
// LIFECYCLE API
// ============================================================================

result_t node_state_init(node_state_t **state, const roole_config_t *config,
                         size_t num_executor_threads);

result_t node_state_start(node_state_t *state);

result_t node_state_bootstrap(node_state_t *state, const roole_config_t *config);

void node_state_shutdown(node_state_t *state);

void node_state_destroy(node_state_t *state);

// ============================================================================
// ACCESSORS (Read-only access to internal state)
// ============================================================================

const node_identity_t* node_state_get_identity(const node_state_t *state);
const node_capabilities_t* node_state_get_capabilities(const node_state_t *state);

dag_catalog_t* node_state_get_dag_catalog(node_state_t *state);
peer_pool_t* node_state_get_peer_pool(node_state_t *state);
execution_tracker_t* node_state_get_exec_tracker(node_state_t *state);
message_queue_t* node_state_get_message_queue(node_state_t *state);
cluster_view_t* node_state_get_cluster_view(node_state_t *state);

metrics_registry_t* node_state_get_metrics(node_state_t *state);
event_bus_t* node_state_get_event_bus(node_state_t *state);

// ============================================================================
// STATISTICS
// ============================================================================

typedef struct node_statistics {
    uint64_t uptime_ms;
    uint32_t active_executions;
    uint64_t messages_processed;
    uint64_t messages_failed;
    uint64_t messages_routed;
    size_t queue_depth;
    size_t cluster_size;
} node_statistics_t;

void node_state_get_statistics(const node_state_t *state, node_statistics_t *stats);

#endif // ROOLE_NODE_STATE_H