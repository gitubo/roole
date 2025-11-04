#ifndef ROOLE_NODE_STATE_H
#define ROOLE_NODE_STATE_H

#include "roole/common.h"
#include "roole/config.h"
#include "roole/cluster.h"
#include "roole/dag.h"
#include "roole/metrics.h"
#include "roole/event_bus.h"
#include "roole/node.h"
#include <pthread.h>

// ============================================================================
// NODE IDENTITY (Immutable after initialization)
// ============================================================================

typedef struct node_identity {
    node_id_t node_id;
    node_type_t node_type;           // Legacy field
    char cluster_name[64];
    char bind_addr[MAX_IP_LEN];
    uint16_t gossip_port;
    uint16_t data_port;
    uint16_t ingress_port;           // 0 if disabled
    uint16_t metrics_port;           // 0 if disabled
} node_identity_t;

// ============================================================================
// NODE CAPABILITIES (Derived from configuration)
// ============================================================================

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
    message_queue_t *message_queue;        // NULL if !can_execute
    
    // Cluster membership (owns the view)
    cluster_view_t *cluster_view;          // Authoritative cluster state
    membership_handle_t *membership;       // Gossip engine wrapper
    
    // Observability
    metrics_registry_t *metrics_registry;
    metrics_server_t *metrics_server;
    event_bus_t *event_bus;
    
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
    
} node_state_t;

// ============================================================================
// LIFECYCLE API
// ============================================================================

/**
 * Initialize node state from configuration
 * All subsystems are initialized but not started
 */
result_t node_state_init(node_state_t **state, const roole_config_t *config,
                         size_t num_executor_threads);

/**
 * Start all background threads and RPC servers
 */
result_t node_state_start(node_state_t *state);

/**
 * Bootstrap from seed routers
 */
result_t node_state_bootstrap(node_state_t *state, const roole_config_t *config);

/**
 * Graceful shutdown (blocking until complete)
 */
void node_state_shutdown(node_state_t *state);

/**
 * Destroy and free all resources
 */
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