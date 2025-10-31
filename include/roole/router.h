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
#include "roole/node.h"  // KEEP THIS
#include <pthread.h>

// ============================================================================
// LEGACY ROUTER STATE (For backward compatibility with old router binary)
// ============================================================================

typedef struct router_state router_state_t;

// Forward declare for legacy compatibility
struct router_state {
    node_id_t router_id;
    char cluster_name[MAX_CONFIG_STRING]; 
    uint16_t gossip_port;
    uint16_t data_port;
    uint16_t ingress_port;
    char bind_addr[MAX_IP_LEN];

    dag_catalog_t dag_catalog;
    peer_pool_t worker_pool;  // FIX: Changed from worker_pool_t
    execution_tracker_t exec_tracker;

    cluster_view_t cluster_view;
    membership_handle_t *membership;
    gossip_engine_t *gossip_engine;

    pthread_t cleanup_thread;

    // Metrics
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
};

// ============================================================================
// RPC HANDLERS (Legacy)
// ============================================================================

// REMOVE: void router_set_rpc_state(router_state_t *router);

extern rpc_service_entry_t router_rpc_service_table[];

// ============================================================================
// ROUTER API (Legacy - For backward compatibility)
// ============================================================================

int router_init(router_state_t *router, node_id_t router_id,
               uint16_t gossip_port, uint16_t data_port, uint16_t ingress_port,
               const char *bind_addr, const char *metrics_addr, const char *cluster_name);
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

void router_update_cluster_metrics(router_state_t *router);

// ============================================================================
// LOAD BALANCING (Legacy)
// ============================================================================

typedef enum {
    LOAD_BALANCE_ROUND_ROBIN = 0,
    LOAD_BALANCE_LEAST_LOADED = 1,
    LOAD_BALANCE_RANDOM = 2
} load_balance_strategy_t;

node_id_t router_select_worker(router_state_t *router, load_balance_strategy_t strategy);

#endif // ROOLE_ROUTER_H