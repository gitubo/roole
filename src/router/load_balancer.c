// src/router/load_balancer.c - Legacy wrapper for backward compatibility

#define _POSIX_C_SOURCE 200809L

#include "roole/router.h"
#include "roole/node.h"
#include "roole/common.h"
#include <string.h>
#include <stdlib.h>

// ============================================================================
// LEGACY WRAPPERS (Redirect to peer_pool functions)
// ============================================================================

node_id_t router_select_worker(router_state_t *router, load_balance_strategy_t strategy) {
    if (!router) return 0;
    
    switch (strategy) {
        case LOAD_BALANCE_LEAST_LOADED:
            return peer_pool_select_least_loaded(&router->worker_pool);
        
        case LOAD_BALANCE_ROUND_ROBIN:
            return peer_pool_select_round_robin(&router->worker_pool);
        
        case LOAD_BALANCE_RANDOM: {
            node_id_t alive_peers[MAX_PEERS];
            size_t count = peer_pool_list_alive(&router->worker_pool, 
                                               alive_peers, MAX_PEERS);
            if (count == 0) return 0;
            
            size_t idx = rand() % count;
            return alive_peers[idx];
        }
        
        default:
            return peer_pool_select_least_loaded(&router->worker_pool);
    }
}