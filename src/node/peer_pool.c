// src/node/peer_pool.c

#define _POSIX_C_SOURCE 200809L

#include "roole/node.h"
#include "roole/common.h"
#include <stdlib.h>
#include <string.h>

// ============================================================================
// PEER POOL IMPLEMENTATION
// ============================================================================

int peer_pool_init(peer_pool_t *pool, size_t capacity) {
    if (!pool || capacity == 0) return RESULT_ERR_INVALID;
    
    memset(pool, 0, sizeof(peer_pool_t));
    
    pool->peers = safe_calloc(capacity, sizeof(peer_info_t));
    if (!pool->peers) {
        return RESULT_ERR_NOMEM;
    }
    
    pool->capacity = capacity;
    pool->count = 0;
    
    if (pthread_mutex_init(&pool->lock, NULL) != 0) {
        safe_free(pool->peers);
        return RESULT_ERR_INVALID;
    }
    
    LOG_INFO("Peer pool initialized (capacity: %zu)", capacity);
    return RESULT_OK;
}

void peer_pool_destroy(peer_pool_t *pool) {
    if (!pool) return;

    pthread_mutex_lock(&pool->lock);

    // Close all RPC channels
    for (size_t i = 0; i < pool->count; i++) {
        if (pool->peers[i].data_channel) {
            rpc_channel_destroy(pool->peers[i].data_channel);
            safe_free(pool->peers[i].data_channel);
        }
    }

    safe_free(pool->peers);
    pool->peers = NULL;
    pool->count = 0;
    pool->capacity = 0;

    pthread_mutex_unlock(&pool->lock);
    pthread_mutex_destroy(&pool->lock);

    LOG_INFO("Peer pool destroyed");
}

int peer_pool_add(peer_pool_t *pool, node_id_t node_id, const char *ip,
                  uint16_t gossip_port, uint16_t data_port) {
    if (!pool || !ip) return RESULT_ERR_INVALID;

    pthread_mutex_lock(&pool->lock);

    // Check if already exists
    for (size_t i = 0; i < pool->count; i++) {
        if (pool->peers[i].node_id == node_id) {
            pthread_mutex_unlock(&pool->lock);
            LOG_DEBUG("Peer %u already in pool", node_id);
            return RESULT_OK;
        }
    }

    // Add new peer
    if (pool->count >= pool->capacity) {
        pthread_mutex_unlock(&pool->lock);
        LOG_ERROR("Peer pool full (capacity: %zu)", pool->capacity);
        return RESULT_ERR_FULL;
    }

    peer_info_t *peer = &pool->peers[pool->count];
    memset(peer, 0, sizeof(peer_info_t));

    peer->node_id = node_id;
    peer->node_type = NODE_TYPE_WORKER;  // Legacy default
    safe_strncpy(peer->ip, ip, MAX_IP_LEN);
    peer->gossip_port = gossip_port;
    peer->data_port = data_port;
    peer->status = NODE_STATUS_ALIVE;
    peer->active_executions = 0;
    peer->load_score = 0.0f;
    peer->last_seen_ms = time_now_ms();
    
    // Initialize as NULL - created on-demand
    peer->data_channel = NULL;
    
    // Default capabilities (detected via gossip later)
    peer->capabilities.has_ingress = 0;
    peer->capabilities.can_execute = 1;  // Assume can execute
    peer->capabilities.can_route = 1;

    pool->count++;

    pthread_mutex_unlock(&pool->lock);

    LOG_INFO("Added peer %u (%s gossip:%u data:%u) to pool",
             node_id, ip, gossip_port, data_port);
    return RESULT_OK;
}

int peer_pool_remove(peer_pool_t *pool, node_id_t node_id) {
    if (!pool) return RESULT_ERR_INVALID;

    pthread_mutex_lock(&pool->lock);

    for (size_t i = 0; i < pool->count; i++) {
        if (pool->peers[i].node_id == node_id) {
            // Close RPC channel
            if (pool->peers[i].data_channel) {
                rpc_channel_destroy(pool->peers[i].data_channel);
                safe_free(pool->peers[i].data_channel);
            }

            // Shift remaining peers
            if (i < pool->count - 1) {
                memmove(&pool->peers[i], &pool->peers[i + 1],
                       (pool->count - i - 1) * sizeof(peer_info_t));
            }

            pool->count--;

            pthread_mutex_unlock(&pool->lock);
            LOG_INFO("Removed peer %u from pool", node_id);
            return RESULT_OK;
        }
    }

    pthread_mutex_unlock(&pool->lock);
    return RESULT_ERR_NOTFOUND;
}

int peer_pool_update_status(peer_pool_t *pool, node_id_t node_id, node_status_t status) {
    if (!pool) return RESULT_ERR_INVALID;
    
    pthread_mutex_lock(&pool->lock);
    
    for (size_t i = 0; i < pool->count; i++) {
        if (pool->peers[i].node_id == node_id) {
            pool->peers[i].status = status;
            pool->peers[i].last_seen_ms = time_now_ms();
            pthread_mutex_unlock(&pool->lock);
            LOG_DEBUG("Peer %u status updated to %d", node_id, status);
            return RESULT_OK;
        }
    }
    
    pthread_mutex_unlock(&pool->lock);
    return RESULT_ERR_NOTFOUND;
}

int peer_pool_update_load(peer_pool_t *pool, node_id_t node_id, 
                          uint32_t active_execs, float load_score) {
    if (!pool) return RESULT_ERR_INVALID;
    
    pthread_mutex_lock(&pool->lock);
    
    for (size_t i = 0; i < pool->count; i++) {
        if (pool->peers[i].node_id == node_id) {
            pool->peers[i].active_executions = active_execs;
            pool->peers[i].load_score = load_score;
            pool->peers[i].last_seen_ms = time_now_ms();
            
            pthread_mutex_unlock(&pool->lock);
            LOG_DEBUG("Peer %u load: %u execs, score %.2f", 
                     node_id, active_execs, load_score);
            return RESULT_OK;
        }
    }
    
    pthread_mutex_unlock(&pool->lock);
    return RESULT_ERR_NOTFOUND;
}

int peer_pool_update_capabilities(peer_pool_t *pool, node_id_t node_id,
                                  const node_capabilities_t *caps) {
    if (!pool || !caps) return RESULT_ERR_INVALID;
    
    pthread_mutex_lock(&pool->lock);
    
    for (size_t i = 0; i < pool->count; i++) {
        if (pool->peers[i].node_id == node_id) {
            pool->peers[i].capabilities = *caps;
            pthread_mutex_unlock(&pool->lock);
            LOG_DEBUG("Peer %u capabilities updated (ingress:%d execute:%d route:%d)",
                     node_id, caps->has_ingress, caps->can_execute, caps->can_route);
            return RESULT_OK;
        }
    }
    
    pthread_mutex_unlock(&pool->lock);
    return RESULT_ERR_NOTFOUND;
}

peer_info_t* peer_pool_get(peer_pool_t *pool, node_id_t node_id) {
    if (!pool) return NULL;
    
    pthread_mutex_lock(&pool->lock);
    
    for (size_t i = 0; i < pool->count; i++) {
        if (pool->peers[i].node_id == node_id) {
            // Return pointer - caller must call peer_pool_release
            return &pool->peers[i];
        }
    }
    
    pthread_mutex_unlock(&pool->lock);
    return NULL;
}

void peer_pool_release(peer_pool_t *pool) {
    if (pool) {
        pthread_mutex_unlock(&pool->lock);
    }
}

size_t peer_pool_list_alive(peer_pool_t *pool, node_id_t *out_peer_ids, size_t max_count) {
    if (!pool || !out_peer_ids || max_count == 0) return 0;
    
    pthread_mutex_lock(&pool->lock);
    
    size_t found = 0;
    for (size_t i = 0; i < pool->count && found < max_count; i++) {
        if (pool->peers[i].status == NODE_STATUS_ALIVE) {
            out_peer_ids[found++] = pool->peers[i].node_id;
        }
    }
    
    pthread_mutex_unlock(&pool->lock);
    
    return found;
}

size_t peer_pool_list_by_capability(peer_pool_t *pool, int can_execute,
                                    node_id_t *out_peer_ids, size_t max_count) {
    if (!pool || !out_peer_ids || max_count == 0) return 0;
    
    pthread_mutex_lock(&pool->lock);
    
    size_t found = 0;
    for (size_t i = 0; i < pool->count && found < max_count; i++) {
        if (pool->peers[i].status == NODE_STATUS_ALIVE &&
            pool->peers[i].capabilities.can_execute == can_execute) {
            out_peer_ids[found++] = pool->peers[i].node_id;
        }
    }
    
    pthread_mutex_unlock(&pool->lock);
    
    return found;
}

// ============================================================================
// LOAD BALANCING
// ============================================================================

node_id_t peer_pool_select_least_loaded(peer_pool_t *pool) {
    if (!pool) return 0;
    
    pthread_mutex_lock(&pool->lock);
    
    node_id_t best_peer = 0;
    float best_score = 1e9f;
    
    for (size_t i = 0; i < pool->count; i++) {
        if (pool->peers[i].status == NODE_STATUS_ALIVE &&
            pool->peers[i].capabilities.can_execute) {
            
            float score = (float)pool->peers[i].active_executions + 
                         pool->peers[i].load_score * 10.0f;
            
            if (score < best_score) {
                best_score = score;
                best_peer = pool->peers[i].node_id;
            }
        }
    }
    
    pthread_mutex_unlock(&pool->lock);
    
    return best_peer;
}

static size_t g_round_robin_index = 0;

node_id_t peer_pool_select_round_robin(peer_pool_t *pool) {
    if (!pool) return 0;
    
    pthread_mutex_lock(&pool->lock);
    
    if (pool->count == 0) {
        pthread_mutex_unlock(&pool->lock);
        return 0;
    }
    
    // Find next alive peer with execution capability
    size_t attempts = 0;
    while (attempts < pool->count) {
        size_t idx = g_round_robin_index % pool->count;
        g_round_robin_index++;
        
        if (pool->peers[idx].status == NODE_STATUS_ALIVE &&
            pool->peers[idx].capabilities.can_execute) {
            node_id_t peer_id = pool->peers[idx].node_id;
            pthread_mutex_unlock(&pool->lock);
            return peer_id;
        }
        
        attempts++;
    }
    
    pthread_mutex_unlock(&pool->lock);
    return 0;  // No alive peers with execution capability
}