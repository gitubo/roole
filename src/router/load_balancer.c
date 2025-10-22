// src/router/load_balancer.c

#define _POSIX_C_SOURCE 200809L

#include "roole/router.h"
#include "roole/common.h"
#include <string.h>
#include <stdlib.h>

// ============================================================================
// WORKER POOL IMPLEMENTATION
// ============================================================================

int worker_pool_init(worker_pool_t *pool, size_t capacity) {
    if (!pool || capacity == 0) return ROOLE_ERR_INVALID;
    
    memset(pool, 0, sizeof(worker_pool_t));
    
    pool->workers = roole_calloc(capacity, sizeof(worker_info_t));
    if (!pool->workers) {
        return ROOLE_ERR_NOMEM;
    }
    
    pool->capacity = capacity;
    pool->count = 0;
    
    if (pthread_mutex_init(&pool->lock, NULL) != 0) {
        roole_free(pool->workers);
        return ROOLE_ERR_INVALID;
    }
    
    ROOLE_LOG_INFO("Worker pool initialized (capacity: %zu)", capacity);
    return ROOLE_OK;
}

void worker_pool_destroy(worker_pool_t *pool) {
    if (!pool) return;

    pthread_mutex_lock(&pool->lock);

    // Close all RPC channels (both service and data)
    for (size_t i = 0; i < pool->count; i++) {
        if (pool->workers[i].service_channel) {
            rpc_channel_destroy(pool->workers[i].service_channel);
            roole_free(pool->workers[i].service_channel);
        }
        if (pool->workers[i].data_channel) {
            rpc_channel_destroy(pool->workers[i].data_channel);
            roole_free(pool->workers[i].data_channel);
        }
    }

    roole_free(pool->workers);
    pool->workers = NULL;
    pool->count = 0;
    pool->capacity = 0;

    pthread_mutex_unlock(&pool->lock);
    pthread_mutex_destroy(&pool->lock);

    ROOLE_LOG_INFO("Worker pool destroyed");
}

int worker_pool_add(worker_pool_t *pool, node_id_t worker_id, const char *ip,
                    uint16_t service_port, uint16_t data_port) {
    if (!pool || !ip) return ROOLE_ERR_INVALID;

    pthread_mutex_lock(&pool->lock);

    // Check if already exists
    for (size_t i = 0; i < pool->count; i++) {
        if (pool->workers[i].worker_id == worker_id) {
            pthread_mutex_unlock(&pool->lock);
            ROOLE_LOG_DEBUG("Worker %u already in pool", worker_id);
            return ROOLE_OK;
        }
    }

    // Add new worker
    if (pool->count >= pool->capacity) {
        pthread_mutex_unlock(&pool->lock);
        ROOLE_LOG_ERROR("Worker pool full (capacity: %zu)", pool->capacity);
        return ROOLE_ERR_FULL;
    }

    worker_info_t *worker = &pool->workers[pool->count];
    memset(worker, 0, sizeof(worker_info_t));

    worker->worker_id = worker_id;
    roole_strncpy_safe(worker->ip, ip, MAX_IP_LEN);
    worker->service_port = service_port;
    worker->data_port = data_port;
    worker->status = NODE_STATUS_ALIVE;
    worker->active_executions = 0;
    worker->load_score = 0.0f;
    worker->last_heartbeat_ms = roole_time_now_ms();
    worker->service_channel = NULL;  // Will be initialized when needed
    worker->data_channel = NULL;     // Will be initialized when needed

    pool->count++;

    pthread_mutex_unlock(&pool->lock);

    ROOLE_LOG_INFO("Added worker %u (%s SERVICE:%u DATA:%u) to pool",
                   worker_id, ip, service_port, data_port);
    return ROOLE_OK;
}

int worker_pool_remove(worker_pool_t *pool, node_id_t worker_id) {
    if (!pool) return ROOLE_ERR_INVALID;

    pthread_mutex_lock(&pool->lock);

    for (size_t i = 0; i < pool->count; i++) {
        if (pool->workers[i].worker_id == worker_id) {
            // Close RPC channels (both service and data)
            if (pool->workers[i].service_channel) {
                rpc_channel_destroy(pool->workers[i].service_channel);
                roole_free(pool->workers[i].service_channel);
            }
            if (pool->workers[i].data_channel) {
                rpc_channel_destroy(pool->workers[i].data_channel);
                roole_free(pool->workers[i].data_channel);
            }

            // Shift remaining workers
            if (i < pool->count - 1) {
                memmove(&pool->workers[i], &pool->workers[i + 1],
                       (pool->count - i - 1) * sizeof(worker_info_t));
            }

            pool->count--;

            pthread_mutex_unlock(&pool->lock);
            ROOLE_LOG_INFO("Removed worker %u from pool", worker_id);
            return ROOLE_OK;
        }
    }

    pthread_mutex_unlock(&pool->lock);
    return ROOLE_ERR_NOTFOUND;
}

int worker_pool_update_status(worker_pool_t *pool, node_id_t worker_id, node_status_t status) {
    if (!pool) return ROOLE_ERR_INVALID;
    
    pthread_mutex_lock(&pool->lock);
    
    for (size_t i = 0; i < pool->count; i++) {
        if (pool->workers[i].worker_id == worker_id) {
            pool->workers[i].status = status;
            pthread_mutex_unlock(&pool->lock);
            ROOLE_LOG_DEBUG("Worker %u status updated to %d", worker_id, status);
            return ROOLE_OK;
        }
    }
    
    pthread_mutex_unlock(&pool->lock);
    return ROOLE_ERR_NOTFOUND;
}

int worker_pool_update_load(worker_pool_t *pool, node_id_t worker_id, 
                            uint32_t active_execs, float load_score) {
    if (!pool) return ROOLE_ERR_INVALID;
    
    pthread_mutex_lock(&pool->lock);
    
    for (size_t i = 0; i < pool->count; i++) {
        if (pool->workers[i].worker_id == worker_id) {
            pool->workers[i].active_executions = active_execs;
            pool->workers[i].load_score = load_score;
            pool->workers[i].last_heartbeat_ms = roole_time_now_ms();
            
            pthread_mutex_unlock(&pool->lock);
            ROOLE_LOG_DEBUG("Worker %u load: %u execs, score %.2f", 
                           worker_id, active_execs, load_score);
            return ROOLE_OK;
        }
    }
    
    pthread_mutex_unlock(&pool->lock);
    return ROOLE_ERR_NOTFOUND;
}

worker_info_t* worker_pool_get(worker_pool_t *pool, node_id_t worker_id) {
    if (!pool) return NULL;
    
    pthread_mutex_lock(&pool->lock);
    
    for (size_t i = 0; i < pool->count; i++) {
        if (pool->workers[i].worker_id == worker_id) {
            // Return pointer - caller must call worker_pool_release
            return &pool->workers[i];
        }
    }
    
    pthread_mutex_unlock(&pool->lock);
    return NULL;
}

void worker_pool_release(worker_pool_t *pool) {
    if (pool) {
        pthread_mutex_unlock(&pool->lock);
    }
}

size_t worker_pool_list_alive(worker_pool_t *pool, node_id_t *out_worker_ids, size_t max_count) {
    if (!pool || !out_worker_ids || max_count == 0) return 0;
    
    pthread_mutex_lock(&pool->lock);
    
    size_t found = 0;
    for (size_t i = 0; i < pool->count && found < max_count; i++) {
        if (pool->workers[i].status == NODE_STATUS_ALIVE) {
            out_worker_ids[found++] = pool->workers[i].worker_id;
        }
    }
    
    pthread_mutex_unlock(&pool->lock);
    
    return found;
}

// ============================================================================
// LOAD BALANCING ALGORITHMS
// ============================================================================

node_id_t worker_pool_select_least_loaded(worker_pool_t *pool) {
    if (!pool) return 0;
    
    pthread_mutex_lock(&pool->lock);
    
    node_id_t best_worker = 0;
    float best_score = 1e9f;
    
    for (size_t i = 0; i < pool->count; i++) {
        if (pool->workers[i].status == NODE_STATUS_ALIVE) {
            // Combined score: active executions + load score
            float score = (float)pool->workers[i].active_executions + 
                         pool->workers[i].load_score * 10.0f;
            
            if (score < best_score) {
                best_score = score;
                best_worker = pool->workers[i].worker_id;
            }
        }
    }
    
    pthread_mutex_unlock(&pool->lock);
    
    return best_worker;
}

static size_t g_round_robin_index = 0;

node_id_t worker_pool_select_round_robin(worker_pool_t *pool) {
    if (!pool) return 0;
    
    pthread_mutex_lock(&pool->lock);
    
    if (pool->count == 0) {
        pthread_mutex_unlock(&pool->lock);
        return 0;
    }
    
    // Find next alive worker
    size_t attempts = 0;
    while (attempts < pool->count) {
        size_t idx = g_round_robin_index % pool->count;
        g_round_robin_index++;
        
        if (pool->workers[idx].status == NODE_STATUS_ALIVE) {
            node_id_t worker_id = pool->workers[idx].worker_id;
            pthread_mutex_unlock(&pool->lock);
            return worker_id;
        }
        
        attempts++;
    }
    
    pthread_mutex_unlock(&pool->lock);
    return 0;  // No alive workers
}

node_id_t router_select_worker(router_state_t *router, load_balance_strategy_t strategy) {
    if (!router) return 0;
    
    switch (strategy) {
        case LOAD_BALANCE_LEAST_LOADED:
            return worker_pool_select_least_loaded(&router->worker_pool);
        
        case LOAD_BALANCE_ROUND_ROBIN:
            return worker_pool_select_round_robin(&router->worker_pool);
        
        case LOAD_BALANCE_RANDOM: {
            // Simple random selection among alive workers
            node_id_t alive_workers[MAX_WORKERS];
            size_t count = worker_pool_list_alive(&router->worker_pool, 
                                                 alive_workers, MAX_WORKERS);
            if (count == 0) return 0;
            
            size_t idx = rand() % count;
            return alive_workers[idx];
        }
        
        default:
            return worker_pool_select_least_loaded(&router->worker_pool);
    }
}