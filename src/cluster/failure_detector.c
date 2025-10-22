// src/cluster/failure_detector.c

#define _POSIX_C_SOURCE 200809L

#include "roole/cluster.h"
#include "roole/common.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

// ============================================================================
// HEARTBEAT TRACKER IMPLEMENTATION
// ============================================================================

#define MAX_TRACKED_NODES 256

typedef struct tracked_node {
    node_id_t node_id;
    node_status_t status;
    uint64_t last_heartbeat_ms;
    int active;  // 1 if tracking, 0 if slot free
} tracked_node_t;

struct heartbeat_tracker {
    heartbeat_config_t config;
    tracked_node_t nodes[MAX_TRACKED_NODES];
    size_t node_count;
    pthread_mutex_t lock;
};

int heartbeat_tracker_init(heartbeat_tracker_t **tracker, const heartbeat_config_t *config) {
    if (!tracker) return ROOLE_ERR_INVALID;
    
    heartbeat_tracker_t *t = roole_calloc(1, sizeof(heartbeat_tracker_t));
    if (!t) return ROOLE_ERR_NOMEM;
    
    if (config) {
        t->config = *config;
    } else {
        // Default config
        t->config.interval_ms = DEFAULT_HEARTBEAT_INTERVAL_MS;
        t->config.timeout_ms = DEFAULT_HEARTBEAT_TIMEOUT_MS;
        t->config.dead_timeout_ms = DEFAULT_HEARTBEAT_TIMEOUT_MS * 3;
    }
    
    t->node_count = 0;
    
    if (pthread_mutex_init(&t->lock, NULL) != 0) {
        roole_free(t);
        return ROOLE_ERR_INVALID;
    }
    
    *tracker = t;
    ROOLE_LOG_INFO("Heartbeat tracker initialized (timeout=%ums, dead=%ums)", 
                   t->config.timeout_ms, t->config.dead_timeout_ms);
    return ROOLE_OK;
}

void heartbeat_tracker_destroy(heartbeat_tracker_t *tracker) {
    if (!tracker) return;
    
    pthread_mutex_destroy(&tracker->lock);
    roole_free(tracker);
    
    ROOLE_LOG_INFO("Heartbeat tracker destroyed");
}

int heartbeat_tracker_add_node(heartbeat_tracker_t *tracker, node_id_t node_id) {
    if (!tracker) return ROOLE_ERR_INVALID;
    
    pthread_mutex_lock(&tracker->lock);
    
    // Check if already tracking
    for (size_t i = 0; i < MAX_TRACKED_NODES; i++) {
        if (tracker->nodes[i].active && tracker->nodes[i].node_id == node_id) {
            pthread_mutex_unlock(&tracker->lock);
            ROOLE_LOG_DEBUG("Node %u already tracked", node_id);
            return ROOLE_OK;
        }
    }
    
    // Find free slot
    for (size_t i = 0; i < MAX_TRACKED_NODES; i++) {
        if (!tracker->nodes[i].active) {
            tracker->nodes[i].node_id = node_id;
            tracker->nodes[i].status = NODE_STATUS_ALIVE;
            tracker->nodes[i].last_heartbeat_ms = roole_time_now_ms();
            tracker->nodes[i].active = 1;
            tracker->node_count++;
            
            pthread_mutex_unlock(&tracker->lock);
            ROOLE_LOG_INFO("Now tracking node %u", node_id);
            return ROOLE_OK;
        }
    }
    
    pthread_mutex_unlock(&tracker->lock);
    ROOLE_LOG_ERROR("Heartbeat tracker full (max %d nodes)", MAX_TRACKED_NODES);
    return ROOLE_ERR_FULL;
}

int heartbeat_tracker_remove_node(heartbeat_tracker_t *tracker, node_id_t node_id) {
    if (!tracker) return ROOLE_ERR_INVALID;
    
    pthread_mutex_lock(&tracker->lock);
    
    for (size_t i = 0; i < MAX_TRACKED_NODES; i++) {
        if (tracker->nodes[i].active && tracker->nodes[i].node_id == node_id) {
            tracker->nodes[i].active = 0;
            tracker->node_count--;
            
            pthread_mutex_unlock(&tracker->lock);
            ROOLE_LOG_INFO("Stopped tracking node %u", node_id);
            return ROOLE_OK;
        }
    }
    
    pthread_mutex_unlock(&tracker->lock);
    return ROOLE_ERR_NOTFOUND;
}

int heartbeat_tracker_update(heartbeat_tracker_t *tracker, node_id_t node_id) {
    if (!tracker) return ROOLE_ERR_INVALID;
    
    pthread_mutex_lock(&tracker->lock);
    
    for (size_t i = 0; i < MAX_TRACKED_NODES; i++) {
        if (tracker->nodes[i].active && tracker->nodes[i].node_id == node_id) {
            uint64_t now = roole_time_now_ms();
            tracker->nodes[i].last_heartbeat_ms = now;
            
            // If was suspect/dead, revive it
            if (tracker->nodes[i].status != NODE_STATUS_ALIVE) {
                ROOLE_LOG_INFO("Node %u recovered (was %d)", 
                              node_id, tracker->nodes[i].status);
                tracker->nodes[i].status = NODE_STATUS_ALIVE;
            }
            
            pthread_mutex_unlock(&tracker->lock);
            ROOLE_LOG_DEBUG("Heartbeat received from node %u", node_id);
            return ROOLE_OK;
        }
    }
    
    pthread_mutex_unlock(&tracker->lock);
    ROOLE_LOG_WARN("Received heartbeat from untracked node %u", node_id);
    return ROOLE_ERR_NOTFOUND;
}

int heartbeat_tracker_check_timeouts(heartbeat_tracker_t *tracker, 
                                    heartbeat_timeout_cb callback, void *user_data) {
    if (!tracker) return ROOLE_ERR_INVALID;
    
    uint64_t now = roole_time_now_ms();
    
    pthread_mutex_lock(&tracker->lock);
    
    int timeouts_detected = 0;
    
    for (size_t i = 0; i < MAX_TRACKED_NODES; i++) {
        if (!tracker->nodes[i].active) continue;
        
        tracked_node_t *node = &tracker->nodes[i];
        uint64_t elapsed = now - node->last_heartbeat_ms;
        
        node_status_t old_status = node->status;
        node_status_t new_status = old_status;
        
        // State transitions: ALIVE -> SUSPECT -> DEAD
        if (old_status == NODE_STATUS_ALIVE && elapsed > tracker->config.timeout_ms) {
            new_status = NODE_STATUS_SUSPECT;
            ROOLE_LOG_WARN("Node %u is now SUSPECT (no heartbeat for %lums)", 
                          node->node_id, elapsed);
        }
        else if (old_status == NODE_STATUS_SUSPECT && elapsed > tracker->config.dead_timeout_ms) {
            new_status = NODE_STATUS_DEAD;
            ROOLE_LOG_ERROR("Node %u is now DEAD (no heartbeat for %lums)", 
                           node->node_id, elapsed);
        }
        
        if (new_status != old_status) {
            node->status = new_status;
            timeouts_detected++;
            
            // Call user callback
            if (callback) {
                callback(node->node_id, new_status, user_data);
            }
        }
    }
    
    pthread_mutex_unlock(&tracker->lock);
    
    return timeouts_detected;
}