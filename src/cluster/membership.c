// src/cluster/membership.c
// PHASE 1: Refactored to use shared cluster_view instead of internal_view

#define _POSIX_C_SOURCE 200809L

#include "roole/cluster.h"
#include "roole/common.h"
#include "roole/gossip.h" 
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

// ============================================================================
// CLUSTER VIEW IMPLEMENTATION (Unchanged)
// ============================================================================

int cluster_view_init(cluster_view_t *view, size_t capacity) {
    if (!view || capacity == 0) return RESULT_ERR_INVALID;
    
    memset(view, 0, sizeof(cluster_view_t));
    
    view->members = safe_calloc(capacity, sizeof(cluster_member_t));
    if (!view->members) {
        return RESULT_ERR_NOMEM;
    }
    
    view->capacity = capacity;
    view->count = 0;
    
    if (pthread_rwlock_init(&view->lock, NULL) != 0) {
        safe_free(view->members);
        return RESULT_ERR_INVALID;
    }
    
    LOG_INFO("Cluster view initialized (capacity: %zu)", capacity);
    return RESULT_OK;
}

void cluster_view_destroy(cluster_view_t *view) {
    if (!view) return;
    
    pthread_rwlock_wrlock(&view->lock);
    
    safe_free(view->members);
    view->members = NULL;
    view->count = 0;
    view->capacity = 0;
    
    pthread_rwlock_unlock(&view->lock);
    pthread_rwlock_destroy(&view->lock);
    
    LOG_INFO("Cluster view destroyed");
}

int cluster_view_add(cluster_view_t *view, const cluster_member_t *member) {
    if (!view || !member) return RESULT_ERR_INVALID;
    
    pthread_rwlock_wrlock(&view->lock);
    
    // Check if node already exists
    for (size_t i = 0; i < view->count; i++) {
        if (view->members[i].node_id == member->node_id) {
            // Node exists - update it if new incarnation or status change
            if (member->status == NODE_STATUS_ALIVE && 
                view->members[i].status == NODE_STATUS_DEAD) {
                // Rejoining node - update everything
                view->members[i] = *member;
                view->members[i].last_seen_ms = time_now_ms();
                view->members[i].incarnation = member->incarnation + 1;
                pthread_rwlock_unlock(&view->lock);
                LOG_INFO("Node %u rejoined cluster (was DEAD, now ALIVE, incarnation=%lu)", 
                         member->node_id, view->members[i].incarnation);
                return RESULT_OK;
            }
            else if (member->incarnation >= view->members[i].incarnation) {
                view->members[i] = *member;
                view->members[i].last_seen_ms = time_now_ms();
                pthread_rwlock_unlock(&view->lock);
                LOG_DEBUG("Updated existing member %u (incarnation=%lu)", 
                          member->node_id, member->incarnation);
                return RESULT_OK;
            }
            pthread_rwlock_unlock(&view->lock);
            LOG_DEBUG("Ignoring stale update for node %u (inc %lu <= %lu)", 
                      member->node_id, member->incarnation, view->members[i].incarnation);
            return RESULT_OK;
        }
    }
    
    // New node - add it
    if (view->count >= view->capacity) {
        pthread_rwlock_unlock(&view->lock);
        LOG_ERROR("Cluster view full (capacity: %zu)", view->capacity);
        return RESULT_ERR_FULL;
    }
    
    view->members[view->count] = *member;
    view->members[view->count].last_seen_ms = time_now_ms();
    view->count++;
    
    pthread_rwlock_unlock(&view->lock);
    
    LOG_INFO("Added member %u (%s:%u, type=%d)", 
             member->node_id, member->ip_address, member->gossip_port, member->node_type);
    return RESULT_OK;
}

int cluster_view_update_status(cluster_view_t *view, node_id_t node_id, 
                               node_status_t status, uint64_t incarnation) {
    if (!view) return RESULT_ERR_INVALID;
    
    pthread_rwlock_wrlock(&view->lock);
    
    for (size_t i = 0; i < view->count; i++) {
        if (view->members[i].node_id == node_id) {
            if (incarnation >= view->members[i].incarnation) {
                node_status_t old_status = view->members[i].status;
                
                view->members[i].status = status;
                view->members[i].incarnation = incarnation;
                
                if (status == NODE_STATUS_ALIVE) {
                    view->members[i].last_seen_ms = time_now_ms();
                } else if (status == NODE_STATUS_SUSPECT && old_status == NODE_STATUS_ALIVE) {
                    view->members[i].last_seen_ms = time_now_ms();
                }
                
                pthread_rwlock_unlock(&view->lock);
                LOG_DEBUG("Updated node %u status to %d (incarnation %lu)", 
                          node_id, status, incarnation);
                return RESULT_OK;
            }
            pthread_rwlock_unlock(&view->lock);
            return RESULT_OK;
        }
    }
    
    pthread_rwlock_unlock(&view->lock);
    LOG_WARN("Node %u not found for status update", node_id);
    return RESULT_ERR_NOTFOUND;
}

int cluster_view_remove(cluster_view_t *view, node_id_t node_id) {
    if (!view) return RESULT_ERR_INVALID;
    
    pthread_rwlock_wrlock(&view->lock);
    
    for (size_t i = 0; i < view->count; i++) {
        if (view->members[i].node_id == node_id) {
            if (i < view->count - 1) {
                memmove(&view->members[i], &view->members[i + 1], 
                       (view->count - i - 1) * sizeof(cluster_member_t));
            }
            view->count--;
            pthread_rwlock_unlock(&view->lock);
            LOG_INFO("Removed member %u", node_id);
            return RESULT_OK;
        }
    }
    
    pthread_rwlock_unlock(&view->lock);
    return RESULT_ERR_NOTFOUND;
}

cluster_member_t* cluster_view_get(cluster_view_t *view, node_id_t node_id) {
    if (!view) return NULL;
    
    pthread_rwlock_rdlock(&view->lock);
    
    for (size_t i = 0; i < view->count; i++) {
        if (view->members[i].node_id == node_id) {
            return &view->members[i];
        }
    }
    
    pthread_rwlock_unlock(&view->lock);
    return NULL;
}

void cluster_view_release(cluster_view_t *view) {
    if (view) {
        pthread_rwlock_unlock(&view->lock);
    }
}

size_t cluster_view_list_by_type(cluster_view_t *view, node_type_t type,
                                 node_id_t *out_node_ids, size_t max_count) {
    if (!view || !out_node_ids || max_count == 0) return 0;
    
    pthread_rwlock_rdlock(&view->lock);
    
    size_t found = 0;
    for (size_t i = 0; i < view->count && found < max_count; i++) {
        if (view->members[i].node_type == type) {
            out_node_ids[found++] = view->members[i].node_id;
        }
    }
    
    pthread_rwlock_unlock(&view->lock);
    
    return found;
}

size_t cluster_view_list_alive(cluster_view_t *view, node_type_t type,
                               node_id_t *out_node_ids, size_t max_count) {
    if (!view || !out_node_ids || max_count == 0) return 0;
    
    pthread_rwlock_rdlock(&view->lock);
    
    size_t found = 0;
    for (size_t i = 0; i < view->count && found < max_count; i++) {
        if (view->members[i].node_type == type && 
            view->members[i].status == NODE_STATUS_ALIVE) {
            out_node_ids[found++] = view->members[i].node_id;
        }
    }
    
    pthread_rwlock_unlock(&view->lock);
    
    return found;
}

// ============================================================================
// MEMBERSHIP HANDLE IMPLEMENTATION - ✅ PHASE 1 REFACTORED
// ============================================================================

int membership_init(membership_handle_t **handle, 
                   node_id_t my_id, 
                   node_type_t my_type, 
                   const char *bind_addr, 
                   uint16_t gossip_port, 
                   uint16_t data_port,
                   cluster_view_t *shared_view) {  // ✅ NEW parameter
    
    // ✅ VALIDATION: Ensure shared_view is provided
    if (!handle || !shared_view) {
        LOG_ERROR("membership_init: handle or shared_view is NULL");
        return RESULT_ERR_INVALID;
    }
    
    membership_handle_t *h = safe_calloc(1, sizeof(membership_handle_t));
    if (!h) return RESULT_ERR_NOMEM;
    
    h->my_id = my_id;
    h->my_type = my_type;
    safe_strncpy(h->bind_addr, bind_addr ? bind_addr : "0.0.0.0", MAX_IP_LEN);
    h->gossip_port = gossip_port;
    h->data_port = data_port;
    h->shutdown_flag = 0;
    
    // ✅ PHASE 1 CHANGE: Store pointer to shared view instead of creating internal copy
    // OLD: cluster_view_init(&h->internal_view, MAX_CLUSTER_NODES);
    h->shared_view = shared_view;
    
    LOG_DEBUG("membership_init: Using shared cluster_view at %p", (void*)shared_view);
    
    // Add self to shared cluster view (not internal copy)
    cluster_member_t self = {
        .node_id = my_id,
        .node_type = my_type,
        .gossip_port = gossip_port,
        .data_port = data_port,
        .status = NODE_STATUS_ALIVE,
        .incarnation = 0,
        .last_seen_ms = time_now_ms()
    };
    safe_strncpy(self.ip_address, h->bind_addr, MAX_IP_LEN);
    
    // ✅ CHANGED: Add to shared_view instead of internal_view
    cluster_view_add(h->shared_view, &self);
    
    gossip_config_t gossip_config = gossip_default_config();
    
    // ✅ CHANGED: Pass shared_view to gossip engine (already correct in original code)
    h->gossip_engine = gossip_engine_init(
        my_id,
        my_type,
        h->bind_addr,
        gossip_port,
        data_port,
        &gossip_config,
        h->shared_view,  // ✅ Use shared_view
        NULL,
        NULL
    );
    
    if (!h->gossip_engine) {
        LOG_ERROR("Failed to initialize gossip engine");
        // ❌ REMOVED: cluster_view_destroy(&h->internal_view);
        safe_free(h);
        return RESULT_ERR_INVALID;
    }
    
    if (gossip_engine_start(h->gossip_engine) != 0) {
        LOG_ERROR("Failed to start gossip engine");
        gossip_engine_shutdown(h->gossip_engine);
        // ❌ REMOVED: cluster_view_destroy(&h->internal_view);
        safe_free(h);
        return RESULT_ERR_INVALID;
    }
    
    *handle = h;
    LOG_INFO("Membership initialized (node_id=%u, type=%d, gossip_port=%u, shared_view=%p)", 
             my_id, my_type, gossip_port, (void*)shared_view);
    return RESULT_OK;
}

int membership_join(membership_handle_t *handle, const char *seed_addr, uint16_t seed_port) {
    if (!handle || !seed_addr) return RESULT_ERR_INVALID;
    
    LOG_INFO("Joining cluster via seed %s:%u", seed_addr, seed_port);
    
    if (gossip_engine_add_seed(handle->gossip_engine, seed_addr, seed_port) != 0) {
        LOG_ERROR("Failed to send JOIN to seed");
        return RESULT_ERR_NETWORK;
    }
    
    gossip_engine_announce_join(handle->gossip_engine);
    
    LOG_INFO("JOIN sent to seed, waiting for cluster view to populate...");
    return RESULT_OK;
}

int membership_set_callback(membership_handle_t *handle, member_event_cb callback, void *user_data) {
    if (!handle) return RESULT_ERR_INVALID;
    
    handle->event_callback = callback;
    handle->event_callback_user_data = user_data;
    
    if (handle->gossip_engine) {
        gossip_engine_set_callback(handle->gossip_engine, callback, user_data);
    }
    
    return RESULT_OK;
}

int membership_leave(membership_handle_t *handle) {
    if (!handle) return RESULT_ERR_INVALID;
    
    LOG_INFO("Gracefully leaving cluster");
    
    if (handle->gossip_engine) {
        gossip_engine_leave(handle->gossip_engine);
    }
    
    sleep(1);
    
    return RESULT_OK;
}

void membership_shutdown(membership_handle_t *handle) {
    if (!handle) return;
    
    LOG_INFO("Shutting down membership...");
    
    handle->shutdown_flag = 1;
    
    if (handle->gossip_engine) {
        gossip_engine_shutdown(handle->gossip_engine);
        handle->gossip_engine = NULL;
    }
    
    // ❌ REMOVED: cluster_view_destroy(&handle->internal_view);
    // ✅ RATIONALE: shared_view is owned by caller (node_state), not membership
    LOG_DEBUG("Membership shutdown: shared_view ownership retained by caller");
    
    safe_free(handle);
    
    LOG_INFO("Membership shutdown complete");
}

// ✅ CHANGED: Use shared_view instead of internal_view
size_t membership_get_members(membership_handle_t *handle, 
                              cluster_member_t *out_members, 
                              size_t max_count) {
    if (!handle || !out_members || max_count == 0) return 0;
    
    // ✅ CHANGED: Access shared_view
    pthread_rwlock_rdlock(&handle->shared_view->lock);
    
    size_t count = ROOLE_MIN(handle->shared_view->count, max_count);
    memcpy(out_members, handle->shared_view->members, 
           count * sizeof(cluster_member_t));
    
    pthread_rwlock_unlock(&handle->shared_view->lock);
    
    return count;
}

// ============================================================================
// DEBUG HELPERS (Unchanged)
// ============================================================================

void cluster_view_dump(cluster_view_t *view, const char *label) {
    if (!view) return;
    
    pthread_rwlock_rdlock(&view->lock);
    
    LOG_INFO("========================================");
    LOG_INFO("Cluster View Dump: %s", label ? label : "");
    LOG_INFO("========================================");
    LOG_INFO("Total members: %zu (capacity: %zu)", view->count, view->capacity);
    
    if (view->count == 0) {
        LOG_INFO("  (empty)");
    } else {
        for (size_t i = 0; i < view->count; i++) {
            cluster_member_t *m = &view->members[i];
            const char *status_str = (m->status == NODE_STATUS_ALIVE) ? "ALIVE" :
                                    (m->status == NODE_STATUS_SUSPECT) ? "SUSPECT" : "DEAD";
            const char *type_str = (m->node_type == NODE_TYPE_ROUTER) ? "ROUTER" : "WORKER";
            
            LOG_INFO("  [%zu] node_id=%u type=%s status=%s ip=%s:%u data=%u inc=%lu last_seen=%lu",
                    i, m->node_id, type_str, status_str,
                    m->ip_address, m->gossip_port, m->data_port,
                    m->incarnation, m->last_seen_ms);
        }
    }
    
    LOG_INFO("========================================");
    
    pthread_rwlock_unlock(&view->lock);
}