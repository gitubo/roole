// src/cluster/membership.c

#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

#include "roole/cluster.h"
#include "roole/common.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

// ============================================================================
// CLUSTER VIEW IMPLEMENTATION
// ============================================================================

int cluster_view_init(cluster_view_t *view, size_t capacity) {
    if (!view || capacity == 0) return ROOLE_ERR_INVALID;
    
    memset(view, 0, sizeof(cluster_view_t));
    
    view->members = roole_calloc(capacity, sizeof(cluster_member_t));
    if (!view->members) {
        return ROOLE_ERR_NOMEM;
    }
    
    view->capacity = capacity;
    view->count = 0;
    
    if (pthread_rwlock_init(&view->lock, NULL) != 0) {
        roole_free(view->members);
        return ROOLE_ERR_INVALID;
    }
    
    ROOLE_LOG_INFO("Cluster view initialized (capacity: %zu)", capacity);
    return ROOLE_OK;
}

void cluster_view_destroy(cluster_view_t *view) {
    if (!view) return;
    
    pthread_rwlock_wrlock(&view->lock);
    
    roole_free(view->members);
    view->members = NULL;
    view->count = 0;
    view->capacity = 0;
    
    pthread_rwlock_unlock(&view->lock);
    pthread_rwlock_destroy(&view->lock);
    
    ROOLE_LOG_INFO("Cluster view destroyed");
}

int cluster_view_add(cluster_view_t *view, const cluster_member_t *member) {
    if (!view || !member) return ROOLE_ERR_INVALID;
    
    pthread_rwlock_wrlock(&view->lock);
    
    // Check if member already exists
    for (size_t i = 0; i < view->count; i++) {
        if (view->members[i].node_id == member->node_id) {
            // Update existing member
            view->members[i] = *member;
            view->members[i].last_seen_ms = roole_time_now_ms();
            pthread_rwlock_unlock(&view->lock);
            ROOLE_LOG_DEBUG("Updated existing member %u", member->node_id);
            return ROOLE_OK;
        }
    }
    
    // Add new member
    if (view->count >= view->capacity) {
        pthread_rwlock_unlock(&view->lock);
        ROOLE_LOG_ERROR("Cluster view full (capacity: %zu)", view->capacity);
        return ROOLE_ERR_FULL;
    }
    
    view->members[view->count] = *member;
    view->members[view->count].last_seen_ms = roole_time_now_ms();
    view->count++;
    
    pthread_rwlock_unlock(&view->lock);
    
    ROOLE_LOG_INFO("Added member %u (%s:%u, type=%d)", 
                   member->node_id, member->ip_address, member->port, member->node_type);
    return ROOLE_OK;
}

int cluster_view_update_status(cluster_view_t *view, node_id_t node_id, 
                               node_status_t status, uint64_t incarnation) {
    if (!view) return ROOLE_ERR_INVALID;
    
    pthread_rwlock_wrlock(&view->lock);
    
    for (size_t i = 0; i < view->count; i++) {
        if (view->members[i].node_id == node_id) {
            // Only update if incarnation is newer or equal
            if (incarnation >= view->members[i].incarnation) {
                view->members[i].status = status;
                view->members[i].incarnation = incarnation;
                view->members[i].last_seen_ms = roole_time_now_ms();
                pthread_rwlock_unlock(&view->lock);
                ROOLE_LOG_DEBUG("Updated node %u status to %d (incarnation %lu)", 
                               node_id, status, incarnation);
                return ROOLE_OK;
            }
            pthread_rwlock_unlock(&view->lock);
            return ROOLE_OK; // Stale update, ignore
        }
    }
    
    pthread_rwlock_unlock(&view->lock);
    ROOLE_LOG_WARN("Node %u not found for status update", node_id);
    return ROOLE_ERR_NOTFOUND;
}

int cluster_view_remove(cluster_view_t *view, node_id_t node_id) {
    if (!view) return ROOLE_ERR_INVALID;
    
    pthread_rwlock_wrlock(&view->lock);
    
    for (size_t i = 0; i < view->count; i++) {
        if (view->members[i].node_id == node_id) {
            // Shift remaining members
            if (i < view->count - 1) {
                memmove(&view->members[i], &view->members[i + 1], 
                       (view->count - i - 1) * sizeof(cluster_member_t));
            }
            view->count--;
            pthread_rwlock_unlock(&view->lock);
            ROOLE_LOG_INFO("Removed member %u", node_id);
            return ROOLE_OK;
        }
    }
    
    pthread_rwlock_unlock(&view->lock);
    return ROOLE_ERR_NOTFOUND;
}

cluster_member_t* cluster_view_get(cluster_view_t *view, node_id_t node_id) {
    if (!view) return NULL;
    
    pthread_rwlock_rdlock(&view->lock);
    
    for (size_t i = 0; i < view->count; i++) {
        if (view->members[i].node_id == node_id) {
            // Return pointer - caller must call cluster_view_release
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
// MEMBERSHIP HANDLE (Simple Gossip Implementation)
// ============================================================================

#define MAX_GOSSIP_MEMBERS 512
#define GOSSIP_INTERVAL_MS 2000
#define GOSSIP_FANOUT 3

struct membership_handle {
    node_id_t my_id;
    node_type_t my_type;
    char bind_addr[MAX_IP_LEN];
    uint16_t bind_port;
    
    cluster_view_t internal_view;
    
    member_event_cb event_callback;
    void *event_callback_user_data;
    
    pthread_t gossip_thread;
    int shutdown_flag;
};

static void* membership_gossip_thread(void *arg) {
    membership_handle_t *handle = (membership_handle_t*)arg;
    
    ROOLE_LOG_INFO("Membership gossip thread started");
    
    while (!handle->shutdown_flag) {
        usleep(GOSSIP_INTERVAL_MS * 1000);
        
        // TODO: Implement actual gossip protocol
        // 1. Select random GOSSIP_FANOUT members
        // 2. Send our view + ping
        // 3. Merge received view
        // 4. Detect failures (increment suspicion)
        
        ROOLE_LOG_DEBUG("Gossip tick");
    }
    
    ROOLE_LOG_INFO("Membership gossip thread stopped");
    return NULL;
}

int membership_init(membership_handle_t **handle, node_id_t my_id, 
                   node_type_t my_type, const char *bind_addr, uint16_t bind_port) {
    if (!handle) return ROOLE_ERR_INVALID;
    
    membership_handle_t *h = roole_calloc(1, sizeof(membership_handle_t));
    if (!h) return ROOLE_ERR_NOMEM;
    
    h->my_id = my_id;
    h->my_type = my_type;
    roole_strncpy_safe(h->bind_addr, bind_addr ? bind_addr : "0.0.0.0", MAX_IP_LEN);
    h->bind_port = bind_port;
    h->shutdown_flag = 0;
    
    if (cluster_view_init(&h->internal_view, MAX_GOSSIP_MEMBERS) != ROOLE_OK) {
        roole_free(h);
        return ROOLE_ERR_INVALID;
    }
    
    // Add ourselves to the view
    cluster_member_t self = {
        .node_id = my_id,
        .node_type = my_type,
        .port = bind_port,
        .status = NODE_STATUS_ALIVE,
        .incarnation = 0
    };
    roole_strncpy_safe(self.ip_address, h->bind_addr, MAX_IP_LEN);
    cluster_view_add(&h->internal_view, &self);
    
    // Start gossip thread
    if (pthread_create(&h->gossip_thread, NULL, membership_gossip_thread, h) != 0) {
        cluster_view_destroy(&h->internal_view);
        roole_free(h);
        return ROOLE_ERR_INVALID;
    }
    
    *handle = h;
    ROOLE_LOG_INFO("Membership initialized (node_id=%u, type=%d, port=%u)", 
                   my_id, my_type, bind_port);
    return ROOLE_OK;
}

int membership_join(membership_handle_t *handle, const char *seed_addr, uint16_t seed_port) {
    if (!handle || !seed_addr) return ROOLE_ERR_INVALID;
    
    // TODO: Implement join protocol
    // 1. Connect to seed node
    // 2. Send JOIN message with our info
    // 3. Receive member list
    // 4. Add to internal view
    
    ROOLE_LOG_INFO("Joining cluster via seed %s:%u", seed_addr, seed_port);
    
    // Placeholder: add seed as a member
    cluster_member_t seed = {
        .node_id = 0, // Unknown yet
        .node_type = NODE_TYPE_UNKNOWN,
        .port = seed_port,
        .status = NODE_STATUS_ALIVE,
        .incarnation = 0
    };
    roole_strncpy_safe(seed.ip_address, seed_addr, MAX_IP_LEN);
    
    return ROOLE_OK;
}

int membership_set_callback(membership_handle_t *handle, member_event_cb callback, void *user_data) {
    if (!handle) return ROOLE_ERR_INVALID;
    
    handle->event_callback = callback;
    handle->event_callback_user_data = user_data;
    
    return ROOLE_OK;
}

int membership_leave(membership_handle_t *handle) {
    if (!handle) return ROOLE_ERR_INVALID;
    
    // TODO: Send LEAVE message to all known members
    ROOLE_LOG_INFO("Leaving cluster");
    
    return ROOLE_OK;
}

void membership_shutdown(membership_handle_t *handle) {
    if (!handle) return;
    
    handle->shutdown_flag = 1;
    pthread_join(handle->gossip_thread, NULL);
    
    cluster_view_destroy(&handle->internal_view);
    roole_free(handle);
    
    ROOLE_LOG_INFO("Membership shutdown complete");
}

size_t membership_get_members(membership_handle_t *handle, cluster_member_t *out_members, size_t max_count) {
    if (!handle || !out_members || max_count == 0) return 0;
    
    pthread_rwlock_rdlock(&handle->internal_view.lock);
    
    size_t count = ROOLE_MIN(handle->internal_view.count, max_count);
    memcpy(out_members, handle->internal_view.members, count * sizeof(cluster_member_t));
    
    pthread_rwlock_unlock(&handle->internal_view.lock);
    
    return count;
}