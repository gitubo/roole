// include/roole/cluster.h

#ifndef ROOLE_CLUSTER_H
#define ROOLE_CLUSTER_H

#include "roole/common.h"
#include <pthread.h>

// ============================================================================
// NODE TYPES & STATUS
// ============================================================================

typedef enum {
    NODE_TYPE_UNKNOWN = 0,
    NODE_TYPE_ROUTER = 1,
    NODE_TYPE_WORKER = 2
} node_type_t;

typedef enum {
    NODE_STATUS_ALIVE = 0,
    NODE_STATUS_SUSPECT = 1,
    NODE_STATUS_DEAD = 2
} node_status_t;

// ============================================================================
// CLUSTER MEMBER
// ============================================================================

#define MAX_IP_LEN 16
#define MAX_CLUSTER_NODES 512

typedef struct cluster_member {
    node_id_t node_id;
    node_type_t node_type;
    char ip_address[MAX_IP_LEN];
    uint16_t port;
    node_status_t status;
    uint64_t last_seen_ms;
    uint64_t incarnation;  // For gossip conflict resolution (SWIM)
} cluster_member_t;

// ============================================================================
// CLUSTER VIEW
// ============================================================================

typedef struct cluster_view {
    cluster_member_t *members;
    size_t count;
    size_t capacity;
    pthread_rwlock_t lock;
} cluster_view_t;

// Cluster view management
int cluster_view_init(cluster_view_t *view, size_t capacity);
void cluster_view_destroy(cluster_view_t *view);

// Member operations (thread-safe)
int cluster_view_add(cluster_view_t *view, const cluster_member_t *member);
int cluster_view_update_status(cluster_view_t *view, node_id_t node_id, 
                               node_status_t status, uint64_t incarnation);
int cluster_view_remove(cluster_view_t *view, node_id_t node_id);
cluster_member_t* cluster_view_get(cluster_view_t *view, node_id_t node_id);
void cluster_view_release(cluster_view_t *view);

// Query operations
size_t cluster_view_list_by_type(cluster_view_t *view, node_type_t type,
                                 node_id_t *out_node_ids, size_t max_count);
size_t cluster_view_list_alive(cluster_view_t *view, node_type_t type,
                               node_id_t *out_node_ids, size_t max_count);

// ============================================================================
// MEMBERSHIP (Gossip Protocol - Serf-like wrapper)
// ============================================================================

typedef struct membership_handle membership_handle_t;

// Event types
#define MEMBER_EVENT_JOIN "member-join"
#define MEMBER_EVENT_LEAVE "member-leave"
#define MEMBER_EVENT_FAILED "member-failed"
#define MEMBER_EVENT_UPDATE "member-update"

// Callback for membership events
typedef void (*member_event_cb)(node_id_t node_id, node_type_t type, 
                                const char *ip, uint16_t port,
                                const char *event_type, void *user_data);

// Membership API
int membership_init(membership_handle_t **handle, node_id_t my_id, 
                   node_type_t my_type, const char *bind_addr, uint16_t bind_port);
int membership_join(membership_handle_t *handle, const char *seed_addr, uint16_t seed_port);
int membership_set_callback(membership_handle_t *handle, member_event_cb callback, void *user_data);
int membership_leave(membership_handle_t *handle);
void membership_shutdown(membership_handle_t *handle);

// Get current members (for debugging/monitoring)
size_t membership_get_members(membership_handle_t *handle, cluster_member_t *out_members, size_t max_count);

// ============================================================================
// HEARTBEAT & FAILURE DETECTION
// ============================================================================

#define DEFAULT_HEARTBEAT_INTERVAL_MS 1000
#define DEFAULT_HEARTBEAT_TIMEOUT_MS 5000

typedef struct heartbeat_config {
    uint32_t interval_ms;       // How often to send heartbeat
    uint32_t timeout_ms;        // When to mark as suspect
    uint32_t dead_timeout_ms;   // When to mark as dead
} heartbeat_config_t;

typedef struct heartbeat_tracker heartbeat_tracker_t;

// Heartbeat tracker (used by routers to track workers)
int heartbeat_tracker_init(heartbeat_tracker_t **tracker, const heartbeat_config_t *config);
void heartbeat_tracker_destroy(heartbeat_tracker_t *tracker);

// Register/unregister nodes to track
int heartbeat_tracker_add_node(heartbeat_tracker_t *tracker, node_id_t node_id);
int heartbeat_tracker_remove_node(heartbeat_tracker_t *tracker, node_id_t node_id);

// Update heartbeat (called when heartbeat received)
int heartbeat_tracker_update(heartbeat_tracker_t *tracker, node_id_t node_id);

// Check for timeouts (should be called periodically)
typedef void (*heartbeat_timeout_cb)(node_id_t node_id, node_status_t new_status, void *user_data);
int heartbeat_tracker_check_timeouts(heartbeat_tracker_t *tracker, 
                                    heartbeat_timeout_cb callback, void *user_data);

#endif // ROOLE_CLUSTER_H