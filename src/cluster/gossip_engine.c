// src/cluster/gossip_engine.c - SWIM gossip protocol engine
// FIX: UDP listener was blocking after initial messages due to incorrect socket handling

#define _POSIX_C_SOURCE 200809L

#include "roole/gossip.h"
#include "roole/common.h"
#include "roole/config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

// Forward declarations from gossip_transport.c
int gossip_create_udp_socket(const char *bind_addr, uint16_t port);
ssize_t gossip_send_udp(int sock_fd, const uint8_t *data, size_t len,
                        const char *dest_ip, uint16_t dest_port);
ssize_t gossip_recv_udp(int sock_fd, uint8_t *buffer, size_t buffer_size,
                        char *src_ip, size_t src_ip_len, uint16_t *src_port);

// ============================================================================
// INTERNAL STRUCTURES
// ============================================================================

#define MAX_PENDING_ACKS 64
#define MAX_UPDATE_QUEUE 256

typedef struct pending_ack {
    node_id_t target_node;
    uint64_t ping_sent_ms;
    int active;
} pending_ack_t;

typedef struct update_queue {
    gossip_member_update_t updates[MAX_UPDATE_QUEUE];
    size_t head;
    size_t tail;
    size_t count;
    pthread_mutex_t lock;
} update_queue_t;

struct gossip_engine {
    node_id_t my_id;
    node_type_t my_type;
    char my_ip[MAX_IP_LEN];
    uint16_t gossip_port;
    uint16_t data_port;
    uint64_t incarnation;
    uint64_t sequence_num;
    
    gossip_config_t config;
    
    int udp_socket;
    
    cluster_view_t *cluster_view;
    
    member_event_cb event_callback;
    void *event_callback_data;
    
    pending_ack_t pending_acks[MAX_PENDING_ACKS];
    pthread_mutex_t pending_acks_lock;
    
    update_queue_t update_queue;
    
    pthread_t protocol_thread;
    pthread_t listener_thread;
    
    int shutdown_flag;
};

// ============================================================================
// UPDATE QUEUE MANAGEMENT
// ============================================================================

static int update_queue_init(update_queue_t *queue) {
    memset(queue, 0, sizeof(update_queue_t));
    return pthread_mutex_init(&queue->lock, NULL);
}

static void update_queue_destroy(update_queue_t *queue) {
    pthread_mutex_destroy(&queue->lock);
}

static int update_queue_push(update_queue_t *queue, const gossip_member_update_t *update) {
    pthread_mutex_lock(&queue->lock);
    
    if (queue->count >= MAX_UPDATE_QUEUE) {
        pthread_mutex_unlock(&queue->lock);
        return -1;
    }
    
    queue->updates[queue->tail] = *update;
    queue->tail = (queue->tail + 1) % MAX_UPDATE_QUEUE;
    queue->count++;
    
    pthread_mutex_unlock(&queue->lock);
    return 0;
}

static int update_queue_pop_batch(update_queue_t *queue, gossip_member_update_t *out, size_t max_count) {
    pthread_mutex_lock(&queue->lock);
    
    size_t to_copy = (queue->count < max_count) ? queue->count : max_count;
    
    for (size_t i = 0; i < to_copy; i++) {
        out[i] = queue->updates[queue->head];
        queue->head = (queue->head + 1) % MAX_UPDATE_QUEUE;
        queue->count--;
    }
    
    pthread_mutex_unlock(&queue->lock);
    return (int)to_copy;
}

// ============================================================================
// PENDING ACK TRACKING
// ============================================================================

static int add_pending_ack(gossip_engine_t *engine, node_id_t target_node) {
    pthread_mutex_lock(&engine->pending_acks_lock);
    
    for (int i = 0; i < MAX_PENDING_ACKS; i++) {
        if (!engine->pending_acks[i].active) {
            engine->pending_acks[i].target_node = target_node;
            engine->pending_acks[i].ping_sent_ms = time_now_ms();
            engine->pending_acks[i].active = 1;
            pthread_mutex_unlock(&engine->pending_acks_lock);
            return 0;
        }
    }
    
    pthread_mutex_unlock(&engine->pending_acks_lock);
    LOG_WARN("Pending ACK table full");
    return -1;
}

static int remove_pending_ack(gossip_engine_t *engine, node_id_t target_node) {
    pthread_mutex_lock(&engine->pending_acks_lock);
    
    for (int i = 0; i < MAX_PENDING_ACKS; i++) {
        if (engine->pending_acks[i].active && 
            engine->pending_acks[i].target_node == target_node) {
            engine->pending_acks[i].active = 0;
            pthread_mutex_unlock(&engine->pending_acks_lock);
            return 0;
        }
    }
    
    pthread_mutex_unlock(&engine->pending_acks_lock);
    return -1;
}

static void check_ack_timeouts(gossip_engine_t *engine) {
    uint64_t now = time_now_ms();
    
    pthread_mutex_lock(&engine->pending_acks_lock);
    
    for (int i = 0; i < MAX_PENDING_ACKS; i++) {
        if (!engine->pending_acks[i].active) continue;
        
        uint64_t elapsed = now - engine->pending_acks[i].ping_sent_ms;
        
        if (elapsed > engine->config.ack_timeout_ms) {
            node_id_t node = engine->pending_acks[i].target_node;
            
            LOG_WARN("ACK timeout for node %u (no response after %lums)", 
                     node, elapsed);
            
            cluster_view_update_status(engine->cluster_view, node, 
                                      NODE_STATUS_SUSPECT, 0);
            
            gossip_member_update_t update = {
                .node_id = node,
                .status = NODE_STATUS_SUSPECT,
                .timestamp_ms = now
            };
            update_queue_push(&engine->update_queue, &update);
            
            engine->pending_acks[i].active = 0;
        }
    }
    
    pthread_mutex_unlock(&engine->pending_acks_lock);
}

// ============================================================================
// INITIALIZATION
// ============================================================================

gossip_engine_t* gossip_engine_init(
    node_id_t my_id,
    node_type_t my_type,
    const char *bind_addr,
    uint16_t gossip_port,
    uint16_t data_port,
    const gossip_config_t *config,
    cluster_view_t *cluster_view,
    member_event_cb event_callback,
    void *user_data)
{
    if (!bind_addr || !cluster_view) {
        LOG_ERROR("Invalid parameters for gossip engine");
        return NULL;
    }
    
    gossip_engine_t *engine = calloc(1, sizeof(gossip_engine_t));
    if (!engine) {
        LOG_ERROR("Failed to allocate gossip engine");
        return NULL;
    }
    
    engine->my_id = my_id;
    engine->my_type = my_type;
    safe_strncpy(engine->my_ip, bind_addr, MAX_IP_LEN);
    engine->gossip_port = gossip_port;
    engine->data_port = data_port;
    engine->incarnation = 0;
    engine->sequence_num = 0;
    engine->cluster_view = cluster_view;
    engine->event_callback = event_callback;
    engine->event_callback_data = user_data;
    engine->shutdown_flag = 0;
    
    if (config) {
        engine->config = *config;
    } else {
        engine->config = gossip_default_config();
    }
    
    if (update_queue_init(&engine->update_queue) != 0) {
        LOG_ERROR("Failed to initialize update queue");
        free(engine);
        return NULL;
    }
    
    if (pthread_mutex_init(&engine->pending_acks_lock, NULL) != 0) {
        LOG_ERROR("Failed to initialize pending ACKs lock");
        update_queue_destroy(&engine->update_queue);
        free(engine);
        return NULL;
    }
    
    engine->udp_socket = gossip_create_udp_socket(bind_addr, gossip_port);
    if (engine->udp_socket < 0) {
        LOG_ERROR("Failed to create gossip UDP socket");
        pthread_mutex_destroy(&engine->pending_acks_lock);
        update_queue_destroy(&engine->update_queue);
        free(engine);
        return NULL;
    }
    
    LOG_INFO("Gossip engine initialized (node_id=%u, type=%d, gossip_port=%u)",
             my_id, my_type, gossip_port);
    
    return engine;
}

// ============================================================================
// MESSAGE HANDLERS
// ============================================================================

static void handle_ping_message(gossip_engine_t *engine, 
                               const gossip_message_t *msg,
                               const char *src_ip, 
                               uint16_t src_port) {
    LOG_DEBUG("Received PING from node %u (%s:%u)", 
              msg->sender_id, src_ip, src_port);
    
    // Process piggyback updates - CRITICAL: avoid lock upgrade deadlock
    for (uint8_t i = 0; i < msg->num_updates; i++) {
        const gossip_member_update_t *upd = &msg->updates[i];
        
        cluster_member_t member = {
            .node_id = upd->node_id,
            .node_type = upd->node_type,
            .gossip_port = upd->gossip_port,
            .data_port = upd->data_port,
            .status = upd->status,
            .incarnation = upd->incarnation
        };
        
        if (upd->node_id == msg->sender_id) {
            safe_strncpy(member.ip_address, src_ip, MAX_IP_LEN);
        } else {
            safe_strncpy(member.ip_address, upd->ip_address, MAX_IP_LEN);
        }
        
        // Check if exists WITHOUT holding lock for next operation
        cluster_member_t *existing = cluster_view_get(engine->cluster_view, upd->node_id);
        int should_add = (existing == NULL);
        int should_update = 0;
        node_status_t old_status = NODE_STATUS_ALIVE;
        
        if (existing) {
            old_status = existing->status;
            should_update = (upd->incarnation > existing->incarnation);
            cluster_view_release(engine->cluster_view);  // MUST release before add/update!
        }
        
        // Now perform operations without holding previous locks
        if (should_add) {
            cluster_view_add(engine->cluster_view, &member);
            LOG_INFO("Discovered new member %u (%s:%u, type=%d)",
                     upd->node_id, member.ip_address, member.gossip_port, upd->node_type);
            
            if (engine->event_callback) {
                engine->event_callback(upd->node_id, upd->node_type,
                                     upd->ip_address, upd->gossip_port, upd->data_port,
                                     MEMBER_EVENT_JOIN, engine->event_callback_data);
            }
        } else if (should_update) {
            cluster_view_update_status(engine->cluster_view, upd->node_id,
                                      upd->status, upd->incarnation);
            
            LOG_INFO("Updated node %u status to %d (incarnation %lu)",
                     upd->node_id, upd->status, upd->incarnation);
            
            if (upd->status != old_status && engine->event_callback) {
                const char *event_type = NULL;
                if (upd->status == NODE_STATUS_SUSPECT) {
                    event_type = MEMBER_EVENT_FAILED;
                } else if (upd->status == NODE_STATUS_DEAD) {
                    event_type = MEMBER_EVENT_LEAVE;
                } else if (upd->status == NODE_STATUS_ALIVE) {
                    event_type = MEMBER_EVENT_UPDATE;
                }
                
                if (event_type) {
                    engine->event_callback(upd->node_id, upd->node_type,
                                         upd->ip_address, upd->gossip_port, upd->data_port,
                                         event_type, engine->event_callback_data);
                }
            }
        }
    }
    
    // Send ACK
    gossip_message_t ack_msg = {
        .version = 1,
        .msg_type = GOSSIP_MSG_ACK,
        .flags = 0,
        .sender_id = engine->my_id,
        .sequence_num = __sync_fetch_and_add(&engine->sequence_num, 1),
        .num_updates = 0
    };
    
    ack_msg.num_updates = update_queue_pop_batch(&engine->update_queue,
                                                  ack_msg.updates,
                                                  GOSSIP_MAX_PIGGYBACK_UPDATES);
    
    uint8_t buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    ssize_t msg_size = gossip_message_serialize(&ack_msg, buffer, sizeof(buffer));
    if (msg_size > 0) {
        gossip_send_udp(engine->udp_socket, buffer, msg_size, src_ip, src_port);
        LOG_DEBUG("Sent ACK to node %u (%s:%u)", msg->sender_id, src_ip, src_port);
    }
}

static void handle_ack_message(gossip_engine_t *engine,
                              const gossip_message_t *msg,
                              const char *src_ip,
                              uint16_t src_port) {
    LOG_DEBUG("Received ACK from node %u (%s:%u)", 
              msg->sender_id, src_ip, src_port);
    
    remove_pending_ack(engine, msg->sender_id);
    
    // Check if sender was suspect - AVOID lock upgrade deadlock
    cluster_member_t *member = cluster_view_get(engine->cluster_view, msg->sender_id);
    int was_suspect = 0;
    uint64_t incarnation = 0;
    node_type_t node_type = NODE_TYPE_UNKNOWN;
    char ip[MAX_IP_LEN] = {0};
    uint16_t gossip_port = 0;
    uint16_t data_port = 0;
    
    if (member) {
        was_suspect = (member->status == NODE_STATUS_SUSPECT);
        incarnation = member->incarnation;
        node_type = member->node_type;
        safe_strncpy(ip, member->ip_address, MAX_IP_LEN);
        gossip_port = member->gossip_port;
        data_port = member->data_port;
        cluster_view_release(engine->cluster_view);  // Release before update!
    }
    
    if (was_suspect) {
        LOG_INFO("Node %u recovered (was SUSPECT, now ALIVE)", msg->sender_id);
        cluster_view_update_status(engine->cluster_view, msg->sender_id,
                                  NODE_STATUS_ALIVE, incarnation);
        
        if (engine->event_callback) {
            engine->event_callback(msg->sender_id, node_type, ip, gossip_port, data_port,
                                 MEMBER_EVENT_UPDATE, engine->event_callback_data);
        }
    }
    
    // Process piggyback updates - same deadlock avoidance
    for (uint8_t i = 0; i < msg->num_updates; i++) {
        const gossip_member_update_t *upd = &msg->updates[i];
        
        cluster_member_t *existing = cluster_view_get(engine->cluster_view, upd->node_id);
        int should_update = 0;
        
        if (existing) {
            should_update = (upd->incarnation > existing->incarnation);
            cluster_view_release(engine->cluster_view);  // Release!
        }
        
        if (should_update) {
            cluster_view_update_status(engine->cluster_view, upd->node_id,
                                      upd->status, upd->incarnation);
            LOG_DEBUG("Updated node %u from piggyback (status=%d)", 
                      upd->node_id, upd->status);
        }
    }
}

static void handle_suspect_message(gossip_engine_t *engine,
                                   const gossip_message_t *msg,
                                   const char *src_ip,
                                   uint16_t src_port) {
    (void)src_ip;
    (void)src_port;
    
    LOG_DEBUG("Received SUSPECT from node %u", msg->sender_id);
    
    for (uint8_t i = 0; i < msg->num_updates; i++) {
        const gossip_member_update_t *upd = &msg->updates[i];
        
        if (upd->node_id == engine->my_id && upd->status == NODE_STATUS_SUSPECT) {
            // Someone suspects US - refute with higher incarnation
            engine->incarnation++;
            
            LOG_WARN("Refuting suspicion (node %u suspected us), new incarnation=%lu",
                     msg->sender_id, engine->incarnation);
            
            gossip_member_update_t alive_update = {
                .node_id = engine->my_id,
                .node_type = engine->my_type,
                .status = NODE_STATUS_ALIVE,
                .incarnation = engine->incarnation,
                .gossip_port = engine->gossip_port,
                .data_port = engine->data_port,
                .timestamp_ms = time_now_ms()
            };
            safe_strncpy(alive_update.ip_address, engine->my_ip, MAX_IP_LEN);
            
            for (int j = 0; j < 3; j++) {
                update_queue_push(&engine->update_queue, &alive_update);
            }
        } else if (upd->node_id != engine->my_id && upd->status == NODE_STATUS_SUSPECT) {  // FIX: Only suspect others
            // Check if we should mark as suspect - AVOID deadlock
            cluster_member_t *existing = cluster_view_get(engine->cluster_view, upd->node_id);
            int should_suspect = 0;
            
            if (existing) {
                should_suspect = (upd->incarnation >= existing->incarnation && 
                                 existing->status == NODE_STATUS_ALIVE);
                cluster_view_release(engine->cluster_view);
            }
            
            if (should_suspect) {
                cluster_view_update_status(engine->cluster_view, upd->node_id,
                                          NODE_STATUS_SUSPECT, upd->incarnation);
                
                LOG_INFO("Marking node %u as SUSPECT (based on gossip from %u)",
                         upd->node_id, msg->sender_id);
                
                update_queue_push(&engine->update_queue, upd);
            }
        }
    }
}

static void handle_alive_message(gossip_engine_t *engine,
                                const gossip_message_t *msg,
                                const char *src_ip,
                                uint16_t src_port) {
    (void)src_ip;
    (void)src_port;
    
    LOG_DEBUG("Received ALIVE from node %u", msg->sender_id);
    
    for (uint8_t i = 0; i < msg->num_updates; i++) {
        const gossip_member_update_t *upd = &msg->updates[i];
        
        // Check if should update - AVOID deadlock
        cluster_member_t *existing = cluster_view_get(engine->cluster_view, upd->node_id);
        int should_update = 0;
        
        if (existing) {
            should_update = (upd->incarnation > existing->incarnation);
            cluster_view_release(engine->cluster_view);  // Release!
        }
        
        if (should_update) {
            cluster_view_update_status(engine->cluster_view, upd->node_id,
                                      NODE_STATUS_ALIVE, upd->incarnation);
            
            LOG_INFO("Node %u refuted suspicion (new incarnation=%lu)",
                     upd->node_id, upd->incarnation);
            
            update_queue_push(&engine->update_queue, upd);
        }
    }
}

static void handle_dead_message(gossip_engine_t *engine,
                               const gossip_message_t *msg,
                               const char *src_ip,
                               uint16_t src_port) {
    (void)src_ip;
    (void)src_port;
    
    LOG_DEBUG("Received DEAD from node %u", msg->sender_id);
    
    for (uint8_t i = 0; i < msg->num_updates; i++) {
        const gossip_member_update_t *upd = &msg->updates[i];
        
        // Get member info before updating - AVOID deadlock
        cluster_member_t *existing = cluster_view_get(engine->cluster_view, upd->node_id);
        int should_update = (existing != NULL);
        node_type_t node_type = NODE_TYPE_UNKNOWN;
        char ip[MAX_IP_LEN] = {0};
        uint16_t gossip_port = 0;
        uint16_t data_port = 0;
        
        if (existing) {
            node_type = existing->node_type;
            safe_strncpy(ip, existing->ip_address, MAX_IP_LEN);
            gossip_port = existing->gossip_port;
            data_port = existing->data_port;
            cluster_view_release(engine->cluster_view);  // Release!
        }
        
        if (should_update) {
            cluster_view_update_status(engine->cluster_view, upd->node_id,
                                      NODE_STATUS_DEAD, upd->incarnation);
            
            LOG_INFO("Node %u marked as DEAD", upd->node_id);
            
            if (engine->event_callback) {
                engine->event_callback(upd->node_id, node_type, ip, gossip_port, data_port,
                                     MEMBER_EVENT_LEAVE, engine->event_callback_data);
            }
        }
    }
}

static void handle_join_message(gossip_engine_t *engine,
                               const gossip_message_t *msg,
                               const char *src_ip,
                               uint16_t src_port) {
    LOG_INFO("Received JOIN from node %u (%s:%u)", 
             msg->sender_id, src_ip, src_port);
    
    for (uint8_t i = 0; i < msg->num_updates; i++) {
        const gossip_member_update_t *upd = &msg->updates[i];
        
        if (upd->node_id == msg->sender_id) {
            cluster_member_t member = {
                .node_id = upd->node_id,
                .node_type = upd->node_type,
                .gossip_port = upd->gossip_port,
                .data_port = upd->data_port,
                .status = NODE_STATUS_ALIVE,
                .incarnation = upd->incarnation
            };
            safe_strncpy(member.ip_address, src_ip, MAX_IP_LEN);
        
            cluster_view_add(engine->cluster_view, &member);
            
            LOG_INFO("Added new member %u (%s:%u, type=%d)",
                     upd->node_id, upd->ip_address, upd->gossip_port, upd->node_type);
            
            if (engine->event_callback) {
                engine->event_callback(upd->node_id, upd->node_type,
                                     upd->ip_address, upd->gossip_port, upd->data_port,
                                     MEMBER_EVENT_JOIN, engine->event_callback_data);
            }
            
            update_queue_push(&engine->update_queue, upd);
            
            break;
        }
    }
}

// ============================================================================
// WORKER JOIN HANDLER (NEW)
// ============================================================================

static void handle_worker_join_message(gossip_engine_t *engine,
                                       const gossip_message_t *msg,
                                       const char *src_ip,
                                       uint16_t src_port) {
    LOG_INFO("Received WORKER_JOIN from node %u (%s:%u)", 
             msg->sender_id, src_ip, src_port);
    
    // Only ROUTER nodes handle WORKER_JOIN requests
    if (engine->my_type != NODE_TYPE_ROUTER) {
        LOG_WARN("Non-router received WORKER_JOIN, ignoring");
        return;
    }
    
    // Add worker to cluster view
    if (msg->num_updates > 0) {
        const gossip_member_update_t *upd = &msg->updates[0];
        
        cluster_member_t member = {
            .node_id = upd->node_id,
            .node_type = NODE_TYPE_WORKER,
            .gossip_port = upd->gossip_port,
            .data_port = upd->data_port,
            .status = NODE_STATUS_ALIVE,
            .incarnation = upd->incarnation
        };
        safe_strncpy(member.ip_address, src_ip, MAX_IP_LEN);
        
        cluster_view_add(engine->cluster_view, &member);
        
        LOG_INFO("Worker %u joined cluster", upd->node_id);
        
        // Trigger event callback
        if (engine->event_callback) {
            engine->event_callback(upd->node_id, NODE_TYPE_WORKER,
                                 src_ip, upd->gossip_port, upd->data_port,
                                 MEMBER_EVENT_JOIN, engine->event_callback_data);
        }
        
        // =================================================================
        // FIX: ACTIVELY PROPAGATE join to existing cluster members
        // =================================================================
        
        gossip_member_update_t join_update = *upd;
        safe_strncpy(join_update.ip_address, src_ip, MAX_IP_LEN);
        join_update.timestamp_ms = time_now_ms();
        
        // Queue for background gossip (5x for redundancy)
        for (int i = 0; i < 5; i++) {
            update_queue_push(&engine->update_queue, &join_update);
        }
        
        // Immediate gossip burst to all existing members
        pthread_rwlock_rdlock(&engine->cluster_view->lock);
        
        for (size_t i = 0; i < engine->cluster_view->count; i++) {
            cluster_member_t *peer = &engine->cluster_view->members[i];
            
            // Skip self, joining node, and dead nodes
            if (peer->node_id == engine->my_id || 
                peer->node_id == upd->node_id ||
                peer->status == NODE_STATUS_DEAD) {
                continue;
            }
            
            // Build immediate gossip with join announcement
            gossip_message_t immediate_gossip = {
                .version = 1,
                .msg_type = GOSSIP_MSG_ALIVE,
                .sender_id = engine->my_id,
                .sequence_num = __sync_fetch_and_add(&engine->sequence_num, 1),
                .num_updates = 1
            };
            immediate_gossip.updates[0] = join_update;
            
            // Send UDP gossip immediately (don't wait for next round)
            uint8_t buffer[GOSSIP_MAX_PAYLOAD_SIZE];
            ssize_t msg_size = gossip_message_serialize(&immediate_gossip, 
                                                        buffer, sizeof(buffer));
            if (msg_size > 0) {
                gossip_send_udp(engine->udp_socket, buffer, msg_size,
                               peer->ip_address, peer->gossip_port);
                LOG_INFO("Immediately gossiped Worker %u join to node %u", 
                         upd->node_id, peer->node_id);
            }
        }
        
        pthread_rwlock_unlock(&engine->cluster_view->lock);
        
        LOG_INFO("Join propagation initiated for Worker %u", upd->node_id);
    }
    
    // Build bootstrap response with list of active ROUTERS
    gossip_bootstrap_response_t bootstrap_resp;
    memset(&bootstrap_resp, 0, sizeof(bootstrap_resp));
    
    pthread_rwlock_rdlock(&engine->cluster_view->lock);
    
    for (size_t i = 0; i < engine->cluster_view->count; i++) {
        cluster_member_t *m = &engine->cluster_view->members[i];
        
        if (m->node_type == NODE_TYPE_ROUTER && 
            m->status == NODE_STATUS_ALIVE &&
            bootstrap_resp.num_routers < MAX_CONFIG_ROUTERS) {
            
            bootstrap_resp.routers[bootstrap_resp.num_routers].node_id = m->node_id;
            
            snprintf(bootstrap_resp.routers[bootstrap_resp.num_routers].gossip_addr,
                    MAX_CONFIG_STRING, "%s:%u", m->ip_address, m->gossip_port);
            
            snprintf(bootstrap_resp.routers[bootstrap_resp.num_routers].data_addr,
                    MAX_CONFIG_STRING, "%s:%u", m->ip_address, m->data_port);
            
            bootstrap_resp.num_routers++;
        }
    }
    
    pthread_rwlock_unlock(&engine->cluster_view->lock);
    
    LOG_INFO("Sending JOIN_RESPONSE with %u routers to worker %u", 
             bootstrap_resp.num_routers, msg->sender_id);
    
    // Build JOIN_RESPONSE message
    gossip_message_t response = {
        .version = 1,
        .msg_type = GOSSIP_MSG_JOIN_RESPONSE,
        .sender_id = engine->my_id,
        .sequence_num = __sync_fetch_and_add(&engine->sequence_num, 1),
        .num_updates = 0,
        .flags = 0
    };
    
    uint8_t buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    
    // Serialize gossip message header
    ssize_t msg_size = gossip_message_serialize(&response, buffer, sizeof(buffer));
    
    // Append bootstrap data
    ssize_t bootstrap_size = gossip_serialize_bootstrap_response(&bootstrap_resp, 
                                                                 buffer + msg_size,
                                                                 sizeof(buffer) - msg_size);
    
    if (msg_size > 0 && bootstrap_size > 0) {
        ssize_t total_size = msg_size + bootstrap_size;
        
        if (gossip_send_udp(engine->udp_socket, buffer, total_size, 
                           src_ip, src_port) > 0) {
            LOG_INFO("Sent JOIN_RESPONSE to worker %u (%s:%u) with %u routers", 
                     msg->sender_id, src_ip, src_port, bootstrap_resp.num_routers);
        } else {
            LOG_ERROR("Failed to send JOIN_RESPONSE to worker %u", msg->sender_id);
        }
    } else {
        LOG_ERROR("Failed to serialize JOIN_RESPONSE");
    }
}

// ============================================================================
// MESSAGE DISPATCHER
// ============================================================================

static void dispatch_message(gossip_engine_t *engine,
                            const gossip_message_t *msg,
                            const char *src_ip,
                            uint16_t src_port) {
    
    switch (msg->msg_type) {
        case GOSSIP_MSG_PING:
            handle_ping_message(engine, msg, src_ip, src_port);
            break;
            
        case GOSSIP_MSG_ACK:
            handle_ack_message(engine, msg, src_ip, src_port);
            break;
            
        case GOSSIP_MSG_SUSPECT:
            handle_suspect_message(engine, msg, src_ip, src_port);
            break;
            
        case GOSSIP_MSG_ALIVE:
            handle_alive_message(engine, msg, src_ip, src_port);
            break;
            
        case GOSSIP_MSG_DEAD:
            handle_dead_message(engine, msg, src_ip, src_port);
            break;
            
        case GOSSIP_MSG_JOIN:
            handle_join_message(engine, msg, src_ip, src_port);
            break;
            
        case GOSSIP_MSG_WORKER_JOIN:
            handle_worker_join_message(engine, msg, src_ip, src_port);
            break;
            
        case GOSSIP_MSG_JOIN_RESPONSE:
            // Handled by worker bootstrap code
            LOG_DEBUG("Received JOIN_RESPONSE (handled by caller)");
            break;
            
        case GOSSIP_MSG_LEAVE:
            LOG_INFO("Node %u gracefully leaving", msg->sender_id);
            cluster_view_update_status(engine->cluster_view, msg->sender_id,
                                      NODE_STATUS_DEAD, msg->sequence_num);
            break;
            
        case GOSSIP_MSG_PING_REQ:
            LOG_DEBUG("PING_REQ not yet implemented");
            break;
            
        default:
            LOG_WARN("Unknown gossip message type: %u", msg->msg_type);
            break;
    }
}

// ============================================================================
// UDP LISTENER THREAD - FIXED: Use select() with timeout to prevent blocking
// ============================================================================

static void* gossip_listener_thread(void *arg) {
    gossip_engine_t *engine = (gossip_engine_t*)arg;
    
    LOG_INFO("Gossip UDP listener thread started (fd=%d)", engine->udp_socket);
    
    uint8_t recv_buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    char src_ip[MAX_IP_LEN];
    uint16_t src_port;
    
    // Set up select() for non-blocking receive with timeout
    fd_set read_fds;
    struct timeval tv;
    
    while (!engine->shutdown_flag) {
        FD_ZERO(&read_fds);
        FD_SET(engine->udp_socket, &read_fds);
        
        // Timeout: 50ms - this prevents blocking forever and allows frequent checking
        tv.tv_sec = 0;
        tv.tv_usec = 50000;
        
        int ret = select(engine->udp_socket + 1, &read_fds, NULL, NULL, &tv);
        
        if (ret < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR("select() failed: %s", strerror(errno));
            usleep(10000);
            continue;
        }
        
        if (ret == 0) {
            // Timeout - no data available, continue loop
            continue;
        }
        
        // Data is available, read it
        ssize_t received = gossip_recv_udp(engine->udp_socket, recv_buffer,
                                          sizeof(recv_buffer), src_ip,
                                          sizeof(src_ip), &src_port);
        
        if (received < 0) {
            // EAGAIN/EWOULDBLOCK should not happen after select(), but handle it
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            LOG_ERROR("gossip_recv_udp failed: %s", strerror(errno));
            usleep(10000);
            continue;
        }
        
        if (received == 0) {
            // Connection closed (shouldn't happen with UDP)
            continue;
        }

        LOG_DEBUG("UDP received %zd bytes from %s:%u", received, src_ip, src_port); 
        
        if (received < 16) {
            LOG_WARN("Received malformed gossip packet (too small: %zd bytes)", received);
            continue;
        }
        
        gossip_message_t msg;
        if (gossip_message_deserialize(recv_buffer, received, &msg) != 0) {
            LOG_WARN("Failed to deserialize gossip message from %s:%u", src_ip, src_port);
            continue;
        }
        
        if (msg.sender_id == engine->my_id) {
            LOG_DEBUG("Ignoring message from self");
            continue;
        }
        
        LOG_DEBUG("Received %s from node %u (%s:%u, seq=%lu, updates=%u)",
                  (msg.msg_type == GOSSIP_MSG_PING) ? "PING" :
                  (msg.msg_type == GOSSIP_MSG_ACK) ? "ACK" :
                  (msg.msg_type == GOSSIP_MSG_SUSPECT) ? "SUSPECT" :
                  (msg.msg_type == GOSSIP_MSG_ALIVE) ? "ALIVE" :
                  (msg.msg_type == GOSSIP_MSG_DEAD) ? "DEAD" :
                  (msg.msg_type == GOSSIP_MSG_JOIN) ? "JOIN" :
                  (msg.msg_type == GOSSIP_MSG_WORKER_JOIN) ? "WORKER_JOIN" :
                  (msg.msg_type == GOSSIP_MSG_JOIN_RESPONSE) ? "JOIN_RESPONSE" :
                  (msg.msg_type == GOSSIP_MSG_LEAVE) ? "LEAVE" : "UNKNOWN",
                  msg.sender_id, src_ip, src_port, msg.sequence_num, msg.num_updates);
        
        // CRITICAL: Process message immediately without blocking
        dispatch_message(engine, &msg, src_ip, src_port);
    }
    
    LOG_INFO("Gossip UDP listener thread stopped");
    return NULL;
}

// ============================================================================
// PROTOCOL LOOP HELPERS
// ============================================================================

static node_id_t select_random_peer(gossip_engine_t *engine) {
    node_id_t peer_list[MAX_CLUSTER_NODES];
    size_t peer_count = 0;
    
    pthread_rwlock_rdlock(&engine->cluster_view->lock);
    
    for (size_t i = 0; i < engine->cluster_view->count; i++) {
        cluster_member_t *member = &engine->cluster_view->members[i];
        
        // CRITICAL FIX: Include SUSPECT nodes in PING selection!
        // Only exclude ourselves and DEAD nodes
        if (member->node_id == engine->my_id || 
            member->status == NODE_STATUS_DEAD) {
            continue;
        }
        
        if (peer_count < MAX_CLUSTER_NODES) {
            peer_list[peer_count++] = member->node_id;
        }
    }
    
    pthread_rwlock_unlock(&engine->cluster_view->lock);
    
    if (peer_count == 0) {
        return 0;
    }
    
    size_t random_index = rand() % peer_count;
    return peer_list[random_index];
}

static int send_ping_to_node(gossip_engine_t *engine, node_id_t target_node) {
    cluster_member_t *target = cluster_view_get(engine->cluster_view, target_node);
    if (!target) {
        LOG_WARN("Cannot ping node %u - not in cluster view", target_node);
        return -1;
    }
    
    char target_ip[MAX_IP_LEN];
    uint16_t target_gossip_port = target->gossip_port;
    safe_strncpy(target_ip, target->ip_address, MAX_IP_LEN);
    
    cluster_view_release(engine->cluster_view);
    
    gossip_message_t ping_msg = {
        .version = 1,
        .msg_type = GOSSIP_MSG_PING,
        .flags = 0,
        .sender_id = engine->my_id,
        .sequence_num = __sync_fetch_and_add(&engine->sequence_num, 1),
        .num_updates = 0
    };
    
    ping_msg.num_updates = update_queue_pop_batch(&engine->update_queue,
                                                   ping_msg.updates,
                                                   engine->config.max_piggyback);
    
    uint8_t buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    ssize_t msg_size = gossip_message_serialize(&ping_msg, buffer, sizeof(buffer));
    if (msg_size < 0) {
        LOG_ERROR("Failed to serialize PING message");
        return -1;
    }
    
    if (gossip_send_udp(engine->udp_socket, buffer, msg_size, 
                       target_ip, target_gossip_port) < 0) {
        LOG_WARN("Failed to send PING to node %u (%s:%u)", 
                 target_node, target_ip, target_gossip_port);
        return -1;
    }
    
    LOG_DEBUG("Sent PING to node %u (%s:%u, seq=%lu, updates=%u)",
              target_node, target_ip, target_gossip_port, 
              ping_msg.sequence_num, ping_msg.num_updates);
    
    add_pending_ack(engine, target_node);
    
    return 0;
}

static void check_suspect_timeouts(gossip_engine_t *engine) {
    uint64_t now = time_now_ms();
    
    pthread_rwlock_rdlock(&engine->cluster_view->lock);
    
    for (size_t i = 0; i < engine->cluster_view->count; i++) {
        cluster_member_t *member = &engine->cluster_view->members[i];
        
        if (member->status != NODE_STATUS_SUSPECT) {
            continue;
        }
        
        uint64_t elapsed = now - member->last_seen_ms;
        
        if (elapsed > engine->config.dead_timeout_ms) {
            node_id_t node_id = member->node_id;
            node_type_t node_type = member->node_type;
            char ip[MAX_IP_LEN];
            uint16_t gossip_port = member->gossip_port;
            uint16_t data_port = member->data_port;
            safe_strncpy(ip, member->ip_address, MAX_IP_LEN);
            
            pthread_rwlock_unlock(&engine->cluster_view->lock);
            
            LOG_ERROR("Node %u suspected for %lums, marking as DEAD", 
                     node_id, elapsed);
            
            cluster_view_update_status(engine->cluster_view, node_id,
                                      NODE_STATUS_DEAD, member->incarnation);
            
            gossip_member_update_t dead_update = {
                .node_id = node_id,
                .node_type = node_type,
                .status = NODE_STATUS_DEAD,
                .incarnation = member->incarnation,
                .timestamp_ms = now
            };
            safe_strncpy(dead_update.ip_address, ip, MAX_IP_LEN);
            
            update_queue_push(&engine->update_queue, &dead_update);
            
            if (engine->event_callback) {
                engine->event_callback(node_id, node_type, ip, gossip_port, data_port,
                                     MEMBER_EVENT_LEAVE, 
                                     engine->event_callback_data);
            }
            
            pthread_rwlock_rdlock(&engine->cluster_view->lock);
        }
    }
    
    pthread_rwlock_unlock(&engine->cluster_view->lock);
}

// ============================================================================
// GOSSIP TO RANDOM PEERS (Piggybacking state updates)
// ============================================================================

static void gossip_to_random_peers(gossip_engine_t *engine) {
    node_id_t peer_list[MAX_CLUSTER_NODES];
    size_t peer_count = 0;
    
    // Build list of potential gossip targets (exclude self and dead nodes)
    pthread_rwlock_rdlock(&engine->cluster_view->lock);
    
    for (size_t i = 0; i < engine->cluster_view->count; i++) {
        cluster_member_t *member = &engine->cluster_view->members[i];
        
        if (member->node_id == engine->my_id || 
            member->status == NODE_STATUS_DEAD) {
            continue;
        }
        
        if (peer_count < MAX_CLUSTER_NODES) {
            peer_list[peer_count++] = member->node_id;
        }
    }
    
    pthread_rwlock_unlock(&engine->cluster_view->lock);
    
    // No peers to gossip to
    if (peer_count == 0) {
        return;
    }
    
    // Determine fanout (how many peers to gossip to)
    size_t fanout = (peer_count < engine->config.fanout) ? 
                    peer_count : engine->config.fanout;
    
    // Select random peers using Fisher-Yates shuffle
    for (size_t i = 0; i < fanout; i++) {
        size_t j = i + (rand() % (peer_count - i));
        node_id_t temp = peer_list[i];
        peer_list[i] = peer_list[j];
        peer_list[j] = temp;
        
        // Get peer information
        cluster_member_t *peer = cluster_view_get(engine->cluster_view, peer_list[i]);
        if (!peer) continue;
        
        char peer_ip[MAX_IP_LEN];
        uint16_t peer_gossip_port = peer->gossip_port;
        safe_strncpy(peer_ip, peer->ip_address, MAX_IP_LEN);
        
        cluster_view_release(engine->cluster_view);
        
        // Build gossip message
        gossip_message_t gossip_msg = {
            .version = 1,
            .msg_type = GOSSIP_MSG_SUSPECT,  // Generic gossip message type
            .flags = 0,
            .sender_id = engine->my_id,
            .sequence_num = __sync_fetch_and_add(&engine->sequence_num, 1),
            .num_updates = 0
        };
        
        // Pop updates from queue to piggyback
        gossip_msg.num_updates = update_queue_pop_batch(&engine->update_queue,
                                                        gossip_msg.updates,
                                                        engine->config.max_piggyback);
        
        // FIX: Only gossip if we have real updates
        // Don't create artificial keepalive updates - let PING/ACK handle liveness
        if (gossip_msg.num_updates == 0) {
            continue;  // Skip this peer if no state changes to propagate
        }
        
        // Serialize and send
        uint8_t buffer[GOSSIP_MAX_PAYLOAD_SIZE];
        ssize_t msg_size = gossip_message_serialize(&gossip_msg, buffer, sizeof(buffer));
        if (msg_size > 0) {
            gossip_send_udp(engine->udp_socket, buffer, msg_size, 
                           peer_ip, peer_gossip_port);
            LOG_DEBUG("Gossiped %u updates to node %u", 
                     gossip_msg.num_updates, peer_list[i]);
        }
    }
}

static void update_self_in_cluster_view(gossip_engine_t *engine) {
    cluster_member_t *self = cluster_view_get(engine->cluster_view, engine->my_id);
    if (self) {
        self->last_seen_ms = time_now_ms();
        self->incarnation = engine->incarnation;
        self->status = NODE_STATUS_ALIVE;
        cluster_view_release(engine->cluster_view);
    }
}

// ============================================================================
// SWIM PROTOCOL LOOP THREAD
// ============================================================================

static void* gossip_protocol_thread(void *arg) {
    gossip_engine_t *engine = (gossip_engine_t*)arg;
    
    LOG_INFO("SWIM protocol thread started (period=%ums)", 
             engine->config.protocol_period_ms);
    
    uint64_t round = 0;
    
    while (!engine->shutdown_flag) {
        round++;
        
        LOG_DEBUG("=== SWIM Protocol Round %lu ===", round);
        
        update_self_in_cluster_view(engine);
        
        node_id_t target = select_random_peer(engine);
        if (target != 0) {
            send_ping_to_node(engine, target);
        } else {
            LOG_DEBUG("No peers available for PING");
        }
        
        check_ack_timeouts(engine);
        
        check_suspect_timeouts(engine);
        
        if (round % 3 == 0) {
            gossip_to_random_peers(engine);
        }
        
        if (round % 10 == 0) {
            pthread_rwlock_rdlock(&engine->cluster_view->lock);
            size_t alive_count = 0, suspect_count = 0, dead_count = 0;
            
            for (size_t i = 0; i < engine->cluster_view->count; i++) {
                switch (engine->cluster_view->members[i].status) {
                    case NODE_STATUS_ALIVE: alive_count++; break;
                    case NODE_STATUS_SUSPECT: suspect_count++; break;
                    case NODE_STATUS_DEAD: dead_count++; break;
                    default: break;
                }
            }
            
            pthread_rwlock_unlock(&engine->cluster_view->lock);
            
            LOG_INFO("Cluster state: %zu members (%zu alive, %zu suspect, %zu dead)",
                     engine->cluster_view->count, alive_count, suspect_count, dead_count);
        }
        
        usleep(engine->config.protocol_period_ms * 1000);
    }
    
    LOG_INFO("SWIM protocol thread stopped");
    return NULL;
}

// ============================================================================
// ENGINE START/STOP
// ============================================================================

int gossip_engine_start(gossip_engine_t *engine) {
    if (!engine) {
        return -1;
    }
    
    if (pthread_create(&engine->listener_thread, NULL, 
                      gossip_listener_thread, engine) != 0) {
        LOG_ERROR("Failed to start gossip listener thread");
        return -1;
    }
    
    if (pthread_create(&engine->protocol_thread, NULL,
                      gossip_protocol_thread, engine) != 0) {
        LOG_ERROR("Failed to start SWIM protocol thread");
        engine->shutdown_flag = 1;
        pthread_join(engine->listener_thread, NULL);
        return -1;
    }
    
    LOG_INFO("Gossip engine started (listener + protocol threads)");
    return 0;
}

void gossip_engine_shutdown(gossip_engine_t *engine) {
    if (!engine) return;
    
    LOG_INFO("Shutting down gossip engine");
    
    engine->shutdown_flag = 1;
    
    pthread_join(engine->listener_thread, NULL);
    pthread_join(engine->protocol_thread, NULL);
    
    if (engine->udp_socket >= 0) {
        close(engine->udp_socket);
        engine->udp_socket = -1;
    }
    
    pthread_mutex_destroy(&engine->pending_acks_lock);
    update_queue_destroy(&engine->update_queue);
    
    free(engine);
    
    LOG_INFO("Gossip engine shutdown complete");
}

// ============================================================================
// PUBLIC API FUNCTIONS
// ============================================================================

int gossip_engine_add_seed(gossip_engine_t *engine, const char *ip, uint16_t gossip_port) {
    if (!engine || !ip) {
        return -1;
    }
    
    LOG_INFO("Adding seed node: %s:%u", ip, gossip_port);
    
    gossip_message_t join_msg = {
        .version = 1,
        .msg_type = GOSSIP_MSG_JOIN,
        .flags = 0,
        .sender_id = engine->my_id,
        .sequence_num = __sync_fetch_and_add(&engine->sequence_num, 1),
        .num_updates = 1
    };
    
    join_msg.updates[0].node_id = engine->my_id;
    join_msg.updates[0].node_type = engine->my_type;
    join_msg.updates[0].status = NODE_STATUS_ALIVE;
    join_msg.updates[0].incarnation = engine->incarnation;
    join_msg.updates[0].gossip_port = engine->gossip_port;
    join_msg.updates[0].data_port = engine->data_port;
    join_msg.updates[0].timestamp_ms = time_now_ms();
    safe_strncpy(join_msg.updates[0].ip_address, engine->my_ip, MAX_IP_LEN);
    
    uint8_t buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    ssize_t msg_size = gossip_message_serialize(&join_msg, buffer, sizeof(buffer));
    if (msg_size > 0) {
        gossip_send_udp(engine->udp_socket, buffer, msg_size, ip, gossip_port);
        LOG_INFO("Sent JOIN to seed %s:%u", ip, gossip_port);
        return 0;
    }
    
    LOG_ERROR("Failed to send JOIN to seed");
    return -1;
}

int gossip_engine_announce_join(gossip_engine_t *engine) {
    if (!engine) {
        return -1;
    }
    
    LOG_INFO("Announcing join to cluster");
    
    cluster_member_t self = {
        .node_id = engine->my_id,
        .node_type = engine->my_type,
        .gossip_port = engine->gossip_port,
        .data_port = engine->data_port,
        .status = NODE_STATUS_ALIVE,
        .incarnation = engine->incarnation,
        .last_seen_ms = time_now_ms()
    };
    safe_strncpy(self.ip_address, engine->my_ip, MAX_IP_LEN);
    
    cluster_view_add(engine->cluster_view, &self);
    
    gossip_member_update_t join_update = {
        .node_id = engine->my_id,
        .node_type = engine->my_type,
        .status = NODE_STATUS_ALIVE,
        .incarnation = engine->incarnation,
        .gossip_port = engine->gossip_port,
        .data_port = engine->data_port,
        .timestamp_ms = time_now_ms()
    };
    safe_strncpy(join_update.ip_address, engine->my_ip, MAX_IP_LEN);
    
    for (int i = 0; i < 5; i++) {
        update_queue_push(&engine->update_queue, &join_update);
    }
    
    return 0;
}

int gossip_engine_leave(gossip_engine_t *engine) {
    if (!engine) {
        return -1;
    }
    
    LOG_INFO("Announcing graceful leave to cluster");
    
    gossip_message_t leave_msg = {
        .version = 1,
        .msg_type = GOSSIP_MSG_LEAVE,
        .flags = 0,
        .sender_id = engine->my_id,
        .sequence_num = __sync_fetch_and_add(&engine->sequence_num, 1),
        .num_updates = 1
    };
    
    leave_msg.updates[0].node_id = engine->my_id;
    leave_msg.updates[0].status = NODE_STATUS_DEAD;
    leave_msg.updates[0].incarnation = engine->incarnation;
    leave_msg.updates[0].timestamp_ms = time_now_ms();
    
    pthread_rwlock_rdlock(&engine->cluster_view->lock);
    
    for (size_t i = 0; i < engine->cluster_view->count; i++) {
        cluster_member_t *member = &engine->cluster_view->members[i];
        
        if (member->node_id == engine->my_id) continue;
        
        uint16_t peer_gossip_port = member->gossip_port;
        
        uint8_t buffer[GOSSIP_MAX_PAYLOAD_SIZE];
        ssize_t msg_size = gossip_message_serialize(&leave_msg, buffer, sizeof(buffer));
        if (msg_size > 0) {
            gossip_send_udp(engine->udp_socket, buffer, msg_size,
                           member->ip_address, peer_gossip_port);
        }
    }
    
    pthread_rwlock_unlock(&engine->cluster_view->lock);
    
    LOG_INFO("LEAVE announced to all peers");
    return 0;
}

void gossip_engine_set_callback(gossip_engine_t *engine,
                                member_event_cb callback,
                                void *user_data) {
    if (!engine) return;
    
    engine->event_callback = callback;
    engine->event_callback_data = user_data;
    
    LOG_DEBUG("Gossip engine callback updated");
}