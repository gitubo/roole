// src/cluster/gossip_engine.c - SWIM gossip protocol engine

#define _POSIX_C_SOURCE 200809L

#include "roole/gossip.h"
#include "roole/common.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

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

// Pending ACK tracking (for PING timeout detection)
typedef struct pending_ack {
    node_id_t target_node;
    uint64_t ping_sent_ms;
    int active;
} pending_ack_t;

// Update queue (for piggybacking state changes)
typedef struct update_queue {
    gossip_member_update_t updates[MAX_UPDATE_QUEUE];
    size_t head;
    size_t tail;
    size_t count;
    pthread_mutex_t lock;
} update_queue_t;

// Gossip engine state
struct gossip_engine {
    // Identity
    node_id_t my_id;
    node_type_t my_type;
    char my_ip[MAX_IP_LEN];
    uint16_t gossip_port;
    uint16_t service_port;
    uint64_t incarnation;        // Our current incarnation number
    uint64_t sequence_num;       // Message sequence counter
    
    // Configuration
    gossip_config_t config;
    
    // Network
    int udp_socket;
    
    // Shared cluster view
    cluster_view_t *cluster_view;
    
    // Event callback
    member_event_cb event_callback;
    void *event_callback_data;
    
    // Protocol state
    pending_ack_t pending_acks[MAX_PENDING_ACKS];
    pthread_mutex_t pending_acks_lock;
    
    update_queue_t update_queue;
    
    // Threads
    pthread_t protocol_thread;   // SWIM protocol loop
    pthread_t listener_thread;   // UDP listener
    
    // Control
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
    
    // Find free slot
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
            
            // Mark as SUSPECT in cluster view
            cluster_view_update_status(engine->cluster_view, node, 
                                      NODE_STATUS_SUSPECT, 0);
            
            // Add SUSPECT update to queue for propagation
            gossip_member_update_t update = {
                .node_id = node,
                .status = NODE_STATUS_SUSPECT,
                .timestamp_ms = now
            };
            update_queue_push(&engine->update_queue, &update);
            
            // Remove from pending
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
    uint16_t service_port,
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
    engine->service_port = service_port;
    engine->incarnation = 0;
    engine->sequence_num = 0;
    engine->cluster_view = cluster_view;
    engine->event_callback = event_callback;
    engine->event_callback_data = user_data;
    engine->shutdown_flag = 0;
    
    // Use provided config or defaults
    if (config) {
        engine->config = *config;
    } else {
        engine->config = gossip_default_config();
    }
    
    // Initialize update queue
    if (update_queue_init(&engine->update_queue) != 0) {
        LOG_ERROR("Failed to initialize update queue");
        free(engine);
        return NULL;
    }
    
    // Initialize pending ACKs lock
    if (pthread_mutex_init(&engine->pending_acks_lock, NULL) != 0) {
        LOG_ERROR("Failed to initialize pending ACKs lock");
        update_queue_destroy(&engine->update_queue);
        free(engine);
        return NULL;
    }
    
    // Create UDP socket
    engine->udp_socket = gossip_create_udp_socket(bind_addr, gossip_port);
    if (engine->udp_socket < 0) {
        LOG_ERROR("Failed to create gossip UDP socket");
        pthread_mutex_destroy(&engine->pending_acks_lock);
        update_queue_destroy(&engine->update_queue);
        free(engine);
        return NULL;
    }
    
    LOG_INFO("Gossip engine initialized (node_id=%u, type=%d, port=%u)",
             my_id, my_type, gossip_port);
    
    return engine;
}

// ============================================================================
// MESSAGE HANDLING
// ============================================================================

/**
 * Handle incoming PING message
 */
static void handle_ping_message(gossip_engine_t *engine, 
                               const gossip_message_t *msg,
                               const char *src_ip, 
                               uint16_t src_port) {
    LOG_DEBUG("Received PING from node %u (%s:%u)", 
              msg->sender_id, src_ip, src_port);
    
    // Process any piggyback updates
    for (uint8_t i = 0; i < msg->num_updates; i++) {
        const gossip_member_update_t *upd = &msg->updates[i];
        
        // Update cluster view
        cluster_member_t member = {
            .node_id = upd->node_id,
            .node_type = upd->node_type,
            .port = upd->service_port,
            .status = upd->status,
            .incarnation = upd->incarnation
        };
        safe_strncpy(member.ip_address, upd->ip_address, MAX_IP_LEN);
        
        // Check if this is newer information
        cluster_member_t *existing = cluster_view_get(engine->cluster_view, upd->node_id);
        if (existing) {
            if (upd->incarnation > existing->incarnation) {
                cluster_view_update_status(engine->cluster_view, upd->node_id,
                                          upd->status, upd->incarnation);
                
                LOG_INFO("Updated node %u status to %d (incarnation %lu)",
                         upd->node_id, upd->status, upd->incarnation);
                
                // Trigger event callback if status changed
                if (upd->status != existing->status && engine->event_callback) {
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
                                             upd->ip_address, upd->service_port,
                                             event_type, engine->event_callback_data);
                    }
                }
            }
            cluster_view_release(engine->cluster_view);
        } else {
            // New member
            cluster_view_add(engine->cluster_view, &member);
            
            LOG_INFO("Discovered new member %u (%s:%u, type=%d)",
                     upd->node_id, upd->ip_address, upd->service_port, upd->node_type);
            
            if (engine->event_callback) {
                engine->event_callback(upd->node_id, upd->node_type,
                                     upd->ip_address, upd->service_port,
                                     MEMBER_EVENT_JOIN, engine->event_callback_data);
            }
        }
    }
    
    // Send ACK response
    gossip_message_t ack_msg = {
        .version = 1,
        .msg_type = GOSSIP_MSG_ACK,
        .flags = 0,
        .sender_id = engine->my_id,
        .sequence_num = __sync_fetch_and_add(&engine->sequence_num, 1),
        .num_updates = 0
    };
    
    // Piggyback some updates on ACK (if available)
    ack_msg.num_updates = update_queue_pop_batch(&engine->update_queue,
                                                  ack_msg.updates,
                                                  GOSSIP_MAX_PIGGYBACK_UPDATES);
    
    // Serialize and send
    uint8_t buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    ssize_t msg_size = gossip_message_serialize(&ack_msg, buffer, sizeof(buffer));
    if (msg_size > 0) {
        gossip_send_udp(engine->udp_socket, buffer, msg_size, src_ip, src_port);
        LOG_DEBUG("Sent ACK to node %u (%s:%u)", msg->sender_id, src_ip, src_port);
    } else {
        LOG_ERROR("Failed to serialize ACK message");
    }
}

/**
 * Handle incoming ACK message
 */
static void handle_ack_message(gossip_engine_t *engine,
                              const gossip_message_t *msg,
                              const char *src_ip,
                              uint16_t src_port) {
    LOG_DEBUG("Received ACK from node %u (%s:%u)", 
              msg->sender_id, src_ip, src_port);
    
    // Remove from pending ACKs (node is alive)
    remove_pending_ack(engine, msg->sender_id);
    
    // Update node status to ALIVE if it was SUSPECT
    cluster_member_t *member = cluster_view_get(engine->cluster_view, msg->sender_id);
    if (member && member->status == NODE_STATUS_SUSPECT) {
        LOG_INFO("Node %u recovered (was SUSPECT, now ALIVE)", msg->sender_id);
        cluster_view_update_status(engine->cluster_view, msg->sender_id,
                                  NODE_STATUS_ALIVE, member->incarnation);
        
        if (engine->event_callback) {
            engine->event_callback(msg->sender_id, member->node_type,
                                 member->ip_address, member->port,
                                 MEMBER_EVENT_UPDATE, engine->event_callback_data);
        }
    }
    if (member) {
        cluster_view_release(engine->cluster_view);
    }
    
    // Process piggyback updates
    for (uint8_t i = 0; i < msg->num_updates; i++) {
        const gossip_member_update_t *upd = &msg->updates[i];
        
        cluster_member_t *existing = cluster_view_get(engine->cluster_view, upd->node_id);
        if (existing) {
            if (upd->incarnation > existing->incarnation) {
                cluster_view_update_status(engine->cluster_view, upd->node_id,
                                          upd->status, upd->incarnation);
                LOG_DEBUG("Updated node %u from piggyback (status=%d)", 
                          upd->node_id, upd->status);
            }
            cluster_view_release(engine->cluster_view);
        }
    }
}

/**
 * Handle incoming SUSPECT message
 */
static void handle_suspect_message(gossip_engine_t *engine,
                                   const gossip_message_t *msg,
                                   const char *src_ip,
                                   uint16_t src_port) {
    (void)src_ip;
    (void)src_port;
    
    LOG_DEBUG("Received SUSPECT from node %u", msg->sender_id);
    
    // Process piggyback updates (which should contain the SUSPECT announcement)
    for (uint8_t i = 0; i < msg->num_updates; i++) {
        const gossip_member_update_t *upd = &msg->updates[i];
        
        if (upd->node_id == engine->my_id && upd->status == NODE_STATUS_SUSPECT) {
            // Someone suspects US! Refute by incrementing incarnation
            engine->incarnation++;
            
            LOG_WARN("Refuting suspicion (node %u suspected us), new incarnation=%lu",
                     msg->sender_id, engine->incarnation);
            
            // Broadcast ALIVE message with new incarnation
            gossip_member_update_t alive_update = {
                .node_id = engine->my_id,
                .node_type = engine->my_type,
                .status = NODE_STATUS_ALIVE,
                .incarnation = engine->incarnation,
                .gossip_port = engine->gossip_port,
                .service_port = engine->service_port,
                .timestamp_ms = time_now_ms()
            };
            safe_strncpy(alive_update.ip_address, engine->my_ip, MAX_IP_LEN);
            
            // Add to update queue with high priority (push to front conceptually)
            for (int j = 0; j < 3; j++) {  // Add multiple times for faster propagation
                update_queue_push(&engine->update_queue, &alive_update);
            }
            
        } else {
            // Someone else is suspected
            cluster_member_t *existing = cluster_view_get(engine->cluster_view, upd->node_id);
            if (existing) {
                if (upd->incarnation >= existing->incarnation && 
                    existing->status == NODE_STATUS_ALIVE) {
                    
                    cluster_view_update_status(engine->cluster_view, upd->node_id,
                                              NODE_STATUS_SUSPECT, upd->incarnation);
                    
                    LOG_INFO("Marking node %u as SUSPECT (based on gossip from %u)",
                             upd->node_id, msg->sender_id);
                    
                    // Propagate this suspicion
                    update_queue_push(&engine->update_queue, upd);
                }
                cluster_view_release(engine->cluster_view);
            }
        }
    }
}

/**
 * Handle incoming ALIVE message (refutation)
 */
static void handle_alive_message(gossip_engine_t *engine,
                                const gossip_message_t *msg,
                                const char *src_ip,
                                uint16_t src_port) {
    (void)src_ip;
    (void)src_port;
    
    LOG_DEBUG("Received ALIVE from node %u", msg->sender_id);
    
    // Process piggyback updates
    for (uint8_t i = 0; i < msg->num_updates; i++) {
        const gossip_member_update_t *upd = &msg->updates[i];
        
        cluster_member_t *existing = cluster_view_get(engine->cluster_view, upd->node_id);
        if (existing) {
            // ALIVE refutation wins if incarnation is higher
            if (upd->incarnation > existing->incarnation) {
                cluster_view_update_status(engine->cluster_view, upd->node_id,
                                          NODE_STATUS_ALIVE, upd->incarnation);
                
                LOG_INFO("Node %u refuted suspicion (new incarnation=%lu)",
                         upd->node_id, upd->incarnation);
                
                // Propagate the refutation
                update_queue_push(&engine->update_queue, upd);
            }
            cluster_view_release(engine->cluster_view);
        }
    }
}

/**
 * Handle incoming DEAD message
 */
static void handle_dead_message(gossip_engine_t *engine,
                               const gossip_message_t *msg,
                               const char *src_ip,
                               uint16_t src_port) {
    (void)src_ip;
    (void)src_port;
    
    LOG_DEBUG("Received DEAD from node %u", msg->sender_id);
    
    // Process piggyback updates
    for (uint8_t i = 0; i < msg->num_updates; i++) {
        const gossip_member_update_t *upd = &msg->updates[i];
        
        cluster_member_t *existing = cluster_view_get(engine->cluster_view, upd->node_id);
        if (existing) {
            // Mark as DEAD
            cluster_view_update_status(engine->cluster_view, upd->node_id,
                                      NODE_STATUS_DEAD, upd->incarnation);
            
            LOG_INFO("Node %u marked as DEAD", upd->node_id);
            
            // Trigger event callback
            if (engine->event_callback) {
                engine->event_callback(upd->node_id, upd->node_type,
                                     upd->ip_address, upd->service_port,
                                     MEMBER_EVENT_LEAVE, engine->event_callback_data);
            }
            
            cluster_view_release(engine->cluster_view);
        }
    }
}

/**
 * Handle incoming JOIN message
 */
static void handle_join_message(gossip_engine_t *engine,
                               const gossip_message_t *msg,
                               const char *src_ip,
                               uint16_t src_port) {
    LOG_INFO("Received JOIN from node %u (%s:%u)", 
             msg->sender_id, src_ip, src_port);
    
    // Process piggyback updates (should contain the joining member's info)
    for (uint8_t i = 0; i < msg->num_updates; i++) {
        const gossip_member_update_t *upd = &msg->updates[i];
        
        if (upd->node_id == msg->sender_id) {
            cluster_member_t member = {
                .node_id = upd->node_id,
                .node_type = upd->node_type,
                .port = upd->service_port,
                .status = NODE_STATUS_ALIVE,
                .incarnation = upd->incarnation
            };
            safe_strncpy(member.ip_address, upd->ip_address, MAX_IP_LEN);
            
            cluster_view_add(engine->cluster_view, &member);
            
            LOG_INFO("Added new member %u (%s:%u, type=%d)",
                     upd->node_id, upd->ip_address, upd->service_port, upd->node_type);
            
            // Trigger join event
            if (engine->event_callback) {
                engine->event_callback(upd->node_id, upd->node_type,
                                     upd->ip_address, upd->service_port,
                                     MEMBER_EVENT_JOIN, engine->event_callback_data);
            }
            
            // Propagate the join
            update_queue_push(&engine->update_queue, upd);
            
            break;
        }
    }
}

/**
 * Dispatch incoming message to appropriate handler
 */
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
            
        case GOSSIP_MSG_PING_REQ:
            LOG_DEBUG("PING_REQ not yet implemented");
            break;
            
        case GOSSIP_MSG_LEAVE:
            LOG_INFO("Node %u gracefully leaving", msg->sender_id);
            cluster_view_update_status(engine->cluster_view, msg->sender_id,
                                      NODE_STATUS_DEAD, msg->sequence_num);
            break;
            
        default:
            LOG_WARN("Unknown gossip message type: %u", msg->msg_type);
            break;
    }
}

// ============================================================================
// UDP LISTENER THREAD
// ============================================================================

/**
 * UDP listener thread - receives and processes incoming gossip messages
 */
static void* gossip_listener_thread(void *arg) {
    gossip_engine_t *engine = (gossip_engine_t*)arg;
    
    LOG_INFO("Gossip UDP listener thread started (fd=%d)", engine->udp_socket);
    
    uint8_t recv_buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    char src_ip[MAX_IP_LEN];
    uint16_t src_port;
    
    while (!engine->shutdown_flag) {
        // Receive UDP packet (non-blocking)
        ssize_t received = gossip_recv_udp(engine->udp_socket, recv_buffer,
                                          sizeof(recv_buffer), src_ip,
                                          sizeof(src_ip), &src_port);
        
        if (received < 0) {
            // No data available, sleep briefly
            usleep(10000);  // 10ms
            continue;
        }
        
        if (received < 16) {
            LOG_WARN("Received malformed gossip packet (too small: %zd bytes)", received);
            continue;
        }
        
        // Deserialize message
        gossip_message_t msg;
        if (gossip_message_deserialize(recv_buffer, received, &msg) != 0) {
            LOG_WARN("Failed to deserialize gossip message from %s:%u", src_ip, src_port);
            continue;
        }
        
        // Ignore messages from ourselves
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
                  (msg.msg_type == GOSSIP_MSG_LEAVE) ? "LEAVE" : "UNKNOWN",
                  msg.sender_id, src_ip, src_port, msg.sequence_num, msg.num_updates);
        
        // Dispatch to handler
        dispatch_message(engine, &msg, src_ip, src_port);
    }
    
    LOG_INFO("Gossip UDP listener thread stopped");
    return NULL;
}

// ============================================================================
// ENGINE START/STOP
// ============================================================================


/**
 * Send a test PING manually (for debugging)
 */
int gossip_engine_send_ping(gossip_engine_t *engine, const char *dest_ip, uint16_t dest_port) {
    gossip_message_t ping_msg = {
        .version = 1,
        .msg_type = GOSSIP_MSG_PING,
        .flags = 0,
        .sender_id = engine->my_id,
        .sequence_num = __sync_fetch_and_add(&engine->sequence_num, 1),
        .num_updates = 0
    };
    
    uint8_t buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    ssize_t msg_size = gossip_message_serialize(&ping_msg, buffer, sizeof(buffer));
    if (msg_size > 0) {
        return gossip_send_udp(engine->udp_socket, buffer, msg_size, dest_ip, dest_port);
    }
    return -1;
}

// ============================================================================
// HELPER FUNCTIONS FOR PROTOCOL LOOP
// ============================================================================

/**
 * Select a random alive member from cluster view (excluding self)
 */
static node_id_t select_random_peer(gossip_engine_t *engine) {
    node_id_t peer_list[MAX_CLUSTER_NODES];
    size_t peer_count = 0;
    
    // Get all alive members from cluster view
    pthread_rwlock_rdlock(&engine->cluster_view->lock);
    
    for (size_t i = 0; i < engine->cluster_view->count; i++) {
        cluster_member_t *member = &engine->cluster_view->members[i];
        
        // Skip ourselves and non-alive members
        if (member->node_id == engine->my_id || 
            member->status != NODE_STATUS_ALIVE) {
            continue;
        }
        
        if (peer_count < MAX_CLUSTER_NODES) {
            peer_list[peer_count++] = member->node_id;
        }
    }
    
    pthread_rwlock_unlock(&engine->cluster_view->lock);
    
    if (peer_count == 0) {
        return 0;  // No peers available
    }
    
    // Select random peer
    size_t random_index = rand() % peer_count;
    return peer_list[random_index];
}

/**
 * Send PING to a specific node
 */
static int send_ping_to_node(gossip_engine_t *engine, node_id_t target_node) {
    // Get target node info from cluster view
    cluster_member_t *target = cluster_view_get(engine->cluster_view, target_node);
    if (!target) {
        LOG_WARN("Cannot ping node %u - not in cluster view", target_node);
        return -1;
    }
    
    char target_ip[MAX_IP_LEN];
    uint16_t target_gossip_port = target->port + 1000;  // Gossip port = service_port + 1000
    safe_strncpy(target_ip, target->ip_address, MAX_IP_LEN);
    
    cluster_view_release(engine->cluster_view);
    
    // Build PING message
    gossip_message_t ping_msg = {
        .version = 1,
        .msg_type = GOSSIP_MSG_PING,
        .flags = 0,
        .sender_id = engine->my_id,
        .sequence_num = __sync_fetch_and_add(&engine->sequence_num, 1),
        .num_updates = 0
    };
    
    // Add piggyback updates from queue
    ping_msg.num_updates = update_queue_pop_batch(&engine->update_queue,
                                                   ping_msg.updates,
                                                   engine->config.max_piggyback);
    
    // Serialize message
    uint8_t buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    ssize_t msg_size = gossip_message_serialize(&ping_msg, buffer, sizeof(buffer));
    if (msg_size < 0) {
        LOG_ERROR("Failed to serialize PING message");
        return -1;
    }
    
    // Send UDP packet
    if (gossip_send_udp(engine->udp_socket, buffer, msg_size, 
                       target_ip, target_gossip_port) < 0) {
        LOG_WARN("Failed to send PING to node %u (%s:%u)", 
                 target_node, target_ip, target_gossip_port);
        return -1;
    }
    
    LOG_DEBUG("Sent PING to node %u (%s:%u, seq=%lu, updates=%u)",
              target_node, target_ip, target_gossip_port, 
              ping_msg.sequence_num, ping_msg.num_updates);
    
    // Add to pending ACKs for timeout tracking
    add_pending_ack(engine, target_node);
    
    return 0;
}

/**
 * Check for SUSPECT nodes that have timed out and mark them DEAD
 */
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
            uint16_t port = member->port;
            safe_strncpy(ip, member->ip_address, MAX_IP_LEN);
            
            pthread_rwlock_unlock(&engine->cluster_view->lock);
            
            // Mark as DEAD
            LOG_ERROR("Node %u suspected for %lums, marking as DEAD", 
                     node_id, elapsed);
            
            cluster_view_update_status(engine->cluster_view, node_id,
                                      NODE_STATUS_DEAD, member->incarnation);
            
            // Add DEAD update to queue for propagation
            gossip_member_update_t dead_update = {
                .node_id = node_id,
                .node_type = node_type,
                .status = NODE_STATUS_DEAD,
                .incarnation = member->incarnation,
                .timestamp_ms = now
            };
            safe_strncpy(dead_update.ip_address, ip, MAX_IP_LEN);
            
            update_queue_push(&engine->update_queue, &dead_update);
            
            // Trigger event callback
            if (engine->event_callback) {
                engine->event_callback(node_id, node_type, ip, port,
                                     MEMBER_EVENT_LEAVE, 
                                     engine->event_callback_data);
            }
            
            // Re-acquire lock for next iteration
            pthread_rwlock_rdlock(&engine->cluster_view->lock);
        }
    }
    
    pthread_rwlock_unlock(&engine->cluster_view->lock);
}

/**
 * Gossip state to random peers (for anti-entropy)
 */
static void gossip_to_random_peers(gossip_engine_t *engine) {
    // Get list of alive peers
    node_id_t peer_list[MAX_CLUSTER_NODES];
    size_t peer_count = 0;
    
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
    
    if (peer_count == 0) {
        return;  // No peers to gossip to
    }
    
    // Select random subset (fanout)
    size_t fanout = (peer_count < engine->config.fanout) ? 
                    peer_count : engine->config.fanout;
    
    for (size_t i = 0; i < fanout; i++) {
        // Fisher-Yates shuffle to select random peer
        size_t j = i + (rand() % (peer_count - i));
        node_id_t temp = peer_list[i];
        peer_list[i] = peer_list[j];
        peer_list[j] = temp;
        
        // Get peer info
        cluster_member_t *peer = cluster_view_get(engine->cluster_view, peer_list[i]);
        if (!peer) continue;
        
        char peer_ip[MAX_IP_LEN];
        uint16_t peer_gossip_port = peer->port + 1000;
        safe_strncpy(peer_ip, peer->ip_address, MAX_IP_LEN);
        
        cluster_view_release(engine->cluster_view);
        
        // Build gossip message with state updates
        gossip_message_t gossip_msg = {
            .version = 1,
            .msg_type = GOSSIP_MSG_SUSPECT,  // Reuse SUSPECT as general gossip
            .flags = 0,
            .sender_id = engine->my_id,
            .sequence_num = __sync_fetch_and_add(&engine->sequence_num, 1),
            .num_updates = 0
        };
        
        // Add updates from queue
        gossip_msg.num_updates = update_queue_pop_batch(&engine->update_queue,
                                                        gossip_msg.updates,
                                                        engine->config.max_piggyback);
        
        // If no queued updates, add our own state
        if (gossip_msg.num_updates == 0) {
            gossip_msg.updates[0].node_id = engine->my_id;
            gossip_msg.updates[0].node_type = engine->my_type;
            gossip_msg.updates[0].status = NODE_STATUS_ALIVE;
            gossip_msg.updates[0].incarnation = engine->incarnation;
            gossip_msg.updates[0].gossip_port = engine->gossip_port;
            gossip_msg.updates[0].service_port = engine->service_port;
            gossip_msg.updates[0].timestamp_ms = time_now_ms();
            safe_strncpy(gossip_msg.updates[0].ip_address, engine->my_ip, MAX_IP_LEN);
            gossip_msg.num_updates = 1;
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

/**
 * Update our uptime metric in cluster view
 */
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

/**
 * Main SWIM protocol loop - runs periodically to:
 * 1. Select random peer and send PING
 * 2. Check for ACK timeouts
 * 3. Check for SUSPECT -> DEAD transitions
 * 4. Gossip state to random peers (anti-entropy)
 */
static void* gossip_protocol_thread(void *arg) {
    gossip_engine_t *engine = (gossip_engine_t*)arg;
    
    LOG_INFO("SWIM protocol thread started (period=%ums)", 
             engine->config.protocol_period_ms);
    
    uint64_t round = 0;
    
    while (!engine->shutdown_flag) {
        round++;
        
        LOG_DEBUG("=== SWIM Protocol Round %lu ===", round);
        
        // Update our own state
        update_self_in_cluster_view(engine);
        
        // Step 1: Select random alive peer and send PING
        node_id_t target = select_random_peer(engine);
        if (target != 0) {
            send_ping_to_node(engine, target);
        } else {
            LOG_DEBUG("No peers available for PING");
        }
        
        // Step 2: Check for pending ACK timeouts
        check_ack_timeouts(engine);
        
        // Step 3: Check for SUSPECT -> DEAD transitions
        check_suspect_timeouts(engine);
        
        // Step 4: Gossip state to random peers (anti-entropy)
        // Do this every few rounds to avoid excessive traffic
        if (round % 3 == 0) {
            gossip_to_random_peers(engine);
        }
        
        // Step 5: Log current cluster state (debug)
        if (round % 10 == 0) {  // Every 10 rounds
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
        
        // Sleep until next protocol period
        usleep(engine->config.protocol_period_ms * 1000);
    }
    
    LOG_INFO("SWIM protocol thread stopped");
    return NULL;
}


int gossip_engine_start(gossip_engine_t *engine) {
    if (!engine) {
        return -1;
    }
    
    // Start UDP listener thread
    if (pthread_create(&engine->listener_thread, NULL, 
                      gossip_listener_thread, engine) != 0) {
        LOG_ERROR("Failed to start gossip listener thread");
        return -1;
    }
    
    // Start SWIM protocol thread
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

// Update gossip_engine_shutdown to wait for both threads:

void gossip_engine_shutdown(gossip_engine_t *engine) {
    if (!engine) return;
    
    LOG_INFO("Shutting down gossip engine");
    
    engine->shutdown_flag = 1;
    
    // Wait for both threads to exit
    pthread_join(engine->listener_thread, NULL);
    pthread_join(engine->protocol_thread, NULL);
    
    // Close socket
    if (engine->udp_socket >= 0) {
        close(engine->udp_socket);
        engine->udp_socket = -1;
    }
    
    // Cleanup
    pthread_mutex_destroy(&engine->pending_acks_lock);
    update_queue_destroy(&engine->update_queue);
    
    free(engine);
    
    LOG_INFO("Gossip engine shutdown complete");
}

// ============================================================================
// ADD SEED AND JOIN FUNCTIONS
// ============================================================================

int gossip_engine_add_seed(gossip_engine_t *engine, const char *ip, uint16_t gossip_port) {
    if (!engine || !ip) {
        return -1;
    }
    
    LOG_INFO("Adding seed node: %s:%u", ip, gossip_port);
    
    // We don't know the seed's node_id yet, so we'll discover it via JOIN
    // For now, just send a JOIN announcement to the seed
    
    gossip_message_t join_msg = {
        .version = 1,
        .msg_type = GOSSIP_MSG_JOIN,
        .flags = 0,
        .sender_id = engine->my_id,
        .sequence_num = __sync_fetch_and_add(&engine->sequence_num, 1),
        .num_updates = 1
    };
    
    // Add our info as update
    join_msg.updates[0].node_id = engine->my_id;
    join_msg.updates[0].node_type = engine->my_type;
    join_msg.updates[0].status = NODE_STATUS_ALIVE;
    join_msg.updates[0].incarnation = engine->incarnation;
    join_msg.updates[0].gossip_port = engine->gossip_port;
    join_msg.updates[0].service_port = engine->service_port;
    join_msg.updates[0].timestamp_ms = time_now_ms();
    safe_strncpy(join_msg.updates[0].ip_address, engine->my_ip, MAX_IP_LEN);
    
    // Serialize and send
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
    
    // Add ourselves to cluster view first
    cluster_member_t self = {
        .node_id = engine->my_id,
        .node_type = engine->my_type,
        .port = engine->service_port,
        .status = NODE_STATUS_ALIVE,
        .incarnation = engine->incarnation,
        .last_seen_ms = time_now_ms()
    };
    safe_strncpy(self.ip_address, engine->my_ip, MAX_IP_LEN);
    
    cluster_view_add(engine->cluster_view, &self);
    
    // Add JOIN update to queue for propagation
    gossip_member_update_t join_update = {
        .node_id = engine->my_id,
        .node_type = engine->my_type,
        .status = NODE_STATUS_ALIVE,
        .incarnation = engine->incarnation,
        .gossip_port = engine->gossip_port,
        .service_port = engine->service_port,
        .timestamp_ms = time_now_ms()
    };
    safe_strncpy(join_update.ip_address, engine->my_ip, MAX_IP_LEN);
    
    // Add multiple times for faster propagation
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
    
    // Build LEAVE message
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
    
    // Send to all known peers
    pthread_rwlock_rdlock(&engine->cluster_view->lock);
    
    for (size_t i = 0; i < engine->cluster_view->count; i++) {
        cluster_member_t *member = &engine->cluster_view->members[i];
        
        if (member->node_id == engine->my_id) continue;
        
        uint16_t peer_gossip_port = member->port + 1000;
        
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