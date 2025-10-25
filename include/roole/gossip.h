// include/roole/gossip.h

#ifndef ROOLE_GOSSIP_H
#define ROOLE_GOSSIP_H

#include "roole/common.h"
#include "roole/cluster.h"
#include <stdint.h>
#include <netinet/in.h>

// ============================================================================
// GOSSIP MESSAGE TYPES (SWIM Protocol)
// ============================================================================

typedef enum {
    GOSSIP_MSG_PING = 1,        // Direct health check
    GOSSIP_MSG_ACK = 2,         // Response to PING
    GOSSIP_MSG_PING_REQ = 3,    // Indirect ping request (future)
    GOSSIP_MSG_SUSPECT = 4,     // Announce suspicion
    GOSSIP_MSG_ALIVE = 5,       // Refute suspicion
    GOSSIP_MSG_DEAD = 6,        // Declare node dead
    GOSSIP_MSG_JOIN = 7,        // New member joining
    GOSSIP_MSG_LEAVE = 8,        // Graceful leave
    GOSSIP_MSG_WORKER_JOIN = 9,  
    GOSSIP_MSG_JOIN_RESPONSE = 10 
} gossip_msg_type_t;

// ============================================================================
// GOSSIP MESSAGE STRUCTURE
// ============================================================================

#define GOSSIP_MAX_PAYLOAD_SIZE 1400  // Stay under MTU (1500 - IP/UDP headers)
#define GOSSIP_MAX_PIGGYBACK_UPDATES 10

// Member update (piggybacked on gossip messages)
typedef struct gossip_member_update {
    node_id_t node_id;
    node_type_t node_type;
    char ip_address[MAX_IP_LEN];
    uint16_t gossip_port;        // Port for gossip UDP
    uint16_t service_port;       // Port for service RPC
    node_status_t status;
    uint64_t incarnation;
    uint64_t timestamp_ms;       // When this update was created
} gossip_member_update_t;

#define MAX_CONFIG_STRING 256
#define MAX_CONFIG_ROUTERS 16

// Gossip message header
typedef struct gossip_message {
    uint8_t version;             // Protocol version (currently 1)
    uint8_t msg_type;            // gossip_msg_type_t
    uint16_t flags;              // Reserved for future use
    
    node_id_t sender_id;         // Who sent this message
    uint64_t sequence_num;       // Sender's sequence number
    
    // Piggyback updates (for efficient state propagation)
    uint8_t num_updates;
    gossip_member_update_t updates[GOSSIP_MAX_PIGGYBACK_UPDATES];
    
} gossip_message_t;

typedef struct gossip_bootstrap_response {
    uint8_t num_routers;
    struct {
        node_id_t node_id;
        char gossip_addr[MAX_CONFIG_STRING];
        char data_addr[MAX_CONFIG_STRING];
    } routers[MAX_CONFIG_ROUTERS];
} gossip_bootstrap_response_t;

// ============================================================================
// GOSSIP PROTOCOL CONFIGURATION
// ============================================================================

typedef struct gossip_config {
    uint32_t protocol_period_ms;    // How often to run protocol (default: 1000ms)
    uint32_t ack_timeout_ms;        // How long to wait for ACK (default: 500ms)
    uint32_t suspect_timeout_ms;    // When to mark SUSPECT (default: 5000ms)
    uint32_t dead_timeout_ms;       // When to mark DEAD (default: 15000ms)
    uint32_t fanout;                // How many peers to gossip to (default: 3)
    uint32_t max_piggyback;         // Max updates per message (default: 10)
} gossip_config_t;

// Default configuration for medium clusters
static inline gossip_config_t gossip_default_config(void) {
    gossip_config_t config = {
        .protocol_period_ms = 1000,
        .ack_timeout_ms = 500,
        .suspect_timeout_ms = 5000,
        .dead_timeout_ms = 15000,
        .fanout = 3,
        .max_piggyback = 10
    };
    return config;
}

// ============================================================================
// GOSSIP ENGINE
// ============================================================================

typedef struct gossip_engine gossip_engine_t;

/**
 * Initialize gossip engine
 * @param my_id This node's ID
 * @param my_type This node's type (ROUTER or WORKER)
 * @param bind_addr Address to bind UDP socket (e.g., "0.0.0.0")
 * @param gossip_port UDP port for gossip protocol
 * @param config Protocol configuration (NULL for defaults)
 * @param cluster_view Shared cluster view to update
 * @param event_callback Callback for member events (join/leave/fail)
 * @param user_data User data for callback
 */
gossip_engine_t* gossip_engine_init(
    node_id_t my_id,
    node_type_t my_type,
    const char *bind_addr,
    uint16_t gossip_port,
    uint16_t service_port,
    const gossip_config_t *config,
    cluster_view_t *cluster_view,
    member_event_cb event_callback,
    void *user_data
);

/**
 * Start gossip engine (spawns UDP listener and protocol threads)
 */
int gossip_engine_start(gossip_engine_t *engine);

/**
 * Manually add a seed member (for bootstrapping)
 */
int gossip_engine_add_seed(gossip_engine_t *engine, const char *ip, uint16_t gossip_port);

/**
 * Announce this node is joining the cluster
 */
int gossip_engine_announce_join(gossip_engine_t *engine);

/**
 * Gracefully leave the cluster
 */
int gossip_engine_leave(gossip_engine_t *engine);

/**
 * Shutdown gossip engine
 */
void gossip_engine_shutdown(gossip_engine_t *engine);

// ============================================================================
// MESSAGE SERIALIZATION
// ============================================================================

/**
 * Serialize gossip message to buffer
 * @return Number of bytes written, or -1 on error
 */
ssize_t gossip_message_serialize(const gossip_message_t *msg, uint8_t *buffer, size_t buffer_size);

/**
 * Deserialize gossip message from buffer
 * @return 0 on success, -1 on error
 */
int gossip_message_deserialize(const uint8_t *buffer, size_t buffer_size, gossip_message_t *msg);

ssize_t gossip_serialize_bootstrap_response(const gossip_bootstrap_response_t *resp, 
                                            uint8_t *buffer, size_t buffer_size);

int gossip_deserialize_bootstrap_response(const uint8_t *buffer, size_t buffer_size,
                                          gossip_bootstrap_response_t *resp);

size_t gossip_message_serialized_size(const gossip_message_t *msg);

void gossip_engine_set_callback(gossip_engine_t *engine, 
                                member_event_cb callback, 
                                void *user_data);
                                
#endif // ROOLE_GOSSIP_H