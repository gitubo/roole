// src/cluster/gossip_protocol.c - Message serialization/deserialization
// FIXES:
// 1. Serialize data_port correctly instead of gossip_port twice
// 2. Deserialize data_port correctly

#define _POSIX_C_SOURCE 200809L

#include "roole/gossip.h"
#include "roole/common.h"
#include <string.h>
#include <arpa/inet.h>
#include <endian.h>

// ============================================================================
// HELPER: Network Byte Order Conversions (DRY)
// ============================================================================

static inline void write_u16_net(uint8_t *buffer, uint16_t value) {
    uint16_t net_value = htons(value);
    memcpy(buffer, &net_value, sizeof(uint16_t));
}

static inline void write_u64_net(uint8_t *buffer, uint64_t value) {
    uint64_t net_value = htobe64(value);
    memcpy(buffer, &net_value, sizeof(uint64_t));
}

static inline uint16_t read_u16_net(const uint8_t *buffer) {
    uint16_t net_value;
    memcpy(&net_value, buffer, sizeof(uint16_t));
    return ntohs(net_value);
}

static inline uint64_t read_u64_net(const uint8_t *buffer) {
    uint64_t net_value;
    memcpy(&net_value, buffer, sizeof(uint64_t));
    return be64toh(net_value);
}

// ============================================================================
// HELPER: Member Update Serialization (DRY)
// ============================================================================

#define MEMBER_UPDATE_SIZE 44  // Fixed size: 2+1+1+16+2+2+8+8+4(padding)

/**
 * Serialize a single member update into buffer
 * Returns number of bytes written (always MEMBER_UPDATE_SIZE)
 */
static size_t gossip_serialize_member_update(const gossip_member_update_t *update,
                                              uint8_t *buffer) {
    size_t offset = 0;
    
    // Node ID (2 bytes)
    write_u16_net(buffer + offset, update->node_id);
    offset += 2;
    
    // Node type (1 byte)
    buffer[offset++] = (uint8_t)update->node_type;
    
    // Status (1 byte)
    buffer[offset++] = (uint8_t)update->status;
    
    // IP address (16 bytes, null-padded string)
    memset(buffer + offset, 0, 16);
    strncpy((char*)(buffer + offset), update->ip_address, 15);
    offset += 16;
    
    // Gossip port (2 bytes)
    write_u16_net(buffer + offset, update->gossip_port);
    offset += 2;
    
    // Data port (2 bytes)
    write_u16_net(buffer + offset, update->data_port);
    offset += 2;
    
    // Incarnation (8 bytes)
    write_u64_net(buffer + offset, update->incarnation);
    offset += 8;
    
    // Timestamp (8 bytes)
    write_u64_net(buffer + offset, update->timestamp_ms);
    offset += 8;
    
    return offset;  // Should always be MEMBER_UPDATE_SIZE
}

/**
 * Deserialize a single member update from buffer
 * Returns number of bytes read (always MEMBER_UPDATE_SIZE)
 */
static size_t gossip_deserialize_member_update(const uint8_t *buffer,
                                                gossip_member_update_t *update) {
    size_t offset = 0;
    
    // Node ID (2 bytes)
    update->node_id = read_u16_net(buffer + offset);
    offset += 2;
    
    // Node type (1 byte)
    update->node_type = (node_type_t)buffer[offset++];
    
    // Status (1 byte)
    update->status = (node_status_t)buffer[offset++];
    
    // IP address (16 bytes)
    memcpy(update->ip_address, buffer + offset, 16);
    update->ip_address[15] = '\0';  // Ensure null termination
    offset += 16;
    
    // Gossip port (2 bytes)
    update->gossip_port = read_u16_net(buffer + offset);
    offset += 2;
    
    // Data port (2 bytes)
    update->data_port = read_u16_net(buffer + offset);
    offset += 2;
    
    // Incarnation (8 bytes)
    update->incarnation = read_u64_net(buffer + offset);
    offset += 8;
    
    // Timestamp (8 bytes)
    update->timestamp_ms = read_u64_net(buffer + offset);
    offset += 8;
    
    return offset;  // Should always be MEMBER_UPDATE_SIZE
}

// ============================================================================
// MESSAGE SERIALIZATION (Refactored using helpers)
// ============================================================================

ssize_t gossip_message_serialize(const gossip_message_t *msg, 
                                  uint8_t *buffer, 
                                  size_t buffer_size) {
    if (!msg || !buffer || buffer_size < 16) {
        return -1;
    }
    
    size_t offset = 0;
    
    // Header (fixed size: 16 bytes)
    buffer[offset++] = msg->version;
    buffer[offset++] = msg->msg_type;
    
    write_u16_net(buffer + offset, msg->flags);
    offset += 2;
    
    write_u16_net(buffer + offset, msg->sender_id);
    offset += 2;
    
    write_u64_net(buffer + offset, msg->sequence_num);
    offset += 8;
    
    buffer[offset++] = msg->num_updates;
    buffer[offset++] = 0;  // Padding
    
    // Piggyback updates - use helper
    for (uint8_t i = 0; i < msg->num_updates && i < GOSSIP_MAX_PIGGYBACK_UPDATES; i++) {
        // Check buffer space
        if (offset + MEMBER_UPDATE_SIZE > buffer_size) {
            LOG_WARN("Buffer too small for all updates, truncating at %u/%u", 
                     i, msg->num_updates);
            // Update header to reflect actual count
            buffer[16] = i;  // Overwrite num_updates field
            break;
        }
        
        size_t written = gossip_serialize_member_update(&msg->updates[i], 
                                                        buffer + offset);
        offset += written;
    }
    
    return (ssize_t)offset;
}

// ============================================================================
// MESSAGE DESERIALIZATION (Refactored using helpers)
// ============================================================================

int gossip_message_deserialize(const uint8_t *buffer, 
                                size_t buffer_size, 
                                gossip_message_t *msg) {
    if (!buffer || !msg || buffer_size < 16) {
        return -1;
    }
    
    memset(msg, 0, sizeof(gossip_message_t));
    
    size_t offset = 0;
    
    // Header (16 bytes)
    msg->version = buffer[offset++];
    msg->msg_type = buffer[offset++];
    
    if (msg->version != 1) {
        LOG_WARN("Unsupported gossip protocol version: %u", msg->version);
        return -1;
    }
    
    msg->flags = read_u16_net(buffer + offset);
    offset += 2;
    
    msg->sender_id = read_u16_net(buffer + offset);
    offset += 2;
    
    msg->sequence_num = read_u64_net(buffer + offset);
    offset += 8;
    
    msg->num_updates = buffer[offset++];
    offset++;  // Skip padding
    
    // Piggyback updates - use helper
    for (uint8_t i = 0; i < msg->num_updates && i < GOSSIP_MAX_PIGGYBACK_UPDATES; i++) {
        if (offset + MEMBER_UPDATE_SIZE > buffer_size) {
            LOG_WARN("Truncated gossip message, stopping at update %u/%u", 
                     i, msg->num_updates);
            msg->num_updates = i;
            break;
        }
        
        size_t read = gossip_deserialize_member_update(buffer + offset, 
                                                       &msg->updates[i]);
        offset += read;
    }
    
    return 0;
}

// ============================================================================
// BOOTSTRAP RESPONSE SERIALIZATION (Refactored)
// ============================================================================

ssize_t gossip_serialize_bootstrap_response(const gossip_bootstrap_response_t *resp, 
                                            uint8_t *buffer, 
                                            size_t buffer_size) {
    if (!resp || !buffer || buffer_size < 1) return -1;
    
    size_t offset = 0;
    
    // Number of routers (1 byte)
    buffer[offset++] = resp->num_routers;
    
    for (uint8_t i = 0; i < resp->num_routers && i < MAX_CONFIG_ROUTERS; i++) {
        // Check buffer space (2 + 64 + 64 = 130 bytes per router)
        if (offset + 130 > buffer_size) {
            LOG_WARN("Buffer too small for all routers, truncating at %u/%u",
                     i, resp->num_routers);
            buffer[0] = i;  // Update count
            break;
        }
        
        // Node ID (2 bytes)
        write_u16_net(buffer + offset, resp->routers[i].node_id);
        offset += 2;
        
        // Gossip address (64 bytes, null-terminated)
        memset(buffer + offset, 0, 64);
        strncpy((char*)(buffer + offset), resp->routers[i].gossip_addr, 63);
        offset += 64;
        
        // Data address (64 bytes, null-terminated)
        memset(buffer + offset, 0, 64);
        strncpy((char*)(buffer + offset), resp->routers[i].data_addr, 63);
        offset += 64;
    }
    
    return (ssize_t)offset;
}

int gossip_deserialize_bootstrap_response(const uint8_t *buffer, 
                                          size_t buffer_size,
                                          gossip_bootstrap_response_t *resp) {
    if (!buffer || !resp || buffer_size < 1) return -1;
    
    memset(resp, 0, sizeof(gossip_bootstrap_response_t));
    
    size_t offset = 0;
    
    // Number of routers
    resp->num_routers = buffer[offset++];
    
    for (uint8_t i = 0; i < resp->num_routers && i < MAX_CONFIG_ROUTERS; i++) {
        if (offset + 130 > buffer_size) {
            LOG_WARN("Truncated bootstrap response, stopping at router %u/%u",
                     i, resp->num_routers);
            resp->num_routers = i;
            break;
        }
        
        // Node ID (2 bytes)
        resp->routers[i].node_id = read_u16_net(buffer + offset);
        offset += 2;
        
        // Gossip address (64 bytes)
        strncpy(resp->routers[i].gossip_addr, (char*)(buffer + offset), 
                MAX_CONFIG_STRING - 1);
        resp->routers[i].gossip_addr[MAX_CONFIG_STRING - 1] = '\0';
        offset += 64;
        
        // Data address (64 bytes)
        strncpy(resp->routers[i].data_addr, (char*)(buffer + offset),
                MAX_CONFIG_STRING - 1);
        resp->routers[i].data_addr[MAX_CONFIG_STRING - 1] = '\0';
        offset += 64;
    }
    
    return 0;
}

// ============================================================================
// HELPER: Calculate serialized message size
// ============================================================================

size_t gossip_message_serialized_size(const gossip_message_t *msg) {
    if (!msg) return 0;
    
    // Header (16 bytes) + updates (MEMBER_UPDATE_SIZE each)
    return 16 + (msg->num_updates * MEMBER_UPDATE_SIZE);
}