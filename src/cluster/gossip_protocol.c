// src/cluster/gossip_protocol.c - Message serialization/deserialization

#define _POSIX_C_SOURCE 200809L

#include "roole/gossip.h"
#include "roole/common.h"
#include <string.h>
#include <arpa/inet.h>
#include <endian.h>

// ============================================================================
// MESSAGE SERIALIZATION
// ============================================================================

ssize_t gossip_message_serialize(const gossip_message_t *msg, uint8_t *buffer, size_t buffer_size) {
    if (!msg || !buffer || buffer_size < 16) {
        return -1;
    }
    
    size_t offset = 0;
    
    // Header (fixed size: 16 bytes)
    buffer[offset++] = msg->version;
    buffer[offset++] = msg->msg_type;
    
    uint16_t flags_net = htons(msg->flags);
    memcpy(buffer + offset, &flags_net, 2);
    offset += 2;
    
    uint16_t sender_id_net = htons(msg->sender_id);
    memcpy(buffer + offset, &sender_id_net, 2);
    offset += 2;
    
    uint64_t seq_net = htobe64(msg->sequence_num);
    memcpy(buffer + offset, &seq_net, 8);
    offset += 8;
    
    buffer[offset++] = msg->num_updates;
    
    // Padding to align to 16 bytes
    buffer[offset++] = 0;
    
    // Piggyback updates
    for (uint8_t i = 0; i < msg->num_updates && i < GOSSIP_MAX_PIGGYBACK_UPDATES; i++) {
        const gossip_member_update_t *upd = &msg->updates[i];
        
        // Check buffer space (each update: ~50 bytes)
        if (offset + 64 > buffer_size) {
            LOG_WARN("Buffer too small for all updates, truncating");
            break;
        }
        
        uint16_t node_id_net = htons(upd->node_id);
        memcpy(buffer + offset, &node_id_net, 2);
        offset += 2;
        
        buffer[offset++] = (uint8_t)upd->node_type;
        buffer[offset++] = (uint8_t)upd->status;
        
        // IP address (16 bytes, null-padded)
        memset(buffer + offset, 0, 16);
        strncpy((char*)(buffer + offset), upd->ip_address, 15);
        offset += 16;
        
        uint16_t gossip_port_net = htons(upd->gossip_port);
        memcpy(buffer + offset, &gossip_port_net, 2);
        offset += 2;
        
        uint16_t service_port_net = htons(upd->service_port);
        memcpy(buffer + offset, &service_port_net, 2);
        offset += 2;
        
        uint64_t inc_net = htobe64(upd->incarnation);
        memcpy(buffer + offset, &inc_net, 8);
        offset += 8;
        
        uint64_t ts_net = htobe64(upd->timestamp_ms);
        memcpy(buffer + offset, &ts_net, 8);
        offset += 8;
    }
    
    return (ssize_t)offset;
}

int gossip_message_deserialize(const uint8_t *buffer, size_t buffer_size, gossip_message_t *msg) {
    if (!buffer || !msg || buffer_size < 16) {
        return -1;
    }
    
    memset(msg, 0, sizeof(gossip_message_t));
    
    size_t offset = 0;
    
    // Header
    msg->version = buffer[offset++];
    msg->msg_type = buffer[offset++];
    
    if (msg->version != 1) {
        LOG_WARN("Unsupported gossip protocol version: %u", msg->version);
        return -1;
    }
    
    uint16_t flags_net;
    memcpy(&flags_net, buffer + offset, 2);
    msg->flags = ntohs(flags_net);
    offset += 2;
    
    uint16_t sender_id_net;
    memcpy(&sender_id_net, buffer + offset, 2);
    msg->sender_id = ntohs(sender_id_net);
    offset += 2;
    
    uint64_t seq_net;
    memcpy(&seq_net, buffer + offset, 8);
    msg->sequence_num = be64toh(seq_net);
    offset += 8;
    
    msg->num_updates = buffer[offset++];
    offset++; // Skip padding
    
    // Piggyback updates
    for (uint8_t i = 0; i < msg->num_updates && i < GOSSIP_MAX_PIGGYBACK_UPDATES; i++) {
        if (offset + 44 > buffer_size) {
            LOG_WARN("Truncated gossip message, stopping at update %u", i);
            msg->num_updates = i;
            break;
        }
        
        gossip_member_update_t *upd = &msg->updates[i];
        
        uint16_t node_id_net;
        memcpy(&node_id_net, buffer + offset, 2);
        upd->node_id = ntohs(node_id_net);
        offset += 2;
        
        upd->node_type = (node_type_t)buffer[offset++];
        upd->status = (node_status_t)buffer[offset++];
        
        // IP address
        memcpy(upd->ip_address, buffer + offset, 16);
        upd->ip_address[15] = '\0';
        offset += 16;
        
        uint16_t gossip_port_net;
        memcpy(&gossip_port_net, buffer + offset, 2);
        upd->gossip_port = ntohs(gossip_port_net);
        offset += 2;
        
        uint16_t service_port_net;
        memcpy(&service_port_net, buffer + offset, 2);
        upd->service_port = ntohs(service_port_net);
        offset += 2;
        
        uint64_t inc_net;
        memcpy(&inc_net, buffer + offset, 8);
        upd->incarnation = be64toh(inc_net);
        offset += 8;
        
        uint64_t ts_net;
        memcpy(&ts_net, buffer + offset, 8);
        upd->timestamp_ms = be64toh(ts_net);
        offset += 8;
    }
    
    return 0;
}