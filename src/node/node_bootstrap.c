// src/node/node_bootstrap.c

#define _POSIX_C_SOURCE 200809L

#include "roole/node_state.h"
#include "roole/node.h"
#include "roole/config.h"
#include "roole/gossip.h"
#include "roole/common.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/time.h>

int node_bootstrap_from_config_ex(node_state_t *state, 
                                  const roole_config_t *config) {
    if (!state || !config) {
        LOG_ERROR("Invalid parameters for bootstrap");
        return RESULT_ERR_INVALID;
    }
    
    // Get identity and subsystems
    const node_identity_t *id = node_state_get_identity(state);
    peer_pool_t *pool = node_state_get_peer_pool(state);
    cluster_view_t *view = node_state_get_cluster_view(state);
    
    // No seed routers configured
    if (config->router_count == 0) {
        LOG_INFO("No seed routers configured, starting as standalone/seed node");
        
        // Get gossip engine from membership
        membership_handle_t *membership = state->membership;
        gossip_engine_t *gossip = ((struct membership_handle*)membership)->gossip_engine;
        gossip_engine_announce_join(gossip);
        
        return RESULT_OK;
    }
    
    // Select random seed router
    size_t router_idx = rand() % config->router_count;
    const char *router_addr = config->routers[router_idx];
    
    char router_ip[16];
    uint16_t router_gossip_port;
    config_parse_address(router_addr, router_ip, &router_gossip_port);
    
    LOG_INFO("Bootstrapping from seed router %s:%u", router_ip, router_gossip_port);
    
    // Build JOIN message
    gossip_message_t join_msg = {
        .version = 1,
        .msg_type = GOSSIP_MSG_WORKER_JOIN,
        .sender_id = id->node_id,
        .sequence_num = 1,
        .num_updates = 1
    };
    
    join_msg.updates[0].node_id = id->node_id;
    join_msg.updates[0].node_type = NODE_TYPE_WORKER;
    join_msg.updates[0].status = NODE_STATUS_ALIVE;
    join_msg.updates[0].incarnation = 0;
    safe_strncpy(join_msg.updates[0].ip_address, id->bind_addr, MAX_IP_LEN);
    join_msg.updates[0].gossip_port = id->gossip_port;
    join_msg.updates[0].data_port = id->data_port;
    join_msg.updates[0].timestamp_ms = time_now_ms();
    
    // Create UDP socket for bootstrap
    int bootstrap_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (bootstrap_sock < 0) {
        LOG_ERROR("Cannot create bootstrap socket: %s", strerror(errno));
        return RESULT_ERR_NETWORK;
    }
    
    // Serialize message
    uint8_t buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    ssize_t msg_size = gossip_message_serialize(&join_msg, buffer, sizeof(buffer));
    
    if (msg_size < 0) {
        LOG_ERROR("Failed to serialize JOIN message");
        close(bootstrap_sock);
        return RESULT_ERR_INVALID;
    }
    
    LOG_DEBUG("JOIN message serialized: %zd bytes", msg_size);
    
    // Send to seed router
    struct sockaddr_in router_addr_in;
    memset(&router_addr_in, 0, sizeof(router_addr_in));
    router_addr_in.sin_family = AF_INET;
    router_addr_in.sin_port = htons(router_gossip_port);
    
    if (inet_pton(AF_INET, router_ip, &router_addr_in.sin_addr) <= 0) {
        LOG_ERROR("Invalid router IP address: %s", router_ip);
        close(bootstrap_sock);
        return RESULT_ERR_INVALID;
    }
    
    if (sendto(bootstrap_sock, buffer, msg_size, 0,
              (struct sockaddr*)&router_addr_in, sizeof(router_addr_in)) < 0) {
        LOG_ERROR("Failed to send JOIN: %s", strerror(errno));
        close(bootstrap_sock);
        return RESULT_ERR_NETWORK;
    }
    
    LOG_INFO("JOIN sent to %s:%u, waiting for response...", 
             router_ip, router_gossip_port);
    
    // Set receive timeout (5 seconds)
    struct timeval tv = {.tv_sec = 5, .tv_usec = 0};
    if (setsockopt(bootstrap_sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        LOG_WARN("Failed to set socket timeout: %s", strerror(errno));
    }
    
    // Wait for JOIN_RESPONSE
    uint8_t recv_buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    ssize_t received = recvfrom(bootstrap_sock, recv_buffer, sizeof(recv_buffer), 
                               0, NULL, NULL);
    
    close(bootstrap_sock);
    
    if (received < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            LOG_WARN("Timeout waiting for JOIN_RESPONSE (5 seconds)");
            LOG_WARN("Will continue via background gossip discovery");
            
            // Announce join anyway
            membership_handle_t *membership = state->membership;
            gossip_engine_t *gossip = ((struct membership_handle*)membership)->gossip_engine;
            gossip_engine_announce_join(gossip);
            
            return RESULT_ERR_TIMEOUT;
        }
        LOG_ERROR("Failed to receive JOIN_RESPONSE: %s", strerror(errno));
        return RESULT_ERR_NETWORK;
    }
    
    LOG_DEBUG("Received response: %zd bytes", received);
    
    // Deserialize response
    gossip_message_t response;
    if (gossip_message_deserialize(recv_buffer, received, &response) != 0) {
        LOG_ERROR("Invalid JOIN_RESPONSE (deserialization failed)");
        
        membership_handle_t *membership = state->membership;
        gossip_engine_t *gossip = ((struct membership_handle*)membership)->gossip_engine;
        gossip_engine_announce_join(gossip);
        
        return RESULT_ERR_INVALID;
    }
    
    if (response.msg_type != GOSSIP_MSG_JOIN_RESPONSE) {
        LOG_ERROR("Unexpected message type: %u (expected %u)", 
                  response.msg_type, GOSSIP_MSG_JOIN_RESPONSE);
        
        membership_handle_t *membership = state->membership;
        gossip_engine_t *gossip = ((struct membership_handle*)membership)->gossip_engine;
        gossip_engine_announce_join(gossip);
        
        return RESULT_ERR_INVALID;
    }
    
    LOG_INFO("Received JOIN_RESPONSE from node %u", response.sender_id);
    
    // Log details
    LOG_INFO("[BOOTSTRAP] JOIN_RESPONSE details:");
    LOG_INFO("[BOOTSTRAP]   sender: %u", response.sender_id);
    LOG_INFO("[BOOTSTRAP]   updates: %u", response.num_updates);
    
    for (uint8_t i = 0; i < response.num_updates; i++) {
        LOG_INFO("[BOOTSTRAP]   update[%u]: node=%u type=%d status=%d ip=%s:%u",
                i, response.updates[i].node_id, response.updates[i].node_type,
                response.updates[i].status, response.updates[i].ip_address,
                response.updates[i].gossip_port);
    }
    
    // Parse bootstrap data (list of seed peers)
    gossip_bootstrap_response_t bootstrap_data;
    size_t header_size = 16 + response.num_updates * 44;
    
    if ((size_t)received < header_size) {
        LOG_ERROR("JOIN_RESPONSE too short: %zd bytes (expected at least %zu)", 
                  received, header_size);
        
        membership_handle_t *membership = state->membership;
        gossip_engine_t *gossip = ((struct membership_handle*)membership)->gossip_engine;
        gossip_engine_announce_join(gossip);
        
        return RESULT_ERR_INVALID;
    }
    
    if (gossip_deserialize_bootstrap_response(recv_buffer + header_size,
                                              received - header_size,
                                              &bootstrap_data) != 0) {
        LOG_ERROR("Failed to deserialize bootstrap data");
        
        membership_handle_t *membership = state->membership;
        gossip_engine_t *gossip = ((struct membership_handle*)membership)->gossip_engine;
        gossip_engine_announce_join(gossip);
        
        return RESULT_ERR_INVALID;
    }
    
    LOG_INFO("Received %u peer addresses in bootstrap response", 
             bootstrap_data.num_routers);
    
    // Connect to all bootstrap peers
    for (uint8_t i = 0; i < bootstrap_data.num_routers; i++) {
        char peer_gossip_ip[16];
        uint16_t peer_gossip_port;
        char peer_data_ip[16];
        uint16_t peer_data_port;
        
        config_parse_address(bootstrap_data.routers[i].gossip_addr, 
                           peer_gossip_ip, &peer_gossip_port);
        config_parse_address(bootstrap_data.routers[i].data_addr, 
                           peer_data_ip, &peer_data_port);
        
        LOG_INFO("Adding peer %u: gossip=%s:%u data=%s:%u", 
                 bootstrap_data.routers[i].node_id,
                 peer_gossip_ip, peer_gossip_port,
                 peer_data_ip, peer_data_port);
        
        // Add to peer pool
        int result = peer_pool_add(pool, 
                                   bootstrap_data.routers[i].node_id,
                                   peer_data_ip,
                                   peer_gossip_port,
                                   peer_data_port);
        
        if (result == RESULT_OK) {
            // Establish DATA channel
            peer_info_t *peer = peer_pool_get(pool, 
                                             bootstrap_data.routers[i].node_id);
            if (peer) {
                if (!peer->data_channel) {
                    peer->data_channel = safe_malloc(sizeof(rpc_channel_t));
                    if (peer->data_channel) {
                        if (rpc_client_connect(peer->data_channel, peer_data_ip, 
                                             peer_data_port, RPC_CHANNEL_DATA, 4096) == 0) {
                            LOG_INFO("DATA channel established to peer %u", 
                                   bootstrap_data.routers[i].node_id);
                        } else {
                            LOG_WARN("Failed to connect DATA channel to peer %u", 
                                   bootstrap_data.routers[i].node_id);
                            safe_free(peer->data_channel);
                            peer->data_channel = NULL;
                        }
                    }
                }
                peer_pool_release(pool);
            }
        } else {
            LOG_WARN("Failed to add peer %u (error: %d)", 
                   bootstrap_data.routers[i].node_id, result);
        }
    }
    
    LOG_INFO("Bootstrap completed successfully");
    return RESULT_OK;
}

// ============================================================================
// BOOTSTRAP WITH RETRY
// ============================================================================

int node_bootstrap_with_retry_ex(node_state_t *state,
                                 const roole_config_t *config, 
                                 int max_retries) {
    if (!state || !config) return RESULT_ERR_INVALID;
    
    if (config->router_count == 0) {
        LOG_INFO("No seed routers, starting as seed node");
        
        membership_handle_t *membership = state->membership;
        gossip_engine_t *gossip = ((struct membership_handle*)membership)->gossip_engine;
        gossip_engine_announce_join(gossip);
        
        return RESULT_OK;
    }
    
    int attempts = 0;
    
    while (attempts < max_retries) {
        attempts++;
        
        LOG_INFO("Bootstrap attempt %d/%d", attempts, max_retries);
        
        int result = node_bootstrap_from_config_ex(state, config);
        
        if (result == RESULT_OK) {
            LOG_INFO("Bootstrap succeeded on attempt %d", attempts);
            return RESULT_OK;
        }
        
        if (result == RESULT_ERR_TIMEOUT) {
            LOG_WARN("Bootstrap attempt %d timed out", attempts);
        } else {
            LOG_WARN("Bootstrap attempt %d failed with error %d", attempts, result);
        }
        
        if (attempts < max_retries) {
            LOG_INFO("Waiting 2 seconds before retry...");
            sleep(2);
        }
    }
    
    LOG_WARN("Bootstrap failed after %d attempts", max_retries);
    LOG_WARN("Continuing with gossip-based discovery");
    
    // Announce join anyway
    membership_handle_t *membership = state->membership;
    gossip_engine_t *gossip = ((struct membership_handle*)membership)->gossip_engine;
    gossip_engine_announce_join(gossip);
    
    return RESULT_ERR_TIMEOUT;
}