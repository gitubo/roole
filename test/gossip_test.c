// test/gossip_test.c - Tool to test gossip message serialization/deserialization

#define _POSIX_C_SOURCE 200809L

#include "roole/gossip.h"
#include "roole/common.h"
#include "roole/logger.h"
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <router_ip> <router_gossip_port>\n", argv[0]);
        fprintf(stderr, "Example: %s 127.0.0.1 7000\n", argv[0]);
        return 1;
    }

    const char *router_ip = argv[1];
    uint16_t router_port = (uint16_t)atoi(argv[2]);

    logger_init();
    logger_set_level(LOG_LEVEL_DEBUG);
    logger_set_context(100, "gossip-test", "test");

    LOG_INFO("========================================");
    LOG_INFO("Gossip Message Test Tool");
    LOG_INFO("  Target: %s:%u", router_ip, router_port);
    LOG_INFO("========================================\n");

    // Create WORKER_JOIN message (exactly like the worker does)
    gossip_message_t join_msg = {
        .version = 1,
        .msg_type = GOSSIP_MSG_WORKER_JOIN,
        .sender_id = 100,  // Test worker ID
        .sequence_num = 1,
        .num_updates = 1,
        .flags = 0
    };

    // Fill in update
    join_msg.updates[0].node_id = 100;
    join_msg.updates[0].node_type = NODE_TYPE_WORKER;
    join_msg.updates[0].status = NODE_STATUS_ALIVE;
    join_msg.updates[0].incarnation = 0;
    safe_strncpy(join_msg.updates[0].ip_address, "127.0.0.1", MAX_IP_LEN);
    join_msg.updates[0].gossip_port = 7100;
    join_msg.updates[0].data_port = 7101;
    join_msg.updates[0].timestamp_ms = time_now_ms();

    LOG_INFO("Created WORKER_JOIN message:");
    LOG_INFO("  version=%u type=%u sender=%u seq=%lu updates=%u",
            join_msg.version, join_msg.msg_type, join_msg.sender_id,
            join_msg.sequence_num, join_msg.num_updates);
    LOG_INFO("  Update[0]: node=%u type=%d status=%d ip=%s gossip=%u data=%u",
            join_msg.updates[0].node_id, join_msg.updates[0].node_type,
            join_msg.updates[0].status, join_msg.updates[0].ip_address,
            join_msg.updates[0].gossip_port, join_msg.updates[0].data_port);

    // Serialize
    uint8_t buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    ssize_t msg_size = gossip_message_serialize(&join_msg, buffer, sizeof(buffer));

    if (msg_size < 0) {
        LOG_ERROR("Failed to serialize message");
        logger_shutdown();
        return 1;
    }

    LOG_INFO("\nSerialized message: %zd bytes", msg_size);
    
    // Dump serialized data
    LOG_INFO("Raw bytes:");
    for (ssize_t i = 0; i < msg_size; i += 16) {
        char hex[64] = {0};
        char ascii[20] = {0};
        
        for (ssize_t j = 0; j < 16 && i + j < msg_size; j++) {
            sprintf(hex + j*3, "%02x ", buffer[i + j]);
            ascii[j] = (buffer[i + j] >= 32 && buffer[i + j] <= 126) ? buffer[i + j] : '.';
        }
        
        LOG_INFO("  [%04zd] %-48s | %s", i, hex, ascii);
    }

    // Test deserialization
    LOG_INFO("\nTesting deserialization...");
    gossip_message_t decoded_msg;
    int result = gossip_message_deserialize(buffer, msg_size, &decoded_msg);

    if (result != 0) {
        LOG_ERROR("Deserialization FAILED (returned %d)", result);
        logger_shutdown();
        return 1;
    }

    LOG_INFO("Deserialization SUCCESS!");
    LOG_INFO("  version=%u type=%u sender=%u seq=%lu updates=%u",
            decoded_msg.version, decoded_msg.msg_type, decoded_msg.sender_id,
            decoded_msg.sequence_num, decoded_msg.num_updates);

    if (decoded_msg.num_updates > 0) {
        LOG_INFO("  Update[0]: node=%u type=%d status=%d ip=%s gossip=%u data=%u",
                decoded_msg.updates[0].node_id, decoded_msg.updates[0].node_type,
                decoded_msg.updates[0].status, decoded_msg.updates[0].ip_address,
                decoded_msg.updates[0].gossip_port, decoded_msg.updates[0].data_port);
    }

    // Now send to actual router
    LOG_INFO("\n========================================");
    LOG_INFO("Sending to router at %s:%u", router_ip, router_port);
    LOG_INFO("========================================\n");

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        LOG_ERROR("Failed to create socket: %s", strerror(errno));
        logger_shutdown();
        return 1;
    }

    struct sockaddr_in router_addr;
    memset(&router_addr, 0, sizeof(router_addr));
    router_addr.sin_family = AF_INET;
    router_addr.sin_port = htons(router_port);

    if (inet_pton(AF_INET, router_ip, &router_addr.sin_addr) <= 0) {
        LOG_ERROR("Invalid router IP: %s", router_ip);
        close(sock);
        logger_shutdown();
        return 1;
    }

    ssize_t sent = sendto(sock, buffer, msg_size, 0,
                         (struct sockaddr*)&router_addr, sizeof(router_addr));

    if (sent < 0) {
        LOG_ERROR("sendto() failed: %s", strerror(errno));
        close(sock);
        logger_shutdown();
        return 1;
    }

    LOG_INFO("Sent %zd bytes to router", sent);

    // Wait for response (with 5 second timeout)
    struct timeval tv = {.tv_sec = 5, .tv_usec = 0};
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    LOG_INFO("Waiting for JOIN_RESPONSE (5 second timeout)...");

    uint8_t recv_buffer[GOSSIP_MAX_PAYLOAD_SIZE];
    ssize_t received = recvfrom(sock, recv_buffer, sizeof(recv_buffer), 0, NULL, NULL);

    if (received < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            LOG_WARN("TIMEOUT: No response from router");
        } else {
            LOG_ERROR("recvfrom() failed: %s", strerror(errno));
        }
        close(sock);
        logger_shutdown();
        return 1;
    }

    LOG_INFO("Received %zd bytes from router", received);

    // Deserialize response
    gossip_message_t response;
    if (gossip_message_deserialize(recv_buffer, received, &response) != 0) {
        LOG_ERROR("Failed to deserialize response");
        close(sock);
        logger_shutdown();
        return 1;
    }

    LOG_INFO("Response: type=%u sender=%u", response.msg_type, response.sender_id);

    if (response.msg_type == GOSSIP_MSG_JOIN_RESPONSE) {
        LOG_INFO("✓ Received JOIN_RESPONSE!");
        
        // Try to parse bootstrap data
        size_t header_size = 16 + response.num_updates * 44;
        if ((size_t)received > header_size) {
            gossip_bootstrap_response_t bootstrap;
            if (gossip_deserialize_bootstrap_response(recv_buffer + header_size,
                                                     received - header_size,
                                                     &bootstrap) == 0) {
                LOG_INFO("Bootstrap data: %u routers", bootstrap.num_routers);
                for (uint8_t i = 0; i < bootstrap.num_routers; i++) {
                    LOG_INFO("  Router[%u]: node=%u gossip=%s data=%s",
                            i, bootstrap.routers[i].node_id,
                            bootstrap.routers[i].gossip_addr,
                            bootstrap.routers[i].data_addr);
                }
            }
        }
    } else {
        LOG_WARN("Unexpected message type: %u", response.msg_type);
    }

    close(sock);
    logger_shutdown();

    return 0;
}