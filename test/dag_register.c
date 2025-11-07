// test/dag_register.c - DAG Registration Tool (Updated for New RPC API)

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <errno.h>
#include "roole/rpc.h"
#include "roole/common.h"
#include "roole/logger.h"

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

static int send_full(int fd, const uint8_t *buffer, size_t length) {
    size_t total_sent = 0;
    while (total_sent < length) {
        ssize_t bytes = send(fd, buffer + total_sent, length - total_sent, 0);
        if (bytes <= 0) {
            if (bytes < 0 && errno == EINTR) continue;
            return -1;
        }
        total_sent += bytes;
    }
    return 0;
}

static int recv_full(int fd, uint8_t *buffer, size_t length) {
    size_t total_recv = 0;
    while (total_recv < length) {
        ssize_t bytes = recv(fd, buffer + total_recv, length - total_recv, 0);
        if (bytes <= 0) {
            if (bytes < 0 && errno == EINTR) continue;
            return -1;
        }
        total_recv += bytes;
    }
    return 0;
}

// ============================================================================
// MAIN
// ============================================================================

int main(int argc, char *argv[]) {
    if (argc != 6) {
        fprintf(stderr, "Usage: %s <router_host> <ingress_port> <dag_id> <dag_name> <num_stages>\n", argv[0]);
        fprintf(stderr, "Example: %s 127.0.0.1 8081 1 \"test_dag\" 3\n", argv[0]);
        return 1;
    }

    const char *host = argv[1];
    uint16_t port = (uint16_t)atoi(argv[2]);
    uint32_t dag_id = (uint32_t)atoi(argv[3]);
    const char *dag_name = argv[4];
    uint32_t num_stages = (uint32_t)atoi(argv[5]);

    // Initialize logger
    logger_init();  // ✅ Fixed: no arguments
    logger_set_level(LOG_LEVEL_INFO);
    logger_set_context(0, "dag-register", "register-tool");

    LOG_INFO("========================================");
    LOG_INFO("DAG Registration Tool");
    LOG_INFO("  Router: %s:%u", host, port);
    LOG_INFO("  DAG ID: %u", dag_id);
    LOG_INFO("  DAG Name: %s", dag_name);
    LOG_INFO("  Stages: %u", num_stages);
    LOG_INFO("========================================");

    // Connect to router's INGRESS channel
    rpc_channel_t channel;
    if (rpc_client_connect(&channel, host, port, RPC_CHANNEL_INGRESS, 4096) != 0) {
        LOG_ERROR("Failed to connect to router INGRESS channel");
        logger_shutdown();
        return 1;
    }

    LOG_INFO("Connected to router INGRESS channel");

    // ========================================================================
    // BUILD DAG REGISTRATION PAYLOAD
    // ========================================================================
    
    // Linear DAG: stage0 -> stage1 -> ... -> stageN
    size_t name_len = strlen(dag_name);
    size_t payload_size = sizeof(uint32_t) +  // dag_id
                          sizeof(uint32_t) +  // name_length
                          name_len +          // name
                          sizeof(uint32_t);   // num_stages
    
    // Add stage info: id + num_deps + dep_ids
    for (uint32_t i = 0; i < num_stages; i++) {
        payload_size += sizeof(uint32_t);  // stage_id
        payload_size += sizeof(uint32_t);  // num_dependencies
        if (i > 0) {
            payload_size += sizeof(uint32_t);  // dependency (previous stage)
        }
    }

    uint8_t *payload = malloc(payload_size);
    if (!payload) {
        LOG_ERROR("Memory allocation failed");
        rpc_channel_destroy(&channel);
        logger_shutdown();
        return 1;
    }

    uint8_t *ptr = payload;
    
    // DAG ID (network byte order)
    *(uint32_t*)ptr = htonl(dag_id);
    ptr += sizeof(uint32_t);
    
    // Name length and name
    *(uint32_t*)ptr = htonl((uint32_t)name_len);
    ptr += sizeof(uint32_t);
    memcpy(ptr, dag_name, name_len);
    ptr += name_len;
    
    // Number of stages
    *(uint32_t*)ptr = htonl(num_stages);
    ptr += sizeof(uint32_t);
    
    // Stages (linear chain: each depends on previous)
    for (uint32_t i = 0; i < num_stages; i++) {
        // Stage ID
        *(uint32_t*)ptr = htonl(i);
        ptr += sizeof(uint32_t);
        
        // Number of dependencies
        if (i == 0) {
            *(uint32_t*)ptr = htonl(0);  // First stage has no deps
            ptr += sizeof(uint32_t);
        } else {
            *(uint32_t*)ptr = htonl(1);  // Depends on previous
            ptr += sizeof(uint32_t);
            *(uint32_t*)ptr = htonl(i - 1);  // Dependency ID
            ptr += sizeof(uint32_t);
        }
    }

    LOG_INFO("Payload built: %zu bytes", payload_size);

    // ========================================================================
    // SEND RPC REQUEST
    // ========================================================================
    
    static uint32_t request_id = 1;
    
    size_t rpc_msg_len = rpc_pack_message(
        channel.tx_buffer,
        0,  // sender_id (client = 0)
        request_id++,
        RPC_TYPE_REQUEST,
        RPC_STATUS_UNKNOWN,
        FUNC_ID_ADD_DAG,  // ✅ Correct function ID for DAG registration
        payload,
        payload_size
    );

    free(payload);

    LOG_INFO("Sending DAG registration request...");

    if (send_full(channel.socket_fd, channel.tx_buffer, rpc_msg_len) != 0) {
        LOG_ERROR("Failed to send request: %s", strerror(errno));
        rpc_channel_destroy(&channel);
        logger_shutdown();
        return 1;
    }

    // ========================================================================
    // RECEIVE RESPONSE
    // ========================================================================
    
    LOG_INFO("Waiting for response...");

    rpc_header_t resp_header;
    if (recv_full(channel.socket_fd, channel.rx_buffer, RPC_HEADER_SIZE) != 0) {
        LOG_ERROR("Failed to receive response header: %s", strerror(errno));
        rpc_channel_destroy(&channel);
        logger_shutdown();
        return 1;
    }

    if (rpc_unpack_header(channel.rx_buffer, &resp_header) != 0) {
        LOG_ERROR("Invalid response header");
        rpc_channel_destroy(&channel);
        logger_shutdown();
        return 1;
    }

    // Receive response payload (if any)
    size_t resp_payload_len = resp_header.total_len - RPC_HEADER_SIZE;
    if (resp_payload_len > 0) {
        if (recv_full(channel.socket_fd, channel.rx_buffer + RPC_HEADER_SIZE, 
                     resp_payload_len) != 0) {
            LOG_ERROR("Failed to receive response payload");
            rpc_channel_destroy(&channel);
            logger_shutdown();
            return 1;
        }
    }

    // ========================================================================
    // PARSE RESPONSE
    // ========================================================================
    
    if (resp_header.type_and_status.fields.status != RPC_STATUS_SUCCESS) {
        LOG_ERROR("DAG registration failed (status: %u)", 
                 resp_header.type_and_status.fields.status);
        
        if (resp_payload_len > 0) {
            LOG_ERROR("Error message: %.*s", 
                     (int)resp_payload_len, 
                     (char*)(channel.rx_buffer + RPC_HEADER_SIZE));
        }
        
        rpc_channel_destroy(&channel);
        logger_shutdown();
        return 1;
    }

    LOG_INFO("✓ DAG %u registered successfully", dag_id);

    // Cleanup
    rpc_channel_destroy(&channel);
    logger_shutdown();

    return 0;
}