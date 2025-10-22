// test/client.c - RPC Client for testing

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include "roole/rpc.h"
#include "roole/common.h"

#define FUNC_ID_SUBMIT_TASK 0x40
#define FUNC_ID_GET_STATUS 0x41

// Helper to send and receive RPC
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

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <router_ip> <router_port> [message]\n", argv[0]);
        fprintf(stderr, "Example: %s 127.0.0.1 5000 \"Hello World\"\n", argv[0]);
        return 1;
    }
    
    const char *router_ip = argv[1];
    uint16_t router_port = (uint16_t)atoi(argv[2]);
    const char *message = (argc >= 4) ? argv[3] : "Test Message";
    
    roole_log_set_level(ROOLE_LOG_INFO);
    
    ROOLE_LOG_INFO("========================================");
    ROOLE_LOG_INFO("Roole RPC Client");
    ROOLE_LOG_INFO("  Router: %s:%u", router_ip, router_port);
    ROOLE_LOG_INFO("  Message: %s", message);
    ROOLE_LOG_INFO("========================================\n");
    
    // Connect to router
    rpc_channel_t channel;
    if (rpc_router_init(&channel, router_ip, router_port, 4096) != 0) {
        ROOLE_LOG_ERROR("Failed to connect to router");
        return 1;
    }
    
    ROOLE_LOG_INFO("Connected to router\n");
    
    // ========================================================================
    // TEST 1: Submit Task
    // ========================================================================
    
    ROOLE_LOG_INFO("TEST 1: Submitting task...");
    
    dag_id_t dag_id = 1;  // Use test DAG created by router
    size_t message_len = strlen(message);
    
    // Build request payload: [dag_id][message]
    size_t payload_len = sizeof(dag_id_t) + message_len;
    uint8_t *payload = malloc(payload_len);
    memcpy(payload, &dag_id, sizeof(dag_id_t));
    memcpy(payload + sizeof(dag_id_t), message, message_len);
    
    // Pack RPC message
    static uint32_t request_id = 1;
    size_t rpc_msg_len = rpc_pack_message(
        channel.tx_buffer,
        0,
        request_id++,
        RPC_TYPE_REQUEST,
        RPC_STATUS_UNKNOWN,
        FUNC_ID_SUBMIT_TASK,
        payload,
        payload_len
    );
    free(payload);
    
    // Send request
    if (send_full(channel.socket_fd, channel.tx_buffer, rpc_msg_len) != 0) {
        ROOLE_LOG_ERROR("Failed to send request");
        rpc_channel_destroy(&channel);
        return 1;
    }
    
    // Receive response header
    rpc_header_t resp_header;
    if (recv_full(channel.socket_fd, channel.rx_buffer, RPC_HEADER_SIZE) != 0) {
        ROOLE_LOG_ERROR("Failed to receive response header");
        rpc_channel_destroy(&channel);
        return 1;
    }
    
    if (rpc_unpack_header(channel.rx_buffer, &resp_header) != 0) {
        ROOLE_LOG_ERROR("Invalid response header");
        rpc_channel_destroy(&channel);
        return 1;
    }
    
    // Receive response payload
    size_t resp_payload_len = resp_header.total_len - RPC_HEADER_SIZE;
    if (resp_payload_len > 0) {
        if (recv_full(channel.socket_fd, channel.rx_buffer + RPC_HEADER_SIZE, resp_payload_len) != 0) {
            ROOLE_LOG_ERROR("Failed to receive response payload");
            rpc_channel_destroy(&channel);
            return 1;
        }
    }
    
    // Parse response
    if (resp_header.type_and_status.fields.status != RPC_STATUS_SUCCESS) {
        ROOLE_LOG_ERROR("Task submission failed (status: %d)", 
                       resp_header.type_and_status.fields.status);
        rpc_channel_destroy(&channel);
        return 1;
    }
    
    if (resp_payload_len < 9) {
        ROOLE_LOG_ERROR("Invalid response payload length: %zu", resp_payload_len);
        rpc_channel_destroy(&channel);
        return 1;
    }
    
    execution_id_t exec_id;
    uint8_t exec_status;
    memcpy(&exec_id, channel.rx_buffer + RPC_HEADER_SIZE, sizeof(execution_id_t));
    memcpy(&exec_status, channel.rx_buffer + RPC_HEADER_SIZE + 8, 1);
    
    ROOLE_LOG_INFO("✓ Task submitted successfully!");
    ROOLE_LOG_INFO("  Execution ID: %lu", exec_id);
    ROOLE_LOG_INFO("  Status: %u (PENDING)\n", exec_status);
    
    // ========================================================================
    // TEST 2: Poll Execution Status
    // ========================================================================
    
    ROOLE_LOG_INFO("TEST 2: Polling execution status...\n");
    
    for (int i = 0; i < 10; i++) {
        sleep(1);
        
        // Build status request
        uint8_t status_payload[8];
        memcpy(status_payload, &exec_id, sizeof(execution_id_t));
        
        rpc_msg_len = rpc_pack_message(
            channel.tx_buffer,
            0,
            request_id++,
            RPC_TYPE_REQUEST,
            0,
            FUNC_ID_GET_STATUS,
            status_payload,
            sizeof(execution_id_t)
        );
        
        // Send status request
        if (send_full(channel.socket_fd, channel.tx_buffer, rpc_msg_len) != 0) {
            ROOLE_LOG_ERROR("Failed to send status request");
            break;
        }
        
        // Receive status response header
        if (recv_full(channel.socket_fd, channel.rx_buffer, RPC_HEADER_SIZE) != 0) {
            ROOLE_LOG_ERROR("Failed to receive status response");
            break;
        }
        
        if (rpc_unpack_header(channel.rx_buffer, &resp_header) != 0) {
            ROOLE_LOG_ERROR("Invalid status response header");
            break;
        }
        
        // Receive status payload
        resp_payload_len = resp_header.total_len - RPC_HEADER_SIZE;
        if (resp_payload_len > 0) {
            if (recv_full(channel.socket_fd, channel.rx_buffer + RPC_HEADER_SIZE, resp_payload_len) != 0) {
                ROOLE_LOG_ERROR("Failed to receive status payload");
                break;
            }
        }
        
        if (resp_header.type_and_status.fields.status != RPC_STATUS_SUCCESS) {
            ROOLE_LOG_ERROR("Status query failed");
            break;
        }
        
        uint8_t current_status = channel.rx_buffer[RPC_HEADER_SIZE];
        
        const char *status_str;
        switch (current_status) {
            case 0: status_str = "PENDING"; break;
            case 1: status_str = "RUNNING"; break;
            case 2: status_str = "COMPLETED"; break;
            case 3: status_str = "FAILED"; break;
            default: status_str = "UNKNOWN"; break;
        }
        
        ROOLE_LOG_INFO("[Poll %d/10] Status: %s", i + 1, status_str);
        
        if (current_status == 2) {  // COMPLETED
            ROOLE_LOG_INFO("\n✓ Execution completed successfully!");
            break;
        } else if (current_status == 3) {  // FAILED
            ROOLE_LOG_ERROR("\n✗ Execution failed!");
            break;
        }
    }
    
    // Cleanup
    rpc_channel_destroy(&channel);
    
    ROOLE_LOG_INFO("\n========================================");
    ROOLE_LOG_INFO("Client test completed");
    ROOLE_LOG_INFO("========================================");
    
    return 0;
}