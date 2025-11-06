// test/dag_register.c - Tool to register test DAGs

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "roole/rpc.h"
#include "roole/common.h"
#include "roole/logger.h"

static void print_usage(const char *prog) {
    fprintf(stderr, "Usage: %s <router_host> <ingress_port> <dag_id> <dag_name> <num_stages>\n", prog);
    fprintf(stderr, "Example: %s 127.0.0.1 8081 1 \"test_dag\" 3\n", prog);
    exit(1);
}

int main(int argc, char *argv[]) {
    if (argc != 6) {
        print_usage(argv[0]);
    }

    const char *host = argv[1];
    int port = atoi(argv[2]);
    uint32_t dag_id = (uint32_t)atoi(argv[3]);
    const char *dag_name = argv[4];
    uint32_t num_stages = (uint32_t)atoi(argv[5]);

    // Initialize logger
    logger_init(LOG_LEVEL_INFO, 0, NULL);
    logger_set_context(0, "dag-register", "register-tool");

    LOG_INFO("========================================");
    LOG_INFO("DAG Registration Tool");
    LOG_INFO("  Router: %s:%d", host, port);
    LOG_INFO("  DAG ID: %u", dag_id);
    LOG_INFO("  DAG Name: %s", dag_name);
    LOG_INFO("  Stages: %u", num_stages);
    LOG_INFO("========================================");
    LOG_INFO("");

    // Connect to router
    rpc_client_t *client = rpc_client_create(host, port, RPC_CHANNEL_INGRESS);
    if (!client) {
        LOG_ERROR("Failed to create RPC client");
        return 1;
    }

    LOG_INFO("Connected to router INGRESS channel");
    LOG_INFO("");

    // Build DAG registration request
    LOG_INFO("Registering DAG...");
    
    // Create a simple linear DAG: stage0 -> stage1 -> ... -> stageN
    size_t name_len = strlen(dag_name);
    size_t payload_size = sizeof(uint32_t) +  // dag_id
                          sizeof(uint32_t) +  // name_length
                          name_len +          // name
                          sizeof(uint32_t);   // num_stages
    
    // Add stage info: for each stage we need id + num_dependencies + dependency_ids
    for (uint32_t i = 0; i < num_stages; i++) {
        payload_size += sizeof(uint32_t);  // stage_id
        payload_size += sizeof(uint32_t);  // num_dependencies
        if (i > 0) {
            payload_size += sizeof(uint32_t);  // one dependency (previous stage)
        }
    }

    uint8_t *payload = malloc(payload_size);
    if (!payload) {
        LOG_ERROR("Memory allocation failed");
        rpc_client_destroy(client);
        return 1;
    }

    uint8_t *ptr = payload;
    
    // DAG ID
    *(uint32_t*)ptr = htonl(dag_id);
    ptr += sizeof(uint32_t);
    
    // DAG name length and name
    *(uint32_t*)ptr = htonl((uint32_t)name_len);
    ptr += sizeof(uint32_t);
    memcpy(ptr, dag_name, name_len);
    ptr += name_len;
    
    // Number of stages
    *(uint32_t*)ptr = htonl(num_stages);
    ptr += sizeof(uint32_t);
    
    // Stages (linear chain)
    for (uint32_t i = 0; i < num_stages; i++) {
        // Stage ID
        *(uint32_t*)ptr = htonl(i);
        ptr += sizeof(uint32_t);
        
        // Number of dependencies
        if (i == 0) {
            *(uint32_t*)ptr = htonl(0);  // First stage has no dependencies
            ptr += sizeof(uint32_t);
        } else {
            *(uint32_t*)ptr = htonl(1);  // Depends on previous stage
            ptr += sizeof(uint32_t);
            *(uint32_t*)ptr = htonl(i - 1);  // Dependency: previous stage
            ptr += sizeof(uint32_t);
        }
    }

    // Send registration request
    rpc_response_t response = {0};
    rpc_status_t status = rpc_client_call(
        client,
        RPC_METHOD_REGISTER_DAG,
        payload,
        payload_size,
        &response
    );

    free(payload);

    if (status != RPC_SUCCESS) {
        LOG_ERROR("DAG registration failed (status: %d)", status);
        rpc_client_destroy(client);
        return 1;
    }

    if (response.status_code != RPC_STATUS_OK) {
        LOG_ERROR("DAG registration rejected (code: %d)", response.status_code);
        if (response.payload_size > 0) {
            LOG_ERROR("Error: %.*s", (int)response.payload_size, (char*)response.payload);
        }
        rpc_response_free(&response);
        rpc_client_destroy(client);
        return 1;
    }

    LOG_INFO("✓ DAG %u registered successfully", dag_id);
    LOG_INFO("");

    rpc_response_free(&response);
    rpc_client_destroy(client);
    
    return 0;
}