// test/router.c - Router binary entry point with INI config support

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include "roole/router.h"
#include "roole/config.h"
#include "roole/common.h"

static router_state_t g_router;
static volatile int g_shutdown_requested = 0;

static void signal_handler(int sig) {
    (void)sig;
    LOG_INFO("Shutdown signal received");
    g_shutdown_requested = 1;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <config_file.ini>\n", argv[0]);
        fprintf(stderr, "Example: %s config/router.ini\n", argv[0]);
        return 1;
    }

    // Load configuration from INI file
    roole_config_t config;
    if (config_load_from_file(argv[1], &config) != 0) {
        fprintf(stderr, "Failed to load configuration from %s\n", argv[1]);
        return 1;
    }

    // Verify this is a router configuration
    if (config.node_type != NODE_TYPE_ROUTER) {
        fprintf(stderr, "Configuration is not for ROUTER node (type: %d)\n", config.node_type);
        fprintf(stderr, "Expected: NODE_TYPE_ROUTER (1)\n");
        return 1;
    }

    // Parse port addresses
    char gossip_ip[16], data_ip[16], ingress_ip[16];
    uint16_t gossip_port, data_port, ingress_port;
    
    config_parse_address(config.ports.gossip_addr, gossip_ip, &gossip_port);
    config_parse_address(config.ports.data_addr, data_ip, &data_port);
    config_parse_address(config.ports.ingress_addr, ingress_ip, &ingress_port);

    // Validate required ports
    if (gossip_port == 0) {
        fprintf(stderr, "Invalid gossip port in configuration\n");
        return 1;
    }
    if (data_port == 0) {
        fprintf(stderr, "Invalid data port in configuration\n");
        return 1;
    }
    if (ingress_port == 0) {
        fprintf(stderr, "Invalid ingress port in configuration\n");
        return 1;
    }

    // Setup signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Set log level
    log_set_level(LOG_LEVEL_INFO);
    
    LOG_INFO("========================================");
    LOG_INFO("Roole Router Starting");
    LOG_INFO("========================================");
    LOG_INFO("Configuration:");
    LOG_INFO("  Cluster: %s", config.cluster_name);
    LOG_INFO("  Router ID: %u", config.node_id);
    LOG_INFO("  GOSSIP: %s (port: %u)", config.ports.gossip_addr, gossip_port);
    LOG_INFO("  DATA: %s (port: %u)", config.ports.data_addr, data_port);
    LOG_INFO("  INGRESS: %s (port: %u)", config.ports.ingress_addr, ingress_port);
    
    if (config.router_count > 0) {
        LOG_INFO("  Seed Routers: %zu", config.router_count);
        for (size_t i = 0; i < config.router_count; i++) {
            LOG_INFO("    - %s", config.routers[i]);
        }
    } else {
        LOG_INFO("  No seed routers configured (starting standalone)");
    }
    LOG_INFO("========================================");

    // Initialize router
    if (router_init(&g_router, config.node_id, gossip_port, data_port, ingress_port,
                   gossip_ip) != RESULT_OK) {
        LOG_ERROR("Failed to initialize router");
        return 1;
    }
    
    LOG_INFO("Router initialized successfully");
    
    // Add test DAG for demonstration purposes
    dag_t test_dag = {
        .dag_id = 1,
        .version = 1,
        .step_count = 2,
        .created_at_ms = time_now_ms()
    };
    snprintf(test_dag.name, MAX_DAG_NAME, "test_dag");
    
    // Step 1: Parse
    test_dag.steps[0].step_id = 1;
    snprintf(test_dag.steps[0].name, MAX_STEP_NAME, "parse");
    snprintf(test_dag.steps[0].function_name, MAX_STEP_NAME, "parse_json");
    test_dag.steps[0].dependency_count = 0;
    test_dag.steps[0].config_data = NULL;
    test_dag.steps[0].config_len = 0;
    test_dag.steps[0].timeout_ms = 5000;
    test_dag.steps[0].max_retries = 3;
    
    // Step 2: Transform (depends on Step 1)
    test_dag.steps[1].step_id = 2;
    snprintf(test_dag.steps[1].name, MAX_STEP_NAME, "transform");
    snprintf(test_dag.steps[1].function_name, MAX_STEP_NAME, "transform_data");
    test_dag.steps[1].dependencies[0] = 1;
    test_dag.steps[1].dependency_count = 1;
    test_dag.steps[1].config_data = NULL;
    test_dag.steps[1].config_len = 0;
    test_dag.steps[1].timeout_ms = 10000;
    test_dag.steps[1].max_retries = 3;
    
    if (router_add_dag(&g_router, &test_dag) == RESULT_OK) {
        LOG_INFO("Test DAG (id=%u) added successfully", test_dag.dag_id);
    } else {
        LOG_WARN("Failed to add test DAG (may already exist)");
    }
    
    // Bootstrap: join cluster via gossip if seed routers are configured
    if (config.router_count > 0) {
        LOG_INFO("Joining cluster via seed routers...");
        
        int seeds_added = 0;
        for (size_t i = 0; i < config.router_count; i++) {
            char seed_ip[16];
            uint16_t seed_gossip_port;
            config_parse_address(config.routers[i], seed_ip, &seed_gossip_port);
            
            if (seed_gossip_port == 0) {
                LOG_WARN("Invalid seed router address: %s", config.routers[i]);
                continue;
            }
            
            if (gossip_engine_add_seed(g_router.gossip_engine, seed_ip, seed_gossip_port) == 0) {
                LOG_INFO("Added seed router: %s", config.routers[i]);
                seeds_added++;
            } else {
                LOG_WARN("Failed to add seed router: %s", config.routers[i]);
            }
        }
        
        if (seeds_added > 0) {
            gossip_engine_announce_join(g_router.gossip_engine);
            LOG_INFO("Cluster join announced (%d seeds contacted)", seeds_added);
        } else {
            LOG_WARN("No seed routers could be contacted, running standalone");
        }
    } else {
        LOG_INFO("No seed routers configured, running as standalone cluster");
        gossip_engine_announce_join(g_router.gossip_engine);
    }
    
    // Start router background threads
    if (router_start(&g_router) != RESULT_OK) {
        LOG_ERROR("Failed to start router background threads");
        router_shutdown(&g_router);
        return 1;
    }
    
    LOG_INFO("Router background threads started");
    
    // Set RPC state and start RPC servers (blocking)
    router_set_rpc_state(&g_router);

    LOG_INFO("========================================");
    LOG_INFO("Router is now running");
    LOG_INFO("  Client requests: %s:%u (INGRESS)", ingress_ip, ingress_port);
    LOG_INFO("  Worker data: %s:%u (DATA)", data_ip, data_port);
    LOG_INFO("  Gossip: %s:%u (UDP)", gossip_ip, gossip_port);
    LOG_INFO("========================================");
    LOG_INFO("Press Ctrl+C to stop");
    LOG_INFO("");

    // Start RPC servers (this blocks until shutdown)
    // Note: DATA port = 0 means no separate SERVICE channel
    int rpc_result = rpc_router_run(data_port, ingress_port, router_rpc_service_table);
    
    if (rpc_result != 0) {
        LOG_ERROR("RPC server exited with error: %d", rpc_result);
    }
    
    // Cleanup after RPC server stops
    LOG_INFO("Shutting down router...");
    router_shutdown(&g_router);
    
    LOG_INFO("========================================");
    LOG_INFO("Router stopped successfully");
    LOG_INFO("========================================");
    return 0;
}