// test/router.c - Router binary entry point with INI config support
// UPDATED: Added metrics support

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include "roole/node.h"
#include "roole/config.h"
#include "roole/common.h"

typedef struct router_state {
    node_id_t router_id;
    char cluster_name[256];
    uint16_t gossip_port;
    uint16_t data_port;
    uint16_t ingress_port;
    char bind_addr[16];
    
    // Reuse unified structures
    dag_catalog_t dag_catalog;
    peer_pool_t worker_pool;
    execution_tracker_t exec_tracker;
    cluster_view_t cluster_view;
    membership_handle_t *membership;
    void *gossip_engine;
    
    pthread_t cleanup_thread;
    
    void *metrics_registry;
    void *metrics_server;
    void *metric_messages_routed_total;
    void *metric_messages_routed_failed;
    void *metric_uptime_seconds;
    void *metric_cluster_members_total;
    void *metric_cluster_members_active;
    void *metric_cluster_members_suspect;
    void *metric_cluster_members_dead;
    
    uint64_t start_time_ms;
    int shutdown_flag;
} router_state_t;

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
        fprintf(stderr, "\n");
        fprintf(stderr, "NOTE: This is the LEGACY router binary.\n");
        fprintf(stderr, "Consider using the unified 'node' binary instead:\n");
        fprintf(stderr, "  ./node config/router.ini\n");
        fprintf(stderr, "\n");
        return 1;
    }

    LOG_WARN("========================================");
    LOG_WARN("LEGACY BINARY NOTICE");
    LOG_WARN("========================================");
    LOG_WARN("You are using the legacy 'router' binary.");
    LOG_WARN("This binary will be deprecated in a future release.");
    LOG_WARN("Migration: Use the unified 'node' binary instead:");
    LOG_WARN("  ./node %s", argv[1]);
    LOG_WARN("The unified binary supports:");
    LOG_WARN("  - Same configuration files");
    LOG_WARN("  - Automatic capability detection");
    LOG_WARN("  - Hybrid node deployment");
    LOG_WARN("========================================");
    
    sleep(2);  // Give user time to read notice

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
    char gossip_ip[16], data_ip[16], ingress_ip[16], metrics_ip[16];
    uint16_t gossip_port, data_port, ingress_port, metrics_port = 0;
    
    config_parse_address(config.ports.gossip_addr, gossip_ip, &gossip_port);
    config_parse_address(config.ports.data_addr, data_ip, &data_port);
    config_parse_address(config.ports.ingress_addr, ingress_ip, &ingress_port);
    
    // Parse metrics address if provided
    if (strlen(config.ports.metrics_addr) > 0) {
        config_parse_address(config.ports.metrics_addr, metrics_ip, &metrics_port);
    }

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
    log_set_level(config.log_level);
    
    LOG_INFO("========================================");
    LOG_INFO("Roole Router Starting");
    LOG_INFO("========================================");
    LOG_INFO("Configuration:");
    LOG_INFO("  Cluster: %s", config.cluster_name);
    LOG_INFO("  Router ID: %u", config.node_id);
    LOG_INFO("  GOSSIP: %s (port: %u)", config.ports.gossip_addr, gossip_port);
    LOG_INFO("  DATA: %s (port: %u)", config.ports.data_addr, data_port);
    LOG_INFO("  INGRESS: %s (port: %u)", config.ports.ingress_addr, ingress_port);
    
    if (metrics_port > 0 && strlen(config.ports.metrics_addr) > 0) {
        LOG_INFO("  Metrics: %s (http://%s:%u/metrics)", 
                 config.ports.metrics_addr, metrics_ip, metrics_port);
    } else {
        LOG_INFO("  Metrics: DISABLED (not configured)");
    }
    
    if (config.router_count > 0) {
        LOG_INFO("  Seed Routers: %zu", config.router_count);
        for (size_t i = 0; i < config.router_count; i++) {
            LOG_INFO("    - %s", config.routers[i]);
        }
    } else {
        LOG_INFO("  No seed routers configured (starting standalone)");
    }
    LOG_INFO("========================================");

    // Initialize router (PASS metrics_addr!)
    if (router_init(&g_router, config.node_id, gossip_port, data_port, ingress_port,
                   gossip_ip, config.ports.metrics_addr, config.cluster_name) != RESULT_OK) {
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
    
    // Bootstrap using unified bootstrap
    if (config.router_count > 0) {
        LOG_INFO("Joining cluster via seed routers...");
        
        // Create temporary unified node for bootstrap
        unified_node_t temp_node;
        temp_node.node_id = g_router.router_id;
        safe_strncpy(temp_node.bind_addr, g_router.bind_addr, MAX_IP_LEN);
        temp_node.gossip_port = g_router.gossip_port;
        temp_node.data_port = g_router.data_port;
        temp_node.peer_pool = g_router.worker_pool;  // Map to peer pool
        temp_node.gossip_engine = g_router.gossip_engine;
        
        int bootstrap_result = node_bootstrap_with_retry(&temp_node, &config, 3);
        
        if (bootstrap_result == RESULT_OK) {
            LOG_INFO("Router bootstrap completed successfully");
        } else {
            LOG_WARN("Bootstrap incomplete, running as seed/standalone");
        }
    } else {
        LOG_INFO("No seed routers configured, running as seed node");
        gossip_engine_announce_join(g_router.gossip_engine);
    }
    
    // Start router background threads
    if (router_start(&g_router) != RESULT_OK) {
        LOG_ERROR("Failed to start router background threads");
        router_shutdown(&g_router);
        return 1;
    }
    
    // Create unified node wrapper for capability testing
    unified_node_t test_node;
    memset(&test_node, 0, sizeof(unified_node_t));
    
    test_node.node_id = config.node_id;
    safe_strncpy(test_node.cluster_name, config.cluster_name, MAX_CONFIG_STRING);
    
    // Detect capabilities
    node_detect_capabilities(&test_node, &config);
    node_print_capabilities(&test_node);
    
    // Verify ingress is enabled for routers
    if (!test_node.capabilities.has_ingress) {
        LOG_WARN("Router should have ingress capability!");
        LOG_WARN("Check that ingress_addr is properly configured in %s", argv[1]);
    }
    
    LOG_INFO("Capability Explanation:");
    LOG_INFO("  - INGRESS: Accepts client requests (port %u)", ingress_port);
    LOG_INFO("  - EXECUTE: Processes messages locally (currently disabled for routers)");
    LOG_INFO("  - ROUTE: Forwards messages to worker nodes");

    LOG_INFO("Router background threads started");
    
    // Set RPC state and start RPC servers (blocking)
    //router_set_rpc_state(&g_router);

    LOG_INFO("========================================");
    LOG_INFO("Router is now running");
    LOG_INFO("  Client requests: %s:%u (INGRESS)", ingress_ip, ingress_port);
    LOG_INFO("  Worker data: %s:%u (DATA)", data_ip, data_port);
    LOG_INFO("  Gossip: %s:%u (UDP)", gossip_ip, gossip_port);
    if (metrics_port > 0) {
        LOG_INFO("  Metrics: http://%s:%u/metrics", metrics_ip, metrics_port);
    }
    LOG_INFO("========================================");
    LOG_INFO("Press Ctrl+C to stop");

    // Start RPC servers in a separate thread so we can update metrics
    int rpc_result = rpc_router_run(data_port, ingress_port, router_rpc_service_table);

    if (rpc_result != 0) {
        LOG_ERROR("RPC server exited with error: %d", rpc_result);
    }

    // Main loop: update metrics periodically
    while (!g_shutdown_requested) {
        sleep(1);
        
        // Update uptime metric
        if (g_router.metric_uptime_seconds) {
            uint64_t uptime_seconds = (time_now_ms() - g_router.start_time_ms) / 1000;
            metrics_gauge_set(g_router.metric_uptime_seconds, (double)uptime_seconds);
        }
        
        // Update cluster metrics
        router_update_cluster_metrics(&g_router);
    }
    
    // Cleanup after shutdown signal
    LOG_INFO("Shutting down router...");
    router_shutdown(&g_router);
    
    LOG_INFO("========================================");
    LOG_INFO("Router stopped successfully");
    LOG_INFO("========================================");
    return 0;
}