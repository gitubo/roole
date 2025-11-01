// test/node.c - Complete rewrite for production-ready unified node

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include "roole/node.h"
#include "roole/config.h"
#include "roole/common.h"

static unified_node_t g_node;
static volatile int g_shutdown_requested = 0;
static pthread_t g_rpc_thread;

static void signal_handler(int sig) {
    (void)sig;
    LOG_INFO("Shutdown signal received");
    g_shutdown_requested = 1;
}

static void* rpc_server_thread_fn(void *arg) {
    rpc_service_entry_t *service_table = (rpc_service_entry_t*)arg;
    
    LOG_INFO("RPC server thread starting...");
    
    int result = node_start_rpc_servers(&g_node, service_table);
    
    if (result != 0) {
        LOG_ERROR("RPC server thread exited with error: %d", result);
    }
    
    return NULL;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <config_file.ini> [num_threads]\n", argv[0]);
        fprintf(stderr, "\n");
        fprintf(stderr, "Examples:\n");
        fprintf(stderr, "  %s config/router.ini         # Ingress node (accepts clients)\n", argv[0]);
        fprintf(stderr, "  %s config/worker_100.ini 4   # Compute node (4 threads)\n", argv[0]);
        fprintf(stderr, "  %s config/hybrid_node.ini 8  # Hybrid (ingress + compute)\n", argv[0]);
        fprintf(stderr, "\n");
        fprintf(stderr, "Node capabilities determined by config:\n");
        fprintf(stderr, "  - ingress_addr present = accepts client requests\n");
        fprintf(stderr, "  - Always executes messages (configurable threads)\n");
        fprintf(stderr, "  - Always routes to peers if needed\n");
        return 1;
    }

    // Load configuration
    roole_config_t config;
    if (config_load_from_file(argv[1], &config) != 0) {
        fprintf(stderr, "Failed to load configuration from %s\n", argv[1]);
        return 1;
    }

    // Number of executor threads (default: 4)
    size_t num_threads = (argc >= 3) ? (size_t)atoi(argv[2]) : 4;
    if (num_threads == 0 || num_threads > 16) {
        fprintf(stderr, "Invalid threads: %zu (must be 1-16)\n", num_threads);
        return 1;
    }

    // Setup
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    srand(time(NULL) ^ getpid());
    log_set_level(config.log_level);
    
    LOG_INFO("========================================");
    LOG_INFO("Roole Unified Node");
    LOG_INFO("========================================");
    LOG_INFO("Config: %s", argv[1]);
    LOG_INFO("Node ID: %u", config.node_id);
    LOG_INFO("Cluster: %s", config.cluster_name);
    LOG_INFO("Threads: %zu", num_threads);
    LOG_INFO("========================================");

    // Initialize node
    if (node_init(&g_node, &config, num_threads) != RESULT_OK) {
        LOG_ERROR("Failed to initialize node");
        return 1;
    }

    // Add test DAG (if ingress-capable)
    if (g_node.capabilities.has_ingress) {
        dag_t test_dag = {
            .dag_id = 1,
            .version = 1,
            .step_count = 2,
            .created_at_ms = time_now_ms()
        };
        snprintf(test_dag.name, MAX_DAG_NAME, "test_dag");
        
        test_dag.steps[0].step_id = 1;
        snprintf(test_dag.steps[0].name, MAX_STEP_NAME, "parse");
        snprintf(test_dag.steps[0].function_name, MAX_STEP_NAME, "parse_json");
        test_dag.steps[0].dependency_count = 0;
        test_dag.steps[0].config_data = NULL;
        test_dag.steps[0].config_len = 0;
        test_dag.steps[0].timeout_ms = 5000;
        test_dag.steps[0].max_retries = 3;
        
        test_dag.steps[1].step_id = 2;
        snprintf(test_dag.steps[1].name, MAX_STEP_NAME, "transform");
        snprintf(test_dag.steps[1].function_name, MAX_STEP_NAME, "transform_data");
        test_dag.steps[1].dependencies[0] = 1;
        test_dag.steps[1].dependency_count = 1;
        test_dag.steps[1].config_data = NULL;
        test_dag.steps[1].config_len = 0;
        test_dag.steps[1].timeout_ms = 10000;
        test_dag.steps[1].max_retries = 3;
        
        dag_catalog_add(&g_node.dag_catalog, &test_dag);
        LOG_INFO("Test DAG added (id=%u)", test_dag.dag_id);
    }

    // Start background threads
    if (node_start(&g_node) != RESULT_OK) {
        LOG_ERROR("Failed to start node");
        node_shutdown(&g_node);
        return 1;
    }

    // Bootstrap
    if (config.router_count > 0) {
        LOG_INFO("Bootstrapping from seed routers...");
        node_bootstrap_with_retry(&g_node, &config, 3);
    } else {
        LOG_INFO("No seeds, running as standalone/seed");
        gossip_engine_announce_join(g_node.gossip_engine);
    }

    // Build RPC service table
    rpc_service_entry_t *service_table = node_build_rpc_service_table(&g_node);
    if (!service_table) {
        LOG_ERROR("Failed to build RPC service table");
        node_shutdown(&g_node);
        return 1;
    }

    LOG_INFO("========================================");
    LOG_INFO("Node Running");
    LOG_INFO("  ID: %u", g_node.node_id);
    LOG_INFO("  Data: %s:%u", g_node.bind_addr, g_node.data_port);
    LOG_INFO("  Gossip: %s:%u", g_node.bind_addr, g_node.gossip_port);
    
    if (g_node.capabilities.has_ingress) {
        LOG_INFO("  Ingress: %s:%u ★ CLIENT FACING", 
                 g_node.bind_addr, g_node.ingress_port);
    }
    
    if (g_node.metrics_port > 0) {
        LOG_INFO("  Metrics: http://%s:%u/metrics", 
                 g_node.bind_addr, g_node.metrics_port);
    }
    
    LOG_INFO("========================================");
    LOG_INFO("Press Ctrl+C to stop");

    // Start RPC servers in background thread
    if (pthread_create(&g_rpc_thread, NULL, rpc_server_thread_fn, service_table) != 0) {
        LOG_ERROR("Failed to start RPC thread");
        node_free_rpc_service_table(service_table);
        node_shutdown(&g_node);
        return 1;
    }
    
    pthread_detach(g_rpc_thread);
    sleep(1);  // Let RPC thread initialize

    // Main loop: update metrics periodically
    while (!g_shutdown_requested) {
        sleep(1);
        node_metrics_update_periodic(&g_node);
    }

    // Shutdown
    LOG_INFO("Shutting down...");
    
    node_free_rpc_service_table(service_table);
    node_shutdown(&g_node);
    
    LOG_INFO("Node stopped successfully");
    return 0;
}