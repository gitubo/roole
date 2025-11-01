// test/worker.c - Worker binary entry point with INI config support
// UPDATED: Uses new metrics system from config

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include "roole/node.h"
#include "roole/config.h"
#include "roole/common.h"

typedef struct worker_state {
    node_id_t worker_id;
    char cluster_name[256];
    uint16_t gossip_port;
    uint16_t data_port;
    char bind_addr[16];
    
    // Reuse unified structures
    dag_catalog_t dag_catalog;
    uint64_t catalog_version;
    message_queue_t message_queue;
    uint32_t active_executions;
    
    void *metrics_registry;
    void *metrics_server;
    void *metric_messages_processed;
    void *metric_messages_failed;
    void *metric_queue_size;
    void *metric_active_executions;
    void *metric_uptime_seconds;
    void *metric_cluster_members_total;
    void *metric_cluster_members_active;
    void *metric_cluster_members_suspect;
    void *metric_cluster_members_dead;
    
    uint64_t start_time_ms;
    
    struct router_connection {
        node_id_t router_id;
        char ip[16];
        uint16_t service_port;
        uint16_t data_port;
        void *service_channel;
        void *data_channel;
        uint64_t last_sync_ms;
        int active;
    } routers[16];
    
    size_t router_count;
    pthread_mutex_t routers_lock;
    
    cluster_view_t cluster_view;
    membership_handle_t *membership;
    void *gossip_engine;
    
    pthread_t executor_threads[16];
    size_t num_executor_threads;
    
    int shutdown_flag;
} worker_state_t;

static worker_state_t g_worker;
static volatile int g_shutdown_requested = 0;
static pthread_t g_rpc_server_thread;

static void* worker_rpc_server_thread_fn(void *arg) {
    worker_state_t *worker = (worker_state_t*)arg;
    
    LOG_INFO("RPC server thread starting...");
    
    int result = rpc_worker_run(worker->data_port, worker_rpc_service_table);
    
    if (result != 0) {
        LOG_ERROR("RPC server thread exited with error: %d", result);
    } else {
        LOG_INFO("RPC server thread exited normally");
    }
    
    return NULL;
}

static void signal_handler(int sig) {
    (void)sig;
    LOG_INFO("Shutdown signal received");
    g_shutdown_requested = 1;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <config_file.ini> [num_threads]\n", argv[0]);
        fprintf(stderr, "\n");
        fprintf(stderr, "NOTE: This is the LEGACY worker binary.\n");
        fprintf(stderr, "Consider using the unified 'node' binary instead:\n");
        fprintf(stderr, "  ./node config/worker_100.ini 4\n");
        fprintf(stderr, "\n");
        return 1;
    }

    LOG_WARN("========================================");
    LOG_WARN("LEGACY BINARY NOTICE");
    LOG_WARN("========================================");
    LOG_WARN("You are using the legacy 'worker' binary.");
    LOG_WARN("This binary will be deprecated in a future release.");
    LOG_WARN("Migration: Use the unified 'node' binary instead:");
    
    if (argc >= 3) {
        LOG_WARN("  ./node %s %s", argv[1], argv[2]);
    } else {
        LOG_WARN("  ./node %s", argv[1]);
    }
    
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

    // Verify this is a worker configuration
    if (config.node_type != NODE_TYPE_WORKER) {
        fprintf(stderr, "Configuration is not for WORKER node (type: %d)\n", config.node_type);
        fprintf(stderr, "Expected: NODE_TYPE_WORKER (2)\n");
        return 1;
    }

    // Optional command-line parameter
    size_t num_threads = (argc >= 3) ? (size_t)atoi(argv[2]) : 4;

    // Validate num_threads
    if (num_threads == 0 || num_threads > 16) {
        fprintf(stderr, "Invalid number of threads: %zu (must be 1-16)\n", num_threads);
        return 1;
    }

    // Parse addresses
    char gossip_ip[16], data_ip[16], metrics_ip[16];
    uint16_t gossip_port, data_port, metrics_port = 0;
    
    config_parse_address(config.ports.gossip_addr, gossip_ip, &gossip_port);
    config_parse_address(config.ports.data_addr, data_ip, &data_port);
    
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

    // Setup signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Seed random number generator for load balancing
    srand(time(NULL) ^ getpid());
    
    log_set_level(config.log_level);
    
    LOG_INFO("========================================");
    LOG_INFO("Roole Worker Starting");
    LOG_INFO("========================================");
    LOG_INFO("Configuration:");
    LOG_INFO("  Cluster: %s", config.cluster_name);
    LOG_INFO("  Worker ID: %u", config.node_id);
    LOG_INFO("  GOSSIP: %s (port: %u)", config.ports.gossip_addr, gossip_port);
    LOG_INFO("  DATA: %s (port: %u)", config.ports.data_addr, data_port);
    LOG_INFO("  Executor Threads: %zu", num_threads);
    
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
        LOG_WARN("  No seed routers configured!");
    }
    LOG_INFO("========================================");

    // Initialize worker
    if (worker_init(&g_worker, config.node_id, gossip_port, data_port, 
                   gossip_ip, num_threads, config.ports.metrics_addr, config.cluster_name) != RESULT_OK) {
        LOG_ERROR("Failed to initialize worker");
        return 1;
    }
    
    LOG_INFO("Worker initialized successfully");
    
    // Start worker background threads (executor threads)
    if (worker_start(&g_worker) != RESULT_OK) {
        LOG_ERROR("Failed to start worker background threads");
        worker_shutdown(&g_worker);
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
    
    // Verify worker configuration
    if (test_node.capabilities.has_ingress) {
        LOG_WARN("Worker has ingress capability (unusual configuration)");
        LOG_WARN("Workers typically don't accept direct client requests");
        LOG_WARN("Consider removing ingress_addr from %s", argv[1]);
    }
    
    LOG_INFO("Capability Explanation:");
    LOG_INFO("  - INGRESS: Disabled (workers don't accept client requests)");
    LOG_INFO("  - EXECUTE: Processes messages routed from ingress nodes");
    LOG_INFO("  - ROUTE: Can forward messages to other nodes if needed");
    
    LOG_INFO("Worker executor threads started (%zu threads)", num_threads);
    
    // Set RPC state BEFORE starting RPC servers
    worker_set_rpc_state(&g_worker);

    // Start RPC server in background thread (non-blocking)
    LOG_INFO("Starting RPC server on DATA:%u...", data_port);
    
    if (pthread_create(&g_rpc_server_thread, NULL, worker_rpc_server_thread_fn, &g_worker) != 0) {
        LOG_ERROR("Failed to start RPC server thread");
        worker_shutdown(&g_worker);
        return 1;
    }
    
    // Detach thread so we don't need to join it
    pthread_detach(g_rpc_server_thread);
    
    // Give RPC server time to start listening
    sleep(1);
    LOG_INFO("RPC server thread started");

    // Bootstrap using unified node bootstrap
    if (config.router_count > 0) {
        LOG_INFO("Bootstrapping worker from cluster...");
        
        // Create temporary unified node for bootstrap
        unified_node_t temp_node;
        temp_node.node_id = g_worker.worker_id;
        //temp_node.bind_addr = g_worker.bind_addr;
        safe_strncpy(temp_node.bind_addr, g_worker.bind_addr, MAX_IP_LEN);
        temp_node.gossip_port = g_worker.gossip_port;
        temp_node.data_port = g_worker.data_port;
//        temp_node.peer_pool = g_worker.worker_pool;  // Share peer pool
        temp_node.gossip_engine = g_worker.gossip_engine;
        
        int bootstrap_result = node_bootstrap_with_retry(&temp_node, &config, 3);

        if (bootstrap_result == RESULT_OK) {
            LOG_INFO("Worker bootstrap completed successfully");
        } else {
            LOG_WARN("Bootstrap incomplete, will retry via gossip");
        }
    } else {
        LOG_WARN("No seed routers configured!");
    }
        
    LOG_INFO("========================================");
    LOG_INFO("Worker is now running");
    LOG_INFO("  Data: %s:%u", data_ip, data_port);
    LOG_INFO("  Gossip: %s:%u (UDP)", gossip_ip, gossip_port);
    if (metrics_port > 0) {
        LOG_INFO("  Metrics: http://%s:%u/metrics", metrics_ip, metrics_port);
    }
    LOG_INFO("========================================");
    LOG_INFO("Press Ctrl+C to stop");
    
    // Main loop: wait for shutdown signal and update metrics
    while (!g_shutdown_requested) {
        sleep(1);
        
        // Periodically update uptime metric
        if (g_worker.metric_uptime_seconds) {
            uint64_t uptime_seconds = (time_now_ms() - g_worker.start_time_ms) / 1000;
            metrics_gauge_set(g_worker.metric_uptime_seconds, (double)uptime_seconds);
        }
    }
    
    // Graceful shutdown
    LOG_INFO("========================================");
    LOG_INFO("Shutting down worker...");
    LOG_INFO("========================================");
    
    // Announce leave to cluster
    if (g_worker.membership) {
        LOG_INFO("Announcing graceful leave to cluster...");
        membership_leave(g_worker.membership);
    }
    
    // Stop accepting new work
    g_worker.shutdown_flag = 1;
    
    // Give time for in-flight messages to complete
    LOG_INFO("Waiting for in-flight messages to complete...");
    sleep(2);
    
    // Full shutdown
    worker_shutdown(&g_worker);
    
    LOG_INFO("========================================");
    LOG_INFO("Worker stopped successfully");
    LOG_INFO("========================================");
    return 0;
}