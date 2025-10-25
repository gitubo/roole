// test/worker.c - Worker binary entry point with INI config support

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include "roole/worker.h"
#include "roole/config.h"
#include "roole/common.h"

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
        fprintf(stderr, "Usage: %s <config_file.ini> [num_threads] [metrics_port]\n", argv[0]);
        fprintf(stderr, "Example: %s config/worker.ini 4 9090\n", argv[0]);
        fprintf(stderr, "\n");
        fprintf(stderr, "Arguments:\n");
        fprintf(stderr, "  config_file.ini  - INI configuration file (required)\n");
        fprintf(stderr, "  num_threads      - Number of executor threads (default: 4)\n");
        fprintf(stderr, "  metrics_port     - Prometheus metrics port (default: 9090, 0 to disable)\n");
        return 1;
    }

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

    // Optional command-line parameters
    size_t num_threads = (argc >= 3) ? (size_t)atoi(argv[2]) : 4;
    uint16_t metrics_port = (argc >= 4) ? (uint16_t)atoi(argv[3]) : 9090;

    // Validate num_threads
    if (num_threads == 0 || num_threads > 16) {
        fprintf(stderr, "Invalid number of threads: %zu (must be 1-16)\n", num_threads);
        return 1;
    }

    // Parse addresses
    char gossip_ip[16], data_ip[16];
    uint16_t gossip_port, data_port;
    
    config_parse_address(config.ports.gossip_addr, gossip_ip, &gossip_port);
    config_parse_address(config.ports.data_addr, data_ip, &data_port);

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
    
    log_set_level(LOG_LEVEL_INFO);
    
    LOG_INFO("========================================");
    LOG_INFO("Roole Worker Starting");
    LOG_INFO("========================================");
    LOG_INFO("Configuration:");
    LOG_INFO("  Cluster: %s", config.cluster_name);
    LOG_INFO("  Worker ID: %u", config.node_id);
    LOG_INFO("  GOSSIP: %s (port: %u)", config.ports.gossip_addr, gossip_port);
    LOG_INFO("  DATA: %s (port: %u)", config.ports.data_addr, data_port);
    LOG_INFO("  Executor Threads: %zu", num_threads);
    
    if (metrics_port > 0) {
        LOG_INFO("  Metrics Port: %u (http://0.0.0.0:%u/metrics)", metrics_port, metrics_port);
    } else {
        LOG_INFO("  Metrics: DISABLED");
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
                   gossip_ip, num_threads, metrics_port) != RESULT_OK) {
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

    // Bootstrap: join cluster via gossip
    if (config.router_count > 0) {
        LOG_INFO("Bootstrapping worker from cluster...");
        
        int bootstrap_result = worker_bootstrap_from_config(&g_worker, &config);
        
        if (bootstrap_result == RESULT_OK) {
            LOG_INFO("Worker bootstrap completed successfully");
            LOG_INFO("Connected to cluster, ready to receive messages");
        } else if (bootstrap_result == RESULT_ERR_TIMEOUT) {
            LOG_WARN("Worker bootstrap timed out (no response from seed routers)");
            LOG_WARN("Will retry via gossip protocol in background");
        } else {
            LOG_WARN("Worker bootstrap failed (error: %d)", bootstrap_result);
            LOG_WARN("Will retry via gossip protocol in background");
        }
    } else {
        LOG_WARN("No seed routers configured!");
        LOG_WARN("Worker running in standalone mode (not connected to cluster)");
        LOG_WARN("Add 'routers' configuration to join a cluster");
    }
    
    LOG_INFO("========================================");
    LOG_INFO("Worker is now running");
    LOG_INFO("  Message processing: %s:%u (DATA)", data_ip, data_port);
    LOG_INFO("  Gossip: %s:%u (UDP)", gossip_ip, gossip_port);
    if (metrics_port > 0) {
        LOG_INFO("  Metrics: http://0.0.0.0:%u/metrics", metrics_port);
    }
    LOG_INFO("========================================");
    LOG_INFO("Press Ctrl+C to stop");
    LOG_INFO("");
    
    // Main loop: wait for shutdown signal
    while (!g_shutdown_requested) {
        sleep(1);
        
        // Periodically update metrics
        if (g_worker.metrics && metrics_port > 0) {
            uint64_t uptime_seconds = (time_now_ms() - g_worker.start_time_ms) / 1000;
            worker_metrics_set_uptime(g_worker.metrics, uptime_seconds);
        }
    }
    
    // Graceful shutdown
    LOG_INFO("");
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