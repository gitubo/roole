// test/worker.c - Worker binary entry point

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include "roole/worker.h"
#include "roole/common.h"

static worker_state_t g_worker;
static volatile int g_shutdown_requested = 0;

static void signal_handler(int sig) {
    (void)sig;
    LOG_INFO("Shutdown signal received");
    g_shutdown_requested = 1;
}

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <worker_id> <service_port> <data_port> [num_threads] [router_ip] [router_service_port] [router_data_port]\n", argv[0]);
        fprintf(stderr, "Example: %s 100 5000 5001 4 127.0.0.1 6000 6001\n", argv[0]);
        return 1;
    }

    node_id_t worker_id = (node_id_t)atoi(argv[1]);
    uint16_t service_port = (uint16_t)atoi(argv[2]);
    uint16_t data_port = (uint16_t)atoi(argv[3]);
    size_t num_threads = (argc >= 5) ? (size_t)atoi(argv[4]) : 4;

    const char *router_ip = (argc >= 6) ? argv[5] : NULL;
    uint16_t router_service_port = (argc >= 7) ? (uint16_t)atoi(argv[6]) : 6000;
    uint16_t router_data_port = (argc >= 8) ? (uint16_t)atoi(argv[7]) : 6001;
    
    if (num_threads == 0 || num_threads > 16) {
        fprintf(stderr, "Invalid number of threads: %zu (must be 1-16)\n", num_threads);
        return 1;
    }
    
    // Setup signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Set log level
    log_set_level(LOG_LEVEL_INFO);
    
    LOG_INFO("========================================");
    LOG_INFO("Roole Worker Starting");
    LOG_INFO("  Worker ID: %u", worker_id);
    LOG_INFO("  SERVICE Port: %u", service_port);
    LOG_INFO("  DATA Port: %u", data_port);
    LOG_INFO("  Executor Threads: %zu", num_threads);
    LOG_INFO("========================================");

    // Initialize worker
    if (worker_init(&g_worker, worker_id, service_port, data_port, num_threads) != RESULT_OK) {
        LOG_ERROR("Failed to initialize worker");
        return 1;
    }
    
    // Start workesendr (background threads)
    if (worker_start(&g_worker) != RESULT_OK) {
        LOG_ERROR("Failed to start worker");
        worker_shutdown(&g_worker);
        return 1;
    }
    
    // Register with router if specified
    if (router_ip) {
        LOG_INFO("Attempting to register with router %s (SERVICE:%u, DATA:%u)...",
                       router_ip, router_service_port, router_data_port);
        if (worker_register_with_router(&g_worker, router_ip,
                                       router_service_port, router_data_port) == RESULT_OK) {
            LOG_INFO("Worker registered successfully!");
        } else {
            LOG_WARN("Failed to register with router (will retry via heartbeat)");
        }
    } else {
        LOG_WARN("No router specified. Worker running standalone.");
    }
    
    // Set RPC state and start RPC server (blocking)
    worker_set_rpc_state(&g_worker);

    LOG_INFO("Worker running. Press Ctrl+C to stop.");
    LOG_INFO("Starting RPC servers on SERVICE:%u, DATA:%u...",
                   service_port, data_port);

    // This blocks until shutdown
    rpc_worker_run(service_port, data_port, worker_rpc_service_table);
    
    // Cleanup after RPC server stops
    worker_shutdown(&g_worker);
    
    LOG_INFO("Worker stopped. Goodbye!");
    return 0;
}