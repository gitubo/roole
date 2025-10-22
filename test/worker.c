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
    ROOLE_LOG_INFO("Shutdown signal received");
    g_shutdown_requested = 1;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <worker_id> <port> [num_threads] [router_ip] [router_port]\n", argv[0]);
        fprintf(stderr, "Example: %s 100 6000 4 127.0.0.1 5000\n", argv[0]);
        return 1;
    }
    
    node_id_t worker_id = (node_id_t)atoi(argv[1]);
    uint16_t port = (uint16_t)atoi(argv[2]);
    size_t num_threads = (argc >= 4) ? (size_t)atoi(argv[3]) : 4;
    
    const char *router_ip = (argc >= 5) ? argv[4] : NULL;
    uint16_t router_port = (argc >= 6) ? (uint16_t)atoi(argv[5]) : 5000;
    
    if (num_threads == 0 || num_threads > 16) {
        fprintf(stderr, "Invalid number of threads: %zu (must be 1-16)\n", num_threads);
        return 1;
    }
    
    // Setup signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Set log level
    roole_log_set_level(ROOLE_LOG_INFO);
    
    ROOLE_LOG_INFO("========================================");
    ROOLE_LOG_INFO("Roole Worker Starting");
    ROOLE_LOG_INFO("  Worker ID: %u", worker_id);
    ROOLE_LOG_INFO("  Port: %u", port);
    ROOLE_LOG_INFO("  Executor Threads: %zu", num_threads);
    ROOLE_LOG_INFO("========================================");
    
    // Initialize worker
    if (worker_init(&g_worker, worker_id, port, num_threads) != ROOLE_OK) {
        ROOLE_LOG_ERROR("Failed to initialize worker");
        return 1;
    }
    
    // Start workesendr (background threads)
    if (worker_start(&g_worker) != ROOLE_OK) {
        ROOLE_LOG_ERROR("Failed to start worker");
        worker_shutdown(&g_worker);
        return 1;
    }
    
    // Register with router if specified
    if (router_ip) {
        ROOLE_LOG_INFO("Attempting to register with router %s:%u...", router_ip, router_port);
        if (worker_register_with_router(&g_worker, router_ip, router_port) == ROOLE_OK) {
            ROOLE_LOG_INFO("Worker registered successfully!");
        } else {
            ROOLE_LOG_WARN("Failed to register with router (will retry via heartbeat)");
        }
    } else {
        ROOLE_LOG_WARN("No router specified. Worker running standalone.");
    }
    
    // Set RPC state and start RPC server (blocking)
    worker_set_rpc_state(&g_worker);
    
    ROOLE_LOG_INFO("Worker running. Press Ctrl+C to stop.");
    ROOLE_LOG_INFO("Starting RPC server on port %u...", port);
    
    // This blocks until shutdown
    rpc_worker_run(port, worker_rpc_service_table);
    
    // Cleanup after RPC server stops
    worker_shutdown(&g_worker);
    
    ROOLE_LOG_INFO("Worker stopped. Goodbye!");
    return 0;
}