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
static pthread_t g_rpc_server_thread;

static void* worker_rpc_server_thread_fn(void *arg) {
    worker_state_t *worker = (worker_state_t*)arg;
    
    LOG_INFO("RPC server thread starting...");
    
    // This blocks until shutdown - if it returns, something went wrong
    int result = rpc_worker_run(worker->service_port, worker->data_port, worker_rpc_service_table);
    
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
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <worker_id> <service_port> <data_port> [num_threads] [router_ip] [router_service_port] [router_data_port] [metrics_port]\n", argv[0]);
        fprintf(stderr, "Example: %s 100 5000 5001 4 127.0.0.1 6000 6001 9090\n", argv[0]);
        return 1;
    }

    node_id_t worker_id = (node_id_t)atoi(argv[1]);
    uint16_t service_port = (uint16_t)atoi(argv[2]);
    uint16_t data_port = (uint16_t)atoi(argv[3]);
    size_t num_threads = (argc >= 5) ? (size_t)atoi(argv[4]) : 4;

    const char *router_ip = (argc >= 6) ? argv[5] : NULL;
    uint16_t router_service_port = (argc >= 7) ? (uint16_t)atoi(argv[6]) : 6000;
    uint16_t router_data_port = (argc >= 8) ? (uint16_t)atoi(argv[7]) : 6001;
    uint16_t metrics_port = (argc >= 9) ? (uint16_t)atoi(argv[8]) : 9090; // Default: 9090
    
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
    LOG_INFO("  METRICS Port: %u", metrics_port);
    LOG_INFO("  Executor Threads: %zu", num_threads);
    LOG_INFO("========================================");

    // Initialize worker
    
    if (worker_init(&g_worker, worker_id, service_port, data_port, num_threads, metrics_port) != RESULT_OK) {
        LOG_ERROR("Failed to initialize worker");
        return 1;
    }
    
    // Start workesendr (background threads)
    if (worker_start(&g_worker) != RESULT_OK) {
        LOG_ERROR("Failed to start worker");
        worker_shutdown(&g_worker);
        return 1;
    }
    
    // CRITICAL: Set RPC state BEFORE starting RPC servers
    worker_set_rpc_state(&g_worker);

    // Start RPC servers in background thread (non-blocking)
    LOG_INFO("Starting RPC servers on SERVICE:%u, DATA:%u...",
                   service_port, data_port);
    
    if (pthread_create(&g_rpc_server_thread, NULL, worker_rpc_server_thread_fn, &g_worker) != 0) {
        LOG_ERROR("Failed to start RPC server thread");
        worker_shutdown(&g_worker);
        return 1;
    }
    
    // Detach thread so it doesn't need to be joined if it exits
    pthread_detach(g_rpc_server_thread);
    
    // Give RPC servers time to start listening
    sleep(1);

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
    
    LOG_INFO("Worker running. Press Ctrl+C to stop.");
    
    // Wait for shutdown signal
    while (!g_shutdown_requested) {
        sleep(1);
    }
    
    // Stop RPC server thread
    g_worker.shutdown_flag = 1;
    //pthread_join(rpc_server_thread, NULL);
    
    LOG_INFO("Worker stopped. Goodbye!");
    return 0;
}