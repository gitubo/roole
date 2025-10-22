// test/router.c - Router binary entry point

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include "roole/router.h"
#include "roole/common.h"

static router_state_t g_router;
static volatile int g_shutdown_requested = 0;

static void signal_handler(int sig) {
    (void)sig;
    ROOLE_LOG_INFO("Shutdown signal received");
    g_shutdown_requested = 1;
}

int main(int argc, char **argv) {
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <router_id> <service_port> <data_port> <ingress_port>\n", argv[0]);
        fprintf(stderr, "Example: %s 1 6000 6001 6002\n", argv[0]);
        return 1;
    }

    node_id_t router_id = (node_id_t)atoi(argv[1]);
    uint16_t service_port = (uint16_t)atoi(argv[2]);
    uint16_t data_port = (uint16_t)atoi(argv[3]);
    uint16_t ingress_port = (uint16_t)atoi(argv[4]);
    
    // Setup signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Set log level
    roole_log_set_level(ROOLE_LOG_INFO);
    
    ROOLE_LOG_INFO("========================================");
    ROOLE_LOG_INFO("Roole Router Starting");
    ROOLE_LOG_INFO("  Router ID: %u", router_id);
    ROOLE_LOG_INFO("  SERVICE Port: %u", service_port);
    ROOLE_LOG_INFO("  DATA Port: %u", data_port);
    ROOLE_LOG_INFO("  INGRESS Port: %u", ingress_port);
    ROOLE_LOG_INFO("========================================");

    // Initialize router
    if (router_init(&g_router, router_id, service_port, data_port, ingress_port) != ROOLE_OK) {
        ROOLE_LOG_ERROR("Failed to initialize router");
        return 1;
    }
    
    // Optional: Add a test DAG
    dag_t test_dag = {
        .dag_id = 1,
        .version = 1,
        .step_count = 2,
        .created_at_ms = roole_time_now_ms()
    };
    snprintf(test_dag.name, MAX_DAG_NAME, "test_dag");
    
    // Step 1
    test_dag.steps[0].step_id = 1;
    snprintf(test_dag.steps[0].name, MAX_STEP_NAME, "parse");
    snprintf(test_dag.steps[0].function_name, MAX_STEP_NAME, "parse_json");
    test_dag.steps[0].dependency_count = 0;
    test_dag.steps[0].config_data = NULL;
    test_dag.steps[0].config_len = 0;
    test_dag.steps[0].timeout_ms = 5000;
    test_dag.steps[0].max_retries = 3;
    
    // Step 2 (depends on Step 1)
    test_dag.steps[1].step_id = 2;
    snprintf(test_dag.steps[1].name, MAX_STEP_NAME, "transform");
    snprintf(test_dag.steps[1].function_name, MAX_STEP_NAME, "transform_data");
    test_dag.steps[1].dependencies[0] = 1;
    test_dag.steps[1].dependency_count = 1;
    test_dag.steps[1].config_data = NULL;
    test_dag.steps[1].config_len = 0;
    test_dag.steps[1].timeout_ms = 10000;
    test_dag.steps[1].max_retries = 3;
    
    if (router_add_dag(&g_router, &test_dag) != ROOLE_OK) {
        ROOLE_LOG_WARN("Failed to add test DAG (may be normal if already exists)");
    }
    
    // Start router (background threads)
    if (router_start(&g_router) != ROOLE_OK) {
        ROOLE_LOG_ERROR("Failed to start router");
        router_shutdown(&g_router);
        return 1;
    }
    
    // Set RPC state and start RPC server (blocking)
    router_set_rpc_state(&g_router);

    ROOLE_LOG_INFO("Router running. Press Ctrl+C to stop.");
    ROOLE_LOG_INFO("Starting RPC servers on SERVICE:%u, DATA:%u, INGRESS:%u...",
                   service_port, data_port, ingress_port);

    // This blocks until shutdown
    rpc_router_run(service_port, data_port, ingress_port, router_rpc_service_table);
    
    // Cleanup after RPC server stops
    router_shutdown(&g_router);
    
    ROOLE_LOG_INFO("Router stopped. Goodbye!");
    return 0;
}