// src/core/config_validator.c
#define _POSIX_C_SOURCE 200809L

#include "roole/config_validator.h"
#include "roole/logger.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h> 
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <stdarg.h>

// ============================================================================
// HELPERS
// ============================================================================

void validation_result_init(validation_result_t *result) {
    if (!result) return;
    memset(result, 0, sizeof(validation_result_t));
    result->valid = 1;  // Innocent until proven guilty
}

void validation_add_error(validation_result_t *result, int severity, const char *fmt, ...) {
    if (!result || result->error_count >= MAX_VALIDATION_ERRORS) return;
    
    validation_error_t *error = &result->errors[result->error_count];
    error->severity = severity;
    
    va_list args;
    va_start(args, fmt);
    vsnprintf(error->message, MAX_ERROR_MESSAGE_LEN, fmt, args);
    va_end(args);
    
    result->error_count++;
    
    if (severity >= 1) {  // Error or fatal
        result->valid = 0;
    }
}

void validation_result_print(const validation_result_t *result) {
    if (!result) return;
    
    if (result->valid && result->error_count == 0) {
        LOG_INFO("Configuration validation: PASSED");
        return;
    }
    
    LOG_WARN("========================================");
    LOG_WARN("Configuration Validation Issues");
    LOG_WARN("========================================");
    
    for (size_t i = 0; i < result->error_count; i++) {
        const validation_error_t *err = &result->errors[i];
        const char *severity_str = (err->severity == 0) ? "WARN" :
                                   (err->severity == 1) ? "ERROR" : "FATAL";
        LOG_WARN("[%s] %s", severity_str, err->message);
    }
    
    LOG_WARN("========================================");
    LOG_WARN("Status: %s", result->valid ? "PASSED (with warnings)" : "FAILED");
    LOG_WARN("========================================");
}

// ============================================================================
// NODE ID VALIDATION
// ============================================================================

int validate_node_id(node_id_t node_id, validation_result_t *result) {
    if (node_id == 0) {
        validation_add_error(result, 2, "Node ID cannot be 0 (reserved for invalid)");
        return -1;
    }
    
    // node_id_t is uint16_t, so max value is already 65535 (no need to check MAX_NODE_ID)
    return 0;
}

// ============================================================================
// NODE TYPE VALIDATION
// ============================================================================

int validate_node_type(node_type_t type, validation_result_t *result) {
    if (type == NODE_TYPE_UNKNOWN) {
        validation_add_error(result, 2, "Node type is UNKNOWN (must be ROUTER or WORKER)");
        return -1;
    }
    
    if (type != NODE_TYPE_ROUTER && type != NODE_TYPE_WORKER) {
        validation_add_error(result, 2, "Invalid node type: %d", type);
        return -1;
    }
    
    return 0;
}

// ============================================================================
// CLUSTER NAME VALIDATION
// ============================================================================

int validate_cluster_name(const char *cluster_name, validation_result_t *result) {
    if (!cluster_name || strlen(cluster_name) == 0) {
        validation_add_error(result, 2, "Cluster name is empty");
        return -1;
    }
    
    if (strlen(cluster_name) >= MAX_CONFIG_STRING) {
        validation_add_error(result, 2, "Cluster name too long (%zu chars, max %d)",
                           strlen(cluster_name), MAX_CONFIG_STRING);
        return -1;
    }
    
    // Check for invalid characters
    for (const char *p = cluster_name; *p; p++) {
        if (!isalnum((unsigned char)*p) && *p != '_' && *p != '-') {
            validation_add_error(result, 1, 
                               "Cluster name contains invalid character: '%c' (use alphanumeric, _, -)", *p);
            return -1;
        }
    }
    
    return 0;
}

// ============================================================================
// PORT VALIDATION
// ============================================================================

static int is_valid_port(uint16_t port) {
    return port > 0 && port != 22;  // Avoid SSH port
}

static int parse_port_from_addr(const char *addr_str) {
    if (!addr_str || strlen(addr_str) == 0) return 0;
    
    const char *colon = strchr(addr_str, ':');
    if (!colon) return 0;
    
    return atoi(colon + 1);
}

int validate_ports(const node_ports_t *ports, validation_result_t *result) {
    if (!ports) return -1;
    
    // Gossip port (required)
    int gossip_port = parse_port_from_addr(ports->gossip_addr);
    if (gossip_port <= 0) {
        validation_add_error(result, 2, "Gossip port is invalid or missing: %s", 
                           ports->gossip_addr);
        return -1;
    }
    if (!is_valid_port(gossip_port)) {
        validation_add_error(result, 2, "Gossip port %d is reserved or invalid", gossip_port);
        return -1;
    }
    
    // Data port (required)
    int data_port = parse_port_from_addr(ports->data_addr);
    if (data_port <= 0) {
        validation_add_error(result, 2, "Data port is invalid or missing: %s",
                           ports->data_addr);
        return -1;
    }
    if (!is_valid_port(data_port)) {
        validation_add_error(result, 2, "Data port %d is reserved or invalid", data_port);
        return -1;
    }
    
    // Ingress port (optional)
    int ingress_port = parse_port_from_addr(ports->ingress_addr);
    if (ingress_port > 0 && !is_valid_port(ingress_port)) {
        validation_add_error(result, 1, "Ingress port %d is reserved or invalid", ingress_port);
    }
    
    // Metrics port (optional)
    int metrics_port = parse_port_from_addr(ports->metrics_addr);
    if (metrics_port > 0 && !is_valid_port(metrics_port)) {
        validation_add_error(result, 1, "Metrics port %d is reserved or invalid", metrics_port);
    }
    
    return 0;
}

int validate_port_conflicts(const node_ports_t *ports, validation_result_t *result) {
    if (!ports) return -1;
    
    int gossip_port = parse_port_from_addr(ports->gossip_addr);
    int data_port = parse_port_from_addr(ports->data_addr);
    int ingress_port = parse_port_from_addr(ports->ingress_addr);
    int metrics_port = parse_port_from_addr(ports->metrics_addr);
    
    // Check for port conflicts
    if (gossip_port == data_port) {
        validation_add_error(result, 2, "Port conflict: gossip and data both use port %d", 
                           gossip_port);
        return -1;
    }
    
    if (ingress_port > 0) {
        if (ingress_port == gossip_port) {
            validation_add_error(result, 2, "Port conflict: ingress and gossip both use port %d",
                               ingress_port);
            return -1;
        }
        if (ingress_port == data_port) {
            validation_add_error(result, 2, "Port conflict: ingress and data both use port %d",
                               ingress_port);
            return -1;
        }
    }
    
    if (metrics_port > 0) {
        if (metrics_port == gossip_port || metrics_port == data_port || 
            (ingress_port > 0 && metrics_port == ingress_port)) {
            validation_add_error(result, 2, "Port conflict: metrics port %d conflicts with another service",
                               metrics_port);
            return -1;
        }
    }
    
    return 0;
}

// ============================================================================
// ROUTER VALIDATION
// ============================================================================

int validate_routers(const roole_config_t *config, validation_result_t *result) {
    if (!config) return -1;
    
    // If no routers, this is a seed node (valid)
    if (config->router_count == 0) {
        validation_add_error(result, 0, 
                           "No seed routers configured - node will start as seed/standalone");
        return 0;
    }
    
    // Validate each router address
    for (size_t i = 0; i < config->router_count; i++) {
        const char *router = config->routers[i];
        
        if (strlen(router) == 0) {
            validation_add_error(result, 1, "Router %zu has empty address", i);
            continue;
        }
        
        // Check format (should be ip:port)
        const char *colon = strchr(router, ':');
        if (!colon) {
            validation_add_error(result, 1, "Router %zu has invalid format (expected ip:port): %s",
                               i, router);
            continue;
        }
        
        int port = atoi(colon + 1);
        if (port <= 0 || port >= 65536) {
            validation_add_error(result, 1, "Router %zu has invalid port: %d", i, port);
        }
    }
    
    return 0;
}

// ============================================================================
// CAPABILITY VALIDATION
// ============================================================================

int validate_capabilities(const roole_config_t *config, validation_result_t *result) {
    if (!config) return -1;
    
    // Router nodes should have ingress configured
    if (config->node_type == NODE_TYPE_ROUTER) {
        int ingress_port = parse_port_from_addr(config->ports.ingress_addr);
        if (ingress_port <= 0) {
            validation_add_error(result, 0,
                               "ROUTER node has no ingress port - clients cannot connect");
        }
    }
    
    // Worker nodes shouldn't have ingress (warning, not error)
    if (config->node_type == NODE_TYPE_WORKER) {
        int ingress_port = parse_port_from_addr(config->ports.ingress_addr);
        if (ingress_port > 0) {
            validation_add_error(result, 0,
                               "WORKER node has ingress port configured (unusual - typically only ROUTERs accept clients)");
        }
    }
    
    return 0;
}

// ============================================================================
// NETWORK VALIDATION
// ============================================================================

int validate_network_reachability(const char *addr_str, validation_result_t *result) {
    if (!addr_str || strlen(addr_str) == 0) {
        return 0;  // Optional field, skip validation
    }
    
    char ip[64];
    // REMOVE: uint16_t port;  (not used)
    
    // Parse IP and port
    const char *colon = strchr(addr_str, ':');
    if (!colon) {
        validation_add_error(result, 1, "Invalid address format: %s (expected ip:port)", addr_str);
        return -1;
    }
    
    size_t ip_len = colon - addr_str;
    if (ip_len >= sizeof(ip)) {
        validation_add_error(result, 1, "IP address too long: %s", addr_str);
        return -1;
    }
    
    strncpy(ip, addr_str, ip_len);
    ip[ip_len] = '\0';
    // REMOVE: port = atoi(colon + 1);  (not used after parsing)
    
    // Validate IP format
    struct sockaddr_in sa;
    if (inet_pton(AF_INET, ip, &sa.sin_addr) != 1) {
        validation_add_error(result, 1, "Invalid IP address: %s", ip);
        return -1;
    }
    
    // Check for 0.0.0.0 (bind-only, not connectable)
    if (strcmp(ip, "0.0.0.0") == 0) {
        validation_add_error(result, 0, 
                           "Address %s uses 0.0.0.0 (bind-only, not externally reachable)", addr_str);
    }
    
    return 0;
}

// ============================================================================
// FULL VALIDATION
// ============================================================================

int config_validate(const roole_config_t *config, validation_result_t *result) {
    if (!config || !result) return -1;
    
    validation_result_init(result);
    
    LOG_INFO("Validating configuration...");
    
    // Validate each component
    validate_node_id(config->node_id, result);
    validate_node_type(config->node_type, result);
    validate_cluster_name(config->cluster_name, result);
    validate_ports(&config->ports, result);
    validate_port_conflicts(&config->ports, result);
    validate_routers(config, result);
    validate_capabilities(config, result);
    
    // Network reachability (warnings only)
    validate_network_reachability(config->ports.gossip_addr, result);
    validate_network_reachability(config->ports.data_addr, result);
    validate_network_reachability(config->ports.ingress_addr, result);
    validate_network_reachability(config->ports.metrics_addr, result);
    
    // Print results
    validation_result_print(result);
    
    return result->valid ? 0 : -1;
}