// src/node/node_capabilities.c

#define _POSIX_C_SOURCE 200809L

#include "roole/node.h"
#include "roole/config.h"
#include "roole/common.h"
#include <string.h>

// ============================================================================
// CAPABILITY DETECTION FROM CONFIG
// ============================================================================

void node_detect_capabilities(unified_node_t *node, const roole_config_t *config) {
    if (!node || !config) return;
    
    // Default: all capabilities disabled
    node->capabilities.has_ingress = 0;
    node->capabilities.can_execute = 0;
    node->capabilities.can_route = 0;
    
    // Detect ingress capability
    if (config->ports.ingress_addr[0] != '\0') {
        char ingress_ip[16];
        uint16_t ingress_port;
        config_parse_address(config->ports.ingress_addr, ingress_ip, &ingress_port);
        
        if (ingress_port > 0) {
            node->capabilities.has_ingress = 1;
            node->ingress_port = ingress_port;
            LOG_INFO("Capability: INGRESS enabled (port %u)", ingress_port);
        } else {
            node->ingress_port = 0;
            LOG_INFO("Capability: INGRESS disabled (invalid port)");
        }
    } else {
        node->ingress_port = 0;
        LOG_INFO("Capability: INGRESS disabled (not configured)");
    }
    
    // Detect execution capability (always enabled for now)
    // Future: make this configurable via config file
    node->capabilities.can_execute = 1;
    LOG_INFO("Capability: EXECUTE enabled");
    
    // Detect routing capability
    // Enabled if node has ingress OR if explicitly configured as router
    if (node->capabilities.has_ingress) {
        node->capabilities.can_route = 1;
        LOG_INFO("Capability: ROUTE enabled (ingress node)");
    } else {
        // For now, all nodes can route (will be configurable later)
        node->capabilities.can_route = 1;
        LOG_INFO("Capability: ROUTE enabled (default)");
    }
}

void node_print_capabilities(const unified_node_t *node) {
    if (!node) return;
    
    LOG_INFO("========================================");
    LOG_INFO("Node Capabilities Summary:");
    LOG_INFO("  Ingress:   %s%s", 
             node->capabilities.has_ingress ? "ENABLED" : "DISABLED",
             node->capabilities.has_ingress ? 
                 (node->ingress_port > 0 ? "" : " (port invalid)") : "");
    LOG_INFO("  Execute:   %s", 
             node->capabilities.can_execute ? "ENABLED" : "DISABLED");
    LOG_INFO("  Route:     %s", 
             node->capabilities.can_route ? "ENABLED" : "DISABLED");
    
    if (node->capabilities.has_ingress && node->capabilities.can_execute) {
        LOG_INFO("  Node Type: HYBRID (ingress + execution)");
    } else if (node->capabilities.has_ingress) {
        LOG_INFO("  Node Type: EDGE (ingress only)");
    } else if (node->capabilities.can_execute) {
        LOG_INFO("  Node Type: COMPUTE (execution only)");
    } else {
        LOG_INFO("  Node Type: RELAY (routing only)");
    }
    LOG_INFO("========================================");
}
