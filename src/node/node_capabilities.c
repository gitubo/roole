// src/node/node_capabilities.c

#define _POSIX_C_SOURCE 200809L

#include "roole/node.h"
#include "roole/node_state.h"
#include "roole/config.h"
#include "roole/common.h"
#include <string.h>

// ============================================================================
// CAPABILITY DETECTION FROM CONFIG
// ============================================================================

void node_detect_capabilities_ex(const roole_config_t *config,
                                 node_capabilities_t *caps,
                                 node_identity_t *identity) {
    if (!config || !caps || !identity) return;
    
    // Default: all disabled
    caps->has_ingress = 0;
    caps->can_execute = 0;
    caps->can_route = 0;
    
    // Detect ingress capability
    if (config->ports.ingress_addr[0] != '\0') {
        char ingress_ip[16];
        uint16_t ingress_port;
        config_parse_address(config->ports.ingress_addr, ingress_ip, &ingress_port);
        
        if (ingress_port > 0) {
            caps->has_ingress = 1;
            identity->ingress_port = ingress_port;
            LOG_INFO("Capability: INGRESS enabled (port %u)", ingress_port);
        } else {
            identity->ingress_port = 0;
            LOG_INFO("Capability: INGRESS disabled (invalid port)");
        }
    } else {
        identity->ingress_port = 0;
        LOG_INFO("Capability: INGRESS disabled (not configured)");
    }
    
    // Execution capability (always enabled by default)
    caps->can_execute = 1;
    LOG_INFO("Capability: EXECUTE enabled");
    
    // Routing capability (enabled if ingress OR explicit config)
    if (caps->has_ingress) {
        caps->can_route = 1;
        LOG_INFO("Capability: ROUTE enabled (ingress node)");
    } else {
        caps->can_route = 1;  // Default
        LOG_INFO("Capability: ROUTE enabled (default)");
    }
}

void node_print_capabilities_ex(const node_capabilities_t *caps,
                                const node_identity_t *identity) {
    if (!caps || !identity) return;
    
    LOG_INFO("========================================");
    LOG_INFO("Node Capabilities Summary:");
    LOG_INFO("  Ingress:   %s%s", 
             caps->has_ingress ? "ENABLED" : "DISABLED",
             caps->has_ingress ? 
                 (identity->ingress_port > 0 ? "" : " (port invalid)") : "");
    LOG_INFO("  Execute:   %s", caps->can_execute ? "ENABLED" : "DISABLED");
    LOG_INFO("  Route:     %s", caps->can_route ? "ENABLED" : "DISABLED");
    
    if (caps->has_ingress && caps->can_execute) {
        LOG_INFO("  Node Type: HYBRID (ingress + execution)");
    } else if (caps->has_ingress) {
        LOG_INFO("  Node Type: EDGE (ingress only)");
    } else if (caps->can_execute) {
        LOG_INFO("  Node Type: COMPUTE (execution only)");
    } else {
        LOG_INFO("  Node Type: RELAY (routing only)");
    }
    LOG_INFO("========================================");
}