// include/roole/config.h

#ifndef ROOLE_CONFIG_H
#define ROOLE_CONFIG_H

#include "roole/common.h"
#include "roole/cluster.h"

#define MAX_CONFIG_ROUTERS 16
#define MAX_CONFIG_STRING 256

typedef struct node_ports {
    char gossip_addr[MAX_CONFIG_STRING];
    char data_addr[MAX_CONFIG_STRING];
    char ingress_addr[MAX_CONFIG_STRING];
    char metrics_addr[MAX_CONFIG_STRING];  // NEW: Metrics endpoint address
} node_ports_t;

typedef struct roole_config {
    char cluster_name[MAX_CONFIG_STRING];
    node_id_t node_id;
    node_type_t node_type;
    node_ports_t ports;
    
    char routers[MAX_CONFIG_ROUTERS][MAX_CONFIG_STRING];
    log_level_t log_level;
    size_t router_count;
} roole_config_t;

// Load configuration from INI file
int config_load_from_file(const char *path, roole_config_t *config);

// Parse address string "ip:port" into separate components
void config_parse_address(const char *addr_str, char *ip, uint16_t *port);

#endif // ROOLE_CONFIG_H