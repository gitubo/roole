// src/core/config.c - INI file parser (no external dependencies)

#define _POSIX_C_SOURCE 200809L

#include "roole/config.h"
#include "roole/common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

// ============================================================================
// INI PARSER HELPERS
// ============================================================================

// Trim leading and trailing whitespace
static char* trim_whitespace(char *str) {
    if (!str) return NULL;
    
    // Trim leading
    while (isspace((unsigned char)*str)) str++;
    
    if (*str == '\0') return str;
    
    // Trim trailing
    char *end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;
    
    *(end + 1) = '\0';
    
    return str;
}

// Check if line is a comment (starts with ; or #)
static int is_comment(const char *line) {
    if (!line) return 1;
    
    const char *p = line;
    while (isspace((unsigned char)*p)) p++;
    
    return (*p == ';' || *p == '#' || *p == '\0');
}

// Check if line is a section header [Section]
static int is_section(const char *line, char *section_name, size_t max_len) {
    if (!line || !section_name) return 0;
    
    const char *p = line;
    while (isspace((unsigned char)*p)) p++;
    
    if (*p != '[') return 0;
    
    p++;
    const char *start = p;
    
    while (*p && *p != ']') p++;
    
    if (*p != ']') return 0;
    
    size_t len = p - start;
    if (len >= max_len) len = max_len - 1;
    
    strncpy(section_name, start, len);
    section_name[len] = '\0';
    
    return 1;
}

// Parse key=value pair
static int parse_key_value(const char *line, char *key, char *value, size_t max_len) {
    if (!line || !key || !value) return 0;
    
    const char *equals = strchr(line, '=');
    if (!equals) return 0;
    
    // Extract key
    size_t key_len = equals - line;
    if (key_len >= max_len) key_len = max_len - 1;
    
    strncpy(key, line, key_len);
    key[key_len] = '\0';
    
    char *trimmed_key = trim_whitespace(key);
    if (trimmed_key != key) {
        memmove(key, trimmed_key, strlen(trimmed_key) + 1);
    }
    
    // Extract value
    const char *value_start = equals + 1;
    strncpy(value, value_start, max_len - 1);
    value[max_len - 1] = '\0';
    
    char *trimmed_value = trim_whitespace(value);
    if (trimmed_value != value) {
        memmove(value, trimmed_value, strlen(trimmed_value) + 1);
    }
    
    return 1;
}

// Parse semicolon-separated list (for routers)
static size_t parse_list(const char *value, char items[][MAX_CONFIG_STRING], size_t max_items) {
    if (!value || !items) return 0;
    
    char buffer[MAX_CONFIG_STRING * MAX_CONFIG_ROUTERS];
    strncpy(buffer, value, sizeof(buffer) - 1);
    buffer[sizeof(buffer) - 1] = '\0';
    
    size_t count = 0;
    char *token = strtok(buffer, ";");
    
    while (token && count < max_items) {
        char *trimmed = trim_whitespace(token);
        if (strlen(trimmed) > 0) {
            strncpy(items[count], trimmed, MAX_CONFIG_STRING - 1);
            items[count][MAX_CONFIG_STRING - 1] = '\0';
            count++;
        }
        token = strtok(NULL, ";");
    }
    
    return count;
}

// ============================================================================
// CONFIG LOADER
// ============================================================================

int config_load_from_file(const char *path, roole_config_t *config) {
    if (!path || !config) {
        LOG_ERROR("Invalid parameters for config_load_from_file");
        return -1;
    }
    
    FILE *fp = fopen(path, "r");
    if (!fp) {
        LOG_ERROR("Cannot open config file: %s", path);
        return -1;
    }
    
    memset(config, 0, sizeof(roole_config_t));
    
    char line[1024];
    char current_section[64] = "";
    char key[MAX_CONFIG_STRING];
    char value[MAX_CONFIG_STRING];
    
    int line_num = 0;
    
    while (fgets(line, sizeof(line), fp)) {
        line_num++;
        
        // Remove newline
        size_t len = strlen(line);
        if (len > 0 && line[len - 1] == '\n') {
            line[len - 1] = '\0';
        }
        
        // Skip comments and empty lines
        if (is_comment(line)) {
            continue;
        }
        
        // Check for section
        if (is_section(line, current_section, sizeof(current_section))) {
            LOG_DEBUG("Found section: [%s]", current_section);
            continue;
        }
        
        // Parse key=value
        if (!parse_key_value(line, key, value, MAX_CONFIG_STRING)) {
            LOG_WARN("Invalid line %d: %s", line_num, line);
            continue;
        }
        
        LOG_DEBUG("Parsed: [%s] %s = %s", current_section, key, value);
        
        // Process based on current section
        if (strcasecmp(current_section, "Cluster") == 0) {
            if (strcasecmp(key, "name") == 0) {
                safe_strncpy(config->cluster_name, value, MAX_CONFIG_STRING);
            }
            else if (strcasecmp(key, "routers") == 0) {
                config->router_count = parse_list(value, config->routers, MAX_CONFIG_ROUTERS);
                LOG_INFO("Loaded %zu seed routers", config->router_count);
            }
        }
        else if (strcasecmp(current_section, "Node") == 0) {
            if (strcasecmp(key, "id") == 0) {
                config->node_id = (node_id_t)atoi(value);
            }
            else if (strcasecmp(key, "type") == 0) {
                if (strcasecmp(value, "ROUTER") == 0) {
                    config->node_type = NODE_TYPE_ROUTER;
                } else if (strcasecmp(value, "WORKER") == 0) {
                    config->node_type = NODE_TYPE_WORKER;
                } else {
                    LOG_WARN("Unknown node type: %s", value);
                    config->node_type = NODE_TYPE_UNKNOWN;
                }
            }
            else if (strcasecmp(key, "gossip_addr") == 0 || strcasecmp(key, "gossip") == 0) {
                safe_strncpy(config->ports.gossip_addr, value, MAX_CONFIG_STRING);
            }
            else if (strcasecmp(key, "data_addr") == 0 || strcasecmp(key, "data") == 0) {
                safe_strncpy(config->ports.data_addr, value, MAX_CONFIG_STRING);
            }
            else if (strcasecmp(key, "ingress_addr") == 0 || strcasecmp(key, "ingress") == 0) {
                safe_strncpy(config->ports.ingress_addr, value, MAX_CONFIG_STRING);
            }
        }        
        else if (strcasecmp(current_section, "Logging") == 0) {
            if (strcasecmp(key, "level") == 0) {
                if (strcasecmp(value, "DEBUG") == 0) {
                    config->log_level = LOG_LEVEL_DEBUG;
                } else if (strcasecmp(value, "INFO") == 0) {
                    config->log_level = LOG_LEVEL_INFO;
                } else if (strcasecmp(value, "WARN") == 0) {
                    config->log_level = LOG_LEVEL_WARN;
                } else if (strcasecmp(value, "ERROR") == 0) {
                    config->log_level = LOG_LEVEL_ERROR;
                } else {
                    LOG_WARN("Unknown node type: %s", value);
                    config->log_level = LOG_LEVEL_INFO;
                }
            }
        }
    }
    
    fclose(fp);
    
    // Validation
    if (strlen(config->cluster_name) == 0) {
        LOG_ERROR("Missing cluster name in config");
        return -1;
    }
    
    if (config->node_id == 0) {
        LOG_ERROR("Missing or invalid node id in config");
        return -1;
    }
    
    if (config->node_type == NODE_TYPE_UNKNOWN) {
        LOG_ERROR("Missing or invalid node type in config");
        return -1;
    }
    
    if (strlen(config->ports.gossip_addr) == 0) {
        LOG_ERROR("Missing gossip address in config");
        return -1;
    }
    
    if (strlen(config->ports.data_addr) == 0) {
        LOG_ERROR("Missing data address in config");
        return -1;
    }
    
    if (config->node_type == NODE_TYPE_ROUTER && strlen(config->ports.ingress_addr) == 0) {
        LOG_WARN("Router missing ingress address (required for client connections)");
    }
    
    LOG_INFO("Configuration loaded successfully from %s", path);
    LOG_INFO("  Cluster: %s", config->cluster_name);
    LOG_INFO("  Node: id=%u, type=%s", config->node_id, 
             config->node_type == NODE_TYPE_ROUTER ? "ROUTER" : "WORKER");
    LOG_INFO("  Gossip: %s", config->ports.gossip_addr);
    LOG_INFO("  Data: %s", config->ports.data_addr);
    if (strlen(config->ports.ingress_addr) > 0) {
        LOG_INFO("  Ingress: %s", config->ports.ingress_addr);
    }
    LOG_INFO("  Seed routers: %zu", config->router_count);
    
    return 0;
}

// ============================================================================
// ADDRESS PARSER
// ============================================================================

void config_parse_address(const char *addr_str, char *ip, uint16_t *port) {
    if (!addr_str || !ip || !port) return;
    
    char buffer[MAX_CONFIG_STRING];
    safe_strncpy(buffer, addr_str, MAX_CONFIG_STRING);
    
    char *colon = strchr(buffer, ':');
    if (colon) {
        *colon = '\0';
        safe_strncpy(ip, buffer, 16);
        *port = (uint16_t)atoi(colon + 1);
    } else {
        // No colon found, assume entire string is IP
        safe_strncpy(ip, buffer, 16);
        *port = 0;
    }
}