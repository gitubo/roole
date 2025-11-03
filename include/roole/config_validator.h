// include/roole/config_validator.h
#ifndef ROOLE_CONFIG_VALIDATOR_H
#define ROOLE_CONFIG_VALIDATOR_H

#include "roole/config.h"
#include "roole/common.h"

// ============================================================================
// VALIDATION RESULT
// ============================================================================

#define MAX_VALIDATION_ERRORS 16
#define MAX_ERROR_MESSAGE_LEN 256

typedef struct validation_error {
    char message[MAX_ERROR_MESSAGE_LEN];
    int severity;  // 0=warning, 1=error, 2=fatal
} validation_error_t;

typedef struct validation_result {
    int valid;
    size_t error_count;
    validation_error_t errors[MAX_VALIDATION_ERRORS];
} validation_result_t;

// ============================================================================
// VALIDATION API
// ============================================================================

// Full configuration validation
int config_validate(const roole_config_t *config, validation_result_t *result);

// Individual validators
int validate_node_id(node_id_t node_id, validation_result_t *result);
int validate_node_type(node_type_t type, validation_result_t *result);
int validate_ports(const node_ports_t *ports, validation_result_t *result);
int validate_cluster_name(const char *cluster_name, validation_result_t *result);
int validate_routers(const roole_config_t *config, validation_result_t *result);
int validate_capabilities(const roole_config_t *config, validation_result_t *result);

// Network validation (reachability, port conflicts)
int validate_network_reachability(const char *addr_str, validation_result_t *result);
int validate_port_conflicts(const node_ports_t *ports, validation_result_t *result);

// Helper: Print validation result
void validation_result_print(const validation_result_t *result);

// Helper: Initialize result
void validation_result_init(validation_result_t *result);

// Helper: Add error
void validation_add_error(validation_result_t *result, int severity, const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));

#endif // ROOLE_CONFIG_VALIDATOR_H