// include/roole/metrics_server.h
// Simple HTTP server for /metrics endpoint

#ifndef ROOLE_METRICS_SERVER_H
#define ROOLE_METRICS_SERVER_H

#include "roole/metrics.h"
#include <stdint.h>
#include <pthread.h>

// ============================================================================
// METRICS SERVER
// ============================================================================

typedef struct metrics_server metrics_server_t;

/**
 * Initialize and start metrics HTTP server
 * @param registry Metrics registry to expose
 * @param bind_addr IP address to bind (e.g., "0.0.0.0")
 * @param port Port to listen on
 * @return Server handle, or NULL on error
 */
metrics_server_t* metrics_server_start(metrics_registry_t *registry,
                                        const char *bind_addr,
                                        uint16_t port);

/**
 * Shutdown metrics server
 */
void metrics_server_shutdown(metrics_server_t *server);

#endif // ROOLE_METRICS_SERVER_H