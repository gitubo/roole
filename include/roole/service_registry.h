// include/roole/service_registry.h
#ifndef ROOLE_SERVICE_REGISTRY_H
#define ROOLE_SERVICE_REGISTRY_H

#include <stddef.h>
#include <pthread.h>

// ============================================================================
// SERVICE TYPES
// ============================================================================

typedef enum {
    SERVICE_TYPE_NODE_STATE = 0,
    SERVICE_TYPE_RPC_SERVER,
    SERVICE_TYPE_EVENT_BUS,
    SERVICE_TYPE_METRICS,
    SERVICE_TYPE_DAG_CATALOG,
    SERVICE_TYPE_CLUSTER_VIEW,
    SERVICE_TYPE_PEER_POOL,
    SERVICE_TYPE_EXECUTOR_POOL,
    SERVICE_TYPE_MAX
} service_type_t;

// ============================================================================
// SERVICE REGISTRY
// ============================================================================

typedef struct service_registry service_registry_t;

// Registry lifecycle
service_registry_t* service_registry_create(void);
void service_registry_destroy(service_registry_t *registry);

// Service registration (thread-safe)
int service_registry_register(service_registry_t *registry, 
                              service_type_t type,
                              const char *name,
                              void *service_ptr);

int service_registry_unregister(service_registry_t *registry,
                                service_type_t type,
                                const char *name);

// Service lookup (thread-safe)
void* service_registry_get(service_registry_t *registry,
                           service_type_t type,
                           const char *name);

// Global registry access (singleton pattern)
service_registry_t* service_registry_global(void);
void service_registry_set_global(service_registry_t *registry);
void service_registry_dump(service_registry_t *registry);

#endif // ROOLE_SERVICE_REGISTRY_H