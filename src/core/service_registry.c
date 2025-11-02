// src/core/service_registry.c
#define _POSIX_C_SOURCE 200809L

#include "roole/service_registry.h"
#include "roole/logger.h"
#include <stdlib.h>
#include <string.h>

// ============================================================================
// INTERNAL STRUCTURES
// ============================================================================

#define MAX_SERVICES_PER_TYPE 16
#define MAX_SERVICE_NAME 64

typedef struct service_entry {
    char name[MAX_SERVICE_NAME];
    void *service_ptr;
    int active;
} service_entry_t;

struct service_registry {
    service_entry_t services[SERVICE_TYPE_MAX][MAX_SERVICES_PER_TYPE];
    pthread_rwlock_t locks[SERVICE_TYPE_MAX];
    int initialized;
};

// Global singleton
static service_registry_t *g_global_registry = NULL;
static pthread_mutex_t g_global_lock = PTHREAD_MUTEX_INITIALIZER;

// ============================================================================
// REGISTRY LIFECYCLE
// ============================================================================

service_registry_t* service_registry_create(void) {
    service_registry_t *registry = calloc(1, sizeof(service_registry_t));
    if (!registry) {
        LOG_ERROR("Failed to allocate service registry");
        return NULL;
    }
    
    // Initialize locks for each service type
    for (int i = 0; i < SERVICE_TYPE_MAX; i++) {
        if (pthread_rwlock_init(&registry->locks[i], NULL) != 0) {
            LOG_ERROR("Failed to initialize rwlock for service type %d", i);
            
            // Cleanup already initialized locks
            for (int j = 0; j < i; j++) {
                pthread_rwlock_destroy(&registry->locks[j]);
            }
            free(registry);
            return NULL;
        }
    }
    
    registry->initialized = 1;
    LOG_INFO("Service registry created");
    return registry;
}

void service_registry_destroy(service_registry_t *registry) {
    if (!registry) return;
    
    if (registry->initialized) {
        for (int i = 0; i < SERVICE_TYPE_MAX; i++) {
            pthread_rwlock_destroy(&registry->locks[i]);
        }
    }
    
    free(registry);
    LOG_INFO("Service registry destroyed");
}

// ============================================================================
// SERVICE REGISTRATION
// ============================================================================

int service_registry_register(service_registry_t *registry,
                              service_type_t type,
                              const char *name,
                              void *service_ptr) {
    if (!registry || !name || !service_ptr) {
        LOG_ERROR("Invalid parameters for service registration");
        return -1;
    }
    
    if (type >= SERVICE_TYPE_MAX) {
        LOG_ERROR("Invalid service type: %d", type);
        return -1;
    }
    
    pthread_rwlock_wrlock(&registry->locks[type]);
    
    // Check if already registered
    for (int i = 0; i < MAX_SERVICES_PER_TYPE; i++) {
        if (registry->services[type][i].active &&
            strcmp(registry->services[type][i].name, name) == 0) {
            pthread_rwlock_unlock(&registry->locks[type]);
            LOG_WARN("Service already registered: type=%d name=%s", type, name);
            return -1;
        }
    }
    
    // Find free slot
    int slot = -1;
    for (int i = 0; i < MAX_SERVICES_PER_TYPE; i++) {
        if (!registry->services[type][i].active) {
            slot = i;
            break;
        }
    }
    
    if (slot == -1) {
        pthread_rwlock_unlock(&registry->locks[type]);
        LOG_ERROR("No free slots for service type %d", type);
        return -1;
    }
    
    // Register service
    strncpy(registry->services[type][slot].name, name, MAX_SERVICE_NAME - 1);
    registry->services[type][slot].name[MAX_SERVICE_NAME - 1] = '\0';
    registry->services[type][slot].service_ptr = service_ptr;
    registry->services[type][slot].active = 1;
    
    pthread_rwlock_unlock(&registry->locks[type]);
    
    LOG_INFO("Service registered: type=%d name=%s ptr=%p", type, name, service_ptr);
    return 0;
}

int service_registry_unregister(service_registry_t *registry,
                                service_type_t type,
                                const char *name) {
    if (!registry || !name || type >= SERVICE_TYPE_MAX) {
        return -1;
    }
    
    pthread_rwlock_wrlock(&registry->locks[type]);
    
    for (int i = 0; i < MAX_SERVICES_PER_TYPE; i++) {
        if (registry->services[type][i].active &&
            strcmp(registry->services[type][i].name, name) == 0) {
            
            registry->services[type][i].active = 0;
            registry->services[type][i].service_ptr = NULL;
            
            pthread_rwlock_unlock(&registry->locks[type]);
            LOG_INFO("Service unregistered: type=%d name=%s", type, name);
            return 0;
        }
    }
    
    pthread_rwlock_unlock(&registry->locks[type]);
    LOG_WARN("Service not found for unregistration: type=%d name=%s", type, name);
    return -1;
}

// ============================================================================
// SERVICE LOOKUP
// ============================================================================

void* service_registry_get(service_registry_t *registry,
                           service_type_t type,
                           const char *name) {
    if (!registry || !name || type >= SERVICE_TYPE_MAX) {
        return NULL;
    }
    
    pthread_rwlock_rdlock(&registry->locks[type]);
    
    for (int i = 0; i < MAX_SERVICES_PER_TYPE; i++) {
        if (registry->services[type][i].active &&
            strcmp(registry->services[type][i].name, name) == 0) {
            
            void *ptr = registry->services[type][i].service_ptr;
            pthread_rwlock_unlock(&registry->locks[type]);
            return ptr;
        }
    }
    
    pthread_rwlock_unlock(&registry->locks[type]);
    return NULL;
}

// ============================================================================
// GLOBAL SINGLETON
// ============================================================================

service_registry_t* service_registry_global(void) {
    pthread_mutex_lock(&g_global_lock);
    service_registry_t *registry = g_global_registry;
    pthread_mutex_unlock(&g_global_lock);
    return registry;
}

void service_registry_set_global(service_registry_t *registry) {
    pthread_mutex_lock(&g_global_lock);
    g_global_registry = registry;
    pthread_mutex_unlock(&g_global_lock);
    
    if (registry) {
        LOG_INFO("Global service registry set");
    } else {
        LOG_INFO("Global service registry cleared");
    }
}

// ============================================================================
// DEBUG / INTROSPECTION
// ============================================================================

static const char* service_type_to_string(service_type_t type) {
    switch (type) {
        case SERVICE_TYPE_NODE_STATE: return "NODE_STATE";
        case SERVICE_TYPE_RPC_SERVER: return "RPC_SERVER";
        case SERVICE_TYPE_METRICS: return "METRICS";
        case SERVICE_TYPE_DAG_CATALOG: return "DAG_CATALOG";
        case SERVICE_TYPE_CLUSTER_VIEW: return "CLUSTER_VIEW";
        case SERVICE_TYPE_PEER_POOL: return "PEER_POOL";
        case SERVICE_TYPE_EXECUTOR_POOL: return "EXECUTOR_POOL";
        default: return "UNKNOWN";
    }
}

void service_registry_dump(service_registry_t *registry) {
    if (!registry) {
        LOG_INFO("Service registry: NULL");
        return;
    }
    
    LOG_INFO("========================================");
    LOG_INFO("Service Registry Dump");
    LOG_INFO("========================================");
    
    for (int type = 0; type < SERVICE_TYPE_MAX; type++) {
        pthread_rwlock_rdlock(&registry->locks[type]);
        
        int has_services = 0;
        for (int i = 0; i < MAX_SERVICES_PER_TYPE; i++) {
            if (registry->services[type][i].active) {
                if (!has_services) {
                    LOG_INFO("[%s]", service_type_to_string(type));
                    has_services = 1;
                }
                LOG_INFO("  - %s -> %p", 
                        registry->services[type][i].name,
                        registry->services[type][i].service_ptr);
            }
        }
        
        pthread_rwlock_unlock(&registry->locks[type]);
    }
    
    LOG_INFO("========================================");
}