// include/roole/event_bus.h
#ifndef ROOLE_EVENT_BUS_H
#define ROOLE_EVENT_BUS_H

#include "roole/common.h"
#include "roole/cluster.h"
#include <stdint.h>
#include <pthread.h>
#include <errno.h>

// ============================================================================
// EVENT TYPES
// ============================================================================

typedef enum {
    EVENT_TYPE_PEER_JOINED = 0,
    EVENT_TYPE_PEER_LEFT,
    EVENT_TYPE_PEER_FAILED,
    EVENT_TYPE_PEER_UPDATED,
    EVENT_TYPE_PEER_SUSPECT,
    EVENT_TYPE_EXECUTION_STARTED,
    EVENT_TYPE_EXECUTION_COMPLETED,
    EVENT_TYPE_EXECUTION_FAILED,
    EVENT_TYPE_MESSAGE_RECEIVED,
    EVENT_TYPE_MESSAGE_ROUTED,
    EVENT_TYPE_CATALOG_UPDATED,
    EVENT_TYPE_MAX
} event_type_t;

// ============================================================================
// EVENT STRUCTURES
// ============================================================================

// Peer event data
typedef struct event_peer {
    node_id_t node_id;
    node_type_t node_type;
    char ip_address[MAX_IP_LEN];
    uint16_t gossip_port;
    uint16_t data_port;
    node_status_t status;
    uint64_t incarnation;
} event_peer_t;

// Execution event data
typedef struct event_execution {
    execution_id_t exec_id;
    rule_id_t dag_id;
    node_id_t assigned_peer;
    uint64_t timestamp_ms;
    int status_code;
} event_execution_t;

// Message event data
typedef struct event_message {
    execution_id_t exec_id;
    rule_id_t dag_id;
    node_id_t source_id;
    node_id_t dest_id;
    size_t message_size;
    uint64_t timestamp_ms;
} event_message_t;

// Catalog event data
typedef struct event_catalog {
    rule_id_t dag_id;
    uint64_t version;
    char dag_name[64];
} event_catalog_t;

// Generic event structure
typedef struct event {
    event_type_t type;
    uint64_t timestamp_ms;
    node_id_t source_node_id;
    
    union {
        event_peer_t peer;
        event_execution_t execution;
        event_message_t message;
        event_catalog_t catalog;
    } data;
} event_t;

// ============================================================================
// EVENT HANDLER CALLBACK
// ============================================================================

typedef void (*event_handler_fn)(const event_t *event, void *user_data);

// ============================================================================
// EVENT BUS API
// ============================================================================

typedef struct event_bus event_bus_t;

// Lifecycle
event_bus_t* event_bus_create(void);
void event_bus_destroy(event_bus_t *bus);

// Subscription management (thread-safe)
int event_bus_subscribe(event_bus_t *bus,
                        event_type_t type,
                        event_handler_fn handler,
                        void *user_data);

int event_bus_unsubscribe(event_bus_t *bus,
                          event_type_t type,
                          event_handler_fn handler);

// Event publishing (thread-safe, async)
int event_bus_publish(event_bus_t *bus, const event_t *event);

// Synchronous publish (for testing or critical events)
int event_bus_publish_sync(event_bus_t *bus, const event_t *event);

// ============================================================================
// CONVENIENCE MACROS
// ============================================================================

// Get event bus from global registry
#define EVENT_BUS_GLOBAL() ({ \
    service_registry_t *__reg = service_registry_global(); \
    __reg ? (event_bus_t*)service_registry_get(__reg, SERVICE_TYPE_EVENT_BUS, "main") : NULL; \
})

// Publish event helper (requires node context)
#define PUBLISH_EVENT(bus, type, data_field, ...) do { \
    if (bus) { \
        event_t __event = { \
            .type = (type), \
            .timestamp_ms = time_now_ms(), \
            .source_node_id = 0, \
            .data.data_field = __VA_ARGS__ \
        }; \
        event_bus_publish((bus), &__event); \
    } \
} while(0)

// Statistics
typedef struct event_bus_stats {
    uint64_t events_published;
    uint64_t events_dispatched;
    uint64_t events_dropped;
    uint64_t queue_size;
    uint64_t subscribers_total;
} event_bus_stats_t;

void event_bus_get_stats(event_bus_t *bus, event_bus_stats_t *stats);

// Helper: Get event type name
const char* event_type_to_string(event_type_t type);

#endif // ROOLE_EVENT_BUS_H