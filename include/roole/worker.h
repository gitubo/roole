// include/roole/worker.h

#ifndef ROOLE_WORKER_H
#define ROOLE_WORKER_H

#include "roole/common.h"
#include "roole/dag.h"
#include "roole/cluster.h"
#include "roole/rpc.h"
#include <pthread.h>

// ============================================================================
// TASK QUEUE (Worker-side execution queue)
// ============================================================================

#define MAX_WORKER_QUEUE_SIZE 1000
#define MAX_MESSAGE_SIZE 4096

// Forward declaration (defined in router.h)
typedef enum {
    EXEC_STATUS_PENDING = 0,
    EXEC_STATUS_RUNNING = 1,
    EXEC_STATUS_COMPLETED = 2,
    EXEC_STATUS_FAILED = 3,
    EXEC_STATUS_RETRYING = 4,
    EXEC_STATUS_TIMEOUT = 5
} execution_status_t;

typedef struct message {
    execution_id_t exec_id;
    rule_id_t dag_id;
    uint8_t message_data[MAX_MESSAGE_SIZE];
    size_t message_len;
    uint64_t received_at_ms;
    node_id_t sender_id;  // Original sender (client or router)
} message_t;

typedef struct message_queue {
    message_t *messages;  // CHANGED: tasks -> messages
    size_t head;
    size_t tail;
    size_t capacity;
    size_t count;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} message_queue_t; 

int message_queue_init(message_queue_t *queue, size_t capacity);
void message_queue_destroy(message_queue_t *queue);

int message_queue_push(message_queue_t *queue, const message_t *message);
int message_queue_pop(message_queue_t *queue, message_t *out_message, int timeout_ms);

size_t message_queue_size(message_queue_t *queue);
int message_queue_is_empty(message_queue_t *queue);

// ============================================================================
// ROUTER CONNECTION (Worker maintains connections to routers)
// ============================================================================

#define MAX_ROUTER_CONNECTIONS 16

typedef struct router_connection {
    node_id_t router_id;
    char ip[MAX_IP_LEN];
    uint16_t service_port;  // Port for SERVICE channel
    uint16_t data_port;     // Port for DATA channel

    // Separate RPC channels for service and data communication
    rpc_channel_t *service_channel;  // For heartbeat, registration, catalog sync
    rpc_channel_t *data_channel;     // For execution updates

    uint64_t last_sync_ms;
    int active;
} router_connection_t;

// ============================================================================
// WORKER STATE
// ============================================================================

typedef struct worker_state {
    node_id_t worker_id;
    uint16_t service_port;  // Port for SERVICE channel (heartbeat, registration)
    uint16_t data_port;     // Port for DATA channel (message processing)

    // DAG catalog (read-only, synced from routers)
    dag_catalog_t dag_catalog;
    uint64_t catalog_version;

    // Task execution
    message_queue_t message_queue;
    uint32_t active_executions;

    // Router connections
    router_connection_t routers[MAX_ROUTER_CONNECTIONS];
    size_t router_count;
    pthread_mutex_t routers_lock;

    // Cluster membership
    cluster_view_t cluster_view;
    membership_handle_t *membership;

    // Worker threads
    pthread_t executor_threads[16];
    size_t num_executor_threads;

    pthread_t heartbeat_thread;

    int shutdown_flag;
} worker_state_t;

// ============================================================================
// WORKER API
// ============================================================================

// Initialization
int worker_init(worker_state_t *worker, node_id_t worker_id,
               uint16_t service_port, uint16_t data_port,
               size_t num_executor_threads);
int worker_start(worker_state_t *worker);
void worker_shutdown(worker_state_t *worker);

// Message management
int worker_enqueue_message(worker_state_t *worker, execution_id_t exec_id,
                          rule_id_t dag_id, node_id_t sender_id,
                          const uint8_t *message, size_t message_len);

// Router communication
int worker_add_router(worker_state_t *worker, node_id_t router_id,
                     const char *ip, uint16_t service_port, uint16_t data_port);
int worker_remove_router(worker_state_t *worker, node_id_t router_id);

int worker_send_heartbeat(worker_state_t *worker);
int worker_send_execution_update(worker_state_t *worker, node_id_t router_id,
                                execution_id_t exec_id, execution_status_t status);

// Worker registration
int worker_register_with_router(worker_state_t *worker, const char *router_ip,
                                uint16_t service_port, uint16_t data_port);

// DAG catalog sync
int worker_sync_catalog_from_router(worker_state_t *worker, node_id_t router_id);

// Executor thread function
void* worker_executor_thread_fn(void *arg);

// ============================================================================
// RPC HANDLERS
// ============================================================================

// Set global worker state for RPC handlers
void worker_set_rpc_state(worker_state_t *worker);

// RPC service table for worker
extern rpc_service_entry_t worker_rpc_service_table[];

#endif // ROOLE_WORKER_H