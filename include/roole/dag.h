// include/roole/dag.h

#ifndef ROOLE_DAG_H
#define ROOLE_DAG_H

#include "roole/common.h"
#include <pthread.h>

// ============================================================================
// DAG DEFINITIONS
// ============================================================================

#define MAX_DAG_NAME 64
#define MAX_DAG_STEPS 128
#define MAX_STEP_NAME 64
#define MAX_STEP_DEPENDENCIES 16
#define MAX_STEP_CONFIG_SIZE 4096
#define MAX_DAGS 1024

typedef struct dag_step {
    uint32_t step_id;
    char name[MAX_STEP_NAME];
    char function_name[MAX_STEP_NAME];  // Handler function to call
    
    // Configuration data (JSON, binary, etc.)
    uint8_t *config_data;
    size_t config_len;
    
    // Dependencies (step_ids that must complete before this)
    uint32_t dependencies[MAX_STEP_DEPENDENCIES];
    size_t dependency_count;
    
    // Optional: timeout, retry policy, etc.
    uint32_t timeout_ms;
    uint8_t max_retries;
} dag_step_t;

typedef struct dag {
    dag_id_t dag_id;
    char name[MAX_DAG_NAME];
    uint64_t version;
    
    dag_step_t steps[MAX_DAG_STEPS];
    size_t step_count;
    
    uint64_t created_at_ms;
    uint64_t updated_at_ms;
} dag_t;

// ============================================================================
// DAG CATALOG
// ============================================================================

typedef struct dag_catalog {
    dag_t *dags;
    size_t count;
    size_t capacity;
    pthread_rwlock_t lock;
} dag_catalog_t;

// Catalog management
int dag_catalog_init(dag_catalog_t *catalog, size_t capacity);
void dag_catalog_destroy(dag_catalog_t *catalog);

// CRUD operations (thread-safe)
int dag_catalog_add(dag_catalog_t *catalog, const dag_t *dag);
int dag_catalog_update(dag_catalog_t *catalog, const dag_t *dag);
int dag_catalog_remove(dag_catalog_t *catalog, dag_id_t dag_id);
dag_t* dag_catalog_get(dag_catalog_t *catalog, dag_id_t dag_id);  // Returns pointer (read-lock held during use)
void dag_catalog_release(dag_catalog_t *catalog);  // Release read-lock
size_t dag_catalog_list(dag_catalog_t *catalog, dag_id_t *out_dag_ids, size_t max_count);

// Serialization (for Raft log entries / network transfer)
size_t dag_serialize(const dag_t *dag, uint8_t *buffer, size_t buffer_size);
int dag_deserialize(const uint8_t *buffer, size_t buffer_len, dag_t *out_dag);

// ============================================================================
// DAG EXECUTION (Worker-side)
// ============================================================================

typedef enum {
    DAG_EXEC_STATUS_PENDING = 0,
    DAG_EXEC_STATUS_RUNNING = 1,
    DAG_EXEC_STATUS_COMPLETED = 2,
    DAG_EXEC_STATUS_FAILED = 3
} dag_exec_status_t;

typedef struct dag_execution_context {
    execution_id_t exec_id;
    const dag_t *dag;
    
    // Input message
    uint8_t *input_data;
    size_t input_len;
    
    // Output result
    uint8_t *output_data;
    size_t output_len;
    size_t output_capacity;
    
    // Execution state
    dag_exec_status_t status;
    uint32_t current_step;
    uint64_t start_time_ms;
    uint64_t end_time_ms;
    
    // Step completion tracking
    uint8_t step_completed[MAX_DAG_STEPS];
    
} dag_execution_context_t;

// Execute a DAG on a message (blocking)
int dag_execute(dag_execution_context_t *ctx);

// Step execution (called internally by dag_execute)
int dag_execute_step(dag_execution_context_t *ctx, const dag_step_t *step);

// ============================================================================
// DAG VALIDATION
// ============================================================================

int dag_validate(const dag_t *dag);  // Check for cycles, valid dependencies, etc.

#endif // ROOLE_DAG_H