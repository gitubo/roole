// src/dag/dag_catalog.c

#include "roole/dag.h"
#include "roole/common.h"
#include <string.h>
#include <stdlib.h>

// ============================================================================
// DAG CATALOG IMPLEMENTATION
// ============================================================================

int dag_catalog_init(dag_catalog_t *catalog, size_t capacity) {
    if (!catalog || capacity == 0) return ROOLE_ERR_INVALID;
    
    memset(catalog, 0, sizeof(dag_catalog_t));
    
    catalog->dags = roole_calloc(capacity, sizeof(dag_t));
    if (!catalog->dags) {
        return ROOLE_ERR_NOMEM;
    }
    
    catalog->capacity = capacity;
    catalog->count = 0;
    
    if (pthread_rwlock_init(&catalog->lock, NULL) != 0) {
        roole_free(catalog->dags);
        return ROOLE_ERR_INVALID;
    }
    
    ROOLE_LOG_INFO("DAG catalog initialized (capacity: %zu)", capacity);
    return ROOLE_OK;
}

// ============================================================================
// DAG SERIALIZATION (Stub implementation)
// ============================================================================

size_t dag_serialize(const dag_t *dag, uint8_t *buffer, size_t buffer_size) {
    if (!dag || !buffer || buffer_size < sizeof(dag_t)) {
        return 0;
    }
    
    // Simple memcpy for now (not safe for network transfer with pointers!)
    // TODO: Proper serialization with step config data
    memcpy(buffer, dag, sizeof(dag_t));
    return sizeof(dag_t);
}

int dag_deserialize(const uint8_t *buffer, size_t buffer_len, dag_t *out_dag) {
    if (!buffer || !out_dag || buffer_len < sizeof(dag_t)) {
        return ROOLE_ERR_INVALID;
    }
    
    // Simple memcpy for now
    // TODO: Proper deserialization
    memcpy(out_dag, buffer, sizeof(dag_t));
    
    // Clear config data pointers (not valid after deserialization)
    for (size_t i = 0; i < out_dag->step_count; i++) {
        out_dag->steps[i].config_data = NULL;
        out_dag->steps[i].config_len = 0;
    }
    
    return ROOLE_OK;
}

void dag_catalog_destroy(dag_catalog_t *catalog) {
    if (!catalog) return;
    
    pthread_rwlock_wrlock(&catalog->lock);
    
    // Free all step config data
    for (size_t i = 0; i < catalog->count; i++) {
        for (size_t j = 0; j < catalog->dags[i].step_count; j++) {
            if (catalog->dags[i].steps[j].config_data) {
                roole_free(catalog->dags[i].steps[j].config_data);
            }
        }
    }
    
    roole_free(catalog->dags);
    catalog->dags = NULL;
    catalog->count = 0;
    catalog->capacity = 0;
    
    pthread_rwlock_unlock(&catalog->lock);
    pthread_rwlock_destroy(&catalog->lock);
    
    ROOLE_LOG_INFO("DAG catalog destroyed");
}

int dag_catalog_add(dag_catalog_t *catalog, const dag_t *dag) {
    if (!catalog || !dag) return ROOLE_ERR_INVALID;
    
    pthread_rwlock_wrlock(&catalog->lock);
    
    // Check if DAG already exists
    for (size_t i = 0; i < catalog->count; i++) {
        if (catalog->dags[i].dag_id == dag->dag_id) {
            pthread_rwlock_unlock(&catalog->lock);
            ROOLE_LOG_WARN("DAG %u already exists", dag->dag_id);
            return ROOLE_ERR_EXISTS;
        }
    }
    
    // Check capacity
    if (catalog->count >= catalog->capacity) {
        pthread_rwlock_unlock(&catalog->lock);
        ROOLE_LOG_ERROR("DAG catalog full (capacity: %zu)", catalog->capacity);
        return ROOLE_ERR_FULL;
    }
    
    // Deep copy DAG
    dag_t *new_dag = &catalog->dags[catalog->count];
    memcpy(new_dag, dag, sizeof(dag_t));
    
    // Deep copy step config data
    for (size_t i = 0; i < new_dag->step_count; i++) {
        if (dag->steps[i].config_data && dag->steps[i].config_len > 0) {
            new_dag->steps[i].config_data = roole_malloc(dag->steps[i].config_len);
            if (!new_dag->steps[i].config_data) {
                // Cleanup on error
                for (size_t j = 0; j < i; j++) {
                    if (new_dag->steps[j].config_data) {
                        roole_free(new_dag->steps[j].config_data);
                    }
                }
                pthread_rwlock_unlock(&catalog->lock);
                return ROOLE_ERR_NOMEM;
            }
            memcpy(new_dag->steps[i].config_data, dag->steps[i].config_data, 
                   dag->steps[i].config_len);
        }
    }
    
    catalog->count++;
    
    pthread_rwlock_unlock(&catalog->lock);
    
    ROOLE_LOG_INFO("Added DAG %u '%s' (version %lu, %zu steps)", 
                   dag->dag_id, dag->name, dag->version, dag->step_count);
    return ROOLE_OK;
}

int dag_catalog_update(dag_catalog_t *catalog, const dag_t *dag) {
    if (!catalog || !dag) return ROOLE_ERR_INVALID;
    
    pthread_rwlock_wrlock(&catalog->lock);
    
    // Find existing DAG
    dag_t *existing = NULL;
    for (size_t i = 0; i < catalog->count; i++) {
        if (catalog->dags[i].dag_id == dag->dag_id) {
            existing = &catalog->dags[i];
            break;
        }
    }
    
    if (!existing) {
        pthread_rwlock_unlock(&catalog->lock);
        ROOLE_LOG_WARN("DAG %u not found for update", dag->dag_id);
        return ROOLE_ERR_NOTFOUND;
    }
    
    // Free old config data
    for (size_t i = 0; i < existing->step_count; i++) {
        if (existing->steps[i].config_data) {
            roole_free(existing->steps[i].config_data);
        }
    }
    
    // Copy new DAG
    memcpy(existing, dag, sizeof(dag_t));
    
    // Deep copy new config data
    for (size_t i = 0; i < dag->step_count; i++) {
        if (dag->steps[i].config_data && dag->steps[i].config_len > 0) {
            existing->steps[i].config_data = roole_malloc(dag->steps[i].config_len);
            if (existing->steps[i].config_data) {
                memcpy(existing->steps[i].config_data, dag->steps[i].config_data, 
                       dag->steps[i].config_len);
            }
        }
    }
    
    existing->updated_at_ms = roole_time_now_ms();
    
    pthread_rwlock_unlock(&catalog->lock);
    
    ROOLE_LOG_INFO("Updated DAG %u '%s' (version %lu)", 
                   dag->dag_id, dag->name, dag->version);
    return ROOLE_OK;
}

int dag_catalog_remove(dag_catalog_t *catalog, dag_id_t dag_id) {
    if (!catalog) return ROOLE_ERR_INVALID;
    
    pthread_rwlock_wrlock(&catalog->lock);
    
    // Find DAG
    size_t index = SIZE_MAX;
    for (size_t i = 0; i < catalog->count; i++) {
        if (catalog->dags[i].dag_id == dag_id) {
            index = i;
            break;
        }
    }
    
    if (index == SIZE_MAX) {
        pthread_rwlock_unlock(&catalog->lock);
        ROOLE_LOG_WARN("DAG %u not found for removal", dag_id);
        return ROOLE_ERR_NOTFOUND;
    }
    
    // Free config data
    for (size_t i = 0; i < catalog->dags[index].step_count; i++) {
        if (catalog->dags[index].steps[i].config_data) {
            roole_free(catalog->dags[index].steps[i].config_data);
        }
    }
    
    // Shift remaining DAGs
    if (index < catalog->count - 1) {
        memmove(&catalog->dags[index], &catalog->dags[index + 1], 
                (catalog->count - index - 1) * sizeof(dag_t));
    }
    
    catalog->count--;
    
    pthread_rwlock_unlock(&catalog->lock);
    
    ROOLE_LOG_INFO("Removed DAG %u", dag_id);
    return ROOLE_OK;
}

dag_t* dag_catalog_get(dag_catalog_t *catalog, dag_id_t dag_id) {
    if (!catalog) return NULL;
    
    pthread_rwlock_rdlock(&catalog->lock);
    
    for (size_t i = 0; i < catalog->count; i++) {
        if (catalog->dags[i].dag_id == dag_id) {
            // Return pointer - caller must call dag_catalog_release when done
            return &catalog->dags[i];
        }
    }
    
    pthread_rwlock_unlock(&catalog->lock);
    return NULL;
}

void dag_catalog_release(dag_catalog_t *catalog) {
    if (catalog) {
        pthread_rwlock_unlock(&catalog->lock);
    }
}

size_t dag_catalog_list(dag_catalog_t *catalog, dag_id_t *out_dag_ids, size_t max_count) {
    if (!catalog || !out_dag_ids || max_count == 0) return 0;
    
    pthread_rwlock_rdlock(&catalog->lock);
    
    size_t count = ROOLE_MIN(catalog->count, max_count);
    for (size_t i = 0; i < count; i++) {
        out_dag_ids[i] = catalog->dags[i].dag_id;
    }
    
    pthread_rwlock_unlock(&catalog->lock);
    
    return count;
}

// ============================================================================
// DAG VALIDATION
// ============================================================================

int dag_validate(const dag_t *dag) {
    if (!dag) return ROOLE_ERR_INVALID;
    
    if (dag->step_count == 0 || dag->step_count > MAX_DAG_STEPS) {
        ROOLE_LOG_ERROR("Invalid step count: %zu", dag->step_count);
        return ROOLE_ERR_INVALID;
    }
    
    // Check for valid step IDs and dependencies
    for (size_t i = 0; i < dag->step_count; i++) {
        const dag_step_t *step = &dag->steps[i];
        
        // Check dependencies exist
        for (size_t j = 0; j < step->dependency_count; j++) {
            uint32_t dep_id = step->dependencies[j];
            int found = 0;
            
            for (size_t k = 0; k < dag->step_count; k++) {
                if (dag->steps[k].step_id == dep_id) {
                    found = 1;
                    break;
                }
            }
            
            if (!found) {
                ROOLE_LOG_ERROR("Step %u has invalid dependency %u", 
                               step->step_id, dep_id);
                return ROOLE_ERR_INVALID;
            }
        }
    }
    
    // TODO: Check for cycles using DFS
    
    return ROOLE_OK;
}