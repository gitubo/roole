// src/dag/dag_catalog.c

#include "roole/dag.h"
#include "roole/common.h"
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <endian.h>


// ============================================================================
// DAG CATALOG IMPLEMENTATION
// ============================================================================
// Add new function:
void dag_catalog_set_change_callback(dag_catalog_t *catalog,
                                     dag_catalog_change_cb callback,
                                     void *user_data) {
    if (!catalog) return;
    
    pthread_rwlock_wrlock(&catalog->lock);
    catalog->change_callback = callback;
    catalog->callback_user_data = user_data;
    pthread_rwlock_unlock(&catalog->lock);
}


int dag_catalog_init(dag_catalog_t *catalog, size_t capacity) {
    if (!catalog || capacity == 0) return RESULT_ERR_INVALID;
    
    memset(catalog, 0, sizeof(dag_catalog_t));
    
    catalog->dags = safe_calloc(capacity, sizeof(dag_t));
    if (!catalog->dags) {
        return RESULT_ERR_NOMEM;
    }
    
    catalog->capacity = capacity;
    catalog->count = 0;
    
    if (pthread_rwlock_init(&catalog->lock, NULL) != 0) {
        safe_free(catalog->dags);
        return RESULT_ERR_INVALID;
    }
    
    LOG_INFO("DAG catalog initialized (capacity: %zu)", capacity);

    return RESULT_OK;
}

// ============================================================================
// DAG SERIALIZATION (Stub implementation)
// ============================================================================

/**
 * Serialization format:
 * [dag_id: 4 bytes]
 * [version: 8 bytes]
 * [name_len: 4 bytes]
 * [name: variable]
 * [step_count: 4 bytes]
 * For each step:
 *   [step_id: 4 bytes]
 *   [name_len: 4 bytes]
 *   [name: variable]
 *   [function_name_len: 4 bytes]
 *   [function_name: variable]
 *   [dependency_count: 4 bytes]
 *   [dependencies: dep_count * 4 bytes]
 *   [timeout_ms: 4 bytes]
 *   [max_retries: 1 byte]
 *   [config_len: 4 bytes]
 *   [config_data: variable]
 */

size_t dag_serialize(const dag_t *dag, uint8_t *buffer, size_t buffer_size) {
    if (!dag || !buffer) {
        LOG_ERROR("dag_serialize: NULL parameters");
        return 0;
    }
    
    // Calculate required size first
    size_t required_size = 4 + 8 + 4 + strlen(dag->name) + 4;
    
    for (size_t i = 0; i < dag->step_count; i++) {
        const dag_step_t *step = &dag->steps[i];
        required_size += 4;  // step_id
        required_size += 4 + strlen(step->name);
        required_size += 4 + strlen(step->function_name);
        required_size += 4;  // dependency_count
        required_size += step->dependency_count * 4;
        required_size += 4;  // timeout_ms
        required_size += 1;  // max_retries
        required_size += 4;  // config_len
        required_size += step->config_len;
    }
    
    if (buffer_size < required_size) {
        LOG_ERROR("dag_serialize: Buffer too small (need %zu, have %zu)", 
                  required_size, buffer_size);
        return 0;
    }
    
    uint8_t *ptr = buffer;
    
    // Serialize DAG header
    uint32_t dag_id_net = htonl(dag->dag_id);
    memcpy(ptr, &dag_id_net, 4);
    ptr += 4;
    
    uint64_t version_net = htobe64(dag->version);
    memcpy(ptr, &version_net, 8);
    ptr += 8;
    
    uint32_t name_len = strlen(dag->name);
    uint32_t name_len_net = htonl(name_len);
    memcpy(ptr, &name_len_net, 4);
    ptr += 4;
    memcpy(ptr, dag->name, name_len);
    ptr += name_len;
    
    uint32_t step_count_net = htonl((uint32_t)dag->step_count);
    memcpy(ptr, &step_count_net, 4);
    ptr += 4;
    
    // Serialize each step
    for (size_t i = 0; i < dag->step_count; i++) {
        const dag_step_t *step = &dag->steps[i];
        
        // Step ID
        uint32_t step_id_net = htonl(step->step_id);
        memcpy(ptr, &step_id_net, 4);
        ptr += 4;
        
        // Step name
        uint32_t step_name_len = strlen(step->name);
        uint32_t step_name_len_net = htonl(step_name_len);
        memcpy(ptr, &step_name_len_net, 4);
        ptr += 4;
        memcpy(ptr, step->name, step_name_len);
        ptr += step_name_len;
        
        // Function name
        uint32_t func_name_len = strlen(step->function_name);
        uint32_t func_name_len_net = htonl(func_name_len);
        memcpy(ptr, &func_name_len_net, 4);
        ptr += 4;
        memcpy(ptr, step->function_name, func_name_len);
        ptr += func_name_len;
        
        // Dependencies
        uint32_t dep_count_net = htonl((uint32_t)step->dependency_count);
        memcpy(ptr, &dep_count_net, 4);
        ptr += 4;
        
        for (size_t j = 0; j < step->dependency_count; j++) {
            uint32_t dep_net = htonl(step->dependencies[j]);
            memcpy(ptr, &dep_net, 4);
            ptr += 4;
        }
        
        // Timeout and retries
        uint32_t timeout_net = htonl(step->timeout_ms);
        memcpy(ptr, &timeout_net, 4);
        ptr += 4;
        
        *ptr++ = step->max_retries;
        
        // Config data
        uint32_t config_len_net = htonl((uint32_t)step->config_len);
        memcpy(ptr, &config_len_net, 4);
        ptr += 4;
        
        if (step->config_len > 0 && step->config_data) {
            memcpy(ptr, step->config_data, step->config_len);
            ptr += step->config_len;
        }
    }
    
    size_t serialized_size = ptr - buffer;
    LOG_DEBUG("dag_serialize: Serialized DAG %u (%zu bytes)", 
              dag->dag_id, serialized_size);
    
    return serialized_size;
}

int dag_deserialize(const uint8_t *buffer, size_t buffer_len, dag_t *out_dag) {
    if (!buffer || !out_dag || buffer_len < 16) {
        LOG_ERROR("dag_deserialize: Invalid parameters");
        return RESULT_ERR_INVALID;
    }
    
    memset(out_dag, 0, sizeof(dag_t));
    
    const uint8_t *ptr = buffer;
    const uint8_t *end = buffer + buffer_len;
    
    // Deserialize header
    if (ptr + 4 > end) return RESULT_ERR_INVALID;
    uint32_t dag_id_net;
    memcpy(&dag_id_net, ptr, 4);
    out_dag->dag_id = ntohl(dag_id_net);
    ptr += 4;
    
    if (ptr + 8 > end) return RESULT_ERR_INVALID;
    uint64_t version_net;
    memcpy(&version_net, ptr, 8);
    out_dag->version = be64toh(version_net);
    ptr += 8;
    
    if (ptr + 4 > end) return RESULT_ERR_INVALID;
    uint32_t name_len_net;
    memcpy(&name_len_net, ptr, 4);
    uint32_t name_len = ntohl(name_len_net);
    ptr += 4;
    
    if (name_len >= MAX_DAG_NAME || ptr + name_len > end) {
        return RESULT_ERR_INVALID;
    }
    memcpy(out_dag->name, ptr, name_len);
    out_dag->name[name_len] = '\0';
    ptr += name_len;
    
    if (ptr + 4 > end) return RESULT_ERR_INVALID;
    uint32_t step_count_net;
    memcpy(&step_count_net, ptr, 4);
    out_dag->step_count = ntohl(step_count_net);
    ptr += 4;
    
    if (out_dag->step_count > MAX_DAG_STEPS) {
        LOG_ERROR("dag_deserialize: Too many steps (%zu)", out_dag->step_count);
        return RESULT_ERR_INVALID;
    }
    
    // Deserialize steps
    for (size_t i = 0; i < out_dag->step_count; i++) {
        dag_step_t *step = &out_dag->steps[i];
        
        // Step ID
        if (ptr + 4 > end) return RESULT_ERR_INVALID;
        uint32_t step_id_net;
        memcpy(&step_id_net, ptr, 4);
        step->step_id = ntohl(step_id_net);
        ptr += 4;
        
        // Step name
        if (ptr + 4 > end) return RESULT_ERR_INVALID;
        uint32_t step_name_len_net;
        memcpy(&step_name_len_net, ptr, 4);
        uint32_t step_name_len = ntohl(step_name_len_net);
        ptr += 4;
        
        if (step_name_len >= MAX_STEP_NAME || ptr + step_name_len > end) {
            return RESULT_ERR_INVALID;
        }
        memcpy(step->name, ptr, step_name_len);
        step->name[step_name_len] = '\0';
        ptr += step_name_len;
        
        // Function name
        if (ptr + 4 > end) return RESULT_ERR_INVALID;
        uint32_t func_name_len_net;
        memcpy(&func_name_len_net, ptr, 4);
        uint32_t func_name_len = ntohl(func_name_len_net);
        ptr += 4;
        
        if (func_name_len >= MAX_STEP_NAME || ptr + func_name_len > end) {
            return RESULT_ERR_INVALID;
        }
        memcpy(step->function_name, ptr, func_name_len);
        step->function_name[func_name_len] = '\0';
        ptr += func_name_len;
        
        // Dependencies
        if (ptr + 4 > end) return RESULT_ERR_INVALID;
        uint32_t dep_count_net;
        memcpy(&dep_count_net, ptr, 4);
        step->dependency_count = ntohl(dep_count_net);
        ptr += 4;
        
        if (step->dependency_count > MAX_STEP_DEPENDENCIES) {
            return RESULT_ERR_INVALID;
        }
        
        for (size_t j = 0; j < step->dependency_count; j++) {
            if (ptr + 4 > end) return RESULT_ERR_INVALID;
            uint32_t dep_net;
            memcpy(&dep_net, ptr, 4);
            step->dependencies[j] = ntohl(dep_net);
            ptr += 4;
        }
        
        // Timeout and retries
        if (ptr + 4 > end) return RESULT_ERR_INVALID;
        uint32_t timeout_net;
        memcpy(&timeout_net, ptr, 4);
        step->timeout_ms = ntohl(timeout_net);
        ptr += 4;
        
        if (ptr + 1 > end) return RESULT_ERR_INVALID;
        step->max_retries = *ptr++;
        
        // Config data
        if (ptr + 4 > end) return RESULT_ERR_INVALID;
        uint32_t config_len_net;
        memcpy(&config_len_net, ptr, 4);
        step->config_len = ntohl(config_len_net);
        ptr += 4;
        
        if (step->config_len > 0) {
            if (ptr + step->config_len > end) {
                return RESULT_ERR_INVALID;
            }
            
            step->config_data = safe_malloc(step->config_len);
            if (!step->config_data) {
                // Cleanup previously allocated config data
                for (size_t k = 0; k < i; k++) {
                    if (out_dag->steps[k].config_data) {
                        safe_free(out_dag->steps[k].config_data);
                    }
                }
                return RESULT_ERR_NOMEM;
            }
            
            memcpy(step->config_data, ptr, step->config_len);
            ptr += step->config_len;
        } else {
            step->config_data = NULL;
        }
    }
    
    out_dag->created_at_ms = time_now_ms();
    out_dag->updated_at_ms = out_dag->created_at_ms;
    
    LOG_INFO("dag_deserialize: Deserialized DAG %u '%s' (%zu steps)", 
             out_dag->dag_id, out_dag->name, out_dag->step_count);
    
    return RESULT_OK;
}

void dag_catalog_destroy(dag_catalog_t *catalog) {
    if (!catalog) return;
    
    pthread_rwlock_wrlock(&catalog->lock);
    
    // Free all step config data
    for (size_t i = 0; i < catalog->count; i++) {
        for (size_t j = 0; j < catalog->dags[i].step_count; j++) {
            if (catalog->dags[i].steps[j].config_data) {
                safe_free(catalog->dags[i].steps[j].config_data);
            }
        }
    }
    
    safe_free(catalog->dags);
    catalog->dags = NULL;
    catalog->count = 0;
    catalog->capacity = 0;
    
    pthread_rwlock_unlock(&catalog->lock);
    pthread_rwlock_destroy(&catalog->lock);
    
    LOG_INFO("DAG catalog destroyed");
}

int dag_catalog_add(dag_catalog_t *catalog, const dag_t *dag) {
    if (!catalog || !dag) return RESULT_ERR_INVALID;
    
    pthread_rwlock_wrlock(&catalog->lock);
    
    // Check if DAG already exists
    for (size_t i = 0; i < catalog->count; i++) {
        if (catalog->dags[i].dag_id == dag->dag_id) {
            pthread_rwlock_unlock(&catalog->lock);
            LOG_WARN("DAG %u already exists", dag->dag_id);
            return RESULT_ERR_EXISTS;
        }
    }
    
    // Check capacity
    if (catalog->count >= catalog->capacity) {
        pthread_rwlock_unlock(&catalog->lock);
        LOG_ERROR("DAG catalog full (capacity: %zu)", catalog->capacity);
        return RESULT_ERR_FULL;
    }
    
    // Deep copy DAG
    dag_t *new_dag = &catalog->dags[catalog->count];
    memcpy(new_dag, dag, sizeof(dag_t));
    
    // Deep copy step config data
    for (size_t i = 0; i < new_dag->step_count; i++) {
        if (dag->steps[i].config_data && dag->steps[i].config_len > 0) {
            new_dag->steps[i].config_data = safe_malloc(dag->steps[i].config_len);
            if (!new_dag->steps[i].config_data) {
                // Cleanup on error
                for (size_t j = 0; j < i; j++) {
                    if (new_dag->steps[j].config_data) {
                        safe_free(new_dag->steps[j].config_data);
                    }
                }
                pthread_rwlock_unlock(&catalog->lock);
                return RESULT_ERR_NOMEM;
            }
            memcpy(new_dag->steps[i].config_data, dag->steps[i].config_data, 
                   dag->steps[i].config_len);
        }
    }
    
    catalog->count++;

    if (catalog->change_callback) {
        catalog->change_callback(catalog->count, catalog->callback_user_data);
    }
    
    pthread_rwlock_unlock(&catalog->lock);
    
    LOG_INFO("Added DAG %u '%s' (version %lu, %zu steps)", 
                   dag->dag_id, dag->name, dag->version, dag->step_count);
    return RESULT_OK;
}

int dag_catalog_update(dag_catalog_t *catalog, const dag_t *dag) {
    if (!catalog || !dag) return RESULT_ERR_INVALID;
    
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
        LOG_WARN("DAG %u not found for update", dag->dag_id);
        return RESULT_ERR_NOTFOUND;
    }
    
    // Free old config data
    for (size_t i = 0; i < existing->step_count; i++) {
        if (existing->steps[i].config_data) {
            safe_free(existing->steps[i].config_data);
        }
    }
    
    // Copy new DAG
    memcpy(existing, dag, sizeof(dag_t));
    
    // Deep copy new config data
    for (size_t i = 0; i < dag->step_count; i++) {
        if (dag->steps[i].config_data && dag->steps[i].config_len > 0) {
            existing->steps[i].config_data = safe_malloc(dag->steps[i].config_len);
            if (existing->steps[i].config_data) {
                memcpy(existing->steps[i].config_data, dag->steps[i].config_data, 
                       dag->steps[i].config_len);
            }
        }
    }
    
    existing->updated_at_ms = time_now_ms();
    
    pthread_rwlock_unlock(&catalog->lock);
    
    LOG_INFO("Updated DAG %u '%s' (version %lu)", 
                   dag->dag_id, dag->name, dag->version);
    return RESULT_OK;
}

int dag_catalog_remove(dag_catalog_t *catalog, rule_id_t dag_id) {
    if (!catalog) return RESULT_ERR_INVALID;
    
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
        LOG_WARN("DAG %u not found for removal", dag_id);
        return RESULT_ERR_NOTFOUND;
    }
    
    // Free config data
    for (size_t i = 0; i < catalog->dags[index].step_count; i++) {
        if (catalog->dags[index].steps[i].config_data) {
            safe_free(catalog->dags[index].steps[i].config_data);
        }
    }
    
    // Shift remaining DAGs
    if (index < catalog->count - 1) {
        memmove(&catalog->dags[index], &catalog->dags[index + 1], 
                (catalog->count - index - 1) * sizeof(dag_t));
    }
    
    catalog->count--;

    if (catalog->change_callback) {
        catalog->change_callback(catalog->count, catalog->callback_user_data);
    }
    
    pthread_rwlock_unlock(&catalog->lock);
    
    LOG_INFO("Removed DAG %u", dag_id);
    return RESULT_OK;
}

dag_t* dag_catalog_get(dag_catalog_t *catalog, rule_id_t dag_id) {
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

size_t dag_catalog_list(dag_catalog_t *catalog, rule_id_t *out_dag_ids, size_t max_count) {
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
    if (!dag) return RESULT_ERR_INVALID;
    
    if (dag->step_count == 0 || dag->step_count > MAX_DAG_STEPS) {
        LOG_ERROR("Invalid step count: %zu", dag->step_count);
        return RESULT_ERR_INVALID;
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
                LOG_ERROR("Step %u has invalid dependency %u", 
                               step->step_id, dep_id);
                return RESULT_ERR_INVALID;
            }
        }
    }
    
    // TODO: Check for cycles using DFS
    
    return RESULT_OK;
}