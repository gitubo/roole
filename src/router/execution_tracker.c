// src/router/execution_tracker.c

#include "roole/node.h"
#include "roole/common.h"
#include <string.h>
#include <stdlib.h>

// ============================================================================
// EXECUTION TRACKER IMPLEMENTATION
// ============================================================================

int execution_tracker_init(execution_tracker_t *tracker, size_t capacity) {
    if (!tracker || capacity == 0) return RESULT_ERR_INVALID;
    
    memset(tracker, 0, sizeof(execution_tracker_t));
    
    tracker->records = safe_calloc(capacity, sizeof(execution_record_t));
    if (!tracker->records) {
        return RESULT_ERR_NOMEM;
    }
    
    tracker->capacity = capacity;
    tracker->next_exec_id = 1;  // Start from 1 (0 = invalid)
    
    if (pthread_rwlock_init(&tracker->lock, NULL) != 0) {
        safe_free(tracker->records);
        return RESULT_ERR_INVALID;
    }
    
    LOG_INFO("Execution tracker initialized (capacity: %zu)", capacity);
    return RESULT_OK;
}

void execution_tracker_destroy(execution_tracker_t *tracker) {
    if (!tracker) return;
    
    pthread_rwlock_wrlock(&tracker->lock);
    
    safe_free(tracker->records);
    tracker->records = NULL;
    tracker->capacity = 0;
    
    pthread_rwlock_unlock(&tracker->lock);
    pthread_rwlock_destroy(&tracker->lock);
    
    LOG_INFO("Execution tracker destroyed");
}

execution_id_t execution_tracker_add(execution_tracker_t *tracker, rule_id_t dag_id,
                                    node_id_t worker_id, const uint8_t *message, 
                                    size_t message_len, uint8_t max_retries) {
    if (!tracker || !message || message_len == 0 || message_len > MAX_MESSAGE_SIZE) {
        return 0;  // Invalid execution_id
    }
    
    pthread_rwlock_wrlock(&tracker->lock);
    
    // Find free slot
    size_t slot = SIZE_MAX;
    for (size_t i = 0; i < tracker->capacity; i++) {
        if (!tracker->records[i].active) {
            slot = i;
            break;
        }
    }
    
    if (slot == SIZE_MAX) {
        pthread_rwlock_unlock(&tracker->lock);
        LOG_ERROR("Execution tracker full (capacity: %zu)", tracker->capacity);
        return 0;
    }
    
    // Allocate new execution ID
    execution_id_t exec_id = tracker->next_exec_id++;
    
    // Fill record
    execution_record_t *rec = &tracker->records[slot];
    memset(rec, 0, sizeof(execution_record_t));
    
    rec->exec_id = exec_id;
    rec->dag_id = dag_id;
    rec->assigned_peer = worker_id;
    rec->status = EXEC_STATUS_PENDING;
    rec->submit_time_ms = time_now_ms();
    rec->retry_count = 0;
    rec->max_retries = max_retries;
    rec->active = 1;
    
    // Copy message
    memcpy(rec->message_data, message, message_len);
    rec->message_len = message_len;
    
    pthread_rwlock_unlock(&tracker->lock);
    
    LOG_INFO("Tracking execution %lu (DAG %u, worker %u)", 
                   exec_id, dag_id, worker_id);
    return exec_id;
}

int execution_tracker_update_status(execution_tracker_t *tracker, execution_id_t exec_id,
                                   execution_status_t status) {
    if (!tracker || exec_id == 0) return RESULT_ERR_INVALID;
    
    pthread_rwlock_wrlock(&tracker->lock);
    
    for (size_t i = 0; i < tracker->capacity; i++) {
        if (tracker->records[i].active && tracker->records[i].exec_id == exec_id) {
            execution_record_t *rec = &tracker->records[i];
            
            execution_status_t old_status = rec->status;
            rec->status = status;
            
            // Update timestamps
            uint64_t now = time_now_ms();
            if (status == EXEC_STATUS_RUNNING && old_status == EXEC_STATUS_PENDING) {
                rec->start_time_ms = now;
            }
            else if (status == EXEC_STATUS_COMPLETED || status == EXEC_STATUS_FAILED) {
                rec->complete_time_ms = now;
            }
            
            pthread_rwlock_unlock(&tracker->lock);
            
            LOG_DEBUG("Execution %lu status: %d -> %d", exec_id, old_status, status);
            return RESULT_OK;
        }
    }
    
    pthread_rwlock_unlock(&tracker->lock);
    LOG_WARN("Execution %lu not found for status update", exec_id);
    return RESULT_ERR_NOTFOUND;
}

execution_record_t* execution_tracker_get(execution_tracker_t *tracker, execution_id_t exec_id) {
    if (!tracker || exec_id == 0) return NULL;
    
    pthread_rwlock_rdlock(&tracker->lock);
    
    for (size_t i = 0; i < tracker->capacity; i++) {
        if (tracker->records[i].active && tracker->records[i].exec_id == exec_id) {
            // Return pointer - caller must call execution_tracker_release
            return &tracker->records[i];
        }
    }
    
    pthread_rwlock_unlock(&tracker->lock);
    return NULL;
}

void execution_tracker_release(execution_tracker_t *tracker) {
    if (tracker) {
        pthread_rwlock_unlock(&tracker->lock);
    }
}

size_t execution_tracker_get_by_worker(execution_tracker_t *tracker, node_id_t worker_id,
                                      execution_id_t *out_exec_ids, size_t max_count) {
    if (!tracker || !out_exec_ids || max_count == 0) return 0;
    
    pthread_rwlock_rdlock(&tracker->lock);
    
    size_t found = 0;
    for (size_t i = 0; i < tracker->capacity && found < max_count; i++) {
        if (tracker->records[i].active && 
            tracker->records[i].assigned_peer  == worker_id &&
            tracker->records[i].status != EXEC_STATUS_COMPLETED &&
            tracker->records[i].status != EXEC_STATUS_FAILED) {
            out_exec_ids[found++] = tracker->records[i].exec_id;
        }
    }
    
    pthread_rwlock_unlock(&tracker->lock);
    
    return found;
}

int execution_tracker_remove(execution_tracker_t *tracker, execution_id_t exec_id) {
    if (!tracker || exec_id == 0) return RESULT_ERR_INVALID;
    
    pthread_rwlock_wrlock(&tracker->lock);
    
    for (size_t i = 0; i < tracker->capacity; i++) {
        if (tracker->records[i].active && tracker->records[i].exec_id == exec_id) {
            tracker->records[i].active = 0;
            
            pthread_rwlock_unlock(&tracker->lock);
            LOG_DEBUG("Removed execution %lu from tracker", exec_id);
            return RESULT_OK;
        }
    }
    
    pthread_rwlock_unlock(&tracker->lock);
    return RESULT_ERR_NOTFOUND;
}

size_t execution_tracker_cleanup_completed(execution_tracker_t *tracker) {
    if (!tracker) return 0;
    
    pthread_rwlock_wrlock(&tracker->lock);
    
    size_t cleaned = 0;
    uint64_t now = time_now_ms();
    
    // Remove executions completed/failed more than 5 minutes ago
    const uint64_t CLEANUP_THRESHOLD_MS = 5 * 60 * 1000;
    
    for (size_t i = 0; i < tracker->capacity; i++) {
        if (tracker->records[i].active) {
            execution_record_t *rec = &tracker->records[i];
            
            if ((rec->status == EXEC_STATUS_COMPLETED || rec->status == EXEC_STATUS_FAILED) &&
                rec->complete_time_ms > 0 &&
                (now - rec->complete_time_ms) > CLEANUP_THRESHOLD_MS) {
                
                rec->active = 0;
                cleaned++;
            }
        }
    }
    
    pthread_rwlock_unlock(&tracker->lock);
    
    if (cleaned > 0) {
        LOG_INFO("Cleaned up %zu completed executions", cleaned);
    }
    
    return cleaned;
}