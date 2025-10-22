// src/worker/task_queue.c

#define _POSIX_C_SOURCE 200809L

#include "roole/worker.h"
#include "roole/common.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>

// ============================================================================
// TASK QUEUE IMPLEMENTATION (Thread-safe circular buffer)
// ============================================================================

int task_queue_init(task_queue_t *queue, size_t capacity) {
    if (!queue || capacity == 0) return ROOLE_ERR_INVALID;
    
    memset(queue, 0, sizeof(task_queue_t));
    
    queue->tasks = roole_calloc(capacity, sizeof(task_t));
    if (!queue->tasks) {
        return ROOLE_ERR_NOMEM;
    }
    
    queue->capacity = capacity;
    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;
    
    if (pthread_mutex_init(&queue->lock, NULL) != 0) {
        roole_free(queue->tasks);
        return ROOLE_ERR_INVALID;
    }
    
    if (pthread_cond_init(&queue->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&queue->lock);
        roole_free(queue->tasks);
        return ROOLE_ERR_INVALID;
    }
    
    if (pthread_cond_init(&queue->not_full, NULL) != 0) {
        pthread_cond_destroy(&queue->not_empty);
        pthread_mutex_destroy(&queue->lock);
        roole_free(queue->tasks);
        return ROOLE_ERR_INVALID;
    }
    
    ROOLE_LOG_INFO("Task queue initialized (capacity: %zu)", capacity);
    return ROOLE_OK;
}

void task_queue_destroy(task_queue_t *queue) {
    if (!queue) return;
    
    pthread_mutex_lock(&queue->lock);
    
    roole_free(queue->tasks);
    queue->tasks = NULL;
    queue->capacity = 0;
    queue->count = 0;
    
    pthread_mutex_unlock(&queue->lock);
    
    pthread_cond_destroy(&queue->not_full);
    pthread_cond_destroy(&queue->not_empty);
    pthread_mutex_destroy(&queue->lock);
    
    ROOLE_LOG_INFO("Task queue destroyed");
}

int task_queue_push(task_queue_t *queue, const task_t *task) {
    if (!queue || !task) return ROOLE_ERR_INVALID;
    
    pthread_mutex_lock(&queue->lock);
    
    // Wait if queue is full
    while (queue->count >= queue->capacity) {
        ROOLE_LOG_WARN("Task queue full, waiting...");
        pthread_cond_wait(&queue->not_full, &queue->lock);
    }
    
    // Add task to tail
    queue->tasks[queue->tail] = *task;
    queue->tail = (queue->tail + 1) % queue->capacity;
    queue->count++;
    
    pthread_mutex_unlock(&queue->lock);
    
    // Signal that queue is not empty
    pthread_cond_signal(&queue->not_empty);
    
    ROOLE_LOG_DEBUG("Task %lu enqueued (queue size: %zu)", task->exec_id, queue->count);
    return ROOLE_OK;
}

int task_queue_pop(task_queue_t *queue, task_t *out_task, int timeout_ms) {
    if (!queue || !out_task) return ROOLE_ERR_INVALID;
    
    pthread_mutex_lock(&queue->lock);
    
    // Wait if queue is empty
    if (timeout_ms < 0) {
        // Block indefinitely
        while (queue->count == 0) {
            pthread_cond_wait(&queue->not_empty, &queue->lock);
        }
    } else if (timeout_ms > 0) {
        // Timed wait
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += timeout_ms / 1000;
        ts.tv_nsec += (timeout_ms % 1000) * 1000000;
        if (ts.tv_nsec >= 1000000000) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000;
        }
        
        while (queue->count == 0) {
            int ret = pthread_cond_timedwait(&queue->not_empty, &queue->lock, &ts);
            if (ret == ETIMEDOUT) {
                pthread_mutex_unlock(&queue->lock);
                return ROOLE_ERR_TIMEOUT;
            }
        }
    } else {
        // Non-blocking
        if (queue->count == 0) {
            pthread_mutex_unlock(&queue->lock);
            return ROOLE_ERR_EMPTY;
        }
    }
    
    // Pop task from head
    *out_task = queue->tasks[queue->head];
    queue->head = (queue->head + 1) % queue->capacity;
    queue->count--;
    
    pthread_mutex_unlock(&queue->lock);
    
    // Signal that queue is not full
    pthread_cond_signal(&queue->not_full);
    
    ROOLE_LOG_DEBUG("Task %lu dequeued (queue size: %zu)", out_task->exec_id, queue->count);
    return ROOLE_OK;
}

size_t task_queue_size(task_queue_t *queue) {
    if (!queue) return 0;
    
    pthread_mutex_lock(&queue->lock);
    size_t size = queue->count;
    pthread_mutex_unlock(&queue->lock);
    
    return size;
}

int task_queue_is_empty(task_queue_t *queue) {
    return (task_queue_size(queue) == 0);
}