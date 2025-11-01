// src/node/message_queue.c

#define _POSIX_C_SOURCE 200809L

#include "roole/node.h"
#include "roole/common.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>

// ============================================================================
// MESSAGE QUEUE IMPLEMENTATION (Thread-safe circular buffer)
// ============================================================================

int message_queue_init(message_queue_t *queue, size_t capacity) {
    if (!queue || capacity == 0) return RESULT_ERR_INVALID;
    
    memset(queue, 0, sizeof(message_queue_t));
    
    queue->messages = safe_calloc(capacity, sizeof(message_t));
    if (!queue->messages) {
        return RESULT_ERR_NOMEM;
    }
    
    queue->capacity = capacity;
    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;
    
    if (pthread_mutex_init(&queue->lock, NULL) != 0) {
        safe_free(queue->messages);
        return RESULT_ERR_INVALID;
    }
    
    if (pthread_cond_init(&queue->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&queue->lock);
        safe_free(queue->messages);
        return RESULT_ERR_INVALID;
    }
    
    if (pthread_cond_init(&queue->not_full, NULL) != 0) {
        pthread_cond_destroy(&queue->not_empty);
        pthread_mutex_destroy(&queue->lock);
        safe_free(queue->messages);
        return RESULT_ERR_INVALID;
    }
    
    LOG_INFO("Message queue initialized (capacity: %zu)", capacity);
    return RESULT_OK;
}

void message_queue_destroy(message_queue_t *queue) {
    if (!queue) return;
    
    pthread_mutex_lock(&queue->lock);
    
    safe_free(queue->messages);
    queue->messages = NULL;
    queue->capacity = 0;
    queue->count = 0;
    
    pthread_mutex_unlock(&queue->lock);
    
    pthread_cond_destroy(&queue->not_full);
    pthread_cond_destroy(&queue->not_empty);
    pthread_mutex_destroy(&queue->lock);
    
    LOG_INFO("Message queue destroyed");
}

int message_queue_push(message_queue_t *queue, const message_t *message) {
    if (!queue || !message) return RESULT_ERR_INVALID;
    
    pthread_mutex_lock(&queue->lock);
    
    // Wait if queue is full
    while (queue->count >= queue->capacity) {
        LOG_WARN("Message queue full, waiting...");
        pthread_cond_wait(&queue->not_full, &queue->lock);
    }
    
    // Add message to tail
    queue->messages[queue->tail] = *message;
    queue->tail = (queue->tail + 1) % queue->capacity;
    queue->count++;
    
    pthread_mutex_unlock(&queue->lock);
    
    // Signal that queue is not empty
    pthread_cond_signal(&queue->not_empty);
    
    LOG_DEBUG("Message %lu enqueued (queue size: %zu)", message->exec_id, queue->count);
    return RESULT_OK;
}

int message_queue_pop(message_queue_t *queue, message_t *out_message, int timeout_ms) {
    if (!queue || !out_message) return RESULT_ERR_INVALID;
    
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
                return RESULT_ERR_TIMEOUT;
            }
        }
    } else {
        // Non-blocking
        if (queue->count == 0) {
            pthread_mutex_unlock(&queue->lock);
            return RESULT_ERR_EMPTY;
        }
    }
    
    // Pop message from head
    *out_message = queue->messages[queue->head];
    queue->head = (queue->head + 1) % queue->capacity;
    queue->count--;
    
    pthread_mutex_unlock(&queue->lock);
    
    // Signal that queue is not full
    pthread_cond_signal(&queue->not_full);
    
    LOG_DEBUG("Message %lu dequeued (queue size: %zu)", out_message->exec_id, queue->count);
    return RESULT_OK;
}

size_t message_queue_size(message_queue_t *queue) {
    if (!queue) return 0;
    
    pthread_mutex_lock(&queue->lock);
    size_t size = queue->count;
    pthread_mutex_unlock(&queue->lock);
    
    return size;
}

int message_queue_is_empty(message_queue_t *queue) {
    return (message_queue_size(queue) == 0);
}