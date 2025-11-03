// src/core/event_bus.c
#define _POSIX_C_SOURCE 200809L

#include "roole/event_bus.h"
#include "roole/logger.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

// ============================================================================
// CONFIGURATION
// ============================================================================

#define MAX_SUBSCRIBERS_PER_TYPE 16
#define EVENT_QUEUE_SIZE 1024

// ============================================================================
// INTERNAL STRUCTURES
// ============================================================================

typedef struct subscriber {
    event_handler_fn handler;
    void *user_data;
    int active;
} subscriber_t;

typedef struct event_queue {
    event_t *events;
    size_t head;
    size_t tail;
    size_t count;
    size_t capacity;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} event_queue_t;

struct event_bus {
    subscriber_t subscribers[EVENT_TYPE_MAX][MAX_SUBSCRIBERS_PER_TYPE];
    pthread_rwlock_t subscriber_locks[EVENT_TYPE_MAX];
    
    event_queue_t queue;
    pthread_t dispatch_thread;
    
    // Statistics
    uint64_t events_published;
    uint64_t events_dispatched;
    uint64_t events_dropped;
    pthread_mutex_t stats_lock;
    
    volatile int shutdown;
};

// ============================================================================
// EVENT QUEUE IMPLEMENTATION
// ============================================================================

static int event_queue_init(event_queue_t *queue, size_t capacity) {
    memset(queue, 0, sizeof(event_queue_t));
    
    queue->events = calloc(capacity, sizeof(event_t));
    if (!queue->events) return -1;
    
    queue->capacity = capacity;
    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;
    
    pthread_mutex_init(&queue->lock, NULL);
    pthread_cond_init(&queue->not_empty, NULL);
    pthread_cond_init(&queue->not_full, NULL);
    
    return 0;
}

static void event_queue_destroy(event_queue_t *queue) {
    if (!queue) return;
    
    pthread_mutex_lock(&queue->lock);
    free(queue->events);
    queue->events = NULL;
    pthread_mutex_unlock(&queue->lock);
    
    pthread_cond_destroy(&queue->not_full);
    pthread_cond_destroy(&queue->not_empty);
    pthread_mutex_destroy(&queue->lock);
}

static int event_queue_push(event_queue_t *queue, const event_t *event, int blocking) {
    pthread_mutex_lock(&queue->lock);
    
    while (queue->count >= queue->capacity) {
        if (!blocking) {
            pthread_mutex_unlock(&queue->lock);
            return -1;  // Queue full
        }
        pthread_cond_wait(&queue->not_full, &queue->lock);
    }
    
    queue->events[queue->tail] = *event;
    queue->tail = (queue->tail + 1) % queue->capacity;
    queue->count++;
    
    pthread_mutex_unlock(&queue->lock);
    pthread_cond_signal(&queue->not_empty);
    
    return 0;
}

static int event_queue_pop(event_queue_t *queue, event_t *event, int timeout_ms) {
    pthread_mutex_lock(&queue->lock);
    
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
                return -1;
            }
        }
    } else {
        // Non-blocking
        if (queue->count == 0) {
            pthread_mutex_unlock(&queue->lock);
            return -1;
        }
    }
    
    *event = queue->events[queue->head];
    queue->head = (queue->head + 1) % queue->capacity;
    queue->count--;
    
    pthread_mutex_unlock(&queue->lock);
    pthread_cond_signal(&queue->not_full);
    
    return 0;
}

// ============================================================================
// EVENT DISPATCH THREAD
// ============================================================================

static void dispatch_event_to_subscribers(event_bus_t *bus, const event_t *event) {
    if (event->type >= EVENT_TYPE_MAX) return;
    
    pthread_rwlock_rdlock(&bus->subscriber_locks[event->type]);
    
    for (int i = 0; i < MAX_SUBSCRIBERS_PER_TYPE; i++) {
        subscriber_t *sub = &bus->subscribers[event->type][i];
        
        if (sub->active && sub->handler) {
            // Call handler (note: handlers should be fast, non-blocking)
            sub->handler(event, sub->user_data);
            
            pthread_mutex_lock(&bus->stats_lock);
            bus->events_dispatched++;
            pthread_mutex_unlock(&bus->stats_lock);
        }
    }
    
    pthread_rwlock_unlock(&bus->subscriber_locks[event->type]);
}

static void* dispatch_thread_fn(void *arg) {
    event_bus_t *bus = (event_bus_t*)arg;
    
    logger_push_component("event_bus");
    LOG_INFO("Event bus dispatch thread started");
    
    while (!bus->shutdown) {
        event_t event;
        
        if (event_queue_pop(&bus->queue, &event, 100) == 0) {
            dispatch_event_to_subscribers(bus, &event);
        }
    }
    
    LOG_INFO("Event bus dispatch thread stopped");
    logger_pop_component();
    return NULL;
}

// ============================================================================
// EVENT BUS LIFECYCLE
// ============================================================================

event_bus_t* event_bus_create(void) {
    event_bus_t *bus = calloc(1, sizeof(event_bus_t));
    if (!bus) {
        LOG_ERROR("Failed to allocate event bus");
        return NULL;
    }
    
    // Initialize subscriber locks
    for (int i = 0; i < EVENT_TYPE_MAX; i++) {
        if (pthread_rwlock_init(&bus->subscriber_locks[i], NULL) != 0) {
            LOG_ERROR("Failed to initialize subscriber lock");
            for (int j = 0; j < i; j++) {
                pthread_rwlock_destroy(&bus->subscriber_locks[j]);
            }
            free(bus);
            return NULL;
        }
    }
    
    // Initialize event queue
    if (event_queue_init(&bus->queue, EVENT_QUEUE_SIZE) != 0) {
        LOG_ERROR("Failed to initialize event queue");
        for (int i = 0; i < EVENT_TYPE_MAX; i++) {
            pthread_rwlock_destroy(&bus->subscriber_locks[i]);
        }
        free(bus);
        return NULL;
    }
    
    pthread_mutex_init(&bus->stats_lock, NULL);
    
    // Start dispatch thread
    bus->shutdown = 0;
    if (pthread_create(&bus->dispatch_thread, NULL, dispatch_thread_fn, bus) != 0) {
        LOG_ERROR("Failed to create dispatch thread");
        event_queue_destroy(&bus->queue);
        for (int i = 0; i < EVENT_TYPE_MAX; i++) {
            pthread_rwlock_destroy(&bus->subscriber_locks[i]);
        }
        pthread_mutex_destroy(&bus->stats_lock);
        free(bus);
        return NULL;
    }
    
    LOG_INFO("Event bus created");
    return bus;
}

void event_bus_destroy(event_bus_t *bus) {
    if (!bus) return;
    
    LOG_INFO("Destroying event bus");
    
    // Signal shutdown
    bus->shutdown = 1;
    
    // Wake up dispatch thread
    pthread_cond_signal(&bus->queue.not_empty);
    
    // Wait for dispatch thread
    pthread_join(bus->dispatch_thread, NULL);
    
    // Cleanup
    event_queue_destroy(&bus->queue);
    
    for (int i = 0; i < EVENT_TYPE_MAX; i++) {
        pthread_rwlock_destroy(&bus->subscriber_locks[i]);
    }
    
    pthread_mutex_destroy(&bus->stats_lock);
    free(bus);
    
    LOG_INFO("Event bus destroyed");
}

// ============================================================================
// SUBSCRIPTION MANAGEMENT
// ============================================================================

int event_bus_subscribe(event_bus_t *bus,
                        event_type_t type,
                        event_handler_fn handler,
                        void *user_data) {
    if (!bus || !handler || type >= EVENT_TYPE_MAX) {
        return -1;
    }
    
    pthread_rwlock_wrlock(&bus->subscriber_locks[type]);
    
    // Find free slot
    int slot = -1;
    for (int i = 0; i < MAX_SUBSCRIBERS_PER_TYPE; i++) {
        if (!bus->subscribers[type][i].active) {
            slot = i;
            break;
        }
    }
    
    if (slot == -1) {
        pthread_rwlock_unlock(&bus->subscriber_locks[type]);
        LOG_ERROR("No free subscriber slots for event type %d", type);
        return -1;
    }
    
    bus->subscribers[type][slot].handler = handler;
    bus->subscribers[type][slot].user_data = user_data;
    bus->subscribers[type][slot].active = 1;
    
    pthread_rwlock_unlock(&bus->subscriber_locks[type]);
    
    LOG_INFO("Subscriber added for event type: %s", event_type_to_string(type));
    return 0;
}

int event_bus_unsubscribe(event_bus_t *bus,
                          event_type_t type,
                          event_handler_fn handler) {
    if (!bus || !handler || type >= EVENT_TYPE_MAX) {
        return -1;
    }
    
    pthread_rwlock_wrlock(&bus->subscriber_locks[type]);
    
    for (int i = 0; i < MAX_SUBSCRIBERS_PER_TYPE; i++) {
        if (bus->subscribers[type][i].active &&
            bus->subscribers[type][i].handler == handler) {
            
            bus->subscribers[type][i].active = 0;
            bus->subscribers[type][i].handler = NULL;
            bus->subscribers[type][i].user_data = NULL;
            
            pthread_rwlock_unlock(&bus->subscriber_locks[type]);
            LOG_INFO("Subscriber removed for event type: %s", 
                    event_type_to_string(type));
            return 0;
        }
    }
    
    pthread_rwlock_unlock(&bus->subscriber_locks[type]);
    return -1;
}

// ============================================================================
// EVENT PUBLISHING
// ============================================================================

int event_bus_publish(event_bus_t *bus, const event_t *event) {
    if (!bus || !event) return -1;
    
    pthread_mutex_lock(&bus->stats_lock);
    bus->events_published++;
    pthread_mutex_unlock(&bus->stats_lock);
    
    // Non-blocking push (drop if queue full)
    if (event_queue_push(&bus->queue, event, 0) != 0) {
        pthread_mutex_lock(&bus->stats_lock);
        bus->events_dropped++;
        pthread_mutex_unlock(&bus->stats_lock);
        
        LOG_WARN("Event dropped (queue full): type=%s", 
                event_type_to_string(event->type));
        return -1;
    }
    
    return 0;
}

int event_bus_publish_sync(event_bus_t *bus, const event_t *event) {
    if (!bus || !event) return -1;
    
    pthread_mutex_lock(&bus->stats_lock);
    bus->events_published++;
    pthread_mutex_unlock(&bus->stats_lock);
    
    // Dispatch immediately (bypass queue)
    dispatch_event_to_subscribers(bus, event);
    
    return 0;
}

// ============================================================================
// STATISTICS
// ============================================================================

void event_bus_get_stats(event_bus_t *bus, event_bus_stats_t *stats) {
    if (!bus || !stats) return;
    
    pthread_mutex_lock(&bus->stats_lock);
    stats->events_published = bus->events_published;
    stats->events_dispatched = bus->events_dispatched;
    stats->events_dropped = bus->events_dropped;
    pthread_mutex_unlock(&bus->stats_lock);
    
    pthread_mutex_lock(&bus->queue.lock);
    stats->queue_size = bus->queue.count;
    pthread_mutex_unlock(&bus->queue.lock);
    
    stats->subscribers_total = 0;
    for (int i = 0; i < EVENT_TYPE_MAX; i++) {
        pthread_rwlock_rdlock(&bus->subscriber_locks[i]);
        for (int j = 0; j < MAX_SUBSCRIBERS_PER_TYPE; j++) {
            if (bus->subscribers[i][j].active) {
                stats->subscribers_total++;
            }
        }
        pthread_rwlock_unlock(&bus->subscriber_locks[i]);
    }
}

// ============================================================================
// HELPERS
// ============================================================================

const char* event_type_to_string(event_type_t type) {
    switch (type) {
        case EVENT_TYPE_PEER_JOINED: return "PEER_JOINED";
        case EVENT_TYPE_PEER_LEFT: return "PEER_LEFT";
        case EVENT_TYPE_PEER_FAILED: return "PEER_FAILED";
        case EVENT_TYPE_PEER_UPDATED: return "PEER_UPDATED";
        case EVENT_TYPE_PEER_SUSPECT: return "PEER_SUSPECT";
        case EVENT_TYPE_EXECUTION_STARTED: return "EXECUTION_STARTED";
        case EVENT_TYPE_EXECUTION_COMPLETED: return "EXECUTION_COMPLETED";
        case EVENT_TYPE_EXECUTION_FAILED: return "EXECUTION_FAILED";
        case EVENT_TYPE_MESSAGE_RECEIVED: return "MESSAGE_RECEIVED";
        case EVENT_TYPE_MESSAGE_ROUTED: return "MESSAGE_ROUTED";
        case EVENT_TYPE_CATALOG_UPDATED: return "CATALOG_UPDATED";
        default: return "UNKNOWN";
    }
}