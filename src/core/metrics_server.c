// src/core/metrics_server.c
// Simple HTTP server for Prometheus /metrics endpoint
// Complete rewrite with improved error handling and robustness

#define _POSIX_C_SOURCE 200809L

#include "roole/metrics_server.h"
#include "roole/metrics.h"
#include "roole/common.h"
#include "roole/logger.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/time.h>

// ============================================================================
// CONSTANTS
// ============================================================================

#define HTTP_BUFFER_SIZE 8192
#define MAX_REQUEST_SIZE 4096
#define SERVER_BACKLOG 10
#define ACCEPT_TIMEOUT_SEC 1

// ============================================================================
// SERVER STRUCTURE
// ============================================================================

struct metrics_server {
    metrics_registry_t *registry;
    int server_fd;
    uint16_t port;
    char bind_addr[16];
    pthread_t server_thread;
    volatile int shutdown_flag;
};

// ============================================================================
// HTTP RESPONSE HELPERS
// ============================================================================

static void send_http_response(int client_fd, const char *status_line,
                               const char *content_type, const char *body) {
    char header[1024];
    size_t body_len = body ? strlen(body) : 0;
    
    int header_len = snprintf(header, sizeof(header),
        "HTTP/1.1 %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "Server: roole-metrics/1.0\r\n"
        "\r\n",
        status_line, content_type, body_len);
    
    if (header_len < 0 || (size_t)header_len >= sizeof(header)) {
        LOG_ERROR("Failed to format HTTP header");
        return;
    }
    
    // Send header
    ssize_t sent = send(client_fd, header, header_len, MSG_NOSIGNAL);
    if (sent < 0) {
        LOG_DEBUG("Failed to send HTTP header: %s", strerror(errno));
        return;
    }
    
    // Send body if present
    if (body && body_len > 0) {
        sent = send(client_fd, body, body_len, MSG_NOSIGNAL);
        if (sent < 0) {
            LOG_DEBUG("Failed to send HTTP body: %s", strerror(errno));
        }
    }
}

static void send_400_bad_request(int client_fd) {
    const char *body = "400 Bad Request\n";
    send_http_response(client_fd, "400 Bad Request", "text/plain", body);
}

static void send_404_not_found(int client_fd) {
    const char *body = "404 Not Found\n";
    send_http_response(client_fd, "404 Not Found", "text/plain", body);
}

static void send_500_internal_error(int client_fd) {
    const char *body = "500 Internal Server Error\n";
    send_http_response(client_fd, "500 Internal Server Error", "text/plain", body);
}

// ============================================================================
// HTTP REQUEST PARSING
// ============================================================================

static int parse_http_request(const char *request, char *method, size_t method_size,
                              char *path, size_t path_size) {
    if (!request || !method || !path) {
        return -1;
    }
    
    // Parse request line: METHOD PATH HTTP/VERSION
    int parsed = sscanf(request, "%63s %255s", method, path);
    if (parsed != 2) {
        LOG_DEBUG("Failed to parse HTTP request line");
        return -1;
    }
    
    // Ensure null termination
    method[method_size - 1] = '\0';
    path[path_size - 1] = '\0';
    
    LOG_DEBUG("Parsed HTTP request: %s %s", method, path);
    return 0;
}

// ============================================================================
// HTTP REQUEST HANDLING
// ============================================================================

static void handle_metrics_request(int client_fd, metrics_registry_t *registry) {
    LOG_DEBUG("Handling /metrics request");
    
    // Render metrics in Prometheus format
    char *metrics_text = metrics_registry_render_prometheus(registry);
    
    if (!metrics_text) {
        LOG_ERROR("Failed to render metrics");
        send_500_internal_error(client_fd);
        return;
    }
    
    // Send successful response with metrics
    send_http_response(client_fd, 
                      "200 OK", 
                      "text/plain; version=0.0.4; charset=utf-8",
                      metrics_text);
    
    free(metrics_text);
    LOG_DEBUG("Metrics response sent successfully");
}

static void handle_http_request(int client_fd, metrics_registry_t *registry) {
    char request[MAX_REQUEST_SIZE];
    
    // Receive HTTP request
    ssize_t bytes_read = recv(client_fd, request, sizeof(request) - 1, 0);
    
    if (bytes_read < 0) {
        LOG_DEBUG("Failed to receive HTTP request: %s", strerror(errno));
        return;
    }
    
    if (bytes_read == 0) {
        LOG_DEBUG("Client closed connection before sending request");
        return;
    }
    
    // Null-terminate request
    request[bytes_read] = '\0';
    
    // Parse request
    char method[64];
    char path[256];
    
    if (parse_http_request(request, method, sizeof(method), 
                          path, sizeof(path)) != 0) {
        LOG_DEBUG("Invalid HTTP request");
        send_400_bad_request(client_fd);
        return;
    }
    
    // Route request
    if (strcmp(method, "GET") == 0 && strcmp(path, "/metrics") == 0) {
        handle_metrics_request(client_fd, registry);
    } else if (strcmp(method, "GET") == 0 && strcmp(path, "/") == 0) {
        // Root path - return simple info page
        const char *body = 
            "Roole Metrics Server\n"
            "\n"
            "Available endpoints:\n"
            "  GET /metrics - Prometheus metrics\n";
        send_http_response(client_fd, "200 OK", "text/plain", body);
    } else {
        LOG_DEBUG("Request not found: %s %s", method, path);
        send_404_not_found(client_fd);
    }
}

// ============================================================================
// CLIENT CONNECTION HANDLING
// ============================================================================

static void handle_client_connection(int client_fd, metrics_registry_t *registry) {
    // Set socket timeout to prevent hanging
    struct timeval tv;
    tv.tv_sec = 5;   // 5 second timeout
    tv.tv_usec = 0;
    
    if (setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        LOG_WARN("Failed to set socket receive timeout: %s", strerror(errno));
    }
    
    if (setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
        LOG_WARN("Failed to set socket send timeout: %s", strerror(errno));
    }
    
    // Handle the HTTP request
    handle_http_request(client_fd, registry);
    
    // Close connection (HTTP/1.1 Connection: close)
    close(client_fd);
}

// ============================================================================
// SERVER SOCKET SETUP
// ============================================================================

static int setup_server_socket(const char *bind_addr, uint16_t port) {
    int server_fd;
    struct sockaddr_in addr;
    int opt = 1;
    
    // Create socket
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        LOG_ERROR("Failed to create server socket: %s", strerror(errno));
        return -1;
    }
    
    // Set SO_REUSEADDR to allow quick restart
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        LOG_WARN("Failed to set SO_REUSEADDR: %s", strerror(errno));
    }
    
    // Set SO_REUSEPORT if available (for better multi-threaded performance)
#ifdef SO_REUSEPORT
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
        LOG_DEBUG("Failed to set SO_REUSEPORT (not critical): %s", strerror(errno));
    }
#endif
    
    // Prepare address structure
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, bind_addr, &addr.sin_addr) <= 0) {
        LOG_ERROR("Invalid bind address: %s", bind_addr);
        close(server_fd);
        return -1;
    }
    
    // Bind socket
    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG_ERROR("Failed to bind server socket to %s:%u: %s",
                  bind_addr, port, strerror(errno));
        close(server_fd);
        return -1;
    }
    
    // Listen for connections
    if (listen(server_fd, SERVER_BACKLOG) < 0) {
        LOG_ERROR("Failed to listen on server socket: %s", strerror(errno));
        close(server_fd);
        return -1;
    }
    
    LOG_INFO("Metrics server socket bound to %s:%u", bind_addr, port);
    return server_fd;
}

// ============================================================================
// SERVER THREAD MAIN LOOP
// ============================================================================

static void* metrics_server_thread_fn(void *arg) {
    metrics_server_t *server = (metrics_server_t*)arg;
    
    logger_push_component("metrics:http");
    LOG_INFO("Metrics HTTP server thread started (bind=%s, port=%u)", 
             server->bind_addr, server->port);
    
    // Setup server socket
    server->server_fd = setup_server_socket(server->bind_addr, server->port);
    if (server->server_fd < 0) {
        LOG_ERROR("Failed to setup metrics server socket on %s:%u", 
                  server->bind_addr, server->port);
        return NULL;
    }
    
    LOG_INFO("Metrics server ready at http://%s:%u/metrics",
             server->bind_addr, server->port);
    
    // Set socket to non-blocking for graceful shutdown
    int flags = fcntl(server->server_fd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(server->server_fd, F_SETFL, flags | O_NONBLOCK);
    }
    
    // Main accept loop
    while (!server->shutdown_flag) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        // Accept with timeout using select
        fd_set read_fds;
        struct timeval tv;
        
        FD_ZERO(&read_fds);
        FD_SET(server->server_fd, &read_fds);
        
        tv.tv_sec = ACCEPT_TIMEOUT_SEC;
        tv.tv_usec = 0;
        
        int select_result = select(server->server_fd + 1, &read_fds, 
                                   NULL, NULL, &tv);
        
        if (select_result < 0) {
            if (errno == EINTR) {
                // Interrupted by signal, check shutdown flag
                continue;
            }
            LOG_ERROR("select() failed: %s", strerror(errno));
            break;
        }
        
        if (select_result == 0) {
            // Timeout - check shutdown flag and continue
            continue;
        }
        
        // Socket is ready for accept
        int client_fd = accept(server->server_fd, 
                              (struct sockaddr*)&client_addr, 
                              &client_len);
        
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Would block (shouldn't happen after select, but handle it)
                continue;
            }
            
            if (server->shutdown_flag) {
                // Shutting down
                break;
            }
            
            if (errno == EINTR) {
                // Interrupted by signal
                continue;
            }
            
            LOG_WARN("accept() failed: %s", strerror(errno));
            usleep(100000);  // Brief pause to avoid tight loop on persistent errors
            continue;
        }
        
        // Get client IP for logging
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
        LOG_DEBUG("Accepted connection from %s:%u", 
                  client_ip, ntohs(client_addr.sin_port));
        
        // Handle client request (synchronous - simple but sufficient for metrics)
        handle_client_connection(client_fd, server->registry);
    }
    
    // Cleanup
    if (server->server_fd >= 0) {
        close(server->server_fd);
        server->server_fd = -1;
    }
    
    LOG_INFO("Metrics HTTP server thread stopped");
    logger_pop_component();
    return NULL;
}

// ============================================================================
// PUBLIC API
// ============================================================================

metrics_server_t* metrics_server_start(metrics_registry_t *registry,
                                        const char *bind_addr,
                                        uint16_t port) {
    if (!registry || !bind_addr || port == 0) {
        LOG_ERROR("Invalid parameters for metrics_server_start");
        return NULL;
    }
    
    // Allocate server structure
    metrics_server_t *server = calloc(1, sizeof(metrics_server_t));
    if (!server) {
        LOG_ERROR("Failed to allocate metrics server structure");
        return NULL;
    }
    
    // Initialize server fields
    server->registry = registry;
    server->port = port;
    server->shutdown_flag = 0;
    server->server_fd = -1;
    
    if (safe_strncpy(server->bind_addr, bind_addr, sizeof(server->bind_addr)) != 0) {
        LOG_ERROR("Bind address too long: %s", bind_addr);
        free(server);
        return NULL;
    }
    
    // Start server thread
    if (pthread_create(&server->server_thread, NULL, 
                      metrics_server_thread_fn, server) != 0) {
        LOG_ERROR("Failed to create metrics server thread: %s", strerror(errno));
        free(server);
        return NULL;
    }
    
    // Detach thread (no need to join on shutdown)
    if (pthread_detach(server->server_thread) != 0) {
        LOG_WARN("Failed to detach metrics server thread: %s", strerror(errno));
    }
    
    LOG_INFO("Metrics server starting on http://%s:%u/metrics", bind_addr, port);
    
    // Give server thread time to initialize and bind
    usleep(500000);  // 500ms - increased to ensure binding completes
    
    return server;
}

void metrics_server_shutdown(metrics_server_t *server) {
    if (!server) return;
    
    LOG_INFO("Shutting down metrics server on port %u", server->port);
    
    // Signal shutdown
    server->shutdown_flag = 1;
    
    // Close server socket to unblock accept()
    if (server->server_fd >= 0) {
        shutdown(server->server_fd, SHUT_RDWR);
        close(server->server_fd);
        server->server_fd = -1;
    }
    
    // Give thread time to exit gracefully
    usleep(500000);  // 500ms
    
    // Free server structure
    free(server);
    
    LOG_INFO("Metrics server shutdown complete");
}