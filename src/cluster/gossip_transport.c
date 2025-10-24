// src/cluster/gossip_transport.c - UDP transport for gossip

#define _POSIX_C_SOURCE 200809L

#include "roole/gossip.h"
#include "roole/common.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <errno.h>

// ============================================================================
// UDP SOCKET MANAGEMENT
// ============================================================================

/**
 * Create and bind UDP socket for gossip
 */
int gossip_create_udp_socket(const char *bind_addr, uint16_t port) {
    int sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_fd < 0) {
        LOG_ERROR("Failed to create UDP socket: %s", strerror(errno));
        return -1;
    }
    
    // Set socket to non-blocking
    int flags = fcntl(sock_fd, F_GETFL, 0);
    if (flags == -1 || fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        LOG_ERROR("Failed to set socket non-blocking: %s", strerror(errno));
        close(sock_fd);
        return -1;
    }
    
    // Set SO_REUSEADDR
    int reuse = 1;
    if (setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        LOG_WARN("Failed to set SO_REUSEADDR: %s", strerror(errno));
    }
    
    // Bind to address
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    if (bind_addr && strcmp(bind_addr, "0.0.0.0") != 0) {
        if (inet_pton(AF_INET, bind_addr, &addr.sin_addr) != 1) {
            LOG_ERROR("Invalid bind address: %s", bind_addr);
            close(sock_fd);
            return -1;
        }
    } else {
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
    }
    
    if (bind(sock_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG_ERROR("Failed to bind UDP socket to port %u: %s", port, strerror(errno));
        close(sock_fd);
        return -1;
    }
    
    LOG_INFO("Gossip UDP socket bound to %s:%u (fd=%d)", 
             bind_addr ? bind_addr : "0.0.0.0", port, sock_fd);
    
    return sock_fd;
}

/**
 * Send UDP datagram
 */
ssize_t gossip_send_udp(int sock_fd, const uint8_t *data, size_t len,
                        const char *dest_ip, uint16_t dest_port) {
    struct sockaddr_in dest_addr;
    memset(&dest_addr, 0, sizeof(dest_addr));
    dest_addr.sin_family = AF_INET;
    dest_addr.sin_port = htons(dest_port);
    
    if (inet_pton(AF_INET, dest_ip, &dest_addr.sin_addr) != 1) {
        LOG_ERROR("Invalid destination IP: %s", dest_ip);
        return -1;
    }
    
    ssize_t sent = sendto(sock_fd, data, len, 0,
                         (struct sockaddr*)&dest_addr, sizeof(dest_addr));
    
    if (sent < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            LOG_ERROR("Failed to send UDP packet: %s", strerror(errno));
        }
        return -1;
    }
    
    LOG_DEBUG("Sent %zd bytes to %s:%u", sent, dest_ip, dest_port);
    return sent;
}

/**
 * Receive UDP datagram (non-blocking)
 */
ssize_t gossip_recv_udp(int sock_fd, uint8_t *buffer, size_t buffer_size,
                        char *src_ip, size_t src_ip_len, uint16_t *src_port) {
    struct sockaddr_in src_addr;
    socklen_t addr_len = sizeof(src_addr);
    
    ssize_t received = recvfrom(sock_fd, buffer, buffer_size, 0,
                               (struct sockaddr*)&src_addr, &addr_len);
    
    if (received < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            LOG_ERROR("Failed to receive UDP packet: %s", strerror(errno));
        }
        return -1;
    }
    
    // Extract source IP and port
    if (src_ip && src_ip_len > 0) {
        inet_ntop(AF_INET, &src_addr.sin_addr, src_ip, src_ip_len);
    }
    if (src_port) {
        *src_port = ntohs(src_addr.sin_port);
    }
    
    LOG_DEBUG("Received %zd bytes from %s:%u", 
              received, src_ip ? src_ip : "unknown", src_port ? *src_port : 0);
    
    return received;
}