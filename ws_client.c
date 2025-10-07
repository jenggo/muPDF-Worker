/*
 * WebSocket Client Implementation
 *
 * Thread-safe WebSocket client using wslay for framing and
 * standard POSIX sockets for transport.
 */

#define _GNU_SOURCE
#include "ws_client.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <fcntl.h>
#include <poll.h>
#include <time.h>
#include <pthread.h>

#include <wslay/wslay.h>

// Forward declarations for wslay callbacks
static ssize_t ws_recv_callback(wslay_event_context_ptr ctx, uint8_t *buf, size_t len, int flags, void *user_data);
static ssize_t ws_send_callback(wslay_event_context_ptr ctx, const uint8_t *data, size_t len, int flags, void *user_data);
static int ws_genmask_callback(wslay_event_context_ptr ctx, uint8_t *buf, size_t len, void *user_data);
static void ws_on_msg_recv_callback(wslay_event_context_ptr ctx, const struct wslay_event_on_msg_recv_arg *arg, void *user_data);

// Error message buffer (thread-local)
static __thread char error_buf[512] = {0};

// WebSocket client internal structure
struct ws_client {
    // Configuration
    ws_client_config_t config;

    // Connection state
    ws_client_state_t state;
    pthread_mutex_t state_mutex;
    int sockfd;

    // wslay context
    wslay_event_context_ptr wslay_ctx;

    // Timing
    time_t last_heartbeat;
    time_t last_reconnect_attempt;

    // Shutdown flag
    volatile bool shutdown_requested;
};

// Forward declarations
static int ws_connect_socket(const char *host, int port, int timeout_sec);
static int ws_perform_handshake(int sockfd, const char *host, int port, const char *path);
static int ws_set_nonblocking(int sockfd);
static void ws_set_error(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static ssize_t ws_recv_callback(wslay_event_context_ptr ctx, uint8_t *buf, size_t len,
                                 int flags, void *user_data);
static ssize_t ws_send_callback(wslay_event_context_ptr ctx, const uint8_t *data, size_t len,
                                 int flags, void *user_data);
static void ws_on_msg_recv_callback(wslay_event_context_ptr ctx,
                                     const struct wslay_event_on_msg_recv_arg *arg,
                                     void *user_data);

/**
 * Create WebSocket client
 */
ws_client_t* ws_client_create(const ws_client_config_t *config) {
    if (!config || !config->airag_host || !config->ws_path) {
        ws_set_error("Invalid configuration");
        return NULL;
    }

    ws_client_t *client = calloc(1, sizeof(ws_client_t));
    if (!client) {
        ws_set_error("Memory allocation failed");
        return NULL;
    }

    // Copy configuration
    client->config = *config;
    client->config.airag_host = strdup(config->airag_host);
    client->config.ws_path = strdup(config->ws_path);

    // Set defaults
    if (client->config.heartbeat_interval <= 0) {
        client->config.heartbeat_interval = 10;
    }
    if (client->config.reconnect_interval <= 0) {
        client->config.reconnect_interval = 5;
    }
    if (client->config.connect_timeout <= 0) {
        client->config.connect_timeout = 10;
    }

    // Initialize state
    client->state = WS_STATE_DISCONNECTED;
    client->sockfd = -1;
    pthread_mutex_init(&client->state_mutex, NULL);

    return client;
}

/**
 * Update client state and notify callback
 */
static void ws_update_state(ws_client_t *client, ws_client_state_t new_state) {
    pthread_mutex_lock(&client->state_mutex);
    ws_client_state_t old_state = client->state;
    client->state = new_state;
    pthread_mutex_unlock(&client->state_mutex);

    if (client->config.on_state_change && old_state != new_state) {
        client->config.on_state_change(old_state, new_state, client->config.user_data);
    }
}

/**
 * Connect to Airag WebSocket endpoint
 */
int ws_client_connect(ws_client_t *client) {
    if (!client) {
        ws_set_error("Invalid client handle");
        return -1;
    }

    if (client->state == WS_STATE_CONNECTED || client->state == WS_STATE_CONNECTING) {
        return 0; // Already connected/connecting
    }

    ws_update_state(client, WS_STATE_CONNECTING);

    // Create TCP connection
    client->sockfd = ws_connect_socket(client->config.airag_host,
                                       client->config.airag_port,
                                       client->config.connect_timeout);
    if (client->sockfd < 0) {
        ws_update_state(client, WS_STATE_ERROR);
        return -1;
    }

    // Perform WebSocket handshake
    if (ws_perform_handshake(client->sockfd, client->config.airag_host,
                             client->config.airag_port, client->config.ws_path) < 0) {
        close(client->sockfd);
        client->sockfd = -1;
        ws_update_state(client, WS_STATE_ERROR);
        return -1;
    }

    // Initialize wslay context (before setting non-blocking)
    struct wslay_event_callbacks callbacks = {
        .recv_callback = ws_recv_callback,
        .send_callback = ws_send_callback,
        .genmask_callback = ws_genmask_callback,  // REQUIRED for client masking
        .on_msg_recv_callback = ws_on_msg_recv_callback
    };

    if (wslay_event_context_client_init(&client->wslay_ctx, &callbacks, client) != 0) {
        ws_set_error("Failed to initialize wslay context");
        close(client->sockfd);
        client->sockfd = -1;
        ws_update_state(client, WS_STATE_ERROR);
        return -1;
    }

    // Set socket to non-blocking for event loop
    ws_set_nonblocking(client->sockfd);

    // Connection successful
    ws_update_state(client, WS_STATE_CONNECTED);

    // Queue immediate heartbeat (will be sent in event loop)
    ws_client_send_heartbeat(client);

    return 0;
}

/**
 * Send JSON message
 */
int ws_client_send_message(ws_client_t *client, const char *msg_type, json_object *payload) {
    if (!client || !msg_type) {
        ws_set_error("Invalid parameters");
        return -1;
    }

    if (client->state != WS_STATE_CONNECTED) {
        ws_set_error("Not connected");
        return -1;
    }

    // Build message JSON with data field (matches Go server WorkerMessage struct)
    json_object *msg = json_object_new_object();
    json_object_object_add(msg, "type", json_object_new_string(msg_type));

    if (payload) {
        // Nest payload under "data" field to match server expectations
        json_object_object_add(msg, "data", json_object_get(payload));
    }

    const char *json_str = json_object_to_json_string(msg);
    size_t json_len = strlen(json_str);

    // Copy string to ensure it persists after JSON object is freed
    uint8_t *msg_copy = malloc(json_len);
    if (!msg_copy) {
        json_object_put(msg);
        ws_set_error("Failed to allocate message buffer");
        return -1;
    }
    memcpy(msg_copy, json_str, json_len);
    json_object_put(msg);

    // Queue text frame
    struct wslay_event_msg msg_data = {
        .opcode = WSLAY_TEXT_FRAME,
        .msg = msg_copy,
        .msg_length = json_len
    };

    int ret = wslay_event_queue_msg(client->wslay_ctx, &msg_data);
    if (ret != 0) {
        free(msg_copy);
        ws_set_error("Failed to queue message");
        return -1;
    }

    // TODO: msg_copy needs to be freed after wslay sends it - potential memory leak
    return 0;
}

/**
 * Send heartbeat
 */
int ws_client_send_heartbeat(ws_client_t *client) {
    if (!client || client->state != WS_STATE_CONNECTED) {
        return -1;
    }

    json_object *hb_data = json_object_new_object();
    json_object_object_add(hb_data, "timestamp", json_object_new_int64(time(NULL)));

    int ret = ws_client_send_message(client, WS_MSG_TYPE_HEARTBEAT, hb_data);
    json_object_put(hb_data);

    if (ret == 0) {
        client->last_heartbeat = time(NULL);
    }

    return ret;
}

/**
 * Get current state
 */
ws_client_state_t ws_client_get_state(ws_client_t *client) {
    if (!client) {
        return WS_STATE_ERROR;
    }

    pthread_mutex_lock(&client->state_mutex);
    ws_client_state_t state = client->state;
    pthread_mutex_unlock(&client->state_mutex);

    return state;
}

/**
 * Check if connected
 */
bool ws_client_is_connected(ws_client_t *client) {
    return client && ws_client_get_state(client) == WS_STATE_CONNECTED;
}

/**
 * Process WebSocket events
 */
int ws_client_process(ws_client_t *client, int timeout_ms) {
    if (!client) {
        return -1;
    }

    // Handle reconnection if disconnected
    if (client->state == WS_STATE_DISCONNECTED || client->state == WS_STATE_ERROR) {
        time_t now = time(NULL);
        if (now - client->last_reconnect_attempt >= client->config.reconnect_interval) {
            client->last_reconnect_attempt = now;
            ws_client_connect(client);
        }
        // Sleep to avoid busy-waiting when disconnected (use timeout_ms to match main loop timing)
        if (timeout_ms > 0) {
            usleep(timeout_ms * 1000); // Convert ms to microseconds
        }
        return 0;
    }

    if (client->state != WS_STATE_CONNECTED) {
        // Sleep for other non-connected states to avoid busy-waiting
        if (timeout_ms > 0) {
            usleep(timeout_ms * 1000);
        }
        return 0;
    }

    // Send heartbeat if needed
    time_t now = time(NULL);
    if (now - client->last_heartbeat >= client->config.heartbeat_interval) {
        ws_client_send_heartbeat(client);
    }

    // Poll socket for events
    struct pollfd pfd = {
        .fd = client->sockfd,
        .events = POLLIN | (wslay_event_want_write(client->wslay_ctx) ? POLLOUT : 0)
    };

    int poll_ret = poll(&pfd, 1, timeout_ms);
    if (poll_ret < 0) {
        if (errno == EINTR) {
            return 0;
        }
        ws_set_error("Poll failed: %s", strerror(errno));
        ws_update_state(client, WS_STATE_ERROR);
        return -1;
    }

    if (poll_ret == 0) {
        return 0; // Timeout
    }

    // Handle events
    if (pfd.revents & POLLIN) {
        if (wslay_event_recv(client->wslay_ctx) != 0) {
            ws_set_error("Receive failed");
            ws_update_state(client, WS_STATE_ERROR);
            return -1;
        }
    }

    if (pfd.revents & POLLOUT) {
        if (wslay_event_send(client->wslay_ctx) != 0) {
            ws_set_error("Send failed");
            ws_update_state(client, WS_STATE_ERROR);
            return -1;
        }
    }

    if (pfd.revents & (POLLERR | POLLHUP)) {
        ws_set_error("Socket error or hangup");
        ws_update_state(client, WS_STATE_ERROR);
        return -1;
    }

    return 0;
}

/**
 * Close connection
 */
void ws_client_close(ws_client_t *client) {
    if (!client) {
        return;
    }

    ws_update_state(client, WS_STATE_CLOSING);

    if (client->wslay_ctx) {
        wslay_event_queue_close(client->wslay_ctx, WSLAY_CODE_NORMAL_CLOSURE, NULL, 0);
        wslay_event_send(client->wslay_ctx);
    }

    if (client->sockfd >= 0) {
        close(client->sockfd);
        client->sockfd = -1;
    }

    ws_update_state(client, WS_STATE_DISCONNECTED);
}

/**
 * Destroy client
 */
void ws_client_destroy(ws_client_t *client) {
    if (!client) {
        return;
    }

    ws_client_close(client);

    if (client->wslay_ctx) {
        wslay_event_context_free(client->wslay_ctx);
    }

    pthread_mutex_destroy(&client->state_mutex);

    free((void*)client->config.airag_host);
    free((void*)client->config.ws_path);
    free(client);
}

/**
 * Get last error
 */
const char* ws_client_get_error(void) {
    return error_buf;
}

// ============================================================================
// Internal helper functions
// ============================================================================

static void ws_set_error(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(error_buf, sizeof(error_buf), fmt, ap);
    va_end(ap);
}

static int ws_connect_socket(const char *host, int port, int timeout_sec) {
    struct addrinfo hints = {0}, *result = NULL, *rp = NULL;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", port);

    if (getaddrinfo(host, port_str, &hints, &result) != 0) {
        ws_set_error("DNS resolution failed for %s", host);
        return -1;
    }

    int sockfd = -1;
    for (rp = result; rp != NULL; rp = rp->ai_next) {
        sockfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sockfd < 0) {
            continue;
        }

        // Set socket options
        int flag = 1;
        setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        // Connect with timeout
        fcntl(sockfd, F_SETFL, O_NONBLOCK);
        int conn_ret = connect(sockfd, rp->ai_addr, rp->ai_addrlen);

        if (conn_ret < 0 && errno == EINPROGRESS) {
            struct pollfd pfd = { .fd = sockfd, .events = POLLOUT };
            if (poll(&pfd, 1, timeout_sec * 1000) > 0) {
                int err = 0;
                socklen_t len = sizeof(err);
                if (getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &err, &len) == 0 && err == 0) {
                    break; // Connected
                }
            }
        } else if (conn_ret == 0) {
            break; // Connected immediately
        }

        close(sockfd);
        sockfd = -1;
    }

    freeaddrinfo(result);

    if (sockfd < 0) {
        ws_set_error("Failed to connect to %s:%d", host, port);
        return -1;
    }

    // Set socket back to blocking mode for handshake
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(sockfd, F_SETFL, flags & ~O_NONBLOCK);
    }

    return sockfd;
}

static int ws_perform_handshake(int sockfd, const char *host, int port, const char *path) {
    // Generate Sec-WebSocket-Key (base64 of 16 random bytes)
    unsigned char key_bytes[16];
    FILE *urandom = fopen("/dev/urandom", "r");
    if (urandom) {
        (void)fread(key_bytes, 1, sizeof(key_bytes), urandom);
        fclose(urandom);
    } else {
        // Fallback to time-based random
        srand((unsigned)time(NULL));
        for (size_t i = 0; i < sizeof(key_bytes); i++) {
            key_bytes[i] = (unsigned char)rand();
        }
    }

    // Base64 encode (16 bytes -> 24 chars including padding)
    static const char b64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    char key[25] = {0}; // 24 chars + null terminator

    // Process complete triplets (15 bytes = 5 triplets)
    int i, j;
    for (i = 0, j = 0; i < 15; i += 3, j += 4) {
        uint32_t n = ((uint32_t)key_bytes[i] << 16) |
                     ((uint32_t)key_bytes[i+1] << 8) |
                     (uint32_t)key_bytes[i+2];
        key[j]   = b64[(n >> 18) & 0x3F];
        key[j+1] = b64[(n >> 12) & 0x3F];
        key[j+2] = b64[(n >> 6) & 0x3F];
        key[j+3] = b64[n & 0x3F];
    }

    // Handle remaining byte (16 % 3 = 1) with padding
    uint32_t n = (uint32_t)key_bytes[15] << 16;
    key[j]   = b64[(n >> 18) & 0x3F];
    key[j+1] = b64[(n >> 12) & 0x3F];
    key[j+2] = '=';
    key[j+3] = '=';

    // Build handshake request
    char request[1024];
    int len = snprintf(request, sizeof(request),
        "GET %s HTTP/1.1\r\n"
        "Host: %s:%d\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: %s\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n",
        path, host, port, key);

    // Send handshake
    if (send(sockfd, request, len, 0) != len) {
        ws_set_error("Failed to send handshake");
        return -1;
    }

    // Receive handshake response
    char response[2048] = {0};
    int received = 0;
    while (received < (int)sizeof(response) - 1) {
        int n = recv(sockfd, response + received, sizeof(response) - received - 1, 0);
        if (n <= 0) {
            ws_set_error("Failed to receive handshake response");
            return -1;
        }
        received += n;

        // Check if we received complete response (ends with \r\n\r\n)
        if (strstr(response, "\r\n\r\n")) {
            break;
        }
    }

    // Verify response
    if (strstr(response, "HTTP/1.1 101") == NULL) {
        ws_set_error("Handshake failed: %s", response);
        return -1;
    }

    return 0;
}

static int ws_set_nonblocking(int sockfd) {
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags < 0) {
        return -1;
    }
    return fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);
}

// wslay callbacks

static ssize_t ws_recv_callback(wslay_event_context_ptr ctx, uint8_t *buf, size_t len,
                                 int flags, void *user_data) {
    (void)ctx;
    (void)flags;
    ws_client_t *client = (ws_client_t *)user_data;

    ssize_t r = recv(client->sockfd, buf, len, 0);
    if (r < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            wslay_event_set_error(ctx, WSLAY_ERR_WOULDBLOCK);
        } else {
            wslay_event_set_error(ctx, WSLAY_ERR_CALLBACK_FAILURE);
        }
    }
    return r;
}

static ssize_t ws_send_callback(wslay_event_context_ptr ctx, const uint8_t *data, size_t len,
                                 int flags, void *user_data) {
    (void)ctx;
    (void)flags;
    ws_client_t *client = (ws_client_t *)user_data;

    ssize_t r = send(client->sockfd, data, len, 0);
    if (r < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            wslay_event_set_error(ctx, WSLAY_ERR_WOULDBLOCK);
        } else {
            wslay_event_set_error(ctx, WSLAY_ERR_CALLBACK_FAILURE);
        }
    }
    return r;
}

static int ws_genmask_callback(wslay_event_context_ptr ctx, uint8_t *buf, size_t len,
                                void *user_data) {
    (void)ctx;
    (void)user_data;

    // Generate random masking key (required for client-to-server frames per RFC 6455)
    if (len != 4) {
        return -1; // Masking key must be exactly 4 bytes
    }

    // Use /dev/urandom for cryptographically secure random bytes
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd < 0) {
        return -1;
    }

    ssize_t r = read(fd, buf, 4);
    close(fd);

    return (r == 4) ? 0 : -1;
}

static void ws_on_msg_recv_callback(wslay_event_context_ptr ctx,
                                     const struct wslay_event_on_msg_recv_arg *arg,
                                     void *user_data) {
    (void)ctx;
    ws_client_t *client = (ws_client_t *)user_data;

    if (!wslay_is_ctrl_frame(arg->opcode)) {
        // Parse JSON message
        json_object *msg = json_tokener_parse((const char *)arg->msg);
        if (msg) {
            json_object *type_obj = json_object_object_get(msg, "type");
            const char *msg_type = type_obj ? json_object_get_string(type_obj) : NULL;

            if (msg_type && client->config.on_message) {
                client->config.on_message(msg_type, msg, client->config.user_data);
            }

            json_object_put(msg);
        }
    }
}
