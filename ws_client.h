/*
 * WebSocket Client for Worker Communication
 *
 * Provides WebSocket connectivity to Airag server for:
 * - Receiving job submissions
 * - Sending processing results
 * - Heartbeat/health monitoring
 * - Instant connection state detection
 *
 * Uses wslay library for WebSocket framing.
 */

#ifndef WS_CLIENT_H
#define WS_CLIENT_H

#include <stdint.h>
#include <stdbool.h>
#include <json-c/json.h>

// WebSocket client states
typedef enum {
    WS_STATE_DISCONNECTED = 0,
    WS_STATE_CONNECTING   = 1,
    WS_STATE_CONNECTED    = 2,
    WS_STATE_CLOSING      = 3,
    WS_STATE_ERROR        = 4
} ws_client_state_t;

// WebSocket message types (JSON protocol)
#define WS_MSG_TYPE_JOB_SUBMIT      "job_submit"
#define WS_MSG_TYPE_JOB_RESULT      "job_result"
#define WS_MSG_TYPE_JOB_PROGRESS    "job_progress"
#define WS_MSG_TYPE_HEARTBEAT       "heartbeat"
#define WS_MSG_TYPE_ACK             "ack"

// Message callback function type
// Called when a message is received from Airag
typedef void (*ws_message_callback_t)(const char *msg_type, json_object *msg, void *user_data);

// Connection state callback function type
// Called when connection state changes
typedef void (*ws_state_callback_t)(ws_client_state_t old_state, ws_client_state_t new_state, void *user_data);

// WebSocket client configuration
typedef struct {
    const char *airag_host;          // Airag hostname (e.g., "airag" or "localhost")
    int         airag_port;          // Airag port (e.g., 1807)
    const char *ws_path;             // WebSocket path (e.g., "/ws/worker")
    int         heartbeat_interval;  // Heartbeat interval in seconds (default: 10)
    int         reconnect_interval;  // Reconnect interval in seconds (default: 5)
    int         connect_timeout;     // Connect timeout in seconds (default: 10)
    ws_message_callback_t on_message;
    ws_state_callback_t   on_state_change;
    void       *user_data;           // Passed to callbacks
} ws_client_config_t;

// WebSocket client opaque handle
typedef struct ws_client ws_client_t;

/**
 * Create and configure WebSocket client
 *
 * @param config Client configuration
 * @return Client handle or NULL on error
 */
ws_client_t* ws_client_create(const ws_client_config_t *config);

/**
 * Connect to Airag WebSocket endpoint
 * This is non-blocking and returns immediately.
 * Use callbacks to monitor connection state.
 *
 * @param client Client handle
 * @return 0 on success, -1 on error
 */
int ws_client_connect(ws_client_t *client);

/**
 * Send JSON message to Airag
 *
 * @param client Client handle
 * @param msg_type Message type (e.g., "job_result")
 * @param payload JSON object (will be copied, caller retains ownership)
 * @return 0 on success, -1 on error
 */
int ws_client_send_message(ws_client_t *client, const char *msg_type, json_object *payload);

/**
 * Send heartbeat message
 * This is automatically called based on heartbeat_interval,
 * but can be called manually if needed.
 *
 * @param client Client handle
 * @return 0 on success, -1 on error
 */
int ws_client_send_heartbeat(ws_client_t *client);

/**
 * Get current connection state
 *
 * @param client Client handle
 * @return Current state
 */
ws_client_state_t ws_client_get_state(ws_client_t *client);

/**
 * Check if client is connected
 *
 * @param client Client handle
 * @return true if connected, false otherwise
 */
bool ws_client_is_connected(ws_client_t *client);

/**
 * Process WebSocket events (call in main loop)
 * This handles:
 * - Receiving messages
 * - Sending queued messages
 * - Heartbeat timing
 * - Reconnection logic
 *
 * @param client Client handle
 * @param timeout_ms Maximum time to wait for events (milliseconds)
 * @return 0 on success, -1 on error
 */
int ws_client_process(ws_client_t *client, int timeout_ms);

/**
 * Gracefully close WebSocket connection
 *
 * @param client Client handle
 */
void ws_client_close(ws_client_t *client);

/**
 * Destroy WebSocket client and free resources
 *
 * @param client Client handle
 */
void ws_client_destroy(ws_client_t *client);

/**
 * Get last error message (thread-local)
 *
 * @return Error string (valid until next ws_client_* call)
 */
const char* ws_client_get_error(void);

#endif // WS_CLIENT_H
