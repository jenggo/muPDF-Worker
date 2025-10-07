/*
 * Standalone MuPDF Document Parsing Service
 *
 * A lightweight WebSocket-based service that provides document parsing capabilities
 * using MuPDF. Connects to Airag server for bidirectional communication.
 *
 * Features:
 * - WebSocket connectivity to Airag server
 * - Real-time job progress reporting
 * - Instant health detection via connection state
 * - Text extraction with spatial coordinates
 * - Paragraph-level bounding boxes and embedded image extraction
 * - Redis queue integration for async job processing
 * - JSON message protocol
 */

// Enable GNU extensions for strerror_r and other functions
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200112L

// System includes (alphabetical)
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

// Third-party library includes (alphabetical)
#include <curl/curl.h>
#include <json-c/json.h>
#include <mupdf/fitz.h>

// Local includes (alphabetical)
#include "monitoring.h"
#include "redis_queue.h"
#include "ws_client.h"

#define MAX_WORKERS 8
#define BUFFER_SIZE 4096

// MuPDF thread-safety locks
// Increased from 4 to 6 to support ICC color profile handling in multi-threaded environment
// MuPDF requires at least FZ_LOCK_FREETYPE (6) locks for proper thread safety with color management
#define FZ_LOCK_MAX 6
static pthread_mutex_t mupdf_locks[FZ_LOCK_MAX];

static void lock_mutex(void *user, int lock) {
    (void)user;
    pthread_mutex_lock(&mupdf_locks[lock]);
}

static void unlock_mutex(void *user, int lock) {
    (void)user;
    pthread_mutex_unlock(&mupdf_locks[lock]);
}

static fz_locks_context mupdf_locks_ctx = {
    .user = NULL,
    .lock = lock_mutex,
    .unlock = unlock_mutex
};

// Configuration structure for thread-safe environment variable access
typedef struct {
    char uploads_path[1024];
    int vector_extraction_enabled;
    int max_concurrent_jobs;
} worker_config_t;

// Redis configuration structure is defined in redis_queue.h

// Global configuration loaded at startup
static worker_config_t worker_config = {0};
static redis_config_t redis_config_cached = {0};

// Semaphore for limiting concurrent job processing
static sem_t job_semaphore;

// WebSocket client for communication with Airag
static ws_client_t *ws_client = NULL;

// Helper function to send job progress via WebSocket
static void send_job_progress(const char *job_id, const char *status, int progress_percent, const char *current_step);

// Initialize worker configuration from environment variables (thread-safe)
static void init_worker_config(void) {
    // Initialize uploads path
    const char *uploads_path = getenv("UPLOADS_PATH");
    if (uploads_path) {
        strncpy(worker_config.uploads_path, uploads_path, sizeof(worker_config.uploads_path) - 1);
        worker_config.uploads_path[sizeof(worker_config.uploads_path) - 1] = '\0';
    } else {
        strncpy(worker_config.uploads_path, "/tmp/airag", sizeof(worker_config.uploads_path) - 1);
        worker_config.uploads_path[sizeof(worker_config.uploads_path) - 1] = '\0';
    }

    // Initialize vector extraction setting (enabled by default for enhanced object detection)
    const char *vector_enabled = getenv("RAG_VECTOR_EXTRACTION_ENABLED");
    worker_config.vector_extraction_enabled = (vector_enabled && strcmp(vector_enabled, "false") == 0) ? 0 : 1;

    // Initialize max concurrent jobs setting (default: 2 to prevent memory balloning)
    const char *max_concurrent = getenv("WORKER_MAX_CONCURRENT");
    if (max_concurrent) {
        char *endptr = NULL;
        long max_jobs = strtol(max_concurrent, &endptr, 10);
        if (*endptr == '\0' && max_jobs > 0 && max_jobs <= 32) {
            worker_config.max_concurrent_jobs = (int)max_jobs;
        } else {
            worker_config.max_concurrent_jobs = 2; // Default 2 concurrent jobs
        }
    } else {
        worker_config.max_concurrent_jobs = 2; // Default 2 concurrent jobs
    }

    // Log configuration for debugging
    LOG_INFO_MSG("INIT", "Worker configuration loaded:");
    LOG_INFO_MSG("INIT", "  uploads_path: %s", worker_config.uploads_path);
    LOG_INFO_MSG("INIT", "  vector_extraction_enabled: %s (enhanced object detection)", worker_config.vector_extraction_enabled ? "true" : "false");
    LOG_INFO_MSG("INIT", "  max_concurrent_jobs: %d (prevents memory balloning)", worker_config.max_concurrent_jobs);
}

// Thread-safe getter functions
static const char* get_uploads_path_safe(void) {
    return worker_config.uploads_path;
}

static int get_vector_extraction_enabled_safe(void) {
    return worker_config.vector_extraction_enabled;
}

static int get_max_concurrent_jobs_safe(void) {
    return worker_config.max_concurrent_jobs;
}

// Initialize Redis configuration from environment variables (thread-safe)
static void init_redis_config(void) {
    // Set defaults
    strncpy(redis_config_cached.host, "localhost", sizeof(redis_config_cached.host) - 1);
    redis_config_cached.host[sizeof(redis_config_cached.host) - 1] = '\0';
    redis_config_cached.port = 6379;
    redis_config_cached.password[0] = '\0';
    redis_config_cached.db = 4;
    redis_config_cached.timeout_ms = 5000;

    // Load from environment variables
    const char *redis_host = getenv("REDIS_HOST");
    if (redis_host) {
        strncpy(redis_config_cached.host, redis_host, sizeof(redis_config_cached.host) - 1);
        redis_config_cached.host[sizeof(redis_config_cached.host) - 1] = '\0';
    }

    const char *redis_port = getenv("REDIS_PORT");
    if (redis_port) {
        char *endptr = NULL;
        long port = strtol(redis_port, &endptr, 10);
        if (*endptr == '\0' && port > 0 && port <= 65535) {
            redis_config_cached.port = (int)port;
        }
    }

    const char *redis_password = getenv("REDIS_PASSWORD");
    if (redis_password) {
        strncpy(redis_config_cached.password, redis_password, sizeof(redis_config_cached.password) - 1);
        redis_config_cached.password[sizeof(redis_config_cached.password) - 1] = '\0';
    }

    const char *redis_db = getenv("REDIS_DB");
    if (redis_db) {
        char *endptr = NULL;
        long database = strtol(redis_db, &endptr, 10);
        if (*endptr == '\0' && database >= 0 && database <= 15) {
            redis_config_cached.db = (int)database;
        }
    }
}

// Job structure for async processing
typedef struct {
    char job_id[256];
    char document_id[256]; // Document UUID for ticketing system
    char file_path[1024];
    char image_directory_path[1024];
    int extract_vector_images; // Flag to enable/disable vector image extraction
    int no_filter; // Flag to bypass image filtering (disabled by default)
    pthread_t thread;
} parse_job_t;

// Get images directory path
void get_images_path(char *buffer, size_t buffer_size) {
    (void)snprintf(buffer, buffer_size, "%s/images", get_uploads_path_safe());
}

// Get results directory path - store results alongside images for organization isolation
void get_results_path(char *buffer, size_t buffer_size, const char *image_directory_path) {
    if (image_directory_path && strlen(image_directory_path) > 0) {
        // Extract parent directory from image_directory_path (remove /images suffix)
        char parent_dir[512];
        strncpy(parent_dir, image_directory_path, sizeof(parent_dir) - 1);
        parent_dir[sizeof(parent_dir) - 1] = '\0';
        
        // Remove trailing /images if present to get organization directory
        size_t len = strlen(parent_dir);
        if (len > 7 && strcmp(parent_dir + len - 7, "/images") == 0) {
            parent_dir[len - 7] = '\0';
        }
        
        (void)snprintf(buffer, buffer_size, "%s/results", parent_dir);
    } else {
        // Fallback to uploads/results if no image directory provided
        (void)snprintf(buffer, buffer_size, "%s/results", get_uploads_path_safe());
    }
}

// Get organization images directory path
void get_org_images_path(char *buffer, size_t buffer_size, const char *image_directory_path) {
    (void)snprintf(buffer, buffer_size, "%s", image_directory_path);
}

// Document parsing results
typedef struct {
    json_object *text_blocks;
    json_object *paragraphs;
    json_object *images;
    json_object *metadata;
} parse_result_t;

// Global MuPDF context (thread-safe with cloning)
static fz_context *base_ctx = NULL;

// Global Redis consumer
static redis_consumer_t *redis_consumer = NULL;
static volatile sig_atomic_t shutdown_requested = 0;

// Redis buffering context (separate from consumer for result buffering)
static redisContext *redis_buffer_ctx = NULL;
static pthread_mutex_t redis_buffer_mutex = PTHREAD_MUTEX_INITIALIZER;

// Function declarations
static int init_redis_buffer(void);
static void cleanup_redis_buffer(void);
static void* process_document_job(void *arg);
static parse_result_t* parse_document_with_mupdf_filtered(const char *file_path, const char *image_directory_path, int extract_vector_images, int no_filter);
static void send_result_via_websocket(const char *job_id, const char *document_id, parse_result_t *result, uint64_t processing_time_ms);
// Function declarations for external implementations
json_object* extract_text_blocks(fz_context *ctx, fz_document *doc);
json_object* extract_paragraphs(fz_context *ctx, fz_document *doc);
json_object* extract_images_filtered(fz_context *ctx, fz_document *doc, const char *image_directory_path, int extract_vector_images, int no_filter);
static void cleanup_parse_result(parse_result_t *result);

// Redis and signal handling functions
static void signal_handler(int sig);
static int redis_job_processor(const redis_job_t *job);
static int init_redis_consumer(void);
static void cleanup_redis_consumer(void);
static redis_config_t load_redis_config(void);

/*
 * WebSocket message handler - called when message received from Airag
 */
static void on_ws_message(const char *msg_type, json_object *msg, void *user_data) {
    (void)user_data;

    LOG_DEBUG_MSG("WS", "Received message: %s", msg_type);

    if (strcmp(msg_type, WS_MSG_TYPE_JOB_SUBMIT) == 0) {
        // Extract data object from message
        json_object *data_obj = json_object_object_get(msg, "data");
        if (!data_obj) {
            LOG_ERROR_MSG("WS", "Invalid job_submit message: missing data field");
            return;
        }

        // Extract job details from data object
        json_object *job_id_obj = json_object_object_get(data_obj, "job_id");
        json_object *file_path_obj = json_object_object_get(data_obj, "file_path");
        json_object *options_obj = json_object_object_get(data_obj, "options");

        if (!job_id_obj || !file_path_obj) {
            LOG_ERROR_MSG("WS", "Invalid job_submit message: missing required fields");
            return;
        }

        const char *job_id = json_object_get_string(job_id_obj);
        const char *file_path = json_object_get_string(file_path_obj);

        // Extract options
        int extract_vector_images = get_vector_extraction_enabled_safe();
        int no_filter = 0;
        const char *image_directory_path = NULL;
        const char *document_id = NULL;

        if (options_obj) {
            json_object *doc_id_obj = json_object_object_get(options_obj, "document_id");
            if (doc_id_obj) {
                document_id = json_object_get_string(doc_id_obj);
            }

            json_object *vector_obj = json_object_object_get(options_obj, "vector_extraction");
            if (vector_obj) {
                extract_vector_images = json_object_get_boolean(vector_obj);
            }

            json_object *filter_obj = json_object_object_get(options_obj, "no_image_filter");
            if (filter_obj) {
                no_filter = json_object_get_boolean(filter_obj);
            }

            json_object *img_dir_obj = json_object_object_get(options_obj, "image_directory_path");
            if (img_dir_obj) {
                image_directory_path = json_object_get_string(img_dir_obj);
            }
        }

        LOG_INFO_MSG("WS", "Job received via WebSocket: %s (file: %s)", job_id, file_path);

        // Send initial progress (REQUIRED for frontend tracking)
        send_job_progress(job_id, "started", 0, "job_received");

        // Create job argument structure
        parse_job_t *job = malloc(sizeof(parse_job_t));
        if (!job) {
            LOG_ERROR_MSG("WS", "Failed to allocate memory for job");
            send_job_progress(job_id, "failed", 0, "memory_allocation_failed");
            return;
        }

        strncpy(job->file_path, file_path, sizeof(job->file_path) - 1);
        job->file_path[sizeof(job->file_path) - 1] = '\0';
        strncpy(job->job_id, job_id, sizeof(job->job_id) - 1);
        job->job_id[sizeof(job->job_id) - 1] = '\0';

        if (document_id) {
            strncpy(job->document_id, document_id, sizeof(job->document_id) - 1);
            job->document_id[sizeof(job->document_id) - 1] = '\0';
        } else {
            job->document_id[0] = '\0';
        }

        if (image_directory_path) {
            strncpy(job->image_directory_path, image_directory_path,
                    sizeof(job->image_directory_path) - 1);
            job->image_directory_path[sizeof(job->image_directory_path) - 1] = '\0';
        } else {
            job->image_directory_path[0] = '\0';
        }

        job->extract_vector_images = extract_vector_images;
        job->no_filter = no_filter;

        // Submit job to thread pool
        if (pthread_create(&job->thread, NULL, process_document_job, job) != 0) {
            LOG_ERROR_MSG("WS", "Failed to create processing thread for job %s", job_id);
            send_job_progress(job_id, "failed", 0, "thread_creation_failed");
            free(job);
            return;
        }

        pthread_detach(job->thread);
        LOG_INFO_MSG("WS", "Job %s submitted to processing thread", job_id);
    }
    else if (strcmp(msg_type, "ack") == 0) {
        // Heartbeat acknowledgment from server - silently ignore
        LOG_DEBUG_MSG("WS", "Received heartbeat ACK from server");
    }
    else {
        LOG_WARN_MSG("WS", "Unknown message type: %s", msg_type);
    }
}

/*
 * WebSocket state change handler
 */
static void on_ws_state_change(ws_client_state_t old_state, ws_client_state_t new_state, void *user_data) {
    (void)user_data;

    const char *state_names[] = {"DISCONNECTED", "CONNECTING", "CONNECTED", "CLOSING", "ERROR"};
    LOG_INFO_MSG("WS", "State change: %s -> %s",
             state_names[old_state], state_names[new_state]);

    if (new_state == WS_STATE_CONNECTED) {
        LOG_INFO_MSG("WS", "Worker ONLINE - ready to receive jobs");
        health_update_status("healthy");
        // Ticketing system handles offline delivery automatically
    }
    else if (new_state == WS_STATE_DISCONNECTED || new_state == WS_STATE_ERROR) {
        LOG_WARN_MSG("WS", "Worker OFFLINE - reconnecting...");
        health_update_status("reconnecting");
    }
}

/*
 * Send job progress via WebSocket (REQUIRED for frontend tracking)
 */
static void send_job_progress(const char *job_id, const char *status, int progress_percent, const char *current_step) {
    if (!ws_client || !ws_client_is_connected(ws_client)) {
        LOG_WARN_MSG("WS", "Cannot send progress: not connected");
        return;
    }

    json_object *progress = json_object_new_object();
    json_object_object_add(progress, "job_id", json_object_new_string(job_id));
    json_object_object_add(progress, "status", json_object_new_string(status));
    json_object_object_add(progress, "progress_percent", json_object_new_int(progress_percent));
    json_object_object_add(progress, "current_step", json_object_new_string(current_step));
    json_object_object_add(progress, "timestamp", json_object_new_int64(time(NULL)));

    if (ws_client_send_message(ws_client, WS_MSG_TYPE_JOB_PROGRESS, progress) != 0) {
        LOG_ERROR_MSG("WS", "Failed to send progress for job %s", job_id);
    }

    json_object_put(progress);
}

/*
 * Store result as ticket in Redis and return ticket ID
 * This allows sending large payloads without hitting WebSocket message size limits
 * Returns 0 on success, -1 on error
 */
static int store_result_ticket(const char *job_id, const char *document_id, const char *result_json) {
    pthread_mutex_lock(&redis_buffer_mutex);

    if (!redis_buffer_ctx) {
        pthread_mutex_unlock(&redis_buffer_mutex);
        LOG_ERROR_MSG("REDIS", "Cannot store ticket: Redis not connected");
        return -1;
    }

    if (!document_id || strlen(document_id) == 0) {
        pthread_mutex_unlock(&redis_buffer_mutex);
        LOG_ERROR_MSG("TICKET", "Cannot store ticket: document_id is empty for job %s", job_id);
        return -1;
    }

    // Store ticket in Redis with 5 minute TTL (enough for delivery + confirmation)
    // Use document_id directly as ticket ID for simplified tracking
    char key[512];
    snprintf(key, sizeof(key), "worker:ticket:%s", document_id);

    redisReply *reply = redisCommand(redis_buffer_ctx, "SETEX %s 300 %s", key, result_json);

    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        LOG_ERROR_MSG("REDIS", "Failed to store result ticket for job %s (document %s)", job_id, document_id);
        if (reply) freeReplyObject(reply);
        pthread_mutex_unlock(&redis_buffer_mutex);
        return -1;
    }

    freeReplyObject(reply);
    pthread_mutex_unlock(&redis_buffer_mutex);

    LOG_INFO_MSG("TICKET", "Stored result ticket for document %s (job %s, %zu bytes)",
                 document_id, job_id, strlen(result_json));
    return 0;
}

/*
 * Send result via WebSocket using ticket system (no size limits)
 * Uses document_id as ticket ID for simplified tracking
 */
static void send_result_via_websocket(const char *job_id, const char *document_id, parse_result_t *result, uint64_t processing_time_ms) {
    if (!ws_client || !ws_client_is_connected(ws_client)) {
        LOG_ERROR_MSG("WS", "Cannot send result: not connected");
        return;
    }

    // Build result JSON (same structure as before)
    json_object *result_obj = json_object_new_object();
    json_object_object_add(result_obj, "job_id", json_object_new_string(job_id));
    json_object_object_add(result_obj, "status", json_object_new_string("completed"));

    json_object *data = json_object_new_object();
    if (result) {
        if (result->text_blocks) {
            json_object_object_add(data, "text_blocks", json_object_get(result->text_blocks));
        }
        if (result->paragraphs) {
            json_object_object_add(data, "paragraphs", json_object_get(result->paragraphs));
        }
        if (result->images) {
            json_object_object_add(data, "images", json_object_get(result->images));
        }
    }

    json_object_object_add(data, "processing_time_ms",
                           json_object_new_int64((int64_t)processing_time_ms));

    json_object_object_add(result_obj, "result", data);

    // Convert to JSON string
    const char *result_json = json_object_to_json_string(result_obj);
    size_t result_size = strlen(result_json);

    // Store result as ticket in Redis using document_id
    if (store_result_ticket(job_id, document_id, result_json) != 0) {
        LOG_ERROR_MSG("TICKET", "Failed to store result ticket for job %s (document %s)", job_id, document_id);
        json_object_put(result_obj);
        return;
    }

    // Send tiny ticket message via WebSocket (no size limits)
    // Use document_id as ticket_id for direct document tracking
    json_object *ticket_msg = json_object_new_object();
    json_object_object_add(ticket_msg, "job_id", json_object_new_string(job_id));
    json_object_object_add(ticket_msg, "status", json_object_new_string("completed"));
    json_object_object_add(ticket_msg, "ticket_id", json_object_new_string(document_id));
    json_object_object_add(ticket_msg, "result_size", json_object_new_int64((int64_t)result_size));

    if (ws_client_send_message(ws_client, WS_MSG_TYPE_JOB_RESULT, ticket_msg) != 0) {
        LOG_ERROR_MSG("WS", "Failed to send ticket for job %s", job_id);
        // Ticket is already in Redis, airag can fetch it later via cleanup job
    } else {
        LOG_INFO_MSG("WS", "Sent ticket %s (document %s) for job %s (%zu bytes)", document_id, document_id, job_id, result_size);
    }

    json_object_put(ticket_msg);
    json_object_put(result_obj);
}

int main(int argc __attribute__((unused)), char **argv __attribute__((unused))) {
    // Initialize monitoring system FIRST
    if (monitoring_init() != 0) {
        (void)fprintf(stderr, "Failed to initialize monitoring system\n");
        return 1;
    }

    // Configure log level from environment BEFORE any logging (supports: debug, info, warn, error, fatal)
    const char *log_level_env = getenv("WORKER_LOG_LEVEL");
    if (!log_level_env) {
        log_level_env = getenv("LOG_LEVEL");  // Fallback to generic LOG_LEVEL
    }
    if (log_level_env) {
        log_level_t level = parse_log_level(log_level_env);
        set_log_level(level);
    }

    LOG_INFO_MSG("MAIN", "Starting MuPDF Document Parsing Service with WebSocket");

    // Initialize worker configuration from environment variables
    init_worker_config();

    // Initialize Redis configuration from environment variables
    init_redis_config();

    // Initialize semaphore for concurrency control
    if (sem_init(&job_semaphore, 0, get_max_concurrent_jobs_safe()) != 0) {
        LOG_FATAL_MSG("MAIN", "Failed to initialize job semaphore");
        monitoring_cleanup();
        return 1;
    }
    LOG_INFO_MSG("INIT", "Job semaphore initialized with limit: %d", get_max_concurrent_jobs_safe());

    // Setup signal handlers for graceful shutdown
    (void)signal(SIGINT, signal_handler);
    (void)signal(SIGTERM, signal_handler);

    // Initialize MuPDF locks for thread-safety
    for (int i = 0; i < FZ_LOCK_MAX; i++) {
        if (pthread_mutex_init(&mupdf_locks[i], NULL) != 0) {
            LOG_FATAL_MSG("MAIN", "Failed to initialize MuPDF mutex %d", i);
            for (int j = 0; j < i; j++) {
                pthread_mutex_destroy(&mupdf_locks[j]);
            }
            monitoring_cleanup();
            return 1;
        }
    }

    // Initialize MuPDF with locking for multi-threaded use
    // Increased store size from default (256MB) to 512MB to reduce ICC cache conflicts
    base_ctx = fz_new_context(NULL, &mupdf_locks_ctx, 512 << 20);  // 512MB store
    if (!base_ctx) {
        LOG_FATAL_MSG("MAIN", "Failed to initialize MuPDF context");
        for (int i = 0; i < FZ_LOCK_MAX; i++) {
            pthread_mutex_destroy(&mupdf_locks[i]);
        }
        monitoring_cleanup();
        return 1;
    }

    // Register document handlers
    fz_register_document_handlers(base_ctx);

    // Enable ICC color management with proper initialization
    // This ensures ICC profiles are properly cached and shared across threads
    fz_enable_icc(base_ctx);

    // Results directories will be created per-organization as needed

    // Log path configuration for debugging  
    char images_dir[512];
    get_images_path(images_dir, sizeof(images_dir));
    LOG_INFO_MSG("INIT", "Directory paths configured:");
    LOG_INFO_MSG("INIT", "  uploads: %s", get_uploads_path_safe());
    LOG_INFO_MSG("INIT", "  images (default): %s", images_dir);
    LOG_INFO_MSG("INIT", "  results: dynamic per organization");

    // Directory structure (images + results) will be created by airag service per organization
    LOG_INFO_MSG("INIT", "Worker ready - directory structure managed by airag service");

    health_update_component_status(1, 0, 0); // MuPDF OK

    // Initialize libcurl
    (void)curl_global_init(CURL_GLOBAL_DEFAULT);

    // Initialize Redis consumer for job queue processing
    if (init_redis_consumer() != 0) {
        LOG_WARN_MSG("MAIN", "Redis consumer initialization failed, falling back to HTTP-only mode");
        health_update_component_status(1, 0, 0); // MuPDF OK, Redis failed
    } else {
        health_update_component_status(1, 1, 0); // MuPDF OK, Redis OK
    }

    // Initialize Redis buffer for result buffering (separate connection)
    if (init_redis_buffer() != 0) {
        LOG_WARN_MSG("MAIN", "Redis buffer initialization failed (result buffering disabled)");
    }

    // Initialize WebSocket client configuration
    const char *airag_host = getenv("AIRAG_HOST");
    if (!airag_host) {
        airag_host = "airag"; // Default to Docker service name
    }

    const char *airag_port_str = getenv("AIRAG_PORT");
    int airag_port = airag_port_str ? atoi(airag_port_str) : 1807;

    const char *ws_path = getenv("AIRAG_WS_PATH");
    if (!ws_path) {
        ws_path = "/ws/worker";
    }

    ws_client_config_t ws_config = {
        .airag_host = airag_host,
        .airag_port = airag_port,
        .ws_path = ws_path,
        .heartbeat_interval = 10,  // 10 seconds
        .reconnect_interval = 5,   // 5 seconds
        .connect_timeout = 10,     // 10 seconds
        .on_message = on_ws_message,
        .on_state_change = on_ws_state_change,
        .user_data = NULL
    };

    ws_client = ws_client_create(&ws_config);
    if (!ws_client) {
        LOG_FATAL_MSG("WS", "Failed to create WebSocket client: %s", ws_client_get_error());
        fz_drop_context(base_ctx);
        monitoring_cleanup();
        return 1;
    }

    LOG_INFO_MSG("WS", "Connecting to Airag at ws://%s:%d%s", airag_host, airag_port, ws_path);

    if (ws_client_connect(ws_client) != 0) {
        LOG_WARN_MSG("WS", "Initial connection failed: %s (will retry automatically)", ws_client_get_error());
    }

    LOG_INFO_MSG("MAIN", "MuPDF Worker ready!");
    LOG_INFO_MSG("MAIN", "  WebSocket: ws://%s:%d%s", airag_host, airag_port, ws_path);
    if (redis_consumer) {
        LOG_INFO_MSG("MAIN", "  Redis Queue: Active (queue=worker_jobs)");
    }
    LOG_INFO_MSG("MAIN", "Worker running... (Ctrl+C to shutdown)");

    // Main event loop - process WebSocket events
    while (!shutdown_requested) {
        // Process WebSocket events (1 second timeout)
        ws_client_process(ws_client, 1000);
    }

    LOG_INFO_MSG("MAIN", "Shutdown requested, cleaning up...");

    // Cleanup WebSocket client
    if (ws_client) {
        ws_client_close(ws_client);
        ws_client_destroy(ws_client);
    }

    // Cleanup Redis consumer
    cleanup_redis_consumer();

    // Cleanup Redis buffer
    cleanup_redis_buffer();

    // Cleanup resources
    curl_global_cleanup();
    fz_drop_context(base_ctx);

    // Cleanup MuPDF locks
    for (int i = 0; i < FZ_LOCK_MAX; i++) {
        pthread_mutex_destroy(&mupdf_locks[i]);
    }

    // Cleanup semaphore
    sem_destroy(&job_semaphore);

    // Cleanup monitoring system
    monitoring_cleanup();

    LOG_INFO_MSG("MAIN", "MuPDF Service shutdown complete");
    return 0;
}

/*
 * Async document processing worker
 */
static void* process_document_job(void *arg) {
    parse_job_t *job = (parse_job_t*)arg;

    // Acquire semaphore to limit concurrent processing
    LOG_INFO_MSG("JOB", "Job %s waiting for semaphore slot...", job->job_id);
    sem_wait(&job_semaphore);
    LOG_INFO_MSG("JOB", "Job %s acquired semaphore, starting processing", job->job_id);

    // Progress: Job started (0%)
    send_job_progress(job->job_id, "processing", 0, "started");

    metrics_job_started();
    log_job_start(job->job_id, job->file_path);

    struct timeval start_time;
    struct timeval end_time;
    (void)gettimeofday(&start_time, NULL);

    // Progress: Parsing (25%)
    send_job_progress(job->job_id, "processing", 25, "parsing");

    // Parse document with MuPDF
    parse_result_t *result = parse_document_with_mupdf_filtered(job->file_path, job->image_directory_path, job->extract_vector_images, job->no_filter);

    // Progress: Extracting text (50%)
    send_job_progress(job->job_id, "processing", 50, "extracting_text");

    // Progress: Extracting images (75%)
    send_job_progress(job->job_id, "processing", 75, "extracting_images");

    // Progress: Finalizing (90%)
    send_job_progress(job->job_id, "processing", 90, "finalizing");

    // Calculate processing time
    (void)gettimeofday(&end_time, NULL);
    uint64_t processing_time_ms = ((end_time.tv_sec - start_time.tv_sec) * 1000) +
                                  ((end_time.tv_usec - start_time.tv_usec) / 1000);

    // Send result via WebSocket (with Redis buffering fallback)
    // Always use ticketing system regardless of WebSocket status
    // Ticket stored in Redis with 5-minute TTL for airag to pick up
    send_job_progress(job->job_id, "completed", 100, "completed");
    send_result_via_websocket(job->job_id, job->document_id, result, processing_time_ms);

    // Update metrics based on result
    if (result && result->metadata) {
        json_object *status_obj = NULL;
        if (json_object_object_get_ex(result->metadata, "status", &status_obj)) {
            const char *status = json_object_get_string(status_obj);
            if (strcmp(status, "success") == 0) {
                metrics_job_completed(processing_time_ms);
                log_job_complete(job->job_id, processing_time_ms);
            } else {
                const char *error = "Document processing failed";
                json_object *error_obj = NULL;
                if (json_object_object_get_ex(result->metadata, "error", &error_obj)) {
                    error = json_object_get_string(error_obj);
                }
                metrics_job_failed(processing_time_ms, error);
                log_job_error(job->job_id, error);
            }
        }
    } else {
        metrics_job_failed(processing_time_ms, "No result returned");
        log_job_error(job->job_id, "No result returned");
    }

    // Cleanup
    if (result) {
        cleanup_parse_result(result);
    }
    free(job);

    // Release semaphore to allow next job
    sem_post(&job_semaphore);
    LOG_INFO_MSG("JOB", "Job completed, semaphore slot released");

    return NULL;
}

/*
 * Core document parsing using MuPDF
 * Returns structured document data with coordinates
 */
static parse_result_t* parse_document_with_mupdf_filtered(const char *file_path, const char *image_directory_path, int extract_vector_images, int no_filter) {
    LOG_INFO_MSG("PARSE", "Starting document parsing for file: %s", file_path);

    // Clone context for thread-safety (each thread needs its own context)
    if (!base_ctx) {
        LOG_FATAL_MSG("PARSE", "base_ctx is NULL when trying to parse %s", file_path);
        fprintf(stderr, "ERROR: base_ctx is NULL when trying to parse %s\n", file_path);
        return NULL;
    }

    fz_context *ctx = fz_clone_context(base_ctx);
    if (!ctx) {
        LOG_FATAL_MSG("PARSE", "Failed to clone MuPDF context for %s", file_path);
        fprintf(stderr, "ERROR: Failed to clone context for %s\n", file_path);
        return NULL;
    }
    LOG_DEBUG_MSG("PARSE", "Cloned MuPDF context for thread-safe parsing: %s", file_path);

    parse_result_t *result = calloc(1, sizeof(parse_result_t));
    if (!result) {
        LOG_FATAL_MSG("PARSE", "Failed to allocate memory for parse result: %s", file_path);
        return NULL;
    }
    fz_document *doc = NULL;

    fz_try(ctx) {
        LOG_INFO_MSG("PARSE", "Opening document with MuPDF: %s", file_path);
        // Open document
        doc = fz_open_document(ctx, file_path);
        if (!doc) {
            LOG_ERROR_MSG("PARSE", "Failed to open document: %s", file_path);
            fprintf(stderr, "Failed to open document: %s\n", file_path);
            fz_throw(ctx, FZ_ERROR_GENERIC, "Cannot open document");
        }

        int page_count = fz_count_pages(ctx, doc);
        LOG_INFO_MSG("PARSE", "Document opened successfully: %s (pages: %d)", file_path, page_count);

        // Safeguard: Limit pages for documents with excessive page counts
        const int MAX_PAGES = 5000;
        const int TEST_PAGES_LIMIT = 2; // For testing Excel rendering

        if (page_count > MAX_PAGES) {
            LOG_WARN_MSG("PARSE", "Document has excessive pages (%d > %d), limiting to first %d pages for testing: %s",
                        page_count, MAX_PAGES, TEST_PAGES_LIMIT, file_path);
            page_count = TEST_PAGES_LIMIT; // Override to process only first 2 pages
        }

        // Extract content with coordinates
        LOG_DEBUG_MSG("PARSE", "Extracting text blocks from document: %s", file_path);
        result->text_blocks = extract_text_blocks(ctx, doc);
        int text_block_count = result->text_blocks ? (int)json_object_array_length(result->text_blocks) : 0;
        LOG_INFO_MSG("PARSE", "Extracted %d text blocks from: %s", text_block_count, file_path);

        LOG_DEBUG_MSG("PARSE", "Extracting paragraphs from document: %s", file_path);
        result->paragraphs = extract_paragraphs(ctx, doc);
        int paragraph_count = result->paragraphs ? (int)json_object_array_length(result->paragraphs) : 0;
        LOG_INFO_MSG("PARSE", "Extracted %d paragraphs from: %s", paragraph_count, file_path);

        LOG_DEBUG_MSG("PARSE", "Extracting images from document: %s", file_path);
        result->images = extract_images_filtered(ctx, doc, image_directory_path, extract_vector_images, no_filter);
        int image_count = result->images ? (int)json_object_array_length(result->images) : 0;
        LOG_INFO_MSG("PARSE", "Extracted %d images from: %s", image_count, file_path);

        // Add metadata
        LOG_DEBUG_MSG("PARSE", "Creating metadata for document: %s", file_path);
        result->metadata = json_object_new_object();
        json_object_object_add(result->metadata, "page_count",
                             json_object_new_int(page_count));
        json_object_object_add(result->metadata, "format",
                             json_object_new_string("mupdf"));
        json_object_object_add(result->metadata, "status",
                             json_object_new_string("success"));

        LOG_INFO_MSG("PARSE", "Document parsing completed successfully: %s (text_blocks: %d, paragraphs: %d, images: %d)",
                     file_path, text_block_count, paragraph_count, image_count);
    }
    fz_catch(ctx) {
        (void)fprintf(stderr, "MuPDF error processing %s: %s\n", file_path, fz_caught_message(ctx));

        // Create error response
        if (result->metadata) {
            json_object_put(result->metadata);
        }
        result->metadata = json_object_new_object();
        json_object_object_add(result->metadata, "status",
                             json_object_new_string("error"));
        json_object_object_add(result->metadata, "error",
                             json_object_new_string(fz_caught_message(ctx)));
    }

    if (doc) {
        fz_drop_document(ctx, doc);
    }

    // Drop cloned context (thread-local)
    fz_drop_context(ctx);

    return result;
}



/*
 * Cleanup parsing results
 */
static void cleanup_parse_result(parse_result_t *result) {
    if (!result) {
        return;
    }

    if (result->text_blocks) {
        json_object_put(result->text_blocks);
    }
    if (result->paragraphs) {
        json_object_put(result->paragraphs);
    }
    if (result->images) {
        json_object_put(result->images);
    }
    if (result->metadata) {
        json_object_put(result->metadata);
    }

    free(result);
}

/*
 * Signal handler for graceful shutdown
 */
static void signal_handler(int sig) {
    (void)printf("\n[Signal] Received signal %d, initiating graceful shutdown...\n", sig);
    shutdown_requested = 1;
}

/*
 * Redis job processor callback
 * Processes jobs received from Redis queue
 */
static int redis_job_processor(const redis_job_t *job) {
    if (!job) {
        return -1;
    }

    // Acquire semaphore to limit concurrent processing
    LOG_INFO_MSG("JOB", "Redis job %s waiting for semaphore slot...", job->job_id);
    sem_wait(&job_semaphore);
    LOG_INFO_MSG("JOB", "Redis job %s acquired semaphore, starting processing", job->job_id);

    metrics_redis_job_received();
    metrics_job_started();
    log_job_start(job->job_id, job->file_path);

    struct timeval start_time;
    struct timeval end_time;
    gettimeofday(&start_time, NULL);

    // Parse document with MuPDF
    parse_result_t *result = parse_document_with_mupdf_filtered(job->file_path, job->image_directory_path, job->extract_vector_images, 0);

    // Calculate processing time
    gettimeofday(&end_time, NULL);
    uint64_t processing_time_ms = ((end_time.tv_sec - start_time.tv_sec) * 1000) +
                                  ((end_time.tv_usec - start_time.tv_usec) / 1000);

    // Always use ticketing system regardless of WebSocket status
    // Ticket stored in Redis with 5-minute TTL for airag to pick up
    send_result_via_websocket(job->job_id, job->document_id, result, processing_time_ms);

    // Update metrics based on result
    if (result && result->metadata) {
        json_object *status_obj = NULL;
        if (json_object_object_get_ex(result->metadata, "status", &status_obj)) {
            const char *status = json_object_get_string(status_obj);
            if (strcmp(status, "success") == 0) {
                metrics_job_completed(processing_time_ms);
                metrics_redis_job_acknowledged();
                log_job_complete(job->job_id, processing_time_ms);
            } else {
                const char *error = "Document processing failed";
                json_object *error_obj = NULL;
                if (json_object_object_get_ex(result->metadata, "error", &error_obj)) {
                    error = json_object_get_string(error_obj);
                }
                metrics_job_failed(processing_time_ms, error);
                log_job_error(job->job_id, error);
            }
        }
    }

    // Cleanup
    if (result) {
        cleanup_parse_result(result);
    }

    // Release semaphore even on failure
    sem_post(&job_semaphore);
    LOG_INFO_MSG("JOB", "Redis job completed with errors, semaphore slot released");
    LOG_DEBUG_MSG("CALLBACK", "Finished processing job %s", job->job_id);
    return -1;
}

/*
 * Initialize Redis consumer
 */
static int init_redis_consumer(void) {
    // Load Redis configuration from environment
    redis_config_t config = load_redis_config();

    // Create consumer
    redis_consumer = redis_consumer_create(&config, "worker_jobs", "mupdf_workers", "worker_1");
    if (!redis_consumer) {
        fprintf(stderr, "[Redis] Failed to create consumer\n");
        return -1;
    }

    // Set job processor callback
    g_job_processor = redis_job_processor;

    // Retry Redis connection with exponential backoff
    // This handles cases where Redis starts slower than the worker (common in Docker)
    const int max_retries = 60; // Try for up to ~5 minutes
    int retry_delay = 1; // Start with 1 second
    const int max_retry_delay = 30; // Cap at 30 seconds

    for (int attempt = 1; attempt <= max_retries; attempt++) {
        LOG_INFO_MSG("REDIS", "Attempting to connect to Redis (attempt %d/%d)", attempt, max_retries);

        // Start consumer (includes connection attempt)
        if (redis_consumer_start(redis_consumer) == 0) {
            LOG_INFO_MSG("REDIS", "Consumer started successfully after %d attempt(s)", attempt);
            return 0;
        }

        // Connection failed
        if (attempt < max_retries) {
            LOG_WARN_MSG("REDIS", "Connection failed, retrying in %d seconds...", retry_delay);
            sleep(retry_delay);

            // Exponential backoff with cap
            retry_delay *= 2;
            if (retry_delay > max_retry_delay) {
                retry_delay = max_retry_delay;
            }
        }
    }

    // All retries exhausted
    LOG_ERROR_MSG("REDIS", "Failed to connect after %d attempts, falling back to HTTP-only mode", max_retries);
    redis_consumer_destroy(redis_consumer);
    redis_consumer = NULL;
    return -1;
}

/*
 * Cleanup Redis consumer
 */
static void cleanup_redis_consumer(void) {
    if (redis_consumer) {
        printf("[Redis] Stopping consumer...\n");
        redis_consumer_destroy(redis_consumer);
        redis_consumer = NULL;
    }
}

/*
 * Load Redis configuration from environment variables
 */
static redis_config_t load_redis_config(void) {
    printf("[Redis] Configuration: %s:%d (db=%d)\n",
           redis_config_cached.host, redis_config_cached.port, redis_config_cached.db);
    return redis_config_cached;
}

/*
 * Initialize Redis buffer connection for result buffering
 */
static int init_redis_buffer(void) {
    redis_config_t config = load_redis_config();

    struct timeval timeout = { config.timeout_ms / 1000, (config.timeout_ms % 1000) * 1000 };
    redis_buffer_ctx = redisConnectWithTimeout(config.host, config.port, timeout);

    if (redis_buffer_ctx == NULL || redis_buffer_ctx->err) {
        if (redis_buffer_ctx) {
            LOG_WARN_MSG("REDIS", "Redis buffer connection failed: %s (buffering disabled)", redis_buffer_ctx->errstr);
            redisFree(redis_buffer_ctx);
            redis_buffer_ctx = NULL;
        } else {
            LOG_WARN_MSG("REDIS", "Redis buffer connection failed: can't allocate context");
        }
        return -1;
    }

    // Authenticate if password is set
    if (config.password[0] != '\0') {
        redisReply *reply = redisCommand(redis_buffer_ctx, "AUTH %s", config.password);
        if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
            LOG_ERROR_MSG("REDIS", "Redis buffer auth failed");
            if (reply) freeReplyObject(reply);
            redisFree(redis_buffer_ctx);
            redis_buffer_ctx = NULL;
            return -1;
        }
        freeReplyObject(reply);
    }

    // Select database
    if (config.db != 0) {
        redisReply *reply = redisCommand(redis_buffer_ctx, "SELECT %d", config.db);
        if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
            LOG_ERROR_MSG("REDIS", "Redis buffer SELECT failed");
            if (reply) freeReplyObject(reply);
            redisFree(redis_buffer_ctx);
            redis_buffer_ctx = NULL;
            return -1;
        }
        freeReplyObject(reply);
    }

    LOG_INFO_MSG("REDIS", "Result buffering initialized (fallback for WebSocket failures)");
    return 0;
}

/*
 * Cleanup Redis buffer connection
 */
static void cleanup_redis_buffer(void) {
    pthread_mutex_lock(&redis_buffer_mutex);
    if (redis_buffer_ctx) {
        redisFree(redis_buffer_ctx);
        redis_buffer_ctx = NULL;
        LOG_INFO_MSG("REDIS", "Result buffering connection closed");
    }
    pthread_mutex_unlock(&redis_buffer_mutex);
}

/*
 * Buffer job result in Redis when WebSocket is unavailable
 * Key format: worker:buffered_results:{job_id}
 * TTL: 600 seconds (10 minutes)
 */
// Old buffering functions removed - ticketing system handles all delivery now


