/*
 * Standalone MuPDF Document Parsing Service (worker/main.c)
 *
 * Defensive SIGABRT handling added:
 * - Install per-thread SIGABRT handler around MuPDF parsing to catch internal
 *   MuPDF assertions that call abort().
 * - Use sigsetjmp/siglongjmp to return to a safe point when abort occurs.
 * - Notify Airag via WebSocket immediately with job-level error information.
 *
 * Other behavior remains the same: WebSocket for job submission/results,
 * Redis queue integration, multi-threaded parsing using cloned MuPDF contexts.
 */

// Enable GNU extensions for strerror_r and other functions
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200112L

// System includes (alphabetical)
#include <pthread.h>
#include <semaphore.h>
#include <setjmp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
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

// Thread-local jump buffer to catch aborts from MuPDF assertions
static __thread sigjmp_buf mupdf_abort_jmp;
// Thread-local job id so deep/error handlers can reference the current job
static __thread char thread_job_id[256] = {0};

// Helper function to send job progress via WebSocket
static void send_job_progress(const char *job_id, const char *status, int progress_percent, const char *current_step);

// Send a job-level error message to Airag via WebSocket (immediate notification)
static void send_job_error(const char *job_id, const char *error_msg) {
    if (!job_id) { job_id = ""; }
    if (!ws_client || !ws_client_is_connected(ws_client)) {
        LOG_ERROR_MSG("WS", "Cannot send job error: not connected (job=%s): %s", job_id, error_msg ? error_msg : "(nil)");
        return;
    }

    json_object *err = json_object_new_object();
    json_object_object_add(err, "job_id", json_object_new_string(job_id));
    json_object_object_add(err, "status", json_object_new_string("failed"));
    json_object_object_add(err, "error", json_object_new_string(error_msg ? error_msg : "unknown error"));
    json_object_object_add(err, "timestamp", json_object_new_int64((int64_t)time(NULL)));

    if (ws_client_send_message(ws_client, WS_MSG_TYPE_JOB_PROGRESS, err) != 0) {
        LOG_ERROR_MSG("WS", "Failed to send job error for %s", job_id);
    } else {
        LOG_INFO_MSG("WS", "Sent job error for %s: %s", job_id, error_msg ? error_msg : "(nil)");
    }

    json_object_put(err);
}

// Abort signal handler that performs longjmp back to a safe point
static void mupdf_abort_handler(int sig) {
    // Try to jump back; use a non-zero value
    siglongjmp(mupdf_abort_jmp, sig ? sig : 1);
}

static void init_worker_config(void) {
    const char *uploads_path = getenv("UPLOADS_PATH");
    if (uploads_path) {
        strncpy(worker_config.uploads_path, uploads_path, sizeof(worker_config.uploads_path) - 1);
        worker_config.uploads_path[sizeof(worker_config.uploads_path) - 1] = '\0';
    } else {
        strncpy(worker_config.uploads_path, "/tmp/airag", sizeof(worker_config.uploads_path) - 1);
        worker_config.uploads_path[sizeof(worker_config.uploads_path) - 1] = '\0';
    }

    const char *vector_enabled = getenv("RAG_VECTOR_EXTRACTION_ENABLED");
    worker_config.vector_extraction_enabled = (vector_enabled && strcmp(vector_enabled, "false") == 0) ? 0 : 1;

    const char *max_concurrent = getenv("WORKER_MAX_CONCURRENT");
    if (max_concurrent) {
        char *endptr = NULL;
        long max_jobs = strtol(max_concurrent, &endptr, 10);
        if (*endptr == '\0' && max_jobs > 0 && max_jobs <= 32) {
            worker_config.max_concurrent_jobs = (int)max_jobs;
        } else {
            worker_config.max_concurrent_jobs = 2;
        }
    } else {
        worker_config.max_concurrent_jobs = 2;
    }

    LOG_INFO_MSG("INIT", "Worker configuration loaded:");
    LOG_INFO_MSG("INIT", "  uploads_path: %s", worker_config.uploads_path);
    LOG_INFO_MSG("INIT", "  vector_extraction_enabled: %s (enhanced object detection)", worker_config.vector_extraction_enabled ? "true" : "false");
    LOG_INFO_MSG("INIT", "  max_concurrent_jobs: %d (prevents memory balloning)", worker_config.max_concurrent_jobs);
}

static const char* get_uploads_path_safe(void) {
    return worker_config.uploads_path;
}
static int get_vector_extraction_enabled_safe(void) {
    return worker_config.vector_extraction_enabled;
}
static int get_max_concurrent_jobs_safe(void) {
    return worker_config.max_concurrent_jobs;
}

static void init_redis_config(void) {
    strncpy(redis_config_cached.host, "localhost", sizeof(redis_config_cached.host) - 1);
    redis_config_cached.host[sizeof(redis_config_cached.host) - 1] = '\0';
    redis_config_cached.port = 6379;
    redis_config_cached.password[0] = '\0';
    redis_config_cached.db = 4;
    redis_config_cached.timeout_ms = 5000;

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
    char document_id[256];
    char file_path[1024];
    char image_directory_path[1024];
    int extract_vector_images;
    int no_filter;
    pthread_t thread;
} parse_job_t;

void get_images_path(char *buffer, size_t buffer_size) {
    (void)snprintf(buffer, buffer_size, "%s/images", get_uploads_path_safe());
}
void get_results_path(char *buffer, size_t buffer_size, const char *image_directory_path) {
    if (image_directory_path && strlen(image_directory_path) > 0) {
        char parent_dir[512];
        strncpy(parent_dir, image_directory_path, sizeof(parent_dir) - 1);
        parent_dir[sizeof(parent_dir) - 1] = '\0';
        size_t len = strlen(parent_dir);
        if (len > 7 && strcmp(parent_dir + len - 7, "/images") == 0) {
            parent_dir[len - 7] = '\0';
        }
        (void)snprintf(buffer, buffer_size, "%s/results", parent_dir);
    } else {
        (void)snprintf(buffer, buffer_size, "%s/results", get_uploads_path_safe());
    }
}
void get_org_images_path(char *buffer, size_t buffer_size, const char *image_directory_path) {
    (void)snprintf(buffer, buffer_size, "%s", image_directory_path ? image_directory_path : get_uploads_path_safe());
}

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

static pthread_mutex_t idle_mutex = PTHREAD_MUTEX_INITIALIZER;
static time_t last_job_time = 0;
static pthread_t cleanup_thread;
static int cleanup_thread_running = 0;

static redisContext *redis_buffer_ctx = NULL;
static pthread_mutex_t redis_buffer_mutex = PTHREAD_MUTEX_INITIALIZER;

// Function declarations (note: parse now accepts job_id)
static int init_redis_buffer(void);
static void cleanup_redis_buffer(void);
 
// Forward declarations for functions used before their definitions
static int init_redis_consumer(void);
static void cleanup_redis_consumer(void);
static void signal_handler(int sig);
 
// Verify helper (declared here so it can be used prior to its definition)
static int verify_document_with_mupdf(const char *job_id, const char *file_path);
 
/*
 * Implementations for consumer and buffer initialization/cleanup.
 *
 * These are lightweight wrappers so the main worker can initialize and
 * teardown its Redis consumer and optional result-buffering connection.
 * They intentionally use the redis_consumer_* helpers in redis_queue.c.
 *
 * Returning 0 means success; non-zero indicates a failure to initialize.
 */
static int init_redis_consumer(void) {
    // If already created, treat as success
    if (redis_consumer) {
        return 0;
    }
 
    // Create consumer using cached redis configuration. Use conservative names
    // so operator can find the consumer in logs if needed.
    redis_consumer = redis_consumer_create(&redis_config_cached,
                                           "worker_jobs",
                                           "airag_workers",
                                           "worker_1");
    if (!redis_consumer) {
        fprintf(stderr, "[MAIN] Failed to create redis consumer\n");
        return -1;
    }
 
    if (redis_consumer_start(redis_consumer) != 0) {
        fprintf(stderr, "[MAIN] Failed to start redis consumer\n");
        redis_consumer_destroy(redis_consumer);
        redis_consumer = NULL;
        return -1;
    }
 
    return 0;
}
 
static void cleanup_redis_consumer(void) {
    if (!redis_consumer) {
        return;
    }
 
    // Stop and destroy the consumer
    redis_consumer_stop(redis_consumer);
    redis_consumer_destroy(redis_consumer);
    redis_consumer = NULL;
}
 
static int init_redis_buffer(void) {
    /* Create a dedicated Redis connection for result ticket storage
     * This is separate from the consumer connection to avoid interference
     */
    struct timeval timeout = {
        .tv_sec = redis_config_cached.timeout_ms / 1000,
        .tv_usec = (redis_config_cached.timeout_ms % 1000) * 1000
    };

    redis_buffer_ctx = redisConnectWithTimeout(
        redis_config_cached.host,
        redis_config_cached.port,
        timeout
    );

    if (!redis_buffer_ctx || redis_buffer_ctx->err) {
        LOG_ERROR_MSG("REDIS", "Failed to create buffer connection: %s:%d - %s",
                     redis_config_cached.host,
                     redis_config_cached.port,
                     redis_buffer_ctx ? redis_buffer_ctx->errstr : "allocation failed");
        if (redis_buffer_ctx) {
            redisFree(redis_buffer_ctx);
            redis_buffer_ctx = NULL;
        }
        return -1;
    }

    // Authenticate if password provided
    if (strlen(redis_config_cached.password) > 0) {
        redisReply *reply = redisCommand(redis_buffer_ctx, "AUTH %s", redis_config_cached.password);
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            LOG_ERROR_MSG("REDIS", "Buffer connection authentication failed");
            if (reply) freeReplyObject(reply);
            redisFree(redis_buffer_ctx);
            redis_buffer_ctx = NULL;
            return -1;
        }
        freeReplyObject(reply);
    }

    // Select database
    if (redis_config_cached.db != 0) {
        redisReply *reply = redisCommand(redis_buffer_ctx, "SELECT %d", redis_config_cached.db);
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            LOG_ERROR_MSG("REDIS", "Failed to select database %d for buffer", redis_config_cached.db);
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
 
static void cleanup_redis_buffer(void) {
    if (!redis_buffer_ctx) {
        return;
    }
 
    /* If a hiredis redisContext was created in the future, free it here.
     * Use redisFree from hiredis to release resources.
     */
#if defined(HAVE_HIREDIS) || 1
    redisFree(redis_buffer_ctx);
#endif
    redis_buffer_ctx = NULL;
}

static void* process_document_job(void *arg);
static parse_result_t* parse_document_with_mupdf_filtered(const char *job_id, const char *file_path, const char *image_directory_path, int extract_vector_images, int no_filter);
static void send_result_via_websocket(const char *job_id, const char *document_id, parse_result_t *result, uint64_t processing_time_ms);
static void* idle_cleanup_worker(void *arg);
static void update_last_job_time(void);
static void cleanup_parse_result(parse_result_t *result);

json_object* extract_text_blocks(fz_context *ctx, fz_document *doc);
json_object* extract_paragraphs(fz_context *ctx, fz_document *doc);
json_object* extract_images_filtered(fz_context *ctx, fz_document *doc, const char *image_directory_path, int extract_vector_images, int no_filter);

// -- WebSocket handlers -----------------------------------------------------------------------

static void on_ws_message(const char *msg_type, json_object *msg, void *user_data) {
    (void)user_data;
    LOG_DEBUG_MSG("WS", "Received message: %s", msg_type);

    if (strcmp(msg_type, WS_MSG_TYPE_JOB_SUBMIT) == 0) {
        json_object *data_obj = json_object_object_get(msg, "data");
        if (!data_obj) {
            LOG_ERROR_MSG("WS", "Invalid job_submit message: missing data field");
            return;
        }

        json_object *job_id_obj = json_object_object_get(data_obj, "job_id");
        json_object *file_path_obj = json_object_object_get(data_obj, "file_path");
        json_object *options_obj = json_object_object_get(data_obj, "options");

        if (!job_id_obj || !file_path_obj) {
            LOG_ERROR_MSG("WS", "Invalid job_submit message: missing required fields");
            return;
        }

        const char *job_id = json_object_get_string(job_id_obj);
        const char *file_path = json_object_get_string(file_path_obj);

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
        send_job_progress(job_id, "started", 0, "job_received");

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
            strncpy(job->image_directory_path, image_directory_path, sizeof(job->image_directory_path) - 1);
            job->image_directory_path[sizeof(job->image_directory_path) - 1] = '\0';
        } else {
            job->image_directory_path[0] = '\0';
        }

        job->extract_vector_images = extract_vector_images;
        job->no_filter = no_filter;

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
        LOG_DEBUG_MSG("WS", "Received heartbeat ACK from server");
    }
    else {
        LOG_WARN_MSG("WS", "Unknown message type: %s", msg_type);
    }
}

static void on_ws_state_change(ws_client_state_t old_state, ws_client_state_t new_state, void *user_data) {
    (void)user_data;
    const char *state_names[] = {"DISCONNECTED", "CONNECTING", "CONNECTED", "CLOSING", "ERROR"};
    LOG_INFO_MSG("WS", "State change: %s -> %s", state_names[old_state], state_names[new_state]);
    if (new_state == WS_STATE_CONNECTED) {
        LOG_INFO_MSG("WS", "Worker ONLINE - ready to receive jobs");
        health_update_status("healthy");
    } else if (new_state == WS_STATE_DISCONNECTED || new_state == WS_STATE_ERROR) {
        LOG_WARN_MSG("WS", "Worker OFFLINE - reconnecting...");
        health_update_status("reconnecting");
    }
}

static void send_job_progress(const char *job_id, const char *status, int progress_percent, const char *current_step) {
    if (!ws_client || !ws_client_is_connected(ws_client)) {
        LOG_WARN_MSG("WS", "Cannot send progress: not connected");
        return;
    }

    json_object *progress = json_object_new_object();
    json_object_object_add(progress, "job_id", json_object_new_string(job_id ? job_id : ""));
    json_object_object_add(progress, "status", json_object_new_string(status ? status : ""));
    json_object_object_add(progress, "progress_percent", json_object_new_int(progress_percent));
    json_object_object_add(progress, "current_step", json_object_new_string(current_step ? current_step : ""));
    json_object_object_add(progress, "timestamp", json_object_new_int64((int64_t)time(NULL)));

    if (ws_client_send_message(ws_client, WS_MSG_TYPE_JOB_PROGRESS, progress) != 0) {
        LOG_ERROR_MSG("WS", "Failed to send progress for job %s", job_id ? job_id : "(nil)");
    }

    json_object_put(progress);
}

// Store result ticket in Redis for retrieval by airag service
// Returns 0 on success, -1 on failure
static int store_result_ticket(const char *job_id, const char *document_id, const char *result_json) {
    if (!job_id || !document_id || !result_json) {
        LOG_ERROR_MSG("TICKET", "Invalid parameters: job_id, document_id, or result_json is NULL");
        return -1;
    }

    // Use redis_buffer_ctx if available, otherwise try to reconnect once
    // The ticket system allows airag to retrieve large results from Redis instead of WebSocket
    if (!redis_buffer_ctx) {
        LOG_WARN_MSG("TICKET", "Redis buffer context not available, attempting immediate reconnection...");
        if (init_redis_buffer() != 0) {
            LOG_WARN_MSG("TICKET", "Redis buffer reconnection failed, skipping ticket storage");
            return 0; // Not a fatal error - result will be sent via WebSocket
        }
        LOG_INFO_MSG("TICKET", "Redis buffer reconnected successfully on demand");
    }

    pthread_mutex_lock(&redis_buffer_mutex);

    // Store result with 30-minute TTL (1800 seconds)
    // Key format: worker:ticket:{document_id} (matches airag retrieval key)
    char key[512];
    snprintf(key, sizeof(key), "worker:ticket:%s", document_id);

    redisReply *reply = redisCommand(redis_buffer_ctx, "SETEX %s 1800 %s", key, result_json);

    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        const char *error_msg = "unknown error";
        if (reply == NULL) {
            error_msg = redis_buffer_ctx->errstr ? redis_buffer_ctx->errstr : "connection lost";
        } else if (reply->type == REDIS_REPLY_ERROR) {
            error_msg = reply->str ? reply->str : "redis command error";
        }
        LOG_ERROR_MSG("TICKET", "Failed to store result ticket for job %s (document %s): %s",
                     job_id, document_id, error_msg);
        if (reply) freeReplyObject(reply);
        pthread_mutex_unlock(&redis_buffer_mutex);
        return -1;
    }

    freeReplyObject(reply);
    pthread_mutex_unlock(&redis_buffer_mutex);

    LOG_INFO_MSG("TICKET", "Stored result ticket for job %s (document %s, size: %zu bytes)",
                job_id, document_id, strlen(result_json));
    return 0;
}

static void send_result_via_websocket(const char *job_id, const char *document_id, parse_result_t *result, uint64_t processing_time_ms) {
    if (!ws_client || !ws_client_is_connected(ws_client)) {
        LOG_ERROR_MSG("WS", "Cannot send result: not connected");
        return;
    }

    json_object *result_obj = json_object_new_object();
    json_object_object_add(result_obj, "job_id", json_object_new_string(job_id ? job_id : ""));
    json_object_object_add(result_obj, "status", json_object_new_string("completed"));

    json_object *data = json_object_new_object();
    if (result) {
        if (result->text_blocks) { json_object_object_add(data, "text_blocks", json_object_get(result->text_blocks)); }
        if (result->paragraphs) { json_object_object_add(data, "paragraphs", json_object_get(result->paragraphs)); }
        if (result->images) { json_object_object_add(data, "images", json_object_get(result->images)); }
    }

    json_object_object_add(data, "processing_time_ms", json_object_new_int64((int64_t)processing_time_ms));
    json_object_object_add(result_obj, "result", data);

    const char *result_json = json_object_to_json_string(result_obj);
    size_t result_size = strlen(result_json);

    // Store ticket
    if (store_result_ticket(job_id, document_id, result_json) != 0) {
        LOG_ERROR_MSG("TICKET", "Failed to store result ticket for job %s (document %s)", job_id ? job_id : "(nil)", document_id ? document_id : "(nil)");
        json_object_put(result_obj);
        return;
    }

    // Send ticket message
    json_object *ticket_msg = json_object_new_object();
    json_object_object_add(ticket_msg, "job_id", json_object_new_string(job_id ? job_id : ""));
    json_object_object_add(ticket_msg, "status", json_object_new_string("completed"));
    json_object_object_add(ticket_msg, "ticket_id", json_object_new_string(document_id ? document_id : ""));
    json_object_object_add(ticket_msg, "result_size", json_object_new_int64((int64_t)result_size));

    if (ws_client_send_message(ws_client, WS_MSG_TYPE_JOB_RESULT, ticket_msg) != 0) {
        LOG_ERROR_MSG("WS", "Failed to send ticket for job %s", job_id ? job_id : "(nil)");
    } else {
        LOG_INFO_MSG("WS", "Sent ticket %s (document %s) for job %s (%zu bytes)", document_id ? document_id : "", document_id ? document_id : "", job_id ? job_id : "", result_size);
    }

    json_object_put(ticket_msg);
    json_object_put(result_obj);
}

// Main
int main(int argc __attribute__((unused)), char **argv __attribute__((unused))) {
    if (monitoring_init() != 0) {
        (void)fprintf(stderr, "Failed to initialize monitoring system\n");
        return 1;
    }

    const char *log_level_env = getenv("WORKER_LOG_LEVEL");
    if (!log_level_env) log_level_env = getenv("LOG_LEVEL");
    if (log_level_env) {
        log_level_t level = parse_log_level(log_level_env);
        set_log_level(level);
    }

    LOG_INFO_MSG("MAIN", "Starting MuPDF Document Parsing Service with WebSocket");

    init_worker_config();
    init_redis_config();

    if (sem_init(&job_semaphore, 0, get_max_concurrent_jobs_safe()) != 0) {
        LOG_FATAL_MSG("MAIN", "Failed to initialize job semaphore");
        monitoring_cleanup();
        return 1;
    }

    (void)signal(SIGINT, signal_handler);
    (void)signal(SIGTERM, signal_handler);

    for (int i = 0; i < FZ_LOCK_MAX; i++) {
        if (pthread_mutex_init(&mupdf_locks[i], NULL) != 0) {
            LOG_FATAL_MSG("MAIN", "Failed to initialize MuPDF mutex %d", i);
            for (int j = 0; j < i; j++) pthread_mutex_destroy(&mupdf_locks[j]);
            monitoring_cleanup();
            return 1;
        }
    }

    base_ctx = fz_new_context(NULL, &mupdf_locks_ctx, 512 << 20);
    if (!base_ctx) {
        LOG_FATAL_MSG("MAIN", "Failed to initialize MuPDF context");
        for (int i = 0; i < FZ_LOCK_MAX; i++) pthread_mutex_destroy(&mupdf_locks[i]);
        monitoring_cleanup();
        return 1;
    }

    fz_register_document_handlers(base_ctx);
    fz_enable_icc(base_ctx);

    char images_dir[512];
    get_images_path(images_dir, sizeof(images_dir));
    LOG_INFO_MSG("INIT", "Directory paths configured:");
    LOG_INFO_MSG("INIT", "  uploads: %s", get_uploads_path_safe());
    LOG_INFO_MSG("INIT", "  images (default): %s", images_dir);
    LOG_INFO_MSG("INIT", "  results: dynamic per organization");
    LOG_INFO_MSG("INIT", "Worker ready - directory structure managed by airag service");

    health_update_component_status(1, 0, 0);

    (void)curl_global_init(CURL_GLOBAL_DEFAULT);

    if (init_redis_consumer() != 0) {
        LOG_WARN_MSG("MAIN", "Redis consumer initialization failed, falling back to HTTP-only mode");
        health_update_component_status(1, 0, 0);
    } else {
        health_update_component_status(1, 1, 0);
    }

    if (init_redis_buffer() != 0) {
        LOG_WARN_MSG("MAIN", "Redis buffer initialization failed (result buffering disabled)");
    }

    const char *airag_host = getenv("AIRAG_HOST");
    if (!airag_host) { airag_host = "airag"; }
    const char *airag_port_str = getenv("AIRAG_PORT");
    int airag_port = airag_port_str ? atoi(airag_port_str) : 1807;
    const char *ws_path = getenv("AIRAG_WS_PATH");
    if (!ws_path) { ws_path = "/ws/worker"; }

    ws_client_config_t ws_config = {
        .airag_host = airag_host,
        .airag_port = airag_port,
        .ws_path = ws_path,
        .heartbeat_interval = 10,
        .reconnect_interval = 5,
        .connect_timeout = 10,
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

    if (ws_client_connect(ws_client) != 0) {
        LOG_WARN_MSG("WS", "Initial connection failed: %s (will retry automatically)", ws_client_get_error());
    }

    cleanup_thread_running = 1;
    if (pthread_create(&cleanup_thread, NULL, idle_cleanup_worker, NULL) != 0) {
        LOG_WARN_MSG("MAIN", "Failed to create idle cleanup thread");
        cleanup_thread_running = 0;
    } else {
        LOG_INFO_MSG("MAIN", "Idle cleanup thread started (1 hour threshold)");
    }

    LOG_INFO_MSG("MAIN", "MuPDF Worker ready!");
    LOG_INFO_MSG("MAIN", "  WebSocket: ws://%s:%d%s", airag_host, airag_port, ws_path);
    if (redis_consumer) { LOG_INFO_MSG("MAIN", "  Redis Queue: Active (queue=worker_jobs)"); }
    LOG_INFO_MSG("MAIN", "Worker running... (Ctrl+C to shutdown)");

    while (!shutdown_requested) {
        ws_client_process(ws_client, 1000);
    }

    LOG_INFO_MSG("MAIN", "Shutdown requested, cleaning up...");

    if (cleanup_thread_running) {
        cleanup_thread_running = 0;
        pthread_join(cleanup_thread, NULL);
        LOG_INFO_MSG("MAIN", "Idle cleanup thread stopped");
    }

    if (ws_client) {
        ws_client_close(ws_client);
        ws_client_destroy(ws_client);
    }

    cleanup_redis_consumer();
    cleanup_redis_buffer();

    curl_global_cleanup();
    fz_drop_context(base_ctx);

    for (int i = 0; i < FZ_LOCK_MAX; i++) pthread_mutex_destroy(&mupdf_locks[i]);
    sem_destroy(&job_semaphore);
    monitoring_cleanup();

    LOG_INFO_MSG("MAIN", "MuPDF Service shutdown complete");
    return 0;
}

// Async document processing worker
static void* process_document_job(void *arg) {
    parse_job_t *job = (parse_job_t*)arg;

    LOG_INFO_MSG("JOB", "Job %s waiting for semaphore slot...", job->job_id);
    sem_wait(&job_semaphore);
    LOG_INFO_MSG("JOB", "Job %s acquired semaphore, starting processing", job->job_id);

    send_job_progress(job->job_id, "processing", 0, "started");
    metrics_job_started();
    log_job_start(job->job_id, job->file_path);

    struct timeval start_time, end_time;
    gettimeofday(&start_time, NULL);

    // Progress: Parsing (25%)
    send_job_progress(job->job_id, "processing", 25, "parsing");

    // Set thread-local job id so deep/error handlers (e.g. abort handler) can include job context
    strncpy(thread_job_id, job->job_id, sizeof(thread_job_id) - 1);
    thread_job_id[sizeof(thread_job_id) - 1] = '\0';

    // Verify document using MuPDF quickly (open + page count) under the same SIGABRT protection.
    // If verification fails we create a minimal error result and notify Airag (verify sends job error).
    parse_result_t *result = NULL;
    if (verify_document_with_mupdf(job->job_id, job->file_path) != 0) {
        LOG_ERROR_MSG("PARSE", "Verification failed for %s, skipping full parse", job->file_path);
        result = calloc(1, sizeof(parse_result_t));
        if (!result) {
            LOG_ERROR_MSG("PARSE", "Failed to allocate parse_result for failed verification: %s", job->job_id);
            // As a fallback, create a tiny metadata object directly on the stack is not possible;
            // instead set result to NULL and rely on downstream checks (the job error was already sent).
            result = NULL;
        } else {
            result->metadata = json_object_new_object();
            json_object_object_add(result->metadata, "status", json_object_new_string("error"));
            json_object_object_add(result->metadata, "error", json_object_new_string("Verification failed: MuPDF open/count failed"));
        }
    } else {
        // Pass job_id into parsing so parse can report immediate job-level errors
        result = parse_document_with_mupdf_filtered(job->job_id, job->file_path, job->image_directory_path, job->extract_vector_images, job->no_filter);
    }

    // Clear thread-local job id after parse (avoid stale pointers)
    thread_job_id[0] = '\0';

    send_job_progress(job->job_id, "processing", 50, "extracting_text");
    send_job_progress(job->job_id, "processing", 75, "extracting_images");
    send_job_progress(job->job_id, "processing", 90, "finalizing");

    gettimeofday(&end_time, NULL);
    uint64_t processing_time_ms = ((end_time.tv_sec - start_time.tv_sec) * 1000) + ((end_time.tv_usec - start_time.tv_usec) / 1000);

    send_job_progress(job->job_id, "completed", 100, "completed");
    send_result_via_websocket(job->job_id, job->document_id, result, processing_time_ms);

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

    if (result) cleanup_parse_result(result);
    free(job);
    sem_post(&job_semaphore);
    update_last_job_time();

    LOG_INFO_MSG("JOB", "Job completed, semaphore slot released");
    return NULL;
}

// Redis job processor variant - ensure it passes job_id too
static int redis_job_processor(const redis_job_t *job) {
    if (!job) {
        return -1;
    }

    LOG_INFO_MSG("JOB", "Redis job %s waiting for semaphore slot...", job->job_id);
    sem_wait(&job_semaphore);
    LOG_INFO_MSG("JOB", "Redis job %s acquired semaphore, starting processing", job->job_id);

    metrics_redis_job_received();
    metrics_job_started();
    log_job_start(job->job_id, job->file_path);

    struct timeval start_time, end_time;
    gettimeofday(&start_time, NULL);

    parse_result_t *result = parse_document_with_mupdf_filtered(job->job_id, job->file_path, job->image_directory_path, job->extract_vector_images, 0);

    gettimeofday(&end_time, NULL);
    uint64_t processing_time_ms = ((end_time.tv_sec - start_time.tv_sec) * 1000) + ((end_time.tv_usec - start_time.tv_usec) / 1000);

    send_result_via_websocket(job->job_id, job->document_id, result, processing_time_ms);

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

    if (result) cleanup_parse_result(result);
    sem_post(&job_semaphore);
    update_last_job_time();

    LOG_INFO_MSG("JOB", "Redis job completed with errors, semaphore slot released");
    LOG_DEBUG_MSG("CALLBACK", "Finished processing job %s", job->job_id);
    return -1;
}

/*
 * Lightweight MuPDF verification helper
 *
 * Performs a minimal MuPDF "open + count pages" under the same SIGABRT
 * protection used by the full parser. This lets us detect documents that
 * will cause MuPDF to error/abort before running the heavier extraction
 * pipeline. On failure this function will send an immediate job-level
 * error via WebSocket (using the existing helper) and return non-zero.
 */
static int verify_document_with_mupdf(const char *job_id, const char *file_path) {
    if (!file_path) {
        LOG_ERROR_MSG("VERIFY", "No file path provided for verification (job=%s)", job_id ? job_id : "(nil)");
        if (job_id && strlen(job_id) > 0) send_job_error(job_id, "Verification failed: no file path provided");
        return -1;
    }

    if (!base_ctx) {
        LOG_ERROR_MSG("VERIFY", "base_ctx is NULL during verification (file=%s)", file_path);
        if (job_id && strlen(job_id) > 0) send_job_error(job_id, "Verification failed: internal worker context missing");
        return -1;
    }

    fz_context *ctx = fz_clone_context(base_ctx);
    if (!ctx) {
        LOG_ERROR_MSG("VERIFY", "Failed to clone MuPDF context for verification (file=%s)", file_path);
        if (job_id && strlen(job_id) > 0) send_job_error(job_id, "Verification failed: cannot initialize MuPDF context");
        return -1;
    }

    fz_document *doc = NULL;
    struct sigaction old_act, act;
    memset(&old_act, 0, sizeof(old_act));
    memset(&act, 0, sizeof(act));
    act.sa_handler = mupdf_abort_handler;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;

    if (sigsetjmp(mupdf_abort_jmp, 1) != 0) {
        // Recovered from an abort inside MuPDF during verification
        LOG_ERROR_MSG("VERIFY", "MuPDF aborted while verifying %s", file_path);
        if (job_id && strlen(job_id) > 0) {
            send_job_error(job_id, "MuPDF assertion/abort during verification");
        }
        sigaction(SIGABRT, &old_act, NULL);
        if (doc) fz_drop_document(ctx, doc);
        fz_drop_context(ctx);
        return -1;
    }

    if (sigaction(SIGABRT, &act, &old_act) != 0) {
        LOG_WARN_MSG("VERIFY", "Failed to install SIGABRT handler for verification (continuing without abort protection)");
    }

    fz_try(ctx) {
        doc = fz_open_document(ctx, file_path);
        if (!doc) {
            LOG_ERROR_MSG("VERIFY", "Failed to open document during verification: %s", file_path);
            fz_throw(ctx, FZ_ERROR_GENERIC, "Cannot open document (verification)");
        }

        // Count pages as an additional lightweight structural check
        int page_count = fz_count_pages(ctx, doc);
        LOG_INFO_MSG("VERIFY", "Verification open/count succeeded: %s (pages: %d)", file_path, page_count);
    }
    fz_catch(ctx) {
        const char *msg = fz_caught_message(ctx);
        (void)fprintf(stderr, "MuPDF verification error for %s: %s\n", file_path, msg ? msg : "(no message)");

        if (job_id && strlen(job_id) > 0) {
            send_job_error(job_id, msg ? msg : "MuPDF error during verification");
        }

        if (doc) fz_drop_document(ctx, doc);
        sigaction(SIGABRT, &old_act, NULL);
        fz_drop_context(ctx);
        return -1;
    }

    if (doc) fz_drop_document(ctx, doc);
    sigaction(SIGABRT, &old_act, NULL);
    fz_drop_context(ctx);
    return 0;
}

/*
 * Core document parsing using MuPDF
 * Now protected with a SIGABRT handler via sigsetjmp/siglongjmp to prevent process crash
 * If MuPDF triggers abort() (via assertion), we recover and return an error result.
 */
static parse_result_t* parse_document_with_mupdf_filtered(const char *job_id, const char *file_path, const char *image_directory_path, int extract_vector_images, int no_filter) {
    LOG_INFO_MSG("PARSE", "Starting document parsing for file: %s", file_path);

    if (!base_ctx) {
        LOG_FATAL_MSG("PARSE", "base_ctx is NULL when trying to parse %s", file_path);
        fprintf(stderr, "ERROR: base_ctx is NULL when trying to parse %s\n", file_path);
        return NULL;
    }

    // Clone context for thread-safety (each thread needs its own context)
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
        fz_drop_context(ctx);
        return NULL;
    }
    fz_document *doc = NULL;

    // Install SIGABRT handler and use sigsetjmp to recover from abort() inside MuPDF
    struct sigaction old_act, act;
    memset(&old_act, 0, sizeof(old_act));
    memset(&act, 0, sizeof(act));
    act.sa_handler = mupdf_abort_handler;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;

    if (sigsetjmp(mupdf_abort_jmp, 1) != 0) {
        // We landed here due to a siglongjmp from the abort handler (MuPDF called abort)
        LOG_ERROR_MSG("PARSE", "MuPDF aborted while processing %s", file_path);

        if (result->metadata) json_object_put(result->metadata);
        result->metadata = json_object_new_object();
        json_object_object_add(result->metadata, "status", json_object_new_string("error"));
        json_object_object_add(result->metadata, "error", json_object_new_string("MuPDF assertion/abort during parsing"));

        // Notify Airag immediately
        if (job_id && strlen(job_id) > 0) {
            send_job_error(job_id, "MuPDF assertion/abort during parsing");
        }

        // Restore previous handler and cleanup
        sigaction(SIGABRT, &old_act, NULL);
        if (doc) fz_drop_document(ctx, doc);
        fz_drop_context(ctx);
        return result;
    }

    // Install our handler
    if (sigaction(SIGABRT, &act, &old_act) != 0) {
        LOG_WARN_MSG("PARSE", "Failed to install SIGABRT handler (continuing without abort protection)");
    }

    // Now run MuPDF processing inside fz_try/fz_catch
    fz_try(ctx) {
        LOG_INFO_MSG("PARSE", "Opening document with MuPDF: %s", file_path);
        doc = fz_open_document(ctx, file_path);
        if (!doc) {
            LOG_ERROR_MSG("PARSE", "Failed to open document: %s", file_path);
            fz_throw(ctx, FZ_ERROR_GENERIC, "Cannot open document");
        }

        int page_count = fz_count_pages(ctx, doc);
        LOG_INFO_MSG("PARSE", "Document opened successfully: %s (pages: %d)", file_path, page_count);

        const int MAX_PAGES = 5000;
        const int TEST_PAGES_LIMIT = 2;

        if (page_count > MAX_PAGES) {
            LOG_WARN_MSG("PARSE", "Document has excessive pages (%d > %d), limiting to first %d pages for testing: %s", page_count, MAX_PAGES, TEST_PAGES_LIMIT, file_path);
            page_count = TEST_PAGES_LIMIT;
        }

        // Extract content with coordinates - each of these functions uses fz_try internally
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

        result->metadata = json_object_new_object();
        json_object_object_add(result->metadata, "page_count", json_object_new_int(page_count));
        json_object_object_add(result->metadata, "format", json_object_new_string("mupdf"));
        json_object_object_add(result->metadata, "status", json_object_new_string("success"));

        LOG_INFO_MSG("PARSE", "Document parsing completed successfully: %s (text_blocks: %d, paragraphs: %d, images: %d)", file_path, text_block_count, paragraph_count, image_count);
    }
    fz_catch(ctx) {
        const char *msg = fz_caught_message(ctx);
        (void)fprintf(stderr, "MuPDF error processing %s: %s\n", file_path, msg ? msg : "(no message)");

        if (result->metadata) json_object_put(result->metadata);
        result->metadata = json_object_new_object();
        json_object_object_add(result->metadata, "status", json_object_new_string("error"));
        json_object_object_add(result->metadata, "error", json_object_new_string(msg ? msg : "unknown MuPDF error"));

        // Also notify Airag immediately via WebSocket (includes the MuPDF message)
        if (job_id && strlen(job_id) > 0) {
            send_job_error(job_id, msg ? msg : "MuPDF error");
        }
    }

    // Cleanup document if opened
    if (doc) fz_drop_document(ctx, doc);

    // Restore previous SIGABRT handler
    sigaction(SIGABRT, &old_act, NULL);

    // Drop cloned MuPDF context
    fz_drop_context(ctx);

    return result;
}

// Cleanup parse_result
static void cleanup_parse_result(parse_result_t *result) {
    if (!result) {
        return;
    }
    if (result->text_blocks) json_object_put(result->text_blocks);
    if (result->paragraphs) json_object_put(result->paragraphs);
    if (result->images) json_object_put(result->images);
    if (result->metadata) json_object_put(result->metadata);
    free(result);
}

// Signal handler for graceful shutdown
static void signal_handler(int sig) {
    (void)sig;
    /* Avoid non-async-safe functions in signal handlers (printf is not safe).
       Only set the shutdown flag and return — the main loop will perform
       graceful cleanup. */
    shutdown_requested = 1;
}

// Idle cleanup worker and other helpers reused from prior implementation
static void update_last_job_time(void) {
    pthread_mutex_lock(&idle_mutex);
    last_job_time = time(NULL);
    pthread_mutex_unlock(&idle_mutex);
}

static void* idle_cleanup_worker(void *arg) {
    (void)arg;
    const time_t IDLE_THRESHOLD = 3600;
    const int CHECK_INTERVAL = 60; // Check every 60 seconds (1 minute) for faster Redis reconnection
    LOG_INFO_MSG("CLEANUP", "Idle cleanup worker started (threshold: 1 hour, check interval: 1 minute)");
    while (cleanup_thread_running && !shutdown_requested) {
        sleep(CHECK_INTERVAL);
        if (!cleanup_thread_running || shutdown_requested) break;

        // Check and reconnect Redis buffer if needed
        pthread_mutex_lock(&redis_buffer_mutex);
        int buffer_connected = (redis_buffer_ctx != NULL);
        pthread_mutex_unlock(&redis_buffer_mutex);

        if (!buffer_connected) {
            LOG_INFO_MSG("REDIS", "Redis buffer disconnected, attempting reconnection...");
            if (init_redis_buffer() == 0) {
                LOG_INFO_MSG("REDIS", "Redis buffer reconnected successfully");
            } else {
                LOG_WARN_MSG("REDIS", "Redis buffer reconnection failed, will retry in 1 minute");
            }
        }

        pthread_mutex_lock(&idle_mutex);
        time_t current_time = time(NULL);
        time_t last_job = last_job_time;
        pthread_mutex_unlock(&idle_mutex);
        if (last_job == 0) continue;
        time_t idle_seconds = current_time - last_job;
        if (idle_seconds >= IDLE_THRESHOLD) {
            LOG_INFO_MSG("CLEANUP", "Idle for %ld seconds, triggering cleanup", (long)idle_seconds);
            if (base_ctx) fz_shrink_store(base_ctx, 5);
            pthread_mutex_lock(&idle_mutex);
            last_job_time = 0;
            pthread_mutex_unlock(&idle_mutex);
        } else {
            LOG_DEBUG_MSG("CLEANUP", "Active (idle: %ld/%ld seconds)", (long)idle_seconds, (long)IDLE_THRESHOLD);
        }
    }
    LOG_INFO_MSG("CLEANUP", "Idle cleanup worker stopped");
    return NULL;
}