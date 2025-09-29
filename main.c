/*
 * Standalone MuPDF Document Parsing Service
 *
 * A lightweight HTTP service that provides document parsing capabilities
 * using MuPDF. Supports text extraction with spatial coordinates,
 * paragraph-level bounding boxes, and embedded image extraction.
 *
 * Features:
 * - HTTP API for document parsing requests
 * - Asynchronous callback responses
 * - Concurrent request handling
 * - Support for PDF and other MuPDF-supported formats
 * - JSON request/response format
 */

// Enable GNU extensions for strerror_r and other functions
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200112L

// System includes (alphabetical)
#include <errno.h>
#include <pthread.h>
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
#include "civetweb/include/civetweb.h"
#include "monitoring.h"
#include "redis_queue.h"

#define MAX_WORKERS 8
#define PORT "8080"
#define BUFFER_SIZE 4096

// Configuration structure for thread-safe environment variable access
typedef struct {
    char uploads_path[1024];
    long callback_timeout;
    int vector_extraction_enabled;
} worker_config_t;

// Redis configuration structure is defined in redis_queue.h

// Global configuration loaded at startup
static worker_config_t worker_config = {0};
static redis_config_t redis_config_cached = {0};

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

    // Initialize callback timeout
    const char *timeout_str = getenv("WORKER_CALLBACK_TIMEOUT");
    if (timeout_str) {
        char *endptr = NULL;
        long timeout = strtol(timeout_str, &endptr, 10);
        if (*endptr == '\0' && timeout > 0 && timeout <= 300) {
            worker_config.callback_timeout = timeout;
        } else {
            worker_config.callback_timeout = 30; // Default 30 seconds
        }
    } else {
        worker_config.callback_timeout = 30; // Default 30 seconds
    }

    // Initialize vector extraction setting
    const char *vector_enabled = getenv("RAG_VECTOR_EXTRACTION_ENABLED");
    worker_config.vector_extraction_enabled = (vector_enabled && strcmp(vector_enabled, "true") == 0) ? 1 : 0;

    // Log configuration for debugging
    LOG_INFO_MSG("INIT", "Worker configuration loaded:");
    LOG_INFO_MSG("INIT", "  uploads_path: %s", worker_config.uploads_path);
    LOG_INFO_MSG("INIT", "  callback_timeout: %ld", worker_config.callback_timeout);
    LOG_INFO_MSG("INIT", "  vector_extraction_enabled: %s", worker_config.vector_extraction_enabled ? "true" : "false");
}

// Thread-safe getter functions
static const char* get_uploads_path_safe(void) {
    return worker_config.uploads_path;
}

static long get_callback_timeout_safe(void) {
    return worker_config.callback_timeout;
}

static int get_vector_extraction_enabled_safe(void) {
    return worker_config.vector_extraction_enabled;
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
    char file_path[1024];
    char callback_url[1024];
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

// Function declarations
static int parse_request_handler(struct mg_connection *conn, void *cbdata);
static int health_handler(struct mg_connection *conn, void *cbdata);
static int metrics_handler(struct mg_connection *conn, void *cbdata);
static void* process_document_job(void *arg);
static parse_result_t* parse_document_with_mupdf_filtered(const char *file_path, const char *image_directory_path, int extract_vector_images, int no_filter);
static void send_callback_response(const char *callback_url, const char *job_id, parse_result_t *result);
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

// File-based callback functions
static int create_results_directory_for_org(const char *image_directory_path);
static int write_result_to_file(const char *job_id, const char *json_data, const char *image_directory_path);
static void send_callback_with_file(const char *callback_url, const char *job_id, parse_result_t *result, const char *image_directory_path);

int main(int argc __attribute__((unused)), char **argv __attribute__((unused))) {
    LOG_INFO_MSG("MAIN", "Starting MuPDF Document Parsing Service on port %s", PORT);

    // Initialize worker configuration from environment variables
    init_worker_config();

    // Initialize Redis configuration from environment variables
    init_redis_config();

    // Setup signal handlers for graceful shutdown
    (void)signal(SIGINT, signal_handler);
    (void)signal(SIGTERM, signal_handler);

    // Initialize monitoring system
    if (monitoring_init() != 0) {
        (void)fprintf(stderr, "Failed to initialize monitoring system\n");
        return 1;
    }

    // Initialize MuPDF
    base_ctx = fz_new_context(NULL, NULL, FZ_STORE_DEFAULT);
    if (!base_ctx) {
        LOG_FATAL_MSG("MAIN", "Failed to initialize MuPDF context");
        monitoring_cleanup();
        return 1;
    }

    // Register document handlers
    fz_register_document_handlers(base_ctx);

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

    // Configure CivetWeb options
    const char *options[] = {
        "listening_ports", "0.0.0.0:8080",
        "num_threads", "8",
        "request_timeout_ms", "30000",
        NULL
    };

    // Set up HTTP request handlers
    struct mg_callbacks callbacks;
    memset(&callbacks, 0, sizeof(callbacks));

    // Start the web server
    struct mg_context *ctx = mg_start(&callbacks, NULL, options);
    if (!ctx) {
        (void)fprintf(stderr, "Failed to start HTTP server\n");
        fz_drop_context(base_ctx);
        return 1;
    }

    // Register endpoint handlers
    mg_set_request_handler(ctx, "/parse", parse_request_handler, NULL);
    mg_set_request_handler(ctx, "/health", health_handler, NULL);
    mg_set_request_handler(ctx, "/metrics", metrics_handler, NULL);

    // HTTP service is now ready
    health_update_component_status(1, redis_consumer ? 1 : 0, 1); // Update HTTP status
    health_update_status("healthy");

    LOG_INFO_MSG("MAIN", "MuPDF Service ready! Endpoints:");
    LOG_INFO_MSG("MAIN", "  POST /parse    - Parse document (requires: file_path, job_id, callback_url)");
    LOG_INFO_MSG("MAIN", "  GET  /health   - Production health check with metrics");
    LOG_INFO_MSG("MAIN", "  GET  /metrics  - Detailed performance metrics");
    if (redis_consumer) {
        LOG_INFO_MSG("MAIN", "  Redis Queue   - Consumer active (queue=worker_jobs)");
    }
    LOG_INFO_MSG("MAIN", "Service running... (Ctrl+C to shutdown)");

    // Keep the service running - wait for termination signal
    while (!shutdown_requested) {
        sleep(1);
    }

    LOG_INFO_MSG("MAIN", "Shutdown requested, cleaning up...");

    // Cleanup Redis consumer
    cleanup_redis_consumer();

    // Cleanup HTTP server
    mg_stop(ctx);
    curl_global_cleanup();
    fz_drop_context(base_ctx);

    // Cleanup monitoring system
    monitoring_cleanup();

    LOG_INFO_MSG("MAIN", "MuPDF Service shutdown complete");
    return 0;
}

/*
 * HTTP request handler for document parsing
 * Expected JSON payload:
 * {
 *   "file_path": "/path/to/document.pdf",
 *   "job_id": "unique-job-identifier",
 *   "callback_url": "http://rag-service/callback",
 *   "image_directory_path": "/app/uploads/images/default"
 * }
 */
static int parse_request_handler(struct mg_connection *conn, void *cbdata __attribute__((unused))) {
    metrics_http_request_received();

    if (strcmp(mg_get_request_info(conn)->request_method, "POST") != 0) {
        mg_printf(conn, "HTTP/1.1 405 Method Not Allowed\r\n"
                        "Content-Type: application/json\r\n\r\n"
                        "{\"error\": \"Only POST method allowed\"}\n");
        LOG_WARN_MSG("HTTP", "Invalid request method: %s", mg_get_request_info(conn)->request_method);
        return 1;
    }

    // Read request body
    char buffer[BUFFER_SIZE];
    int data_len = mg_read(conn, buffer, sizeof(buffer) - 1);
    if (data_len <= 0) {
        mg_printf(conn, "HTTP/1.1 400 Bad Request\r\n"
                        "Content-Type: application/json\r\n\r\n"
                        "{\"error\": \"No request body\"}\n");
        return 1;
    }
    buffer[data_len] = '\0';

    // Parse JSON request
    json_object *json = json_tokener_parse(buffer);
    if (!json) {
        mg_printf(conn, "HTTP/1.1 400 Bad Request\r\n"
                        "Content-Type: application/json\r\n\r\n"
                        "{\"error\": \"Invalid JSON\"}\n");
        return 1;
    }

    // Extract required fields
    json_object *file_path_obj = NULL;
    json_object *job_id_obj = NULL;
    json_object *callback_url_obj = NULL;
    json_object *image_directory_path_obj = NULL;
    if (!json_object_object_get_ex(json, "file_path", &file_path_obj) ||
        !json_object_object_get_ex(json, "job_id", &job_id_obj) ||
        !json_object_object_get_ex(json, "callback_url", &callback_url_obj) ||
        !json_object_object_get_ex(json, "image_directory_path", &image_directory_path_obj)) {

        mg_printf(conn, "HTTP/1.1 400 Bad Request\r\n"
                        "Content-Type: application/json\r\n\r\n"
                        "{\"error\": \"Missing required fields: file_path, job_id, callback_url, image_directory_path\"}\n");
        json_object_put(json);
        return 1;
    }

    // Extract string values
    const char *image_directory_path = json_object_get_string(image_directory_path_obj);

    // Extract optional vector extraction flag
    json_object *vector_extraction_obj = NULL;
    int extract_vector_images = get_vector_extraction_enabled_safe(); // Use environment default
    if (json_object_object_get_ex(json, "extract_vector_images", &vector_extraction_obj)) {
        extract_vector_images = json_object_get_boolean(vector_extraction_obj);
    }

    // Extract optional no_filter flag (defaults to false - filtering enabled)
    json_object *no_filter_obj = NULL;
    int no_filter = 0; // By default, image filtering is enabled
    if (json_object_object_get_ex(json, "no_filter", &no_filter_obj)) {
        no_filter = json_object_get_boolean(no_filter_obj);
    }

    // Create job for async processing
    parse_job_t *job = malloc(sizeof(parse_job_t));
    strncpy(job->file_path, json_object_get_string(file_path_obj), sizeof(job->file_path) - 1);
    strncpy(job->job_id, json_object_get_string(job_id_obj), sizeof(job->job_id) - 1);
    strncpy(job->callback_url, json_object_get_string(callback_url_obj), sizeof(job->callback_url) - 1);
    strncpy(job->image_directory_path, json_object_get_string(image_directory_path_obj), sizeof(job->image_directory_path) - 1);
    job->image_directory_path[sizeof(job->image_directory_path) - 1] = '\0';
    job->extract_vector_images = extract_vector_images;
    job->no_filter = no_filter;

    // Start async processing
    if (pthread_create(&job->thread, NULL, process_document_job, job) != 0) {
        mg_printf(conn, "HTTP/1.1 500 Internal Server Error\r\n"
                        "Content-Type: application/json\r\n\r\n"
                        "{\"error\": \"Failed to start processing job\"}\n");
        free(job);
        json_object_put(json);
        return 1;
    }

    // Detach thread for cleanup
    pthread_detach(job->thread);

    // Send immediate response
    mg_printf(conn, "HTTP/1.1 202 Accepted\r\n"
                    "Content-Type: application/json\r\n\r\n"
                    "{\"status\": \"accepted\", \"job_id\": \"%s\"}\n",
                    json_object_get_string(job_id_obj));

    metrics_http_request_completed();
    LOG_INFO_MSG("HTTP", "Job accepted: %s", json_object_get_string(job_id_obj));

    json_object_put(json);
    return 1;
}

/*
 * Async document processing worker
 */
static void* process_document_job(void *arg) {
    parse_job_t *job = (parse_job_t*)arg;

    metrics_job_started();
    log_job_start(job->job_id, job->file_path);

    struct timeval start_time;
    struct timeval end_time;
    (void)gettimeofday(&start_time, NULL);

    // Parse document with MuPDF
    parse_result_t *result = parse_document_with_mupdf_filtered(job->file_path, job->image_directory_path, job->extract_vector_images, job->no_filter);

    // Calculate processing time
    (void)gettimeofday(&end_time, NULL);
    uint64_t processing_time_ms = ((end_time.tv_sec - start_time.tv_sec) * 1000) +
                                  ((end_time.tv_usec - start_time.tv_usec) / 1000);

    // Send results via file-based callback
    send_callback_with_file(job->callback_url, job->job_id, result, job->image_directory_path);

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

    return NULL;
}

/*
 * Core document parsing using MuPDF
 * Returns structured document data with coordinates
 */
static parse_result_t* parse_document_with_mupdf_filtered(const char *file_path, const char *image_directory_path, int extract_vector_images, int no_filter) {
    LOG_INFO_MSG("PARSE", "Starting document parsing for file: %s", file_path);

    // Use global context directly (single-threaded processing)
    if (!base_ctx) {
        LOG_FATAL_MSG("PARSE", "base_ctx is NULL when trying to parse %s", file_path);
        fprintf(stderr, "ERROR: base_ctx is NULL when trying to parse %s\n", file_path);
        return NULL;
    }

    fz_context *ctx = base_ctx; // Use base context directly instead of cloning
    LOG_DEBUG_MSG("PARSE", "Using base MuPDF context for parsing: %s", file_path);

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
        result->images = extract_images_filtered(base_ctx, doc, image_directory_path, extract_vector_images, no_filter);
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
    // Note: Not dropping ctx since we're using base_ctx directly

    return result;
}


/*
 * Send processing results back to RAG service via HTTP callback
 */
static void send_callback_response(const char *callback_url, const char *job_id, parse_result_t *result) {
    LOG_INFO_MSG("CALLBACK", "Preparing to send callback for job %s to %s", job_id, callback_url);

    CURL *curl = curl_easy_init();
    if (!curl) {
        LOG_ERROR_MSG("CALLBACK", "Failed to initialize CURL for callback (job: %s)", job_id);
        fprintf(stderr, "Failed to initialize CURL for callback\n");
        return;
    }

    // Build response JSON
    LOG_DEBUG_MSG("CALLBACK", "Building JSON response for job %s", job_id);
    // Create JSON response
    json_object *response = json_object_new_object();
    (void)json_object_object_add(response, "timestamp", json_object_new_int64((int64_t)time(NULL)));
    json_object_object_add(response, "job_id", json_object_new_string(job_id));

    int text_block_count = 0;
    int paragraph_count = 0;
    int image_count = 0;
    const char *status = "unknown";

    if (result) {
        LOG_DEBUG_MSG("CALLBACK", "Result object available for job %s", job_id);

        if (result->text_blocks) {
            text_block_count = (int)json_object_array_length(result->text_blocks);
            json_object_object_add(response, "text_blocks", json_object_get(result->text_blocks));
            LOG_DEBUG_MSG("CALLBACK", "Added %d text blocks to response for job %s", text_block_count, job_id);
        }

        if (result->paragraphs) {
            paragraph_count = (int)json_object_array_length(result->paragraphs);
            json_object_object_add(response, "paragraphs", json_object_get(result->paragraphs));
            LOG_DEBUG_MSG("CALLBACK", "Added %d paragraphs to response for job %s", paragraph_count, job_id);
        }

        if (result->images) {
            image_count = (int)json_object_array_length(result->images);
            json_object_object_add(response, "images", json_object_get(result->images));
            LOG_DEBUG_MSG("CALLBACK", "Added %d images to response for job %s", image_count, job_id);
        }
        if (result->metadata) {
            json_object_object_add(response, "metadata", json_object_get(result->metadata));

            // Extract status from metadata for logging
            json_object *status_obj = NULL;
            if (json_object_object_get_ex(result->metadata, "status", &status_obj)) {
                status = json_object_get_string(status_obj);
            }
            LOG_DEBUG_MSG("CALLBACK", "Added metadata to response for job %s (status: %s)", job_id, status);
        }
    } else {
        LOG_WARN_MSG("CALLBACK", "No result object for job %s", job_id);
    }

    const char *json_string = json_object_to_json_string(response);
    size_t json_length = strlen(json_string);

    LOG_INFO_MSG("CALLBACK", "Sending callback for job %s: status=%s, text_blocks=%d, paragraphs=%d, images=%d, json_size=%zu",
                 job_id, status, text_block_count, paragraph_count, image_count, json_length);

    // Configure CURL for callback
    curl_easy_setopt(curl, CURLOPT_URL, callback_url);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_string);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)json_length);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, get_callback_timeout_safe()); // Configurable timeout for airag processing

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    LOG_DEBUG_MSG("CALLBACK", "Executing HTTP POST to %s for job %s", callback_url, job_id);

    // Send callback
    CURLcode res = curl_easy_perform(curl);

    long response_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

    if (res != CURLE_OK) {
        LOG_ERROR_MSG("CALLBACK", "Callback failed for job %s: %s (URL: %s)",
                      job_id, curl_easy_strerror(res), callback_url);
        fprintf(stderr, "Callback failed: %s\n", curl_easy_strerror(res));
    } else {
        LOG_INFO_MSG("CALLBACK", "Callback sent successfully for job %s: HTTP %ld (status: %s)",
                     job_id, response_code, status);
        printf("Callback sent for job %s (HTTP %ld)\n", job_id, response_code);
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    (void)json_object_put(response);
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

    if (result && result->metadata) {
        json_object *status_obj = NULL;
        if (json_object_object_get_ex(result->metadata, "status", &status_obj)) {
            const char *status = json_object_get_string(status_obj);
            if (strcmp(status, "success") == 0) {
                // Send results via file-based callback
                send_callback_with_file(job->callback_url, job->job_id, result, job->image_directory_path);

                metrics_job_completed(processing_time_ms);
                metrics_redis_job_acknowledged();
                log_job_complete(job->job_id, processing_time_ms);

                // Cleanup
                cleanup_parse_result(result);
                return 0;
            }
        }
    }

    // Job failed
    const char *error = "Document processing failed";
    if (result && result->metadata) {
        json_object *error_obj = NULL;
        if (json_object_object_get_ex(result->metadata, "error", &error_obj)) {
            error = json_object_get_string(error_obj);
        }
    }

    metrics_job_failed(processing_time_ms, error);
    log_job_error(job->job_id, error);

    // Still send callback even for failed jobs
    send_callback_with_file(job->callback_url, job->job_id, result, job->image_directory_path);

    // Cleanup
    if (result) {
        cleanup_parse_result(result);
    }
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

    // Start consumer
    if (redis_consumer_start(redis_consumer) != 0) {
        fprintf(stderr, "[Redis] Failed to start consumer\n");
        redis_consumer_destroy(redis_consumer);
        redis_consumer = NULL;
        return -1;
    }

    printf("[Redis] Consumer started successfully\n");
    return 0;
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
 * Health check endpoint handler
 */
static int health_handler(struct mg_connection *conn, void *cbdata __attribute__((unused))) {
    health_response_t *response = generate_health_response();
    if (!response || !response->json_response) {
        mg_printf(conn, "HTTP/1.1 500 Internal Server Error\r\n"
                        "Content-Type: application/json\r\n\r\n"
                        "{\"error\": \"Failed to generate health response\"}\n");
        return 1;
    }

    mg_printf(conn, "HTTP/1.1 200 OK\r\n"
                    "Content-Type: application/json\r\n"
                    "Content-Length: %zu\r\n\r\n%s",
                    response->response_size, response->json_response);

    free_health_response(response);
    return 1;
}

/*
 * Metrics endpoint handler
 */
static int metrics_handler(struct mg_connection *conn, void *cbdata __attribute__((unused))) {
    // Update resource usage before generating response
    metrics_update_resource_usage();

    metrics_response_t *response = generate_metrics_response();
    if (!response || !response->json_response) {
        mg_printf(conn, "HTTP/1.1 500 Internal Server Error\r\n"
                        "Content-Type: application/json\r\n\r\n"
                        "{\"error\": \"Failed to generate metrics response\"}\n");
        return 1;
    }

    mg_printf(conn, "HTTP/1.1 200 OK\r\n"
                    "Content-Type: application/json\r\n"
                    "Content-Length: %zu\r\n\r\n%s",
                    response->response_size, response->json_response);

    free_metrics_response(response);
    return 1;
}

/*
 * Recursively create directory path
 */
static int create_directory_recursive(const char *path) {
    char dir_path[512];
    char *p = NULL;
    size_t len;

    // Copy path to work with
    strncpy(dir_path, path, sizeof(dir_path) - 1);
    dir_path[sizeof(dir_path) - 1] = '\0';
    len = strlen(dir_path);

    // Remove trailing slash if present
    if (len > 0 && dir_path[len - 1] == '/') {
        dir_path[len - 1] = '\0';
    }

    // Create directories recursively
    for (p = dir_path + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(dir_path, 0755) != 0 && errno != EEXIST) {
                char error_buf[256];
                strerror_r(errno, error_buf, sizeof(error_buf));
                LOG_ERROR_MSG("INIT", "Failed to create directory %s: %s", dir_path, error_buf);
                return -1;
            }
            *p = '/';
        }
    }

    // Create final directory
    if (mkdir(dir_path, 0755) != 0 && errno != EEXIST) {
        char error_buf[256];
        strerror_r(errno, error_buf, sizeof(error_buf));
        LOG_ERROR_MSG("INIT", "Failed to create directory %s: %s", dir_path, error_buf);
        return -1;
    }

    return 0;
}

/*
 * Create results directory for file-based callbacks using organization path
 */
static int create_results_directory_for_org(const char *image_directory_path) {
    char results_dir[512];
    get_results_path(results_dir, sizeof(results_dir), image_directory_path);

    // Create the full directory path recursively
    if (create_directory_recursive(results_dir) != 0) {
        return -1;
    }

    LOG_INFO_MSG("CALLBACK", "Results directory ready for organization: %s", results_dir);
    return 0;
}

/*
 * Write result JSON to file in shared volume
 */
static int write_result_to_file(const char *job_id, const char *json_data, const char *image_directory_path) {
    char filepath[1024];
    char results_dir[512];
    get_results_path(results_dir, sizeof(results_dir), image_directory_path);
    snprintf(filepath, sizeof(filepath), "%s/%s.json", results_dir, job_id);

    LOG_INFO_MSG("CALLBACK", "Writing result to file: %s", filepath);

    FILE *file = fopen(filepath, "w");
    if (!file) {
        LOG_ERROR_MSG("CALLBACK", "Failed to create result file: %s", filepath);
        return -1;
    }

    size_t written = fwrite(json_data, 1, strlen(json_data), file);
    if (fclose(file) != 0) {
        LOG_ERROR_MSG("CALLBACK", "Failed to close result file");
    }

    if (written != strlen(json_data)) {
        LOG_ERROR_MSG("CALLBACK", "Incomplete write to result file");
        if (unlink(filepath) != 0) {
            LOG_ERROR_MSG("CALLBACK", "Failed to delete incomplete file");
        }
        return -1;
    }

    LOG_INFO_MSG("CALLBACK", "Result file written successfully: %s (%zu bytes)",
                 filepath, written);
    return 0;
}

/*
 * Send callback with file reference instead of full JSON payload
 */
static void send_callback_with_file(const char *callback_url, const char *job_id, parse_result_t *result, const char *image_directory_path) {
    LOG_INFO_MSG("CALLBACK", "Preparing file-based callback for job %s", job_id);
    
    // Ensure results directory exists for this organization
    if (create_results_directory_for_org(image_directory_path) != 0) {
        LOG_ERROR_MSG("CALLBACK", "Failed to create results directory, falling back to HTTP");
        send_callback_response(callback_url, job_id, result);
        return;
    }

    // Build full result JSON as before
    json_object *full_result = json_object_new_object();
    json_object_object_add(full_result, "job_id", json_object_new_string(job_id));

    if (result) {
        if (result->text_blocks) {
            json_object_object_add(full_result, "text_blocks", json_object_get(result->text_blocks));
        }
        if (result->paragraphs) {
            json_object_object_add(full_result, "paragraphs", json_object_get(result->paragraphs));
        }
        if (result->images) {
            json_object_object_add(full_result, "images", json_object_get(result->images));
        }
        if (result->metadata) {
            json_object_object_add(full_result, "metadata", json_object_get(result->metadata));
        }
    }

    const char *full_json = json_object_to_json_string(full_result);
    size_t json_size = strlen(full_json);

    // Write full result to file
    if (write_result_to_file(job_id, full_json, image_directory_path) != 0) {
        LOG_ERROR_MSG("CALLBACK", "Failed to write result file, falling back to HTTP");
        send_callback_response(callback_url, job_id, result); // Fallback to original HTTP
        json_object_put(full_result);
        return;
    }

    // Create minimal HTTP callback with file reference
    json_object *callback = json_object_new_object();
    if (!callback) {
        LOG_ERROR_MSG("CALLBACK", "Failed to create callback JSON object");
        json_object_put(full_result);
        return;
    }

    json_object_object_add(callback, "job_id", json_object_new_string(job_id));
    json_object_object_add(callback, "status", json_object_new_string("success"));
    json_object_object_add(callback, "result_file",
                          json_object_new_string(job_id)); // Just job_id, airag will construct full path
    json_object_object_add(callback, "result_size", json_object_new_int64((int64_t)json_size));

    LOG_INFO_MSG("CALLBACK", "DEBUG: Created callback JSON with result_file='%s', result_size=%zu", job_id, json_size);

    // Add metadata for quick access
    if (result && result->metadata) {
        json_object_object_add(callback, "metadata", json_object_get(result->metadata));
    }

    const char *callback_json = json_object_to_json_string(callback);
    size_t callback_json_len = strlen(callback_json);

    LOG_INFO_MSG("CALLBACK", "Sending file-based callback: job=%s, file_size=%zu, callback_size=%zu",
                 job_id, json_size, callback_json_len);
    LOG_INFO_MSG("CALLBACK", "DEBUG: Callback JSON payload: %s", callback_json);

    // Ensure job_id and json_size are valid
    LOG_INFO_MSG("CALLBACK", "DEBUG: Values - job_id='%s', json_size=%zu", job_id, json_size);

    // Send minimal HTTP callback (should be <1KB vs 11MB+)
    CURL *curl = curl_easy_init();
    if (!curl) {
        LOG_ERROR_MSG("CALLBACK", "Failed to initialize CURL");
        json_object_put(full_result);
        json_object_put(callback);
        return;
    }

    curl_easy_setopt(curl, CURLOPT_URL, callback_url);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, callback_json);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)callback_json_len);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, get_callback_timeout_safe()); // Configurable timeout for airag processing

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    CURLcode res = curl_easy_perform(curl);
    long response_code = 0;
    (void)curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

    if (res != CURLE_OK) {
        LOG_ERROR_MSG("CALLBACK", "File-based callback failed: %s", curl_easy_strerror(res));
    } else {
        LOG_INFO_MSG("CALLBACK", "File-based callback sent successfully: job=%s, HTTP %ld",
                     job_id, response_code);
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    json_object_put(full_result);
    json_object_put(callback);
}