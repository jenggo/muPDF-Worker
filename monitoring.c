/*
 * Production Monitoring Implementation
 *
 * Provides comprehensive metrics collection and health monitoring
 * for the MuPDF worker service.
 */

#include "monitoring.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdarg.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>
#include <json-c/json.h>

// Global instances
worker_metrics_t *g_metrics = NULL;
worker_health_t *g_health = NULL;

// Global log level (default: INFO)
static log_level_t g_current_log_level = LOG_INFO;

// Internal utility functions
static uint64_t get_current_time_ms(void);
static const char* log_level_to_string(log_level_t level);
static void update_average_processing_time(uint64_t new_time_ms);

/*
 * Initialize monitoring system
 */
int monitoring_init(void) {
    // Initialize metrics
    g_metrics = calloc(1, sizeof(worker_metrics_t));
    if (!g_metrics) {
        fprintf(stderr, "[Monitoring] Failed to allocate metrics structure\n");
        return -1;
    }

    if (pthread_mutex_init(&g_metrics->mutex, NULL) != 0) {
        fprintf(stderr, "[Monitoring] Failed to initialize metrics mutex\n");
        free(g_metrics);
        g_metrics = NULL;
        return -1;
    }

    // Initialize health
    g_health = calloc(1, sizeof(worker_health_t));
    if (!g_health) {
        fprintf(stderr, "[Monitoring] Failed to allocate health structure\n");
        pthread_mutex_destroy(&g_metrics->mutex);
        free(g_metrics);
        g_metrics = NULL;
        return -1;
    }

    if (pthread_mutex_init(&g_health->mutex, NULL) != 0) {
        fprintf(stderr, "[Monitoring] Failed to initialize health mutex\n");
        free(g_health);
        g_health = NULL;
        pthread_mutex_destroy(&g_metrics->mutex);
        free(g_metrics);
        g_metrics = NULL;
        return -1;
    }

    // Initialize values
    g_metrics->start_time = time(NULL);
    g_metrics->last_updated = g_metrics->start_time;
    g_metrics->min_processing_time_ms = UINT64_MAX;

    strncpy(g_health->status, "starting", sizeof(g_health->status) - 1);
    strncpy(g_health->version, "1.0.0", sizeof(g_health->version) - 1);
    g_health->last_health_check = time(NULL);

    printf("[Monitoring] System initialized\n");
    return 0;
}

/*
 * Cleanup monitoring system
 */
void monitoring_cleanup(void) {
    if (g_metrics) {
        pthread_mutex_destroy(&g_metrics->mutex);
        free(g_metrics);
        g_metrics = NULL;
    }

    if (g_health) {
        pthread_mutex_destroy(&g_health->mutex);
        free(g_health);
        g_health = NULL;
    }

    printf("[Monitoring] System cleanup complete\n");
}

/*
 * Record job started
 */
void metrics_job_started(void) {
    if (!g_metrics) return;

    pthread_mutex_lock(&g_metrics->mutex);
    g_metrics->total_jobs_processed++;
    g_metrics->last_updated = time(NULL);
    pthread_mutex_unlock(&g_metrics->mutex);
}

/*
 * Record job completed
 */
void metrics_job_completed(uint64_t processing_time_ms) {
    if (!g_metrics) return;

    pthread_mutex_lock(&g_metrics->mutex);
    g_metrics->jobs_completed++;
    g_metrics->total_processing_time_ms += processing_time_ms;

    // Update min/max times
    if (processing_time_ms < g_metrics->min_processing_time_ms) {
        g_metrics->min_processing_time_ms = processing_time_ms;
    }
    if (processing_time_ms > g_metrics->max_processing_time_ms) {
        g_metrics->max_processing_time_ms = processing_time_ms;
    }

    update_average_processing_time(processing_time_ms);
    g_metrics->last_updated = time(NULL);
    pthread_mutex_unlock(&g_metrics->mutex);
}

/*
 * Record job failed
 */
void metrics_job_failed(uint64_t processing_time_ms, const char *error) {
    if (!g_metrics) return;

    pthread_mutex_lock(&g_metrics->mutex);
    g_metrics->jobs_failed++;
    if (processing_time_ms > 0) {
        g_metrics->total_processing_time_ms += processing_time_ms;
        update_average_processing_time(processing_time_ms);
    }
    g_metrics->last_updated = time(NULL);
    pthread_mutex_unlock(&g_metrics->mutex);

    // Update health with error
    if (error) {
        health_set_error(error);
    }
}

/*
 * Record job timeout
 */
void metrics_job_timeout(uint64_t processing_time_ms) {
    if (!g_metrics) return;

    pthread_mutex_lock(&g_metrics->mutex);
    g_metrics->jobs_timeout++;
    if (processing_time_ms > 0) {
        g_metrics->total_processing_time_ms += processing_time_ms;
    }
    g_metrics->last_updated = time(NULL);
    pthread_mutex_unlock(&g_metrics->mutex);

    health_set_error("Job processing timeout");
}

/*
 * Record Redis job received
 */
void metrics_redis_job_received(void) {
    if (!g_metrics) return;

    pthread_mutex_lock(&g_metrics->mutex);
    g_metrics->redis_jobs_received++;
    g_metrics->last_updated = time(NULL);
    pthread_mutex_unlock(&g_metrics->mutex);
}

/*
 * Record Redis job acknowledged
 */
void metrics_redis_job_acknowledged(void) {
    if (!g_metrics) return;

    pthread_mutex_lock(&g_metrics->mutex);
    g_metrics->redis_jobs_acknowledged++;
    g_metrics->last_updated = time(NULL);
    pthread_mutex_unlock(&g_metrics->mutex);
}

/*
 * Record Redis connection error
 */
void metrics_redis_connection_error(void) {
    if (!g_metrics) return;

    pthread_mutex_lock(&g_metrics->mutex);
    g_metrics->redis_connection_errors++;
    g_metrics->last_updated = time(NULL);
    pthread_mutex_unlock(&g_metrics->mutex);

    health_set_error("Redis connection error");
}

/*
 * Record HTTP request received
 */
void metrics_http_request_received(void) {
    if (!g_metrics) return;

    pthread_mutex_lock(&g_metrics->mutex);
    g_metrics->http_requests_received++;
    g_metrics->last_updated = time(NULL);
    pthread_mutex_unlock(&g_metrics->mutex);
}

/*
 * Record HTTP request completed
 */
void metrics_http_request_completed(void) {
    if (!g_metrics) return;

    pthread_mutex_lock(&g_metrics->mutex);
    g_metrics->http_requests_completed++;
    g_metrics->last_updated = time(NULL);
    pthread_mutex_unlock(&g_metrics->mutex);
}

/*
 * Update resource usage metrics
 */
void metrics_update_resource_usage(void) {
    if (!g_metrics) return;

    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
        pthread_mutex_lock(&g_metrics->mutex);

        // Memory usage (RSS in KB converted to bytes)
        g_metrics->memory_usage_bytes = usage.ru_maxrss * 1024;

        g_metrics->last_updated = time(NULL);
        pthread_mutex_unlock(&g_metrics->mutex);
    }
}

/*
 * Update health status
 */
void health_update_status(const char *status) {
    if (!g_health || !status) return;

    pthread_mutex_lock(&g_health->mutex);
    strncpy(g_health->status, status, sizeof(g_health->status) - 1);
    g_health->last_health_check = time(NULL);
    pthread_mutex_unlock(&g_health->mutex);
}

/*
 * Set health error
 */
void health_set_error(const char *error) {
    if (!g_health || !error) return;

    pthread_mutex_lock(&g_health->mutex);
    strncpy(g_health->last_error, error, sizeof(g_health->last_error) - 1);
    g_health->last_error_time = time(NULL);

    // Downgrade status if not already unhealthy
    if (strcmp(g_health->status, "unhealthy") != 0) {
        if (strcmp(g_health->status, "degraded") != 0) {
            strncpy(g_health->status, "degraded", sizeof(g_health->status) - 1);
        }
    }

    pthread_mutex_unlock(&g_health->mutex);
}

/*
 * Clear health error
 */
void health_clear_error(void) {
    if (!g_health) return;

    pthread_mutex_lock(&g_health->mutex);
    memset(g_health->last_error, 0, sizeof(g_health->last_error));
    g_health->last_error_time = 0;

    // Upgrade status if no errors
    if (strlen(g_health->last_error) == 0) {
        strncpy(g_health->status, "healthy", sizeof(g_health->status) - 1);
    }

    pthread_mutex_unlock(&g_health->mutex);
}

/*
 * Update component status
 */
void health_update_component_status(int mupdf_ok, int redis_ok, int http_ok) {
    if (!g_health) return;

    pthread_mutex_lock(&g_health->mutex);
    g_health->mupdf_status = mupdf_ok;
    g_health->redis_status = redis_ok;
    g_health->http_status = http_ok;
    g_health->last_health_check = time(NULL);

    // Update overall status based on components
    if (!mupdf_ok || !http_ok) {
        strncpy(g_health->status, "unhealthy", sizeof(g_health->status) - 1);
    } else if (!redis_ok) {
        strncpy(g_health->status, "degraded", sizeof(g_health->status) - 1);
    } else if (strlen(g_health->last_error) == 0) {
        strncpy(g_health->status, "healthy", sizeof(g_health->status) - 1);
    }

    pthread_mutex_unlock(&g_health->mutex);
}

/*
 * Calculate success rate
 */
double health_calculate_success_rate(void) {
    if (!g_metrics) return 0.0;

    pthread_mutex_lock(&g_metrics->mutex);
    uint64_t total = g_metrics->jobs_completed + g_metrics->jobs_failed + g_metrics->jobs_timeout;
    double rate = 0.0;

    if (total > 0) {
        rate = (double)g_metrics->jobs_completed / total * 100.0;
    }

    pthread_mutex_unlock(&g_metrics->mutex);
    return rate;
}

/*
 * Enhanced logging with structured format
 */
void log_message(log_level_t level, const char *component, const char *format, ...) {
    if (!component || !format) return;

    // Filter by log level
    if (level < g_current_log_level) return;

    // Get timestamp
    struct timeval tv;
    gettimeofday(&tv, NULL);

    struct tm *tm_info = localtime(&tv.tv_sec);
    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_info);

    // Format message
    va_list args;
    va_start(args, format);
    char message[1024];
    vsnprintf(message, sizeof(message), format, args);
    va_end(args);

    // Print structured log
    printf("[%s.%03ld] [%s] [%s] %s\n",
           timestamp, tv.tv_usec / 1000,
           log_level_to_string(level),
           component,
           message);

    // Flush for immediate output
    fflush(stdout);
}

/*
 * Job lifecycle logging
 */
void log_job_start(const char *job_id, const char *file_path) {
    log_message(LOG_INFO, "JOB", "Started: %s -> %s", job_id, file_path);
}

void log_job_complete(const char *job_id, uint64_t processing_time_ms) {
    log_message(LOG_INFO, "JOB", "Completed: %s (%.2fs)",
                job_id, processing_time_ms / 1000.0);
}

void log_job_error(const char *job_id, const char *error) {
    log_message(LOG_ERROR, "JOB", "Failed: %s - %s", job_id, error);
}

void log_redis_event(const char *event, const char *details) {
    log_message(LOG_INFO, "REDIS", "%s: %s", event, details);
}

/*
 * Set current log level
 */
void set_log_level(log_level_t level) {
    g_current_log_level = level;
    log_message(LOG_INFO, "LOG", "Log level set to %s", log_level_to_string(level));
}

/*
 * Parse log level from string (case-insensitive)
 * Returns LOG_INFO if string is invalid
 */
log_level_t parse_log_level(const char *level_str) {
    if (!level_str) return LOG_INFO;

    if (strcasecmp(level_str, "debug") == 0) return LOG_DEBUG;
    if (strcasecmp(level_str, "info") == 0) return LOG_INFO;
    if (strcasecmp(level_str, "warn") == 0 || strcasecmp(level_str, "warning") == 0) return LOG_WARN;
    if (strcasecmp(level_str, "error") == 0) return LOG_ERROR;
    if (strcasecmp(level_str, "fatal") == 0) return LOG_FATAL;

    fprintf(stderr, "[LOG] Unknown log level '%s', defaulting to INFO\n", level_str);
    return LOG_INFO;
}

/*
 * Generate health check response
 */
health_response_t* generate_health_response(void) {
    if (!g_health || !g_metrics) return NULL;

    json_object *root = json_object_new_object();
    json_object *status_obj = json_object_new_string(g_health->status);
    json_object_object_add(root, "status", status_obj);

    // Add uptime
    time_t current_time = time(NULL);
    uint64_t uptime = current_time - g_metrics->start_time;
    json_object *uptime_obj = json_object_new_int64(uptime);
    json_object_object_add(root, "uptime_seconds", uptime_obj);

    // Add success rate
    double success_rate = health_calculate_success_rate();
    json_object *success_rate_obj = json_object_new_double(success_rate);
    json_object_object_add(root, "success_rate", success_rate_obj);

    // Add component status
    json_object *components = json_object_new_object();
    json_object_object_add(components, "mupdf", json_object_new_boolean(g_health->mupdf_status));
    json_object_object_add(components, "redis", json_object_new_boolean(g_health->redis_status));
    json_object_object_add(components, "http", json_object_new_boolean(g_health->http_status));
    json_object_object_add(root, "components", components);

    // Add version
    json_object *version_obj = json_object_new_string(g_health->version);
    json_object_object_add(root, "version", version_obj);

    // Generate response
    const char *json_string = json_object_to_json_string(root);
    health_response_t *response = malloc(sizeof(health_response_t));
    if (response) {
        response->response_size = strlen(json_string);
        response->json_response = malloc(response->response_size + 1);
        if (response->json_response) {
            strncpy(response->json_response, json_string, response->response_size);
            response->json_response[response->response_size] = '\0';
        }
    }

    json_object_put(root);
    return response;
}

void free_health_response(health_response_t *response) {
    if (response) {
        if (response->json_response) {
            free(response->json_response);
        }
        free(response);
    }
}

/*
 * Generate metrics response
 */
metrics_response_t* generate_metrics_response(void) {
    if (!g_metrics) return NULL;

    json_object *root = json_object_new_object();

    pthread_mutex_lock(&g_metrics->mutex);

    // Job metrics
    json_object_object_add(root, "total_jobs", json_object_new_int64(g_metrics->total_jobs_processed));
    json_object_object_add(root, "completed_jobs", json_object_new_int64(g_metrics->jobs_completed));
    json_object_object_add(root, "failed_jobs", json_object_new_int64(g_metrics->jobs_failed));
    json_object_object_add(root, "timeout_jobs", json_object_new_int64(g_metrics->jobs_timeout));

    // Timing metrics
    json_object_object_add(root, "avg_processing_time_ms", json_object_new_double(g_metrics->avg_processing_time_ms));
    if (g_metrics->min_processing_time_ms != UINT64_MAX) {
        json_object_object_add(root, "min_processing_time_ms", json_object_new_int64(g_metrics->min_processing_time_ms));
    }
    json_object_object_add(root, "max_processing_time_ms", json_object_new_int64(g_metrics->max_processing_time_ms));

    // Redis metrics
    json_object_object_add(root, "redis_jobs_received", json_object_new_int64(g_metrics->redis_jobs_received));
    json_object_object_add(root, "redis_jobs_acknowledged", json_object_new_int64(g_metrics->redis_jobs_acknowledged));
    json_object_object_add(root, "redis_connection_errors", json_object_new_int64(g_metrics->redis_connection_errors));

    // HTTP metrics
    json_object_object_add(root, "http_requests_received", json_object_new_int64(g_metrics->http_requests_received));
    json_object_object_add(root, "http_requests_completed", json_object_new_int64(g_metrics->http_requests_completed));

    // Resource metrics
    json_object_object_add(root, "memory_usage_bytes", json_object_new_int64(g_metrics->memory_usage_bytes));

    pthread_mutex_unlock(&g_metrics->mutex);

    // Generate response
    const char *json_string = json_object_to_json_string(root);
    metrics_response_t *response = malloc(sizeof(metrics_response_t));
    if (response) {
        response->response_size = strlen(json_string);
        response->json_response = malloc(response->response_size + 1);
        if (response->json_response) {
            strncpy(response->json_response, json_string, response->response_size);
            response->json_response[response->response_size] = '\0';
        }
    }

    json_object_put(root);
    return response;
}

void free_metrics_response(metrics_response_t *response) {
    if (response) {
        if (response->json_response) {
            free(response->json_response);
        }
        free(response);
    }
}

/*
 * Internal helper functions
 */
static uint64_t get_current_time_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
}

static const char* log_level_to_string(log_level_t level) {
    switch (level) {
        case LOG_DEBUG: return "DEBUG";
        case LOG_INFO:  return "INFO";
        case LOG_WARN:  return "WARN";
        case LOG_ERROR: return "ERROR";
        case LOG_FATAL: return "FATAL";
        default:        return "UNKNOWN";
    }
}

static void update_average_processing_time(uint64_t new_time_ms) {
    // Calculate running average
    uint64_t completed_jobs = g_metrics->jobs_completed;
    if (completed_jobs > 0) {
        g_metrics->avg_processing_time_ms =
            (g_metrics->avg_processing_time_ms * (completed_jobs - 1) + new_time_ms) / completed_jobs;
    } else {
        g_metrics->avg_processing_time_ms = new_time_ms;
    }
}