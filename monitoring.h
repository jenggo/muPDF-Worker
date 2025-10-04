/*
 * Production Monitoring for MuPDF Worker Service
 *
 * Provides metrics collection, health monitoring, and enhanced logging
 * capabilities for production environments.
 */

#ifndef MONITORING_H
#define MONITORING_H

#include <time.h>
#include <pthread.h>
#include <stdint.h>

// Logging levels
typedef enum {
    LOG_DEBUG = 0,
    LOG_INFO = 1,
    LOG_WARN = 2,
    LOG_ERROR = 3,
    LOG_FATAL = 4
} log_level_t;

// Worker metrics
typedef struct {
    uint64_t total_jobs_processed;
    uint64_t jobs_completed;
    uint64_t jobs_failed;
    uint64_t jobs_timeout;

    // Timing metrics
    uint64_t total_processing_time_ms;
    uint64_t min_processing_time_ms;
    uint64_t max_processing_time_ms;
    double avg_processing_time_ms;

    // Resource metrics
    uint64_t memory_usage_bytes;
    double cpu_usage_percent;
    uint64_t disk_space_used_bytes;

    // Redis metrics
    uint64_t redis_jobs_received;
    uint64_t redis_jobs_acknowledged;
    uint64_t redis_connection_errors;

    // HTTP metrics
    uint64_t http_requests_received;
    uint64_t http_requests_completed;

    time_t start_time;
    time_t last_updated;
    pthread_mutex_t mutex;
} worker_metrics_t;

// Health status
typedef struct {
    char status[32];           // healthy, degraded, unhealthy
    char version[64];
    int active_jobs;
    double success_rate;
    uint64_t uptime_seconds;
    char last_error[512];
    time_t last_error_time;

    // Component health
    int mupdf_status;         // 1 = healthy, 0 = unhealthy
    int redis_status;         // 1 = healthy, 0 = unhealthy
    int http_status;          // 1 = healthy, 0 = unhealthy

    time_t last_health_check;
    pthread_mutex_t mutex;
} worker_health_t;

// Global metrics and health instances
extern worker_metrics_t *g_metrics;
extern worker_health_t *g_health;

// Initialization and cleanup
int monitoring_init(void);
void monitoring_cleanup(void);

// Metrics functions
void metrics_job_started(void);
void metrics_job_completed(uint64_t processing_time_ms);
void metrics_job_failed(uint64_t processing_time_ms, const char *error);
void metrics_job_timeout(uint64_t processing_time_ms);
void metrics_redis_job_received(void);
void metrics_redis_job_acknowledged(void);
void metrics_redis_connection_error(void);
void metrics_http_request_received(void);
void metrics_http_request_completed(void);
void metrics_update_resource_usage(void);

// Health monitoring
void health_update_status(const char *status);
void health_set_error(const char *error);
void health_clear_error(void);
void health_update_component_status(int mupdf_ok, int redis_ok, int http_ok);
double health_calculate_success_rate(void);

// Logging functions
void log_message(log_level_t level, const char *component, const char *format, ...);
void log_job_start(const char *job_id, const char *file_path);
void log_job_complete(const char *job_id, uint64_t processing_time_ms);
void log_job_error(const char *job_id, const char *error);
void log_redis_event(const char *event, const char *details);

// Log level configuration
void set_log_level(log_level_t level);
log_level_t parse_log_level(const char *level_str);

// Convenience logging macros
#define LOG_DEBUG_MSG(comp, fmt, ...) log_message(LOG_DEBUG, comp, fmt, ##__VA_ARGS__)
#define LOG_INFO_MSG(comp, fmt, ...)  log_message(LOG_INFO, comp, fmt, ##__VA_ARGS__)
#define LOG_WARN_MSG(comp, fmt, ...)  log_message(LOG_WARN, comp, fmt, ##__VA_ARGS__)
#define LOG_ERROR_MSG(comp, fmt, ...) log_message(LOG_ERROR, comp, fmt, ##__VA_ARGS__)
#define LOG_FATAL_MSG(comp, fmt, ...) log_message(LOG_FATAL, comp, fmt, ##__VA_ARGS__)

// Health check endpoint data
typedef struct {
    char *json_response;
    size_t response_size;
} health_response_t;

health_response_t* generate_health_response(void);
void free_health_response(health_response_t *response);

// Metrics endpoint data
typedef struct {
    char *json_response;
    size_t response_size;
} metrics_response_t;

metrics_response_t* generate_metrics_response(void);
void free_metrics_response(metrics_response_t *response);

#endif // MONITORING_H