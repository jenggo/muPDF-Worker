/*
 * Redis Queue Consumer for MuPDF Worker Service
 *
 * Provides Redis Stream consumption functionality for processing
 * document parsing jobs from a Redis queue with producer-consumer pattern.
 */

#ifndef REDIS_QUEUE_H
#define REDIS_QUEUE_H

#include <hiredis/hiredis.h>
#include <json-c/json.h>
#include <pthread.h>

// Redis configuration
typedef struct {
    char host[256];
    int port;
    char password[256];
    int db;
    int timeout_ms;
} redis_config_t;

// Redis job message
typedef struct {
    char message_id[64];
    char job_id[256];
    char document_id[256]; // Document UUID for ticketing system
    char file_path[1024];
    char callback_url[1024];
    char worker_url[1024];
    int timeout_ms;
    int max_retries;
    char image_directory_path[1024];
    int extract_vector_images; // Flag to enable/disable vector image extraction
} redis_job_t;

// Redis queue consumer
typedef struct {
    redisContext *ctx;
    redis_config_t config;
    char queue_name[256];
    char consumer_group[256];
    char consumer_name[256];
    int running;
    pthread_t consumer_thread;
    pthread_mutex_t mutex;
} redis_consumer_t;

// Function declarations
redis_consumer_t* redis_consumer_create(const redis_config_t *config,
                                       const char *queue_name,
                                       const char *consumer_group,
                                       const char *consumer_name);

int redis_consumer_start(redis_consumer_t *consumer);
void redis_consumer_stop(redis_consumer_t *consumer);
void redis_consumer_destroy(redis_consumer_t *consumer);

// Internal functions
int redis_connect(redis_consumer_t *consumer);
void redis_disconnect(redis_consumer_t *consumer);
void* redis_consumer_thread(void *arg);
int redis_process_jobs(redis_consumer_t *consumer);
int redis_parse_job_message(redisReply *reply, redis_job_t *job);
int redis_acknowledge_job(redis_consumer_t *consumer, const char *message_id);

// Error handling
const char* redis_get_error(redis_consumer_t *consumer);

// Configuration constants
#define REDIS_QUEUE_PREFIX "job_queue:"
#define REDIS_DEFAULT_HOST "localhost"
#define REDIS_DEFAULT_PORT 6379
#define REDIS_DEFAULT_TIMEOUT_MS 5000
#define REDIS_BATCH_SIZE 5
#define REDIS_BLOCK_TIME_MS 1000

// Job processing callback
typedef int (*job_processor_callback_t)(const redis_job_t *job);
extern job_processor_callback_t g_job_processor;

#endif // REDIS_QUEUE_H