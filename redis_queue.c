/*
 * Redis Queue Consumer Implementation
 *
 * Implements Redis Stream consumption for MuPDF worker service
 * using XREADGROUP for reliable job processing.
 */

#include "redis_queue.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <sys/time.h>

// Global job processor callback
job_processor_callback_t g_job_processor = NULL;

/*
 * TCP connectivity check - faster than full Redis connection
 * Returns 0 if port is reachable, -1 if not
 */
static int tcp_ping(const char *host, int port, int timeout_ms) {
    int sockfd = -1;
    struct addrinfo hints, *result_addr = NULL, *rp = NULL;
    struct timeval tv;
    int result = -1;
    char port_str[16];

    // Convert port to string for getaddrinfo
    snprintf(port_str, sizeof(port_str), "%d", port);

    // Setup hints for getaddrinfo (modern replacement for gethostbyname)
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;        // IPv4
    hints.ai_socktype = SOCK_STREAM;  // TCP

    // Resolve hostname (POSIX-compliant, no deprecated functions)
    static int dns_error_count = 0;
    int dns_result = getaddrinfo(host, port_str, &hints, &result_addr);
    if (dns_result != 0) {
        // Only log DNS errors occasionally to avoid log spam
        dns_error_count++;
        if (dns_error_count == 1 || dns_error_count % 20 == 0) {
            fprintf(stderr, "[Redis TCP] DNS resolution failed for %s (attempt #%d, check network/DNS)\n", host, dns_error_count);
        }
        return -1;
    }

    // Reset DNS error counter on successful resolution
    if (dns_error_count > 0) {
        printf("[Redis TCP] DNS resolution recovered after %d failed attempts\n", dns_error_count);
        dns_error_count = 0;
    }

    // Try each address until we successfully connect
    for (rp = result_addr; rp != NULL; rp = rp->ai_next) {
        sockfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sockfd < 0) {
            continue; // Try next address
        }

        // Set socket to non-blocking for timeout control
        int flags = fcntl(sockfd, F_GETFL, 0);
        if (flags >= 0) {
            fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);
        }

        // Set socket timeout
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
        setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, (const char*)&tv, sizeof(tv));

        // Attempt connection
        int conn_result = connect(sockfd, rp->ai_addr, rp->ai_addrlen);

        if (conn_result < 0) {
            if (errno == EINPROGRESS) {
                // Connection in progress, wait for completion
                fd_set fdset;
                FD_ZERO(&fdset);
                FD_SET(sockfd, &fdset);

                if (select(sockfd + 1, NULL, &fdset, NULL, &tv) > 0) {
                    int so_error;
                    socklen_t len = sizeof(so_error);
                    getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &so_error, &len);

                    if (so_error == 0) {
                        result = 0; // Connection successful
                        break;
                    }
                }
            }
            close(sockfd);
            sockfd = -1;
        } else {
            result = 0; // Connected immediately
            break;
        }
    }

    if (result_addr) {
        freeaddrinfo(result_addr);
    }

    if (sockfd >= 0) {
        close(sockfd);
    }

    if (result != 0) {
        fprintf(stderr, "[Redis TCP] Failed to connect to %s:%d\n", host, port);
    }

    return result;
}

/*
 * Create a new Redis consumer instance
 */
redis_consumer_t* redis_consumer_create(const redis_config_t *config,
                                       const char *queue_name,
                                       const char *consumer_group,
                                       const char *consumer_name) {
    if (!config || !queue_name || !consumer_group || !consumer_name) {
        fprintf(stderr, "[Redis] Invalid parameters for consumer creation\n");
        return NULL;
    }

    redis_consumer_t *consumer = calloc(1, sizeof(redis_consumer_t));
    if (!consumer) {
        fprintf(stderr, "[Redis] Failed to allocate consumer memory\n");
        return NULL;
    }

    // Copy configuration
    consumer->config = *config;
    strncpy(consumer->queue_name, queue_name, sizeof(consumer->queue_name) - 1);
    strncpy(consumer->consumer_group, consumer_group, sizeof(consumer->consumer_group) - 1);
    strncpy(consumer->consumer_name, consumer_name, sizeof(consumer->consumer_name) - 1);

    // Initialize mutex
    if (pthread_mutex_init(&consumer->mutex, NULL) != 0) {
        fprintf(stderr, "[Redis] Failed to initialize mutex\n");
        free(consumer);
        return NULL;
    }

    consumer->running = 0;
    consumer->ctx = NULL;

    printf("[Redis] Consumer created: queue=%s, group=%s, consumer=%s\n",
           queue_name, consumer_group, consumer_name);

    return consumer;
}

/*
 * Start the Redis consumer thread
 */
int redis_consumer_start(redis_consumer_t *consumer) {
    if (!consumer) return -1;

    pthread_mutex_lock(&consumer->mutex);

    if (consumer->running) {
        pthread_mutex_unlock(&consumer->mutex);
        printf("[Redis] Consumer already running\n");
        return 0;
    }

    // Connect to Redis
    if (redis_connect(consumer) != 0) {
        pthread_mutex_unlock(&consumer->mutex);
        return -1;
    }

    // Start consumer thread
    consumer->running = 1;
    if (pthread_create(&consumer->consumer_thread, NULL, redis_consumer_thread, consumer) != 0) {
        fprintf(stderr, "[Redis] Failed to create consumer thread: %s\n", strerror(errno));
        consumer->running = 0;
        redis_disconnect(consumer);
        pthread_mutex_unlock(&consumer->mutex);
        return -1;
    }

    pthread_mutex_unlock(&consumer->mutex);
    printf("[Redis] Consumer started successfully\n");
    return 0;
}

/*
 * Stop the Redis consumer
 */
void redis_consumer_stop(redis_consumer_t *consumer) {
    if (!consumer) return;

    pthread_mutex_lock(&consumer->mutex);

    if (!consumer->running) {
        pthread_mutex_unlock(&consumer->mutex);
        return;
    }

    printf("[Redis] Stopping consumer...\n");
    consumer->running = 0;
    pthread_mutex_unlock(&consumer->mutex);

    // Wait for thread to finish
    if (pthread_join(consumer->consumer_thread, NULL) != 0) {
        fprintf(stderr, "[Redis] Failed to join consumer thread\n");
    }

    redis_disconnect(consumer);
    printf("[Redis] Consumer stopped\n");
}

/*
 * Destroy Redis consumer and free resources
 */
void redis_consumer_destroy(redis_consumer_t *consumer) {
    if (!consumer) return;

    redis_consumer_stop(consumer);
    pthread_mutex_destroy(&consumer->mutex);
    free(consumer);
    printf("[Redis] Consumer destroyed\n");
}

/*
 * Connect to Redis server
 */
int redis_connect(redis_consumer_t *consumer) {
    if (!consumer) return -1;

    // Step 1: TCP connectivity check (fast pre-flight check)
    // Don't log every attempt to reduce spam during reconnection
    static int connect_attempts = 0;
    connect_attempts++;

    if (connect_attempts == 1 || connect_attempts % 10 == 0) {
        printf("[Redis] Checking TCP connectivity to %s:%d (attempt #%d)...\n",
               consumer->config.host, consumer->config.port, connect_attempts);
    }

    if (tcp_ping(consumer->config.host, consumer->config.port, 2000) != 0) {
        // Only log every 10th failure to reduce spam
        if (connect_attempts == 1 || connect_attempts % 10 == 0) {
            fprintf(stderr, "[Redis] TCP connectivity check failed: %s:%d unreachable (attempt #%d)\n",
                    consumer->config.host, consumer->config.port, connect_attempts);
        }
        return -1;
    }

    // Reset counter on successful connection
    connect_attempts = 0;
    printf("[Redis] ✓ TCP connectivity OK, establishing Redis connection...\n");

    // Step 2: Create Redis protocol connection with timeout
    struct timeval timeout = {
        .tv_sec = consumer->config.timeout_ms / 1000,
        .tv_usec = (consumer->config.timeout_ms % 1000) * 1000
    };

    consumer->ctx = redisConnectWithTimeout(consumer->config.host, consumer->config.port, timeout);

    if (!consumer->ctx || consumer->ctx->err) {
        fprintf(stderr, "[Redis] Connection failed: %s:%d - %s\n",
                consumer->config.host, consumer->config.port,
                consumer->ctx ? consumer->ctx->errstr : "Can't allocate redis context");
        if (consumer->ctx) {
            redisFree(consumer->ctx);
            consumer->ctx = NULL;
        }
        return -1;
    }

    // Authenticate if password provided
    if (strlen(consumer->config.password) > 0) {
        redisReply *reply = redisCommand(consumer->ctx, "AUTH %s", consumer->config.password);
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            fprintf(stderr, "[Redis] Authentication failed\n");
            if (reply) freeReplyObject(reply);
            redisFree(consumer->ctx);
            consumer->ctx = NULL;
            return -1;
        }
        freeReplyObject(reply);
    }

    // Select database
    if (consumer->config.db != 0) {
        redisReply *reply = redisCommand(consumer->ctx, "SELECT %d", consumer->config.db);
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            fprintf(stderr, "[Redis] Failed to select database %d\n", consumer->config.db);
            if (reply) freeReplyObject(reply);
            redisFree(consumer->ctx);
            consumer->ctx = NULL;
            return -1;
        }
        freeReplyObject(reply);
    }

    // Step 3: Verify Redis is responding
    redisReply *ping_reply = redisCommand(consumer->ctx, "PING");
    if (!ping_reply || ping_reply->type == REDIS_REPLY_ERROR) {
        fprintf(stderr, "[Redis] PING verification failed after connection\n");
        if (ping_reply) freeReplyObject(ping_reply);
        redisFree(consumer->ctx);
        consumer->ctx = NULL;
        return -1;
    }
    freeReplyObject(ping_reply);

    printf("[Redis] ✓ Connected successfully to %s:%d (db=%d)\n",
           consumer->config.host, consumer->config.port, consumer->config.db);

    return 0;
}

/*
 * Disconnect from Redis
 */
void redis_disconnect(redis_consumer_t *consumer) {
    if (consumer && consumer->ctx) {
        redisFree(consumer->ctx);
        consumer->ctx = NULL;
        printf("[Redis] Disconnected\n");
    }
}

/*
 * Consumer thread main loop
 */
void* redis_consumer_thread(void *arg) {
    redis_consumer_t *consumer = (redis_consumer_t*)arg;

    printf("[Redis] Consumer thread started\n");

    // Create consumer group (ignore error if already exists)
    char stream_key[512];
    snprintf(stream_key, sizeof(stream_key), "%s%s", REDIS_QUEUE_PREFIX, consumer->queue_name);

    redisReply *reply = redisCommand(consumer->ctx,
        "XGROUP CREATE %s %s 0 MKSTREAM",
        stream_key, consumer->consumer_group);

    if (reply) {
        if (reply->type == REDIS_REPLY_ERROR &&
            strstr(reply->str, "BUSYGROUP") == NULL) {
            fprintf(stderr, "[Redis] Failed to create consumer group: %s\n", reply->str);
        } else {
            printf("[Redis] Consumer group ready: %s\n", consumer->consumer_group);
        }
        freeReplyObject(reply);
    }

    // Main processing loop with exponential backoff
    int consecutive_failures = 0;
    int max_backoff = 60; // Maximum backoff in seconds

    while (consumer->running) {
        if (redis_process_jobs(consumer) != 0) {
            consecutive_failures++;

            // Check connection health immediately when job processing fails
            int need_reconnect = 0;
            if (!consumer->ctx || consumer->ctx->err) {
                printf("[Redis] Connection error detected: %s\n",
                       consumer->ctx ? consumer->ctx->errstr : "context is NULL");
                need_reconnect = 1;
            } else {
                // Test connection with PING
                redisReply *ping_reply = redisCommand(consumer->ctx, "PING");
                if (!ping_reply) {
                    printf("[Redis] PING command failed - connection lost\n");
                    need_reconnect = 1;
                } else if (ping_reply->type == REDIS_REPLY_ERROR) {
                    printf("[Redis] PING returned error: %s\n", ping_reply->str);
                    need_reconnect = 1;
                }
                if (ping_reply) freeReplyObject(ping_reply);
            }
            
            if (need_reconnect) {
                printf("[Redis] Connection lost, attempting to reconnect...\n");
                fflush(stdout);
                redis_disconnect(consumer);

                // Keep trying to reconnect indefinitely (with exponential backoff)
                int reconnect_attempts = 0;

                printf("[Redis] DEBUG: Starting reconnection loop (consumer->running=%d)\n", consumer->running);
                fflush(stdout);

                while (consumer->running) {
                    reconnect_attempts++;

                    printf("[Redis] DEBUG: Loop iteration %d (consumer->running=%d)\n", reconnect_attempts, consumer->running);
                    fflush(stdout);

                    // Log progress (reduced frequency to avoid spam)
                    if (reconnect_attempts == 1 || reconnect_attempts % 5 == 0) {
                        printf("[Redis] Reconnection attempt #%d...\n", reconnect_attempts);
                        fflush(stdout);
                    }

                    int connect_result = redis_connect(consumer);
                    printf("[Redis] DEBUG: redis_connect() returned %d\n", connect_result);
                    fflush(stdout);

                    if (connect_result == 0) {
                        printf("[Redis] Reconnection successful after %d attempt(s)\n", reconnect_attempts);
                        consecutive_failures = 0; // Reset failure counter on successful reconnect

                        // Recreate consumer group after reconnection
                        char stream_key[512];
                        snprintf(stream_key, sizeof(stream_key), "%s%s", REDIS_QUEUE_PREFIX, consumer->queue_name);
                        redisReply *group_reply = redisCommand(consumer->ctx,
                            "XGROUP CREATE %s %s 0 MKSTREAM",
                            stream_key, consumer->consumer_group);
                        if (group_reply) {
                            if (group_reply->type != REDIS_REPLY_ERROR ||
                                strstr(group_reply->str, "BUSYGROUP") != NULL) {
                                printf("[Redis] Consumer group ready after reconnection: %s\n", consumer->consumer_group);
                            }
                            freeReplyObject(group_reply);
                        }
                        break;
                    }

                    // Calculate exponential backoff with cap
                    int reconnect_backoff = 1 << (reconnect_attempts < 6 ? reconnect_attempts : 6);
                    if (reconnect_backoff > max_backoff) reconnect_backoff = max_backoff;

                    printf("[Redis] Reconnection attempt #%d failed (total attempts: %d), retrying in %d seconds...\n",
                           reconnect_attempts, reconnect_attempts, reconnect_backoff);
                    fflush(stdout); // Force output immediately

                    // Log every 10th attempt to show we're still trying
                    if (reconnect_attempts % 10 == 0) {
                        printf("[Redis] Still attempting to reconnect after %d tries (check network/DNS)...\n", reconnect_attempts);
                        fflush(stdout);
                    }

                    printf("[Redis] DEBUG: About to sleep for %d seconds...\n", reconnect_backoff);
                    fflush(stdout);
                    sleep(reconnect_backoff);
                    printf("[Redis] DEBUG: Sleep completed, looping back\n");
                    fflush(stdout);
                }
            } else {
                // Not a connection error, just a processing error
                // Calculate exponential backoff (1, 2, 4, 8, 16, 32, 60, 60...)
                int backoff = 1 << (consecutive_failures - 1);
                if (backoff > max_backoff) backoff = max_backoff;

                printf("[Redis] Job processing failed (attempt %d), retrying in %d seconds...\n",
                       consecutive_failures, backoff);
                sleep(backoff);
            }
        } else {
            // Reset failure counter on successful processing
            consecutive_failures = 0;
        }
    }

    printf("[Redis] Consumer thread exiting\n");
    return NULL;
}

/*
 * Process jobs from Redis Stream
 */
int redis_process_jobs(redis_consumer_t *consumer) {
    if (!consumer || !consumer->ctx) return -1;

    // Check if connection is still alive
    if (consumer->ctx->err) {
        fprintf(stderr, "[Redis] Connection error detected: %s\n", consumer->ctx->errstr);
        return -1;
    }

    char stream_key[512];
    snprintf(stream_key, sizeof(stream_key), "%s%s", REDIS_QUEUE_PREFIX, consumer->queue_name);

    // Read from stream using XREADGROUP
    redisReply *reply = redisCommand(consumer->ctx,
        "XREADGROUP GROUP %s %s COUNT %d BLOCK %d STREAMS %s >",
        consumer->consumer_group, consumer->consumer_name,
        REDIS_BATCH_SIZE, REDIS_BLOCK_TIME_MS, stream_key);

    if (!reply) {
        fprintf(stderr, "[Redis] XREADGROUP command failed - connection may be lost\n");
        return -1;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        fprintf(stderr, "[Redis] XREADGROUP error: %s\n", reply->str);
        freeReplyObject(reply);

        // Return error to trigger reconnection logic
        return -1;
    }

    // No messages available
    if (reply->type == REDIS_REPLY_NIL || reply->elements == 0) {
        freeReplyObject(reply);
        return 0;
    }

    // Process each stream
    for (size_t i = 0; i < reply->elements; i++) {
        redisReply *stream = reply->element[i];
        if (stream->elements < 2) continue;

        redisReply *messages = stream->element[1];

        // Process each message in the stream
        for (size_t j = 0; j < messages->elements; j++) {
            redisReply *message = messages->element[j];
            if (message->elements < 2) continue;

            redis_job_t job = {0};
            if (redis_parse_job_message(message, &job) == 0) {
                printf("[Redis] Processing job: %s (message: %s)\n", job.job_id, job.message_id);

                // Process the job using the callback
                if (g_job_processor && g_job_processor(&job) == 0) {
                    // Acknowledge successful processing
                    redis_acknowledge_job(consumer, job.message_id);
                    printf("[Redis] Job %s completed successfully\n", job.job_id);
                } else {
                    fprintf(stderr, "[Redis] Job %s processing failed\n", job.job_id);
                    // Note: Failed jobs remain in pending list for retry
                }
            }
        }
    }

    freeReplyObject(reply);
    return 0;
}

/*
 * Parse job message from Redis Stream
 */
int redis_parse_job_message(redisReply *reply, redis_job_t *job) {
    if (!reply || !job || reply->elements < 2) return -1;

    // Extract message ID
    strncpy(job->message_id, reply->element[0]->str, sizeof(job->message_id) - 1);

    // Parse field-value pairs
    redisReply *fields = reply->element[1];
    for (size_t i = 0; i < fields->elements; i += 2) {
        if (i + 1 >= fields->elements) break;

        const char *field = fields->element[i]->str;
        const char *value = fields->element[i + 1]->str;

        if (strcmp(field, "job_id") == 0) {
            strncpy(job->job_id, value, sizeof(job->job_id) - 1);
        } else if (strcmp(field, "document_id") == 0) {
            strncpy(job->document_id, value, sizeof(job->document_id) - 1);
            job->document_id[sizeof(job->document_id) - 1] = '\0';
        } else if (strcmp(field, "file_path") == 0) {
            strncpy(job->file_path, value, sizeof(job->file_path) - 1);
        } else if (strcmp(field, "callback_url") == 0) {
            strncpy(job->callback_url, value, sizeof(job->callback_url) - 1);
        } else if (strcmp(field, "worker_url") == 0) {
            strncpy(job->worker_url, value, sizeof(job->worker_url) - 1);
        } else if (strcmp(field, "timeout_ms") == 0) {
            job->timeout_ms = atoi(value);
        } else if (strcmp(field, "max_retries") == 0) {
            job->max_retries = atoi(value);
        } else if (strcmp(field, "image_directory_path") == 0) {
            strncpy(job->image_directory_path, value, sizeof(job->image_directory_path) - 1);
            job->image_directory_path[sizeof(job->image_directory_path) - 1] = '\0';
        } else if (strcmp(field, "extract_vector_images") == 0) {
            job->extract_vector_images = (strcmp(value, "true") == 0 || strcmp(value, "1") == 0) ? 1 : 0;
        }
    }

    // Validate required fields
    if (strlen(job->job_id) == 0 || strlen(job->file_path) == 0 ||
        strlen(job->callback_url) == 0 || strlen(job->image_directory_path) == 0) {
        fprintf(stderr, "[Redis] Invalid job message: missing required fields (image_directory_path=%s)\n",
                job->image_directory_path);
        return -1;
    }

    return 0;
}

/*
 * Acknowledge job completion
 */
int redis_acknowledge_job(redis_consumer_t *consumer, const char *message_id) {
    if (!consumer || !consumer->ctx || !message_id) return -1;

    char stream_key[512];
    snprintf(stream_key, sizeof(stream_key), "%s%s", REDIS_QUEUE_PREFIX, consumer->queue_name);

    redisReply *reply = redisCommand(consumer->ctx,
        "XACK %s %s %s",
        stream_key, consumer->consumer_group, message_id);

    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        fprintf(stderr, "[Redis] Failed to acknowledge message %s\n", message_id);
        if (reply) freeReplyObject(reply);
        return -1;
    }

    freeReplyObject(reply);
    return 0;
}

/*
 * Get Redis error message
 */
const char* redis_get_error(redis_consumer_t *consumer) {
    if (consumer && consumer->ctx && consumer->ctx->err) {
        return consumer->ctx->errstr;
    }
    return "No error";
}