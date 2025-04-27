#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <curl/curl.h>
#include <unistd.h>
#include <semaphore.h>

#define MAX_URL_LEN 1024
#define MAX_CONCURRENT_WRITES 3
#define MAX_QUEUE_SIZE 100

sem_t write_semaphore;
pthread_mutex_t log_mutex;
pthread_mutex_t queue_mutex;
pthread_cond_t queue_not_empty;

struct DownloadSegment {
    char url[MAX_URL_LEN];
    char output_file[256];
    long start;
    long end;
    int id;
};

struct DownloadTaskQueue {
    struct DownloadSegment *tasks[MAX_QUEUE_SIZE];
    int front;
    int rear;
    int count;
};

struct ProducerArgs {
    const char *url;
    int num_segments;
    long file_size;
};

struct DownloadTaskQueue task_queue;
int total_tasks = 0;
int tasks_processed = 0;
int shutdown_flag = 0;

void enqueue_task(struct DownloadSegment *segment) {
    pthread_mutex_lock(&queue_mutex);
    while (task_queue.count == MAX_QUEUE_SIZE) {
        pthread_mutex_unlock(&queue_mutex);
        usleep(1000);
        pthread_mutex_lock(&queue_mutex);
    }

    task_queue.tasks[task_queue.rear] = segment;
    task_queue.rear = (task_queue.rear + 1) % MAX_QUEUE_SIZE;
    task_queue.count++;
    pthread_cond_signal(&queue_not_empty);
    pthread_mutex_unlock(&queue_mutex);
}

struct DownloadSegment *dequeue_task() {
    pthread_mutex_lock(&queue_mutex);
    while (task_queue.count == 0 && !shutdown_flag) {
        pthread_cond_wait(&queue_not_empty, &queue_mutex);
    }

    if (task_queue.count == 0 && shutdown_flag) {
        pthread_mutex_unlock(&queue_mutex);
        return NULL;
    }

    struct DownloadSegment *segment = task_queue.tasks[task_queue.front];
    task_queue.front = (task_queue.front + 1) % MAX_QUEUE_SIZE;
    task_queue.count--;
    pthread_mutex_unlock(&queue_mutex);
    return segment;
}

size_t write_data(void *ptr, size_t size, size_t nmemb, void *stream) {
    return fwrite(ptr, size, nmemb, (FILE *)stream);
}

void *consumer_thread(void *arg) {
    (void)arg; // Unused

    while (1) {
        struct DownloadSegment *segment = dequeue_task();
        if (!segment)
            break; // Shutdown

        sem_wait(&write_semaphore);

        pthread_mutex_lock(&log_mutex);
        printf("[Thread %d] Downloading bytes %ld to %ld\n", segment->id, segment->start, segment->end);
        pthread_mutex_unlock(&log_mutex);

        CURL *curl = curl_easy_init();
        if (curl) {
            FILE *fp = fopen(segment->output_file, "wb");
            if (!fp) {
                perror("File open failed");
                curl_easy_cleanup(curl);
                sem_post(&write_semaphore);
                free(segment);
                continue;
            }

            char range[64];
            snprintf(range, sizeof(range), "%ld-%ld", segment->start, segment->end);

            curl_easy_setopt(curl, CURLOPT_URL, segment->url);
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);
            curl_easy_setopt(curl, CURLOPT_RANGE, range);

            CURLcode res = curl_easy_perform(curl);
            if (res != CURLE_OK) {
                fprintf(stderr, "[Thread %d] curl_easy_perform() failed: %s\n", segment->id, curl_easy_strerror(res));
            }

            fclose(fp);
            curl_easy_cleanup(curl);
        }

        pthread_mutex_lock(&log_mutex);
        printf("[Thread %d] Done.\n", segment->id);
        pthread_mutex_unlock(&log_mutex);

        sem_post(&write_semaphore);
        free(segment);

        pthread_mutex_lock(&queue_mutex);
        tasks_processed++;
        if (tasks_processed == total_tasks) {
            shutdown_flag = 1;
            pthread_cond_broadcast(&queue_not_empty);
        }
        pthread_mutex_unlock(&queue_mutex);
    }
    return NULL;
}

void *producer_thread(void *arg) {
    struct ProducerArgs *producer_args = (struct ProducerArgs *)arg;
    long chunk = producer_args->file_size / producer_args->num_segments;

    for (int i = 0; i < producer_args->num_segments; ++i) {
        struct DownloadSegment *segment = malloc(sizeof(struct DownloadSegment));
        strncpy(segment->url, producer_args->url, MAX_URL_LEN);
        snprintf(segment->output_file, sizeof(segment->output_file), "part_%d", i);

        segment->start = i * chunk;
        segment->id = i;
        segment->end = segment->start + chunk - 1;
        if (i == producer_args->num_segments - 1) {
            segment->end = producer_args->file_size - 1;
        }

        enqueue_task(segment);
    }
    return NULL;
}

long get_file_size(const char *url) {
    CURL *curl;
    curl_off_t file_size = 0;
    long http_code = 0;

    curl = curl_easy_init();
    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

        CURLcode res = curl_easy_perform(curl);
        if (res == CURLE_OK) {
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
            curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &file_size);
        } else {
            fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
        }

        curl_easy_cleanup(curl);
    }

    printf("📦 File size: %ld bytes (HTTP %ld)\n", (long)file_size, http_code);

    if (http_code != 200 && http_code != 206) {
        return -1;
    }
    return (long)file_size;
}

void merge_files(int num_parts, const char *output_name) {
    FILE *output = fopen(output_name, "wb");
    if (!output) {
        perror("Failed to open output file");
        return;
    }

    for (int i = 0; i < num_parts; ++i) {
        char part_name[256];
        snprintf(part_name, sizeof(part_name), "part_%d", i);
        FILE *part = fopen(part_name, "rb");
        if (!part) continue;

        char buffer[8192];
        size_t bytes;
        while ((bytes = fread(buffer, 1, sizeof(buffer), part)) > 0) {
            fwrite(buffer, 1, bytes, output);
        }

        fclose(part);
        remove(part_name);
    }

    fclose(output);
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <URL> <output_file> <num_threads>\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char *url = argv[1];
    const char *output_file = argv[2];
    int num_threads = atoi(argv[3]);

    curl_global_init(CURL_GLOBAL_ALL);
    sem_init(&write_semaphore, 0, MAX_CONCURRENT_WRITES);
    pthread_mutex_init(&log_mutex, NULL);
    pthread_mutex_init(&queue_mutex, NULL);
    pthread_cond_init(&queue_not_empty, NULL);

    task_queue.front = 0;
    task_queue.rear = 0;
    task_queue.count = 0;

    long file_size = get_file_size(url);
    if (file_size <= 0) {
        fprintf(stderr, "Invalid file size retrieved.\n");
        return EXIT_FAILURE;
    }

    total_tasks = num_threads;

    pthread_t consumers[num_threads];
    for (int i = 0; i < num_threads; ++i) {
        pthread_create(&consumers[i], NULL, consumer_thread, NULL);
    }

    pthread_t producer;
    struct ProducerArgs producer_args = {url, num_threads, file_size};
    pthread_create(&producer, NULL, producer_thread, &producer_args);
    pthread_join(producer, NULL);

    for (int i = 0; i < num_threads; ++i) {
        pthread_join(consumers[i], NULL);
    }

    merge_files(num_threads, output_file);

    sem_destroy(&write_semaphore);
    pthread_mutex_destroy(&log_mutex);
    pthread_mutex_destroy(&queue_mutex);
    pthread_cond_destroy(&queue_not_empty);
    curl_global_cleanup();

    printf("✅ All done. Merged file: %s\n", output_file);
    return EXIT_SUCCESS;
}