#include "mapreduce.h"
#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

Partitioner partitioner;
Mapper mapper;
Reducer reducer;

int NUM_PARTITIONS;
int NUM_REDUCERS;
int NUM_MAPPERS;

typedef struct files {
    int num_files;
    int files_complete;
    char** files;
    pthread_mutex_t lock;
} files;

typedef struct rph {
    int curPartition;
    pthread_mutex_t lock;
} rph;

typedef struct node {
    char* key;
    char* value;

    struct node* next;
} node;

typedef struct partition {
    int num_nodes;
    int np;
    struct node* node_head;
    node** nodeArray;
    pthread_mutex_t lock;
} partition;

struct partition** partitions;
struct files* file_info;
struct rph* rph_information;

char* performReduce(char* key, int p) {
    if ( partitions[p]->np >= partitions[p]->num_nodes ) {
        return NULL;
    }

    node* n = partitions[p]->nodeArray[partitions[p]->np];
    if ( strcmp(n->key, key) == 0 ) {
        partitions[p]->np++;
        return n->value;
    } else {
        return NULL;
    }
}

void MR_Emit(char *key, char *value) {
    unsigned long p = partitioner(key, NUM_PARTITIONS);

    if ( p >= NUM_PARTITIONS || p < 0 ) {
      printf("%ld, %d", p, NUM_PARTITIONS);
      exit(1);
    }

    pthread_mutex_lock(&partitions[p]->lock);
    node* n = malloc(sizeof(node));
    n->key = malloc((sizeof(char) * strlen(key)) + 1);
    strcpy(n->key, key);
    n->value = malloc((sizeof(char) * strlen(value)) + 1);
    strcpy(n->value, value);
    n->next = NULL;

    if ( partitions[p]->node_head == NULL ) {
        partitions[p]->node_head = n;
    } else {
      n->next = partitions[p]->node_head;
      partitions[p]->node_head = n;
    }

    partitions[p]->num_nodes++;

    pthread_mutex_unlock(&partitions[p]->lock);
}

void reducePartition(int p) {
    pthread_mutex_lock(&partitions[p]->lock);
    while ( partitions[p]->np < partitions[p]->num_nodes ) {
        reducer(partitions[p]->nodeArray[partitions[p]->np]->key,
            performReduce, p);
    }
    pthread_mutex_unlock(&partitions[p]->lock);
}

void* handleMapping() {
    while (1) {
        pthread_mutex_lock(&file_info->lock);
        // if all files are processed.
        if ( file_info->files_complete == file_info->num_files ) {
            pthread_mutex_unlock(&file_info->lock);
            return NULL;
        } else {
             char* file = file_info->files[file_info->files_complete++];
             pthread_mutex_unlock(&file_info->lock);
             mapper(file);
        }
    }
    return NULL;
}

void cleanPartition(int p) {
    node* cur = partitions[p]->node_head;
    node* next = NULL;
    while ( cur != NULL ) {
        next = cur->next;
        free(cur->key);
        free(cur->value);
        free(cur);
        cur = next;
    }

    free(partitions[p]->nodeArray);
    pthread_mutex_destroy(&partitions[p]->lock);
    free(partitions[p]);
}

void* handleReducing() {
    while (1) {
        pthread_mutex_lock(&rph_information->lock);
        // if all files are processed.
        if ( rph_information->curPartition == NUM_PARTITIONS ) {
            pthread_mutex_unlock(&rph_information->lock);
            return NULL;
        } else {
             int p = rph_information->curPartition++;
             pthread_mutex_unlock(&rph_information->lock);
             reducePartition(p);
             cleanPartition(p);
        }
    }
    return NULL;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c = 0;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

unsigned long MR_SortedPartition(char *key, int num_partitions) {
    unsigned int i = strtoul(key, 0L, 10);
    int n = 0;
    int r = num_partitions;
    while ( r > 0 ) {
        r /= 2;
        n++;
    }
    unsigned int div = (32 - n) + 1;
    unsigned int out = i >> div;
    return out % num_partitions;
}

int compare(const void * a, const void * b) {
    return strcmp((*(struct node**) a)->key, (*(struct node**) b)->key);
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,
        Reducer reduce, int num_reducers, Partitioner p, int num_partitions) {
    partitioner = p;
    mapper = map;
    reducer = reduce;
    NUM_PARTITIONS = num_partitions;
    NUM_REDUCERS = num_reducers;

    rph_information = malloc(sizeof(rph));
    rph_information->curPartition = 0;
    pthread_mutex_init(&rph_information->lock, NULL);

    file_info = malloc(sizeof(files));
    file_info->files = &argv[1];
    file_info->num_files = argc - 1;
    file_info->files_complete = 0;
    pthread_mutex_init(&file_info->lock, NULL);

    pthread_t* map_threads = malloc(sizeof(pthread_t) * num_mappers);
    pthread_t* reduce_threads = malloc(sizeof(pthread_t) * num_reducers);

    // create partitions
    partitions = malloc(sizeof(partition*) * num_partitions);

    for ( int partition_i = 0; partition_i < num_partitions; partition_i++ ) {
        partitions[partition_i] = malloc(sizeof(partition));
        partitions[partition_i]->num_nodes = 0;
        partitions[partition_i]->np = 0;
        partitions[partition_i]->node_head == NULL;
        partitions[partition_i]->nodeArray = NULL;
        pthread_mutex_init(&partitions[partition_i]->lock, NULL);
    }

    for ( int i = 0; i < num_mappers; i++ ) {
        pthread_create(&map_threads[i], NULL, handleMapping, NULL);
    }

    for ( int i = 0; i < num_mappers; i++ ) {
        pthread_join(map_threads[i], NULL);
    }

    // create arrays and sort
    for ( int i = 0; i < num_partitions; i++ ) {
        int num_nodes = partitions[i]->num_nodes;
        if ( num_nodes == 0 ) {
            continue;
        }
        node** array = malloc((sizeof(node*) * num_nodes));
        node* cur = partitions[i]->node_head;
        for ( int i = 0; i < num_nodes; i++ ) {
            array[i] = cur;
            cur = cur->next;
            if ( cur == NULL ) {
                break;
            }
        }
        qsort(array, num_nodes, sizeof(node*), compare);
        partitions[i]->nodeArray = array;
    }

    for ( int i = 0; i < num_reducers; i++ ) {
        pthread_create(&reduce_threads[i], NULL, handleReducing, NULL);
    }

    for ( int i = 0; i < num_reducers; i++ ) {
        pthread_join(reduce_threads[i], NULL);
    }

    free(rph_information);
    pthread_mutex_destroy(&file_info->lock);
    free(file_info);
    free(map_threads);
    free(reduce_threads);
    free(partitions);
}
