#ifndef ROUTE_OS_H
#define ROUTE_OS_H

#include <stdint.h>

typedef struct {
    void *(*mutex_create)(void);
    void (*mutex_lock)(void *mutex);
    void (*mutex_unlock)(void *mutex);
    void (*mutex_destroy)(void *mutex);
    void *(*sem_create)(void);
    int (*sem_wait)(void *sem, uint32_t timeout_ms);
    void (*sem_post)(void *sem);
    void (*sem_destroy)(void *sem);
} route_os_t;

#endif 
