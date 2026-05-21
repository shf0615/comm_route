/**
 * @file example_mq_transport.c
 * @brief 综合示例：使用消息队列作为底层传输的路由模块（PC 运行版本）
 *
 * 场景：同一进程中 3 个路由实例（节点 A=1, B=2, C=3）
 * 拓扑：链形 A - B - C（A 和 C 不直连，通过 B 中继）
 *
 * 演示：
 * 1. pthread + 简易消息队列作为 port 传输
 * 2. A 异步发送请求到 C（经 B 转发）
 * 3. C 收到后回复响应
 * 4. A 收到响应触发回调
 * 5. B 广播消息到 A 和 C
 *
 * 编译：gcc -o example_mq example_mq_transport.c ../route_*.c -I.. -lpthread
 */

#include "route.h"
#include "route_queue.h"
#include "route_reliability.h"

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <time.h>

// ============ 简易消息队列（基于 pthread mutex + cond） ============

#define MQ_DEPTH    16
#define MQ_MSG_SIZE ROUTE_BLOCK_SIZE

typedef struct {
    uint8_t data[MQ_DEPTH][MQ_MSG_SIZE];
    uint16_t lengths[MQ_DEPTH];
    uint8_t head;
    uint8_t tail;
    uint8_t count;
    pthread_mutex_t lock;
} simple_mq_t;

static void mq_init(simple_mq_t *mq) {
    memset(mq, 0, sizeof(*mq));
    pthread_mutex_init(&mq->lock, NULL);
}

static int mq_send(simple_mq_t *mq, const uint8_t *data, uint16_t len) {
    pthread_mutex_lock(&mq->lock);
    if (mq->count >= MQ_DEPTH) {
        pthread_mutex_unlock(&mq->lock);
        return -1;
    }
    memcpy(mq->data[mq->tail], data, len);
    mq->lengths[mq->tail] = len;
    mq->tail = (mq->tail + 1) % MQ_DEPTH;
    mq->count++;
    pthread_mutex_unlock(&mq->lock);
    return 0;
}

static int mq_recv(simple_mq_t *mq, uint8_t *data, uint16_t *len) {
    pthread_mutex_lock(&mq->lock);
    if (mq->count == 0) {
        pthread_mutex_unlock(&mq->lock);
        return -1;
    }
    *len = mq->lengths[mq->head];
    memcpy(data, mq->data[mq->head], *len);
    mq->head = (mq->head + 1) % MQ_DEPTH;
    mq->count--;
    pthread_mutex_unlock(&mq->lock);
    return 0;
}

// ============ 消息队列实例（每条链路一个，单向） ============

static simple_mq_t queue_ab;  // A -> B
static simple_mq_t queue_ba;  // B -> A
static simple_mq_t queue_bc;  // B -> C
static simple_mq_t queue_cb;  // C -> B

// ============ Port send 函数 ============

static int send_a_to_b(uint8_t port_id, const uint8_t *buf, uint16_t len) {
    (void)port_id;
    return mq_send(&queue_ab, buf, len);
}

static int send_b_to_a(uint8_t port_id, const uint8_t *buf, uint16_t len) {
    (void)port_id;
    return mq_send(&queue_ba, buf, len);
}

static int send_b_to_c(uint8_t port_id, const uint8_t *buf, uint16_t len) {
    (void)port_id;
    return mq_send(&queue_bc, buf, len);
}

static int send_c_to_b(uint8_t port_id, const uint8_t *buf, uint16_t len) {
    (void)port_id;
    return mq_send(&queue_cb, buf, len);
}

// ============ OS 适配层（pthread） ============

typedef struct { pthread_mutex_t m; } pc_mutex_t;
typedef struct { sem_t s; } pc_sem_t;

static void *pc_mutex_create(void) {
    pc_mutex_t *m = calloc(1, sizeof(*m));
    pthread_mutex_init(&m->m, NULL);
    return m;
}
static void pc_mutex_lock(void *mutex) {
    pthread_mutex_lock(&((pc_mutex_t *)mutex)->m);
}
static void pc_mutex_unlock(void *mutex) {
    pthread_mutex_unlock(&((pc_mutex_t *)mutex)->m);
}
static void pc_mutex_destroy(void *mutex) {
    pthread_mutex_destroy(&((pc_mutex_t *)mutex)->m);
    free(mutex);
}
static void *pc_sem_create(void) {
    pc_sem_t *s = calloc(1, sizeof(*s));
    sem_init(&s->s, 0, 0);
    return s;
}
static int pc_sem_wait(void *sem, uint32_t timeout_ms) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += timeout_ms / 1000;
    ts.tv_nsec += (timeout_ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000L;
    }
    return sem_timedwait(&((pc_sem_t *)sem)->s, &ts) == 0 ? 0 : -1;
}
static void pc_sem_post(void *sem) {
    sem_post(&((pc_sem_t *)sem)->s);
}
static void pc_sem_destroy(void *sem) {
    sem_destroy(&((pc_sem_t *)sem)->s);
    free(sem);
}

static const route_os_t pc_os = {
    .mutex_create  = pc_mutex_create,
    .mutex_lock    = pc_mutex_lock,
    .mutex_unlock  = pc_mutex_unlock,
    .mutex_destroy = pc_mutex_destroy,
    .sem_create    = pc_sem_create,
    .sem_wait      = pc_sem_wait,
    .sem_post      = pc_sem_post,
    .sem_destroy   = pc_sem_destroy,
};

// ============ 路由实例 ============

static route_instance_t *inst_a;  // node 1
static route_instance_t *inst_b;  // node 2
static route_instance_t *inst_c;  // node 3

// ============ 辅助：从消息队列取数据 push 到路由接收队列 ============

static void mq_drain_to_route(simple_mq_t *mq, route_instance_t *inst, uint8_t from_port) {
    uint8_t buf[MQ_MSG_SIZE];
    uint16_t len;
    while (mq_recv(mq, buf, &len) == 0) {
        route_queue_push(&inst->recv_queue, buf, len, from_port);
    }
}

// ============ 获取当前时间（毫秒） ============

static uint32_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint32_t)(ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
}

// ============ 应用回调 ============

static void on_recv_c(route_instance_t *inst, uint8_t src, uint8_t trans_id,
                      const uint8_t *data, uint16_t len) {
    printf("[Node C] Received request from node %d: ", src);
    for (uint16_t i = 0; i < len; i++) printf("%02X ", data[i]);
    printf("\n");

    uint8_t reply[] = {0xC0, 0xC1, 0xC2, 0xC3};
    route_reply(inst, src, trans_id, reply, sizeof(reply));
    printf("[Node C] Sent reply to node %d\n", src);
}

static void on_recv_a(route_instance_t *inst, uint8_t src, uint8_t trans_id,
                      const uint8_t *data, uint16_t len) {
    printf("[Node A] Received request from node %d: ", src);
    for (uint16_t i = 0; i < len; i++) printf("%02X ", data[i]);
    printf("\n");

    // 回复
    uint8_t reply[] = {0xA5, 0xA6};
    route_reply(inst, src, trans_id, reply, sizeof(reply));
    printf("[Node A] Sent reply to node %d\n", src);
}

static void on_recv_b(route_instance_t *inst, uint8_t src, uint8_t trans_id,
                      const uint8_t *data, uint16_t len) {
    (void)inst; (void)trans_id;
    printf("[Node B] Received broadcast from node %d: ", src);
    for (uint16_t i = 0; i < len; i++) printf("%02X ", data[i]);
    printf("\n");
}

static volatile int response_received = 0;

static void on_response_a(int result, const uint8_t *data, uint16_t len, void *user_data) {
    (void)user_data;
    if (result == ROUTE_OK) {
        printf("[Node A] Got response from C: ");
        for (uint16_t i = 0; i < len; i++) printf("%02X ", data[i]);
        printf("\n");
    } else {
        printf("[Node A] Request failed: error %d\n", result);
    }
    response_received = 1;
}

// ============ 节点线程 ============

static volatile int running = 1;

static void *thread_node_a(void *param) {
    (void)param;

    // 等待其他节点就绪
    usleep(50000);

    // 异步发送请求到 C
    uint8_t request[] = {0xA0, 0xA1, 0xA2};
    printf("[Node A] Sending async request to node C (via B)...\n");
    int rc = route_send_async(inst_a, 3, request, sizeof(request), on_response_a, NULL);
    if (rc != ROUTE_OK) {
        printf("[Node A] Send failed: %d\n", rc);
    }

    while (running) {
        mq_drain_to_route(&queue_ba, inst_a, 0);
        route_poll(inst_a);
        route_tick(inst_a, now_ms());
        usleep(5000);
    }
    return NULL;
}

static void *thread_node_b(void *param) {
    (void)param;

    while (running) {
        mq_drain_to_route(&queue_ab, inst_b, 0);  // from A (port 0)
        mq_drain_to_route(&queue_cb, inst_b, 1);  // from C (port 1)
        route_poll(inst_b);
        route_tick(inst_b, now_ms());
        usleep(5000);
    }
    return NULL;
}

static void *thread_node_c(void *param) {
    (void)param;

    while (running) {
        mq_drain_to_route(&queue_bc, inst_c, 0);
        route_poll(inst_c);
        route_tick(inst_c, now_ms());
        usleep(5000);
    }
    return NULL;
}

// ============ 主函数 ============

int main(void) {
    printf("=== Route Module Example: Message Queue Transport ===\n");
    printf("Topology: A(1) --[mq]--> B(2) --[mq]--> C(3)\n");
    printf("Scenario: A sends request to C via B, C replies\n");
    printf("          Then B broadcasts to A and C\n\n");

    // 初始化消息队列
    mq_init(&queue_ab);
    mq_init(&queue_ba);
    mq_init(&queue_bc);
    mq_init(&queue_cb);

    // 创建路由实例
    route_config_t cfg = {.os = &pc_os};
    inst_a = route_create(1, &cfg);
    inst_b = route_create(2, &cfg);
    inst_c = route_create(3, &cfg);

    // === 节点 A：port 0 -> B ===
    route_port_t port_a0 = {.port_id = 0, .send = send_a_to_b};
    route_port_register(inst_a, &port_a0);
    route_entry_t routes_a[] = {
        {.dest_id = 2, .next_hop = 2, .port_id = 0},
        {.dest_id = 3, .next_hop = 2, .port_id = 0},  // C via B
    };
    route_table_set(inst_a, routes_a, 2);
    route_reliability_set(inst_a, route_reliability_e2e_strategy());
    route_on_recv(inst_a, on_recv_a);

    // === 节点 B：port 0 -> A, port 1 -> C ===
    route_port_t port_b0 = {.port_id = 0, .send = send_b_to_a};
    route_port_t port_b1 = {.port_id = 1, .send = send_b_to_c};
    route_port_register(inst_b, &port_b0);
    route_port_register(inst_b, &port_b1);
    route_entry_t routes_b[] = {
        {.dest_id = 1, .next_hop = 1, .port_id = 0},
        {.dest_id = 3, .next_hop = 3, .port_id = 1},
    };
    route_table_set(inst_b, routes_b, 2);
    route_reliability_set(inst_b, route_reliability_e2e_strategy());
    route_on_recv(inst_b, on_recv_b);

    // === 节点 C：port 0 -> B ===
    route_port_t port_c0 = {.port_id = 0, .send = send_c_to_b};
    route_port_register(inst_c, &port_c0);
    route_entry_t routes_c[] = {
        {.dest_id = 2, .next_hop = 2, .port_id = 0},
        {.dest_id = 1, .next_hop = 2, .port_id = 0},  // A via B
    };
    route_table_set(inst_c, routes_c, 2);
    route_reliability_set(inst_c, route_reliability_e2e_strategy());
    route_on_recv(inst_c, on_recv_c);

    // 启动线程
    pthread_t ta, tb, tc;
    pthread_create(&ta, NULL, thread_node_a, NULL);
    pthread_create(&tb, NULL, thread_node_b, NULL);
    pthread_create(&tc, NULL, thread_node_c, NULL);

    // 等待 A 收到响应
    while (!response_received) {
        usleep(10000);
    }

    // 等一下让系统稳定，然后 B 广播
    usleep(100000);
    printf("\n[Node B] Broadcasting message...\n");
    uint8_t broadcast_data[] = {0xBB, 0x01, 0x02};
    route_broadcast(inst_b, broadcast_data, sizeof(broadcast_data), NULL, NULL);

    // 等待广播被处理
    usleep(200000);

    // 演示同步发送：A 同步请求 C
    printf("\n[Node A] Sending SYNC request to node C...\n");
    uint8_t sync_req[] = {0xAA, 0xBB};
    uint8_t sync_resp[32];
    uint16_t sync_resp_len = sizeof(sync_resp);
    int rc = route_send_sync(inst_a, 3, sync_req, sizeof(sync_req),
                             sync_resp, &sync_resp_len, 2000);
    if (rc == ROUTE_OK) {
        printf("[Node A] Sync response: ");
        for (uint16_t i = 0; i < sync_resp_len; i++) printf("%02X ", sync_resp[i]);
        printf("\n");
    } else {
        printf("[Node A] Sync request failed: %d\n", rc);
    }

    // 演示：C 发送请求到 A（反向通信，经 B 中继）
    usleep(100000);
    printf("\n[Node C] Sending SYNC request to node A (via B)...\n");
    uint8_t c_req[] = {0xCC, 0xDD, 0xEE};
    uint8_t c_resp[32];
    uint16_t c_resp_len = sizeof(c_resp);
    rc = route_send_sync(inst_c, 1, c_req, sizeof(c_req),
                         c_resp, &c_resp_len, 2000);
    if (rc == ROUTE_OK) {
        printf("[Node C] Sync response from A: ");
        for (uint16_t i = 0; i < c_resp_len; i++) printf("%02X ", c_resp[i]);
        printf("\n");
    } else {
        printf("[Node C] Sync request to A failed: %d\n", rc);
    }

    // 清理
    usleep(100000);
    running = 0;
    pthread_join(ta, NULL);
    pthread_join(tb, NULL);
    pthread_join(tc, NULL);

    route_destroy(inst_a);
    route_destroy(inst_b);
    route_destroy(inst_c);

    printf("\n=== Example complete ===\n");
    return 0;
}
