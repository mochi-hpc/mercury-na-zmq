/**
 * NA ZMQ plugin - internal header
 *
 * ZeroMQ ROUTER-based NA transport plugin. Provides a portable TCP-based
 * backend for Mercury using ZMQ ROUTER sockets with identity-based routing.
 */

#ifndef NA_ZMQ_H
#define NA_ZMQ_H

#include <na_plugin.h>

#include <mercury_atomic.h>
#include <mercury_thread_mutex.h>

#include <zmq.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/**********************/
/* Plugin Constants   */
/**********************/

#define NA_ZMQ_MAX_MSG_SIZE (64 * 1024) /* 64 KB max message */
#define NA_ZMQ_MAX_ADDR_LEN 256

/**********************/
/* Op status flags    */
/**********************/

#define NA_ZMQ_OP_COMPLETED (1 << 0)
#define NA_ZMQ_OP_CANCELED  (1 << 1)
#define NA_ZMQ_OP_QUEUED    (1 << 2)

/**********************/
/* Wire protocol      */
/**********************/

enum na_zmq_msg_type {
    NA_ZMQ_UNEXPECTED = 1,
    NA_ZMQ_EXPECTED   = 2,
    NA_ZMQ_PUT        = 3,
    NA_ZMQ_GET_REQ    = 4,
    NA_ZMQ_GET_RESP   = 5
};

struct na_zmq_msg_hdr {
    uint8_t type;              /* na_zmq_msg_type */
    uint32_t tag;              /* Message tag */
    uint32_t payload_size;     /* Payload size in bytes */
    /* RMA fields */
    uint64_t handle_id;        /* Remote mem handle ID */
    uint64_t offset;           /* Remote offset */
    uint64_t rma_length;       /* RMA data length */
    /* GET_RESP: requester's local handle info */
    uint64_t local_handle_id;
    uint64_t local_offset;
};

/**********************/
/* Data structures    */
/**********************/

/* Address */
struct na_zmq_addr {
    char *zmq_identity;            /* ZMQ routing ID (unique string) */
    size_t zmq_identity_len;
    char *endpoint;                /* "tcp://host:port" for zmq_connect() */
    hg_atomic_int32_t refcount;
    bool self;
    bool connected;
    STAILQ_ENTRY(na_zmq_addr) entry;
};

/* Operation ID */
struct na_zmq_op_id {
    struct na_cb_completion_data completion_data; /* Must be first */
    na_context_t *context;
    struct na_zmq_addr *addr;
    hg_atomic_int32_t status;
    na_tag_t tag;
    void *buf;
    size_t buf_size;
    /* For RMA get response matching */
    struct na_zmq_mem_handle *local_handle;
    na_offset_t local_offset;
    size_t rma_length;
    STAILQ_ENTRY(na_zmq_op_id) entry;
};

/* Stashed unexpected message (received before a recv was posted) */
struct na_zmq_unexpected_msg {
    void *buf;
    size_t buf_size;
    struct na_zmq_addr *addr;    /* Source address */
    na_tag_t tag;
    STAILQ_ENTRY(na_zmq_unexpected_msg) entry;
};

/* Stashed expected message (received before a recv was posted) */
struct na_zmq_expected_msg {
    void *buf;
    size_t buf_size;
    struct na_zmq_addr *addr;    /* Source address */
    na_tag_t tag;
    STAILQ_ENTRY(na_zmq_expected_msg) entry;
};

/* Memory handle */
struct na_zmq_mem_handle {
    void *buf;
    size_t buf_size;
    uint64_t handle_id;
    unsigned long flags;
    STAILQ_ENTRY(na_zmq_mem_handle) entry;
};

/* Plugin class state */
struct na_zmq_class {
    void *zmq_context;
    void *zmq_socket;
    char *endpoint;                /* "tcp://host:port" after bind */
    char *self_identity;           /* ZMQ ROUTING_ID set on our socket */
    char self_addr_str[NA_ZMQ_MAX_ADDR_LEN];
    struct na_zmq_addr *self_addr;

    /* Operation queues (protected by queue_lock) */
    STAILQ_HEAD(, na_zmq_op_id) unexpected_op_queue;
    STAILQ_HEAD(, na_zmq_op_id) expected_op_queue;
    STAILQ_HEAD(, na_zmq_op_id) pending_rma_queue;

    /* Message stash queues (protected by queue_lock) */
    STAILQ_HEAD(, na_zmq_unexpected_msg) unexpected_msg_queue;
    STAILQ_HEAD(, na_zmq_expected_msg) expected_msg_queue;

    /* Memory handle registry (protected by queue_lock) */
    STAILQ_HEAD(, na_zmq_mem_handle) mem_handle_list;
    uint64_t next_mem_handle_id;

    /* Connected addresses for cleanup (protected by queue_lock) */
    STAILQ_HEAD(, na_zmq_addr) addr_list;

    hg_thread_mutex_t socket_lock; /* Protects ZMQ socket operations */
    hg_thread_mutex_t queue_lock;  /* Protects queues and shared state */

    bool listen;
};

#endif /* NA_ZMQ_H */
