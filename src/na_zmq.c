/**
 * NA ZMQ plugin implementation
 *
 * ZeroMQ ROUTER-based NA transport. Uses a single ROUTER socket per na_class,
 * with ZMQ identity set to "tcp://host:port". Messages are framed as
 * [routing_id][header][payload].
 */

#include "na_zmq.h"

#include <netdb.h>
#include <poll.h>
#include <stdio.h>
#include <sys/socket.h>
#include <unistd.h>
#include <uuid/uuid.h>

/*---------------------------------------------------------------------------*/
/* Local helpers                                                             */
/*---------------------------------------------------------------------------*/

static struct na_zmq_class *
na_zmq_class(na_class_t *na_class)
{
    return (struct na_zmq_class *) na_class->plugin_class;
}

static struct na_zmq_addr *
na_zmq_addr_create(const char *identity, const char *endpoint, bool self)
{
    struct na_zmq_addr *addr;

    addr = calloc(1, sizeof(*addr));
    if (addr == NULL)
        return NULL;

    addr->zmq_identity = strdup(identity);
    if (addr->zmq_identity == NULL) {
        free(addr);
        return NULL;
    }
    addr->zmq_identity_len = strlen(identity);

    if (endpoint) {
        addr->endpoint = strdup(endpoint);
        if (addr->endpoint == NULL) {
            free(addr->zmq_identity);
            free(addr);
            return NULL;
        }
    }

    hg_atomic_init32(&addr->refcount, 1);
    addr->self = self;
    addr->connected = false;

    return addr;
}

static void
na_zmq_addr_incref(struct na_zmq_addr *addr)
{
    hg_atomic_incr32(&addr->refcount);
}

static bool
na_zmq_addr_decref(struct na_zmq_addr *addr)
{
    if (hg_atomic_decr32(&addr->refcount) == 0) {
        free(addr->zmq_identity);
        free(addr->endpoint);
        free(addr);
        return true;
    }
    return false;
}

/* Ensure a ZMQ connect() has been called for this addr */
static na_return_t
na_zmq_addr_connect(struct na_zmq_class *priv, struct na_zmq_addr *addr)
{
    if (addr->connected || addr->self)
        return NA_SUCCESS;

    if (addr->endpoint == NULL) {
        /* No endpoint — this peer connected to us (inbound connection).
         * ZMQ ROUTER can send to it using its routing ID without connect. */
        addr->connected = true;
        return NA_SUCCESS;
    }

    if (zmq_connect(priv->zmq_socket, addr->endpoint) != 0) {
        NA_LOG_ERROR("zmq_connect(%s) failed: %s",
            addr->endpoint, zmq_strerror(errno));
        return NA_PROTOCOL_ERROR;
    }
    addr->connected = true;

    return NA_SUCCESS;
}

/*
 * Release callback — called by Mercury after the op has been triggered.
 * Resets the op status so it can be reused.
 */
static void
na_zmq_release(void *arg)
{
    struct na_zmq_op_id *op = (struct na_zmq_op_id *) arg;

    hg_atomic_set32(&op->status, NA_ZMQ_OP_COMPLETED);
}

/* Complete an op: set status, fill callback_info, add to completion queue */
static void
na_zmq_complete_op(struct na_zmq_op_id *op, na_return_t ret)
{
    hg_atomic_set32(&op->status, NA_ZMQ_OP_COMPLETED);
    op->completion_data.callback_info.ret = ret;
    na_cb_completion_add(op->context, &op->completion_data);
}

/* Send a multi-frame ZMQ message: [routing_id][header][payload] */
static na_return_t
na_zmq_send_msg(struct na_zmq_class *priv, struct na_zmq_addr *dest,
    const struct na_zmq_msg_hdr *hdr, const void *payload, size_t payload_size)
{
    na_return_t ret;
    int retries;

    /* Ensure connected */
    ret = na_zmq_addr_connect(priv, dest);
    if (ret != NA_SUCCESS)
        return ret;

    /*
     * With ZMQ_ROUTER_MANDATORY, sends to unconnected peers fail with
     * EHOSTUNREACH. Retry with backoff to allow the TCP connection to
     * complete after zmq_connect().
     */
    for (retries = 0; retries < 50; retries++) {
        /* Frame 1: routing ID (destination identity) */
        if (zmq_send(priv->zmq_socket, dest->zmq_identity,
                dest->zmq_identity_len, ZMQ_SNDMORE) < 0) {
            if (errno == EHOSTUNREACH) {
                usleep(10000); /* 10ms backoff */
                continue;
            }
            NA_LOG_ERROR(
                "zmq_send routing_id failed: %s", zmq_strerror(errno));
            return NA_PROTOCOL_ERROR;
        }

        /* Frame 2: header */
        if (zmq_send(priv->zmq_socket, hdr, sizeof(*hdr),
                payload_size > 0 ? ZMQ_SNDMORE : 0) < 0) {
            NA_LOG_ERROR("zmq_send header failed: %s", zmq_strerror(errno));
            return NA_PROTOCOL_ERROR;
        }

        /* Frame 3: payload (may be empty) */
        if (payload_size > 0) {
            if (zmq_send(priv->zmq_socket, payload, payload_size, 0) < 0) {
                NA_LOG_ERROR(
                    "zmq_send payload failed: %s", zmq_strerror(errno));
                return NA_PROTOCOL_ERROR;
            }
        }

        return NA_SUCCESS;
    }

    NA_LOG_ERROR("zmq_send failed after retries: peer %.*s unreachable",
        (int) dest->zmq_identity_len, dest->zmq_identity);
    return NA_PROTOCOL_ERROR;
}

/* Find or create an addr for a given identity string */
static struct na_zmq_addr *
na_zmq_find_or_create_addr(struct na_zmq_class *priv, const char *identity,
    size_t identity_len, const char *endpoint)
{
    struct na_zmq_addr *a;
    char id_buf[NA_ZMQ_MAX_ADDR_LEN];

    /* Check self */
    if (priv->self_addr &&
        priv->self_addr->zmq_identity_len == identity_len &&
        memcmp(priv->self_addr->zmq_identity, identity, identity_len) == 0) {
        na_zmq_addr_incref(priv->self_addr);
        return priv->self_addr;
    }

    /* Search existing */
    STAILQ_FOREACH(a, &priv->addr_list, entry) {
        if (a->zmq_identity_len == identity_len &&
            memcmp(a->zmq_identity, identity, identity_len) == 0) {
            /* Update endpoint if we now have one and didn't before */
            if (endpoint && !a->endpoint)
                a->endpoint = strdup(endpoint);
            na_zmq_addr_incref(a);
            return a;
        }
    }

    /* Create new */
    if (identity_len >= sizeof(id_buf))
        return NULL;
    memcpy(id_buf, identity, identity_len);
    id_buf[identity_len] = '\0';

    a = na_zmq_addr_create(id_buf, endpoint, false);
    if (a == NULL)
        return NULL;

    STAILQ_INSERT_TAIL(&priv->addr_list, a, entry);
    na_zmq_addr_incref(a); /* one ref for addr_list, one for caller */

    return a;
}

/* Build a proper endpoint string, replacing 0.0.0.0 with actual hostname */
static void
na_zmq_resolve_endpoint(const char *raw_endpoint, char *resolved, size_t size)
{
    char hostname[256];
    unsigned int port;

    /* Parse tcp://0.0.0.0:port format */
    if (sscanf(raw_endpoint, "tcp://0.0.0.0:%u", &port) == 1 ||
        sscanf(raw_endpoint, "tcp://*:%u", &port) == 1) {
        if (gethostname(hostname, sizeof(hostname)) == 0) {
            snprintf(resolved, size, "tcp://%s:%u", hostname, port);
            return;
        }
        /* Fallback to 127.0.0.1 */
        snprintf(resolved, size, "tcp://127.0.0.1:%u", port);
        return;
    }

    /* Already has a real address */
    snprintf(resolved, size, "%s", raw_endpoint);
}

/* Generate a unique identity string using a random UUID */
static void
na_zmq_gen_identity(char *buf, size_t size)
{
    uuid_t uuid;
    char uuid_str[37]; /* 36 chars + NUL */

    uuid_generate(uuid);
    uuid_unparse_lower(uuid, uuid_str);
    if (getenv("NA_ZMQ_USE_FULL_UUID") == NULL)
        uuid_str[8] = '\0';

    const char *cluster = getenv("NA_ZMQ_CLUSTER_NAME");
    if (cluster != NULL)
        snprintf(buf, size, "%s/%s", cluster, uuid_str);
    else
        snprintf(buf, size, "%s", uuid_str);
}

/*---------------------------------------------------------------------------*/
/* Protocol / class_name                                                     */
/*---------------------------------------------------------------------------*/

static na_return_t
na_zmq_get_protocol_info(const struct na_info *na_info NA_UNUSED,
    struct na_protocol_info **na_protocol_info_p)
{
    struct na_protocol_info *entry;
    na_return_t ret = NA_SUCCESS;

    entry = na_protocol_info_alloc("zmq", "tcp", "tcp");
    NA_CHECK_ERROR(
        entry == NULL, error, ret, NA_NOMEM, "Could not allocate protocol info");

    *na_protocol_info_p = entry;

error:
    return ret;
}

static bool
na_zmq_check_protocol(const char *protocol_name)
{
    return (strcmp(protocol_name, "zmq") == 0 ||
            strcmp(protocol_name, "tcp") == 0);
}

/*---------------------------------------------------------------------------*/
/* Initialize / finalize                                                     */
/*---------------------------------------------------------------------------*/

static na_return_t
na_zmq_initialize(na_class_t *na_class, const struct na_info *na_info,
    bool listen)
{
    struct na_zmq_class *priv = NULL;
    na_return_t ret = NA_SUCCESS;
    char bind_addr[NA_ZMQ_MAX_ADDR_LEN];
    char endpoint[NA_ZMQ_MAX_ADDR_LEN];
    size_t endpoint_len = sizeof(endpoint);
    int zero = 0;

    priv = calloc(1, sizeof(*priv));
    NA_CHECK_ERROR(priv == NULL, error, ret, NA_NOMEM, "calloc failed");

    priv->listen = listen;
    STAILQ_INIT(&priv->unexpected_op_queue);
    STAILQ_INIT(&priv->expected_op_queue);
    STAILQ_INIT(&priv->pending_rma_queue);
    STAILQ_INIT(&priv->unexpected_msg_queue);
    STAILQ_INIT(&priv->expected_msg_queue);
    STAILQ_INIT(&priv->mem_handle_list);
    STAILQ_INIT(&priv->addr_list);
    priv->next_mem_handle_id = 1;
    hg_thread_mutex_init(&priv->socket_lock);
    hg_thread_mutex_init(&priv->queue_lock);

    /* Create ZMQ context */
    priv->zmq_context = zmq_ctx_new();
    NA_CHECK_ERROR(
        priv->zmq_context == NULL, error, ret, NA_PROTOCOL_ERROR,
        "zmq_ctx_new() failed: %s", zmq_strerror(errno));

    /* Create ROUTER socket */
    priv->zmq_socket = zmq_socket(priv->zmq_context, ZMQ_ROUTER);
    NA_CHECK_ERROR(
        priv->zmq_socket == NULL, error, ret, NA_PROTOCOL_ERROR,
        "zmq_socket(ROUTER) failed: %s", zmq_strerror(errno));

    /* Set linger to 0 for clean shutdown */
    zmq_setsockopt(priv->zmq_socket, ZMQ_LINGER, &zero, sizeof(zero));

    /* Enable ROUTER_MANDATORY to detect sends to unconnected peers */
    {
        int mandatory = 1;
        zmq_setsockopt(priv->zmq_socket, ZMQ_ROUTER_MANDATORY,
            &mandatory, sizeof(mandatory));
    }

    /* Generate unique identity BEFORE bind (required by ZMQ ROUTER) */
    {
        char identity[NA_ZMQ_MAX_ADDR_LEN];
        na_zmq_gen_identity(identity, sizeof(identity));
        zmq_setsockopt(priv->zmq_socket, ZMQ_ROUTING_ID,
            identity, strlen(identity));
        priv->self_identity = strdup(identity);
        NA_CHECK_ERROR(priv->self_identity == NULL, error, ret, NA_NOMEM,
            "strdup failed");
    }

    /* Determine bind address */
    if (listen && na_info && na_info->host_name &&
        strlen(na_info->host_name) > 0) {
        snprintf(bind_addr, sizeof(bind_addr), "tcp://%s",
            na_info->host_name);
    } else {
        snprintf(bind_addr, sizeof(bind_addr), "tcp://*:*");
    }

    /* Bind */
    if (zmq_bind(priv->zmq_socket, bind_addr) != 0) {
        NA_GOTO_ERROR(error, ret, NA_PROTOCOL_ERROR,
            "zmq_bind(%s) failed: %s", bind_addr, zmq_strerror(errno));
    }

    /* Query actual bound endpoint */
    if (zmq_getsockopt(priv->zmq_socket, ZMQ_LAST_ENDPOINT,
            endpoint, &endpoint_len) != 0) {
        NA_GOTO_ERROR(error, ret, NA_PROTOCOL_ERROR,
            "zmq_getsockopt(LAST_ENDPOINT) failed: %s",
            zmq_strerror(errno));
    }

    /* Resolve 0.0.0.0 → actual hostname */
    {
        char resolved[NA_ZMQ_MAX_ADDR_LEN];
        na_zmq_resolve_endpoint(endpoint, resolved, sizeof(resolved));

        priv->endpoint = strdup(resolved);
        NA_CHECK_ERROR(priv->endpoint == NULL, error, ret, NA_NOMEM,
            "strdup failed");
    }

    snprintf(priv->self_addr_str, sizeof(priv->self_addr_str),
        "%s", priv->endpoint);

    /* Create self address (identity for routing, endpoint for connecting) */
    priv->self_addr = na_zmq_addr_create(
        priv->self_identity, priv->endpoint, true);
    NA_CHECK_ERROR(priv->self_addr == NULL, error, ret, NA_NOMEM,
        "Could not create self address");
    priv->self_addr->connected = true;

    na_class->plugin_class = priv;
    return NA_SUCCESS;

error:
    if (priv) {
        if (priv->zmq_socket)
            zmq_close(priv->zmq_socket);
        if (priv->zmq_context)
            zmq_ctx_destroy(priv->zmq_context);
        hg_thread_mutex_destroy(&priv->socket_lock);
        hg_thread_mutex_destroy(&priv->queue_lock);
        free(priv->endpoint);
        free(priv->self_identity);
        free(priv);
    }
    return ret;
}

static na_return_t
na_zmq_finalize(na_class_t *na_class)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);
    struct na_zmq_unexpected_msg *umsg;
    struct na_zmq_expected_msg *emsg;
    struct na_zmq_mem_handle *mh;
    struct na_zmq_addr *addr;

    if (priv == NULL)
        return NA_SUCCESS;

    /* Drain unexpected message queue */
    while (!STAILQ_EMPTY(&priv->unexpected_msg_queue)) {
        umsg = STAILQ_FIRST(&priv->unexpected_msg_queue);
        STAILQ_REMOVE_HEAD(&priv->unexpected_msg_queue, entry);
        if (umsg->addr)
            na_zmq_addr_decref(umsg->addr);
        free(umsg->buf);
        free(umsg);
    }

    /* Drain expected message queue */
    while (!STAILQ_EMPTY(&priv->expected_msg_queue)) {
        emsg = STAILQ_FIRST(&priv->expected_msg_queue);
        STAILQ_REMOVE_HEAD(&priv->expected_msg_queue, entry);
        if (emsg->addr)
            na_zmq_addr_decref(emsg->addr);
        free(emsg->buf);
        free(emsg);
    }

    /* Free mem handles */
    while (!STAILQ_EMPTY(&priv->mem_handle_list)) {
        mh = STAILQ_FIRST(&priv->mem_handle_list);
        STAILQ_REMOVE_HEAD(&priv->mem_handle_list, entry);
        free(mh);
    }

    /* Free tracked addresses */
    while (!STAILQ_EMPTY(&priv->addr_list)) {
        addr = STAILQ_FIRST(&priv->addr_list);
        STAILQ_REMOVE_HEAD(&priv->addr_list, entry);
        na_zmq_addr_decref(addr);
    }

    /* Free self addr */
    if (priv->self_addr)
        na_zmq_addr_decref(priv->self_addr);

    /* Close ZMQ resources */
    if (priv->zmq_socket)
        zmq_close(priv->zmq_socket);
    if (priv->zmq_context)
        zmq_ctx_destroy(priv->zmq_context);

    hg_thread_mutex_destroy(&priv->socket_lock);
    hg_thread_mutex_destroy(&priv->queue_lock);
    free(priv->endpoint);
    free(priv->self_identity);
    free(priv);
    na_class->plugin_class = NULL;

    return NA_SUCCESS;
}

static void
na_zmq_cleanup(void)
{
    /* No global state */
}

/*---------------------------------------------------------------------------*/
/* Context                                                                   */
/*---------------------------------------------------------------------------*/

static na_return_t
na_zmq_context_create(na_class_t *na_class NA_UNUSED,
    na_context_t *na_context NA_UNUSED, void **plugin_context_p,
    uint8_t id NA_UNUSED)
{
    *plugin_context_p = NULL;
    return NA_SUCCESS;
}

static na_return_t
na_zmq_context_destroy(na_class_t *na_class NA_UNUSED,
    void *plugin_context NA_UNUSED)
{
    return NA_SUCCESS;
}

/*---------------------------------------------------------------------------*/
/* Op create/destroy                                                         */
/*---------------------------------------------------------------------------*/

static na_op_id_t *
na_zmq_op_create(na_class_t *na_class NA_UNUSED, unsigned long flags NA_UNUSED)
{
    struct na_zmq_op_id *op;

    op = calloc(1, sizeof(*op));
    if (op == NULL)
        return NULL;

    hg_atomic_init32(&op->status, NA_ZMQ_OP_COMPLETED);

    /* Set plugin release callback — resets op status after trigger */
    op->completion_data.plugin_callback = na_zmq_release;
    op->completion_data.plugin_callback_args = op;

    return (na_op_id_t *) op;
}

static void
na_zmq_op_destroy(na_class_t *na_class NA_UNUSED, na_op_id_t *op_id)
{
    free(op_id);
}

/*---------------------------------------------------------------------------*/
/* Address operations                                                        */
/*---------------------------------------------------------------------------*/

static na_return_t
na_zmq_addr_lookup(na_class_t *na_class, const char *name, na_addr_t **addr_p)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);
    struct na_zmq_addr *addr;
    na_return_t ret = NA_SUCCESS;
    char tcp_endpoint[NA_ZMQ_MAX_ADDR_LEN];
    char identity[NA_ZMQ_MAX_ADDR_LEN];
    const char *input = name;
    const char *hash;

    /* Strip zmq:// or tcp:// prefix for parsing */
    if (strncmp(input, "zmq://", 6) == 0)
        input = input + 6;
    else if (strncmp(input, "tcp://", 6) == 0)
        input = input + 6;

    /* Parse "host:port#identity" or just "host:port" */
    hash = strchr(input, '#');
    if (hash) {
        size_t ep_len = (size_t)(hash - input);
        if (ep_len >= sizeof(tcp_endpoint))
            ep_len = sizeof(tcp_endpoint) - 1;
        snprintf(tcp_endpoint, ep_len + 7, "tcp://%s", input);
        tcp_endpoint[ep_len + 6] = '\0';
        snprintf(identity, sizeof(identity), "%s", hash + 1);
    } else {
        snprintf(tcp_endpoint, sizeof(tcp_endpoint), "tcp://%s", input);
        /* No identity provided — cannot route without it */
        NA_GOTO_ERROR(error, ret, NA_PROTOCOL_ERROR,
            "Address '%s' missing identity (expected host:port#identity)",
            name);
    }

    hg_thread_mutex_lock(&priv->queue_lock);
    addr = na_zmq_find_or_create_addr(priv, identity, strlen(identity),
        tcp_endpoint);
    hg_thread_mutex_unlock(&priv->queue_lock);
    if (addr == NULL) {
        NA_GOTO_ERROR(error, ret, NA_NOMEM,
            "Could not create addr for %s", name);
    }

    /* Connect */
    hg_thread_mutex_lock(&priv->socket_lock);
    ret = na_zmq_addr_connect(priv, addr);
    hg_thread_mutex_unlock(&priv->socket_lock);

    if (ret != NA_SUCCESS) {
        na_zmq_addr_decref(addr);
        goto error;
    }

    *addr_p = (na_addr_t *) addr;
    return NA_SUCCESS;

error:
    return ret;
}

static void
na_zmq_addr_free(na_class_t *na_class NA_UNUSED, na_addr_t *addr)
{
    struct na_zmq_addr *a = (struct na_zmq_addr *) addr;
    if (a)
        na_zmq_addr_decref(a);
}

static na_return_t
na_zmq_addr_self(na_class_t *na_class, na_addr_t **addr_p)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);

    na_zmq_addr_incref(priv->self_addr);
    *addr_p = (na_addr_t *) priv->self_addr;

    return NA_SUCCESS;
}

static na_return_t
na_zmq_addr_dup(na_class_t *na_class NA_UNUSED, na_addr_t *addr,
    na_addr_t **new_addr_p)
{
    struct na_zmq_addr *a = (struct na_zmq_addr *) addr;

    na_zmq_addr_incref(a);
    *new_addr_p = addr;

    return NA_SUCCESS;
}

static bool
na_zmq_addr_cmp(na_class_t *na_class NA_UNUSED, na_addr_t *addr1,
    na_addr_t *addr2)
{
    struct na_zmq_addr *a1 = (struct na_zmq_addr *) addr1;
    struct na_zmq_addr *a2 = (struct na_zmq_addr *) addr2;

    if (a1 == a2)
        return true;
    if (a1->zmq_identity_len != a2->zmq_identity_len)
        return false;
    return memcmp(a1->zmq_identity, a2->zmq_identity,
        a1->zmq_identity_len) == 0;
}

static bool
na_zmq_addr_is_self(na_class_t *na_class, na_addr_t *addr)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);
    struct na_zmq_addr *a = (struct na_zmq_addr *) addr;

    return na_zmq_addr_cmp(na_class, (na_addr_t *) priv->self_addr,
        (na_addr_t *) a);
}

static na_return_t
na_zmq_addr_to_string(na_class_t *na_class NA_UNUSED, char *buf,
    size_t *buf_size, na_addr_t *addr)
{
    struct na_zmq_addr *a = (struct na_zmq_addr *) addr;
    char str[NA_ZMQ_MAX_ADDR_LEN];
    size_t len;

    /* Format: "tcp://host:port#identity" */
    if (a->endpoint) {
        snprintf(str, sizeof(str), "%s#%s", a->endpoint, a->zmq_identity);
    } else {
        snprintf(str, sizeof(str), "#%s", a->zmq_identity);
    }
    len = strlen(str) + 1;

    if (buf) {
        if (*buf_size < len) {
            *buf_size = len;
            return NA_OVERFLOW;
        }
        memcpy(buf, str, len);
    }
    *buf_size = len;

    return NA_SUCCESS;
}

static size_t
na_zmq_addr_get_serialize_size(na_class_t *na_class NA_UNUSED,
    na_addr_t *addr)
{
    struct na_zmq_addr *a = (struct na_zmq_addr *) addr;
    size_t ep_len = a->endpoint ? strlen(a->endpoint) : 0;
    return sizeof(uint32_t) + a->zmq_identity_len +
           sizeof(uint32_t) + ep_len;
}

static na_return_t
na_zmq_addr_serialize(na_class_t *na_class NA_UNUSED, void *buf,
    size_t buf_size, na_addr_t *addr)
{
    struct na_zmq_addr *a = (struct na_zmq_addr *) addr;
    na_return_t ret = NA_SUCCESS;
    char *buf_ptr = (char *) buf;
    size_t buf_size_left = buf_size;
    uint32_t id_len = (uint32_t) a->zmq_identity_len;
    uint32_t ep_len = a->endpoint ? (uint32_t) strlen(a->endpoint) : 0;

    NA_ENCODE(error, ret, buf_ptr, buf_size_left, &id_len, uint32_t);
    NA_TYPE_ENCODE(error, ret, buf_ptr, buf_size_left,
        a->zmq_identity, a->zmq_identity_len);
    NA_ENCODE(error, ret, buf_ptr, buf_size_left, &ep_len, uint32_t);
    if (ep_len > 0)
        NA_TYPE_ENCODE(error, ret, buf_ptr, buf_size_left,
            a->endpoint, ep_len);

    return NA_SUCCESS;

error:
    return ret;
}

static na_return_t
na_zmq_addr_deserialize(na_class_t *na_class, na_addr_t **addr_p,
    const void *buf, size_t buf_size, uint64_t flags NA_UNUSED)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);
    na_return_t ret = NA_SUCCESS;
    const char *buf_ptr = (const char *) buf;
    size_t buf_size_left = buf_size;
    uint32_t id_len, ep_len;
    char identity[NA_ZMQ_MAX_ADDR_LEN];
    char ep[NA_ZMQ_MAX_ADDR_LEN];
    struct na_zmq_addr *addr;

    NA_DECODE(error, ret, buf_ptr, buf_size_left, &id_len, uint32_t);
    NA_CHECK_ERROR(id_len >= sizeof(identity), error, ret, NA_OVERFLOW,
        "Identity too long: %u", id_len);
    NA_TYPE_DECODE(error, ret, buf_ptr, buf_size_left, identity, id_len);
    identity[id_len] = '\0';

    NA_DECODE(error, ret, buf_ptr, buf_size_left, &ep_len, uint32_t);
    NA_CHECK_ERROR(ep_len >= sizeof(ep), error, ret, NA_OVERFLOW,
        "Endpoint too long: %u", ep_len);
    if (ep_len > 0)
        NA_TYPE_DECODE(error, ret, buf_ptr, buf_size_left, ep, ep_len);
    ep[ep_len] = '\0';

    hg_thread_mutex_lock(&priv->queue_lock);
    addr = na_zmq_find_or_create_addr(priv, identity, id_len,
        ep_len > 0 ? ep : NULL);
    hg_thread_mutex_unlock(&priv->queue_lock);
    if (addr == NULL) {
        NA_GOTO_ERROR(error, ret, NA_NOMEM,
            "Could not create addr from deserialized identity");
    }

    /* Connect if we have an endpoint */
    if (addr->endpoint) {
        hg_thread_mutex_lock(&priv->socket_lock);
        ret = na_zmq_addr_connect(priv, addr);
        hg_thread_mutex_unlock(&priv->socket_lock);
        if (ret != NA_SUCCESS) {
            na_zmq_addr_decref(addr);
            goto error;
        }
    }

    *addr_p = (na_addr_t *) addr;
    return NA_SUCCESS;

error:
    return ret;
}

/*---------------------------------------------------------------------------*/
/* Message size / tag                                                        */
/*---------------------------------------------------------------------------*/

static size_t
na_zmq_msg_get_max_unexpected_size(const na_class_t *na_class NA_UNUSED)
{
    return NA_ZMQ_MAX_MSG_SIZE;
}

static size_t
na_zmq_msg_get_max_expected_size(const na_class_t *na_class NA_UNUSED)
{
    return NA_ZMQ_MAX_MSG_SIZE;
}

static na_tag_t
na_zmq_msg_get_max_tag(const na_class_t *na_class NA_UNUSED)
{
    return UINT32_MAX;
}

/*---------------------------------------------------------------------------*/
/* Send / Recv unexpected                                                    */
/*---------------------------------------------------------------------------*/

static na_return_t
na_zmq_msg_send_unexpected(na_class_t *na_class, na_context_t *context,
    na_cb_t callback, void *arg, const void *buf, size_t buf_size,
    void *plugin_data NA_UNUSED, na_addr_t *dest_addr,
    uint8_t dest_id NA_UNUSED, na_tag_t tag, na_op_id_t *op_id)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);
    struct na_zmq_op_id *op = (struct na_zmq_op_id *) op_id;
    struct na_zmq_addr *dest = (struct na_zmq_addr *) dest_addr;
    struct na_zmq_msg_hdr hdr;
    na_return_t ret;

    /* Set up op */
    hg_atomic_set32(&op->status, 0);
    op->context = context;
    op->completion_data.callback_info.type = NA_CB_SEND_UNEXPECTED;
    op->completion_data.callback_info.arg = arg;
    op->completion_data.callback = callback;

    /* Build header */
    memset(&hdr, 0, sizeof(hdr));
    hdr.type = NA_ZMQ_UNEXPECTED;
    hdr.tag = tag;
    hdr.payload_size = (uint32_t) buf_size;

    /* Send (socket_lock protects ZMQ socket) */
    hg_thread_mutex_lock(&priv->socket_lock);
    ret = na_zmq_send_msg(priv, dest, &hdr, buf, buf_size);
    hg_thread_mutex_unlock(&priv->socket_lock);

    if (ret != NA_SUCCESS) {
        na_zmq_complete_op(op, ret);
        return NA_SUCCESS; /* Op was submitted, error reported via callback */
    }

    /* Send completes immediately */
    na_zmq_complete_op(op, NA_SUCCESS);
    return NA_SUCCESS;
}

static na_return_t
na_zmq_msg_recv_unexpected(na_class_t *na_class, na_context_t *context,
    na_cb_t callback, void *arg, void *buf, size_t buf_size,
    void *plugin_data NA_UNUSED, na_op_id_t *op_id)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);
    struct na_zmq_op_id *op = (struct na_zmq_op_id *) op_id;

    /* Set up op */
    hg_atomic_set32(&op->status, 0);
    op->context = context;
    op->buf = buf;
    op->buf_size = buf_size;
    op->completion_data.callback_info.type = NA_CB_RECV_UNEXPECTED;
    op->completion_data.callback_info.arg = arg;
    op->completion_data.callback = callback;

    /* Check if there's a stashed unexpected message */
    hg_thread_mutex_lock(&priv->queue_lock);
    if (!STAILQ_EMPTY(&priv->unexpected_msg_queue)) {
        struct na_zmq_unexpected_msg *umsg;

        umsg = STAILQ_FIRST(&priv->unexpected_msg_queue);
        STAILQ_REMOVE_HEAD(&priv->unexpected_msg_queue, entry);
        hg_thread_mutex_unlock(&priv->queue_lock);

        /* Copy data */
        size_t copy_size = umsg->buf_size < buf_size ?
            umsg->buf_size : buf_size;
        if (copy_size > 0 && umsg->buf)
            memcpy(buf, umsg->buf, copy_size);

        /* Fill callback info */
        op->completion_data.callback_info.info.recv_unexpected.actual_buf_size =
            umsg->buf_size;
        op->completion_data.callback_info.info.recv_unexpected.source =
            (na_addr_t *) umsg->addr;
        op->completion_data.callback_info.info.recv_unexpected.tag = umsg->tag;

        free(umsg->buf);
        free(umsg);

        na_zmq_complete_op(op, NA_SUCCESS);
        return NA_SUCCESS;
    }

    /* No message available, queue the op */
    hg_atomic_set32(&op->status, NA_ZMQ_OP_QUEUED);
    STAILQ_INSERT_TAIL(&priv->unexpected_op_queue, op, entry);
    hg_thread_mutex_unlock(&priv->queue_lock);

    return NA_SUCCESS;
}

/*---------------------------------------------------------------------------*/
/* Send / Recv expected                                                      */
/*---------------------------------------------------------------------------*/

static na_return_t
na_zmq_msg_send_expected(na_class_t *na_class, na_context_t *context,
    na_cb_t callback, void *arg, const void *buf, size_t buf_size,
    void *plugin_data NA_UNUSED, na_addr_t *dest_addr,
    uint8_t dest_id NA_UNUSED, na_tag_t tag, na_op_id_t *op_id)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);
    struct na_zmq_op_id *op = (struct na_zmq_op_id *) op_id;
    struct na_zmq_addr *dest = (struct na_zmq_addr *) dest_addr;
    struct na_zmq_msg_hdr hdr;
    na_return_t ret;

    /* Set up op */
    hg_atomic_set32(&op->status, 0);
    op->context = context;
    op->completion_data.callback_info.type = NA_CB_SEND_EXPECTED;
    op->completion_data.callback_info.arg = arg;
    op->completion_data.callback = callback;

    /* Build header */
    memset(&hdr, 0, sizeof(hdr));
    hdr.type = NA_ZMQ_EXPECTED;
    hdr.tag = tag;
    hdr.payload_size = (uint32_t) buf_size;

    hg_thread_mutex_lock(&priv->socket_lock);
    ret = na_zmq_send_msg(priv, dest, &hdr, buf, buf_size);
    hg_thread_mutex_unlock(&priv->socket_lock);

    if (ret != NA_SUCCESS) {
        na_zmq_complete_op(op, ret);
        return NA_SUCCESS;
    }

    na_zmq_complete_op(op, NA_SUCCESS);
    return NA_SUCCESS;
}

static na_return_t
na_zmq_msg_recv_expected(na_class_t *na_class, na_context_t *context,
    na_cb_t callback, void *arg, void *buf, size_t buf_size,
    void *plugin_data NA_UNUSED, na_addr_t *source_addr,
    uint8_t source_id NA_UNUSED, na_tag_t tag, na_op_id_t *op_id)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);
    struct na_zmq_op_id *op = (struct na_zmq_op_id *) op_id;

    /* Set up op */
    hg_atomic_set32(&op->status, 0);
    op->context = context;
    op->buf = buf;
    op->buf_size = buf_size;
    op->tag = tag;
    op->addr = (struct na_zmq_addr *) source_addr;
    if (op->addr)
        na_zmq_addr_incref(op->addr);
    op->completion_data.callback_info.type = NA_CB_RECV_EXPECTED;
    op->completion_data.callback_info.arg = arg;
    op->completion_data.callback = callback;

    /* Check if there's a stashed expected message matching this recv */
    hg_thread_mutex_lock(&priv->queue_lock);
    {
        struct na_zmq_expected_msg *emsg;
        struct na_zmq_expected_msg *match = NULL;

        STAILQ_FOREACH(emsg, &priv->expected_msg_queue, entry) {
            bool tag_match = (emsg->tag == tag);
            bool addr_match = !op->addr ||
                (op->addr->zmq_identity_len == emsg->addr->zmq_identity_len &&
                 memcmp(op->addr->zmq_identity, emsg->addr->zmq_identity,
                     emsg->addr->zmq_identity_len) == 0);

            if (tag_match && addr_match) {
                match = emsg;
                break;
            }
        }

        if (match != NULL) {
            STAILQ_REMOVE(&priv->expected_msg_queue, match,
                na_zmq_expected_msg, entry);
            hg_thread_mutex_unlock(&priv->queue_lock);

            size_t copy_size = match->buf_size < buf_size ?
                match->buf_size : buf_size;
            if (copy_size > 0 && match->buf)
                memcpy(buf, match->buf, copy_size);

            op->completion_data.callback_info.info
                .recv_expected.actual_buf_size = match->buf_size;

            if (op->addr)
                na_zmq_addr_decref(op->addr);
            if (match->addr)
                na_zmq_addr_decref(match->addr);

            free(match->buf);
            free(match);

            na_zmq_complete_op(op, NA_SUCCESS);
            return NA_SUCCESS;
        }
    }

    /* No message available, queue the op for matching during poll */
    hg_atomic_set32(&op->status, NA_ZMQ_OP_QUEUED);
    STAILQ_INSERT_TAIL(&priv->expected_op_queue, op, entry);
    hg_thread_mutex_unlock(&priv->queue_lock);

    return NA_SUCCESS;
}

/*---------------------------------------------------------------------------*/
/* Memory handle operations                                                  */
/*---------------------------------------------------------------------------*/

static na_return_t
na_zmq_mem_handle_create(na_class_t *na_class, void *buf, size_t buf_size,
    unsigned long flags, na_mem_handle_t **mem_handle_p)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);
    struct na_zmq_mem_handle *mh;
    na_return_t ret = NA_SUCCESS;

    mh = calloc(1, sizeof(*mh));
    NA_CHECK_ERROR(mh == NULL, error, ret, NA_NOMEM, "calloc failed");

    mh->buf = buf;
    mh->buf_size = buf_size;
    mh->flags = flags;

    hg_thread_mutex_lock(&priv->queue_lock);
    mh->handle_id = priv->next_mem_handle_id++;
    STAILQ_INSERT_TAIL(&priv->mem_handle_list, mh, entry);
    hg_thread_mutex_unlock(&priv->queue_lock);

    *mem_handle_p = (na_mem_handle_t *) mh;

error:
    return ret;
}

static void
na_zmq_mem_handle_free(na_class_t *na_class, na_mem_handle_t *mem_handle)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);
    struct na_zmq_mem_handle *mh = (struct na_zmq_mem_handle *) mem_handle;

    /* Only remove from list if this is a local handle (has a buffer).
     * Deserialized (remote) handles are not in the list. */
    if (mh->buf != NULL) {
        hg_thread_mutex_lock(&priv->queue_lock);
        STAILQ_REMOVE(&priv->mem_handle_list, mh, na_zmq_mem_handle, entry);
        hg_thread_mutex_unlock(&priv->queue_lock);
    }
    free(mh);
}

static size_t
na_zmq_mem_handle_get_serialize_size(na_class_t *na_class NA_UNUSED,
    na_mem_handle_t *mem_handle NA_UNUSED)
{
    return sizeof(uint64_t) + sizeof(uint64_t); /* handle_id + buf_size */
}

static na_return_t
na_zmq_mem_handle_serialize(na_class_t *na_class NA_UNUSED, void *buf,
    size_t buf_size, na_mem_handle_t *mem_handle)
{
    struct na_zmq_mem_handle *mh = (struct na_zmq_mem_handle *) mem_handle;
    na_return_t ret = NA_SUCCESS;
    char *buf_ptr = (char *) buf;
    size_t buf_size_left = buf_size;

    NA_ENCODE(error, ret, buf_ptr, buf_size_left, &mh->handle_id, uint64_t);
    NA_ENCODE(error, ret, buf_ptr, buf_size_left, &mh->buf_size, uint64_t);

    return NA_SUCCESS;

error:
    return ret;
}

static na_return_t
na_zmq_mem_handle_deserialize(na_class_t *na_class NA_UNUSED,
    na_mem_handle_t **mem_handle_p, const void *buf, size_t buf_size)
{
    struct na_zmq_mem_handle *mh;
    na_return_t ret = NA_SUCCESS;
    const char *buf_ptr = (const char *) buf;
    size_t buf_size_left = buf_size;

    mh = calloc(1, sizeof(*mh));
    NA_CHECK_ERROR(mh == NULL, error, ret, NA_NOMEM, "calloc failed");

    NA_DECODE(error_free, ret, buf_ptr, buf_size_left,
        &mh->handle_id, uint64_t);
    NA_DECODE(error_free, ret, buf_ptr, buf_size_left,
        &mh->buf_size, uint64_t);

    mh->buf = NULL; /* Remote handle — no local buffer */

    *mem_handle_p = (na_mem_handle_t *) mh;
    return NA_SUCCESS;

error_free:
    free(mh);
error:
    return ret;
}

/*---------------------------------------------------------------------------*/
/* RMA: put / get                                                            */
/*---------------------------------------------------------------------------*/

static na_return_t
na_zmq_put(na_class_t *na_class, na_context_t *context, na_cb_t callback,
    void *arg, na_mem_handle_t *local_mem_handle, na_offset_t local_offset,
    na_mem_handle_t *remote_mem_handle, na_offset_t remote_offset,
    size_t length, na_addr_t *remote_addr, uint8_t remote_id NA_UNUSED,
    na_op_id_t *op_id)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);
    struct na_zmq_op_id *op = (struct na_zmq_op_id *) op_id;
    struct na_zmq_mem_handle *local_mh =
        (struct na_zmq_mem_handle *) local_mem_handle;
    struct na_zmq_mem_handle *remote_mh =
        (struct na_zmq_mem_handle *) remote_mem_handle;
    struct na_zmq_addr *dest = (struct na_zmq_addr *) remote_addr;
    struct na_zmq_msg_hdr hdr;
    na_return_t ret;

    /* Set up op */
    hg_atomic_set32(&op->status, 0);
    op->context = context;
    op->completion_data.callback_info.type = NA_CB_PUT;
    op->completion_data.callback_info.arg = arg;
    op->completion_data.callback = callback;

    /* Build PUT header */
    memset(&hdr, 0, sizeof(hdr));
    hdr.type = NA_ZMQ_PUT;
    hdr.handle_id = remote_mh->handle_id;
    hdr.offset = remote_offset;
    hdr.payload_size = (uint32_t) length;

    /* Send local data */
    hg_thread_mutex_lock(&priv->socket_lock);
    ret = na_zmq_send_msg(priv, dest, &hdr,
        (const char *) local_mh->buf + local_offset, length);
    hg_thread_mutex_unlock(&priv->socket_lock);

    if (ret != NA_SUCCESS) {
        na_zmq_complete_op(op, ret);
        return NA_SUCCESS;
    }

    na_zmq_complete_op(op, NA_SUCCESS);
    return NA_SUCCESS;
}

static na_return_t
na_zmq_get(na_class_t *na_class, na_context_t *context, na_cb_t callback,
    void *arg, na_mem_handle_t *local_mem_handle, na_offset_t local_offset,
    na_mem_handle_t *remote_mem_handle, na_offset_t remote_offset,
    size_t length, na_addr_t *remote_addr, uint8_t remote_id NA_UNUSED,
    na_op_id_t *op_id)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);
    struct na_zmq_op_id *op = (struct na_zmq_op_id *) op_id;
    struct na_zmq_mem_handle *local_mh =
        (struct na_zmq_mem_handle *) local_mem_handle;
    struct na_zmq_mem_handle *remote_mh =
        (struct na_zmq_mem_handle *) remote_mem_handle;
    struct na_zmq_addr *dest = (struct na_zmq_addr *) remote_addr;
    struct na_zmq_msg_hdr hdr;
    na_return_t ret;

    /* Set up op */
    hg_atomic_set32(&op->status, 0);
    op->context = context;
    op->local_handle = local_mh;
    op->local_offset = local_offset;
    op->rma_length = length;
    op->completion_data.callback_info.type = NA_CB_GET;
    op->completion_data.callback_info.arg = arg;
    op->completion_data.callback = callback;

    /* Build GET_REQ header */
    memset(&hdr, 0, sizeof(hdr));
    hdr.type = NA_ZMQ_GET_REQ;
    hdr.handle_id = remote_mh->handle_id;
    hdr.offset = remote_offset;
    hdr.rma_length = length;
    hdr.local_handle_id = local_mh->handle_id;
    hdr.local_offset = local_offset;

    /* Queue for response matching (before sending, to avoid race) */
    hg_thread_mutex_lock(&priv->queue_lock);
    hg_atomic_set32(&op->status, NA_ZMQ_OP_QUEUED);
    STAILQ_INSERT_TAIL(&priv->pending_rma_queue, op, entry);
    hg_thread_mutex_unlock(&priv->queue_lock);

    hg_thread_mutex_lock(&priv->socket_lock);
    ret = na_zmq_send_msg(priv, dest, &hdr, NULL, 0);
    hg_thread_mutex_unlock(&priv->socket_lock);

    if (ret != NA_SUCCESS) {
        /* Remove from queue and complete with error */
        hg_thread_mutex_lock(&priv->queue_lock);
        STAILQ_REMOVE(&priv->pending_rma_queue, op, na_zmq_op_id, entry);
        hg_thread_mutex_unlock(&priv->queue_lock);
        na_zmq_complete_op(op, ret);
        return NA_SUCCESS;
    }

    return NA_SUCCESS;
}

/*---------------------------------------------------------------------------*/
/* Polling / progress                                                        */
/*---------------------------------------------------------------------------*/

static int
na_zmq_poll_get_fd(na_class_t NA_UNUSED *na_class,
    na_context_t NA_UNUSED *context)
{
    /* Return -1 so Mercury uses the legacy progress path (poll_wait),
     * which properly calls NA_Context_get_completion_count(). The ZMQ FD
     * is edge-triggered and its semantics are not fully compatible with
     * Mercury's poll_set mechanism. */
    return -1;
}

static bool
na_zmq_poll_try_wait(na_class_t *na_class, na_context_t *context NA_UNUSED)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);
    int events = 0;
    size_t events_size = sizeof(events);

    /* Check if there are stashed messages waiting */
    if (!STAILQ_EMPTY(&priv->unexpected_msg_queue) ||
        !STAILQ_EMPTY(&priv->expected_msg_queue))
        return false;

    /* Check ZMQ socket events */
    zmq_getsockopt(priv->zmq_socket, ZMQ_EVENTS, &events, &events_size);
    if (events & ZMQ_POLLIN)
        return false;

    /* Safe to wait (no pending input) */
    return true;
}

/*
 * Process a single received message. Called with socket_lock held (for
 * sending GET_RESP). Acquires queue_lock internally for queue operations.
 */
static na_return_t
na_zmq_process_msg(struct na_zmq_class *priv,
    const void *sender_id, size_t sender_id_len,
    const struct na_zmq_msg_hdr *hdr,
    const void *payload, size_t payload_size)
{
    struct na_zmq_addr *source_addr;
    struct na_zmq_op_id *op;

    hg_thread_mutex_lock(&priv->queue_lock);
    source_addr = na_zmq_find_or_create_addr(priv, sender_id, sender_id_len,
        NULL);
    hg_thread_mutex_unlock(&priv->queue_lock);
    if (source_addr == NULL)
        return NA_NOMEM;

    switch (hdr->type) {
    case NA_ZMQ_UNEXPECTED: {
        /* Look for a posted unexpected recv */
        hg_thread_mutex_lock(&priv->queue_lock);
        if (!STAILQ_EMPTY(&priv->unexpected_op_queue)) {
            op = STAILQ_FIRST(&priv->unexpected_op_queue);
            STAILQ_REMOVE_HEAD(&priv->unexpected_op_queue, entry);
            hg_thread_mutex_unlock(&priv->queue_lock);

            size_t copy_size = payload_size < op->buf_size ?
                payload_size : op->buf_size;
            if (copy_size > 0 && payload)
                memcpy(op->buf, payload, copy_size);

            op->completion_data.callback_info.info
                .recv_unexpected.actual_buf_size = payload_size;
            op->completion_data.callback_info.info
                .recv_unexpected.source = (na_addr_t *) source_addr;
            op->completion_data.callback_info.info
                .recv_unexpected.tag = hdr->tag;

            /* source_addr ownership transferred to callback */
            na_zmq_complete_op(op, NA_SUCCESS);
        } else {
            /* Stash the message */
            struct na_zmq_unexpected_msg *umsg;
            umsg = calloc(1, sizeof(*umsg));
            if (umsg == NULL) {
                hg_thread_mutex_unlock(&priv->queue_lock);
                na_zmq_addr_decref(source_addr);
                return NA_NOMEM;
            }
            if (payload_size > 0 && payload) {
                umsg->buf = malloc(payload_size);
                if (umsg->buf == NULL) {
                    hg_thread_mutex_unlock(&priv->queue_lock);
                    na_zmq_addr_decref(source_addr);
                    free(umsg);
                    return NA_NOMEM;
                }
                memcpy(umsg->buf, payload, payload_size);
            }
            umsg->buf_size = payload_size;
            umsg->addr = source_addr; /* transfer ref */
            umsg->tag = hdr->tag;
            STAILQ_INSERT_TAIL(&priv->unexpected_msg_queue, umsg, entry);
            hg_thread_mutex_unlock(&priv->queue_lock);
        }
        break;
    }

    case NA_ZMQ_EXPECTED: {
        /* Match (source, tag) in expected_op_queue */
        bool found = false;

        hg_thread_mutex_lock(&priv->queue_lock);
        STAILQ_FOREACH(op, &priv->expected_op_queue, entry) {
            bool tag_match = (op->tag == hdr->tag);
            bool addr_match = !op->addr ||
                (op->addr->zmq_identity_len == source_addr->zmq_identity_len &&
                 memcmp(op->addr->zmq_identity, source_addr->zmq_identity,
                     source_addr->zmq_identity_len) == 0);

            if (tag_match && addr_match) {
                STAILQ_REMOVE(&priv->expected_op_queue, op,
                    na_zmq_op_id, entry);
                hg_thread_mutex_unlock(&priv->queue_lock);

                size_t copy_size = payload_size < op->buf_size ?
                    payload_size : op->buf_size;
                if (copy_size > 0 && payload)
                    memcpy(op->buf, payload, copy_size);

                op->completion_data.callback_info.info
                    .recv_expected.actual_buf_size = payload_size;

                if (op->addr)
                    na_zmq_addr_decref(op->addr);

                na_zmq_complete_op(op, NA_SUCCESS);
                found = true;
                break;
            }
        }

        if (!found) {
            /* Stash expected message for later matching */
            struct na_zmq_expected_msg *emsg;
            emsg = calloc(1, sizeof(*emsg));
            if (emsg != NULL) {
                if (payload_size > 0 && payload) {
                    emsg->buf = malloc(payload_size);
                    if (emsg->buf != NULL)
                        memcpy(emsg->buf, payload, payload_size);
                }
                emsg->buf_size = payload_size;
                emsg->addr = source_addr;
                na_zmq_addr_incref(source_addr);
                emsg->tag = hdr->tag;
                STAILQ_INSERT_TAIL(&priv->expected_msg_queue, emsg, entry);
            }
            hg_thread_mutex_unlock(&priv->queue_lock);
        }

        na_zmq_addr_decref(source_addr);
        break;
    }

    case NA_ZMQ_PUT: {
        /* Find local mem_handle by handle_id, copy data at offset */
        struct na_zmq_mem_handle *mh;
        bool found = false;

        hg_thread_mutex_lock(&priv->queue_lock);
        STAILQ_FOREACH(mh, &priv->mem_handle_list, entry) {
            if (mh->handle_id == hdr->handle_id) {
                size_t end = hdr->offset + payload_size;
                if (end <= mh->buf_size && payload) {
                    memcpy((char *) mh->buf + hdr->offset,
                        payload, payload_size);
                }
                found = true;
                break;
            }
        }
        hg_thread_mutex_unlock(&priv->queue_lock);

        if (!found) {
            NA_LOG_WARNING("PUT: no mem_handle with id %" PRIu64,
                hdr->handle_id);
        }

        na_zmq_addr_decref(source_addr);
        break;
    }

    case NA_ZMQ_GET_REQ: {
        /* Find local mem_handle, read data, send GET_RESP.
         * socket_lock is already held by caller (progress). */
        struct na_zmq_mem_handle *mh;
        bool found = false;

        hg_thread_mutex_lock(&priv->queue_lock);
        STAILQ_FOREACH(mh, &priv->mem_handle_list, entry) {
            if (mh->handle_id == hdr->handle_id) {
                struct na_zmq_msg_hdr resp_hdr;
                size_t data_len = hdr->rma_length;
                size_t end = hdr->offset + data_len;

                if (end > mh->buf_size)
                    data_len = mh->buf_size > hdr->offset ?
                        mh->buf_size - hdr->offset : 0;

                memset(&resp_hdr, 0, sizeof(resp_hdr));
                resp_hdr.type = NA_ZMQ_GET_RESP;
                resp_hdr.payload_size = (uint32_t) data_len;
                resp_hdr.local_handle_id = hdr->local_handle_id;
                resp_hdr.local_offset = hdr->local_offset;

                na_zmq_addr_connect(priv, source_addr);
                hg_thread_mutex_unlock(&priv->queue_lock);

                /* Send response (socket_lock already held) */
                na_zmq_send_msg(priv, source_addr, &resp_hdr,
                    (const char *) mh->buf + hdr->offset, data_len);

                found = true;
                break;
            }
        }

        if (!found) {
            hg_thread_mutex_unlock(&priv->queue_lock);
            NA_LOG_WARNING("GET_REQ: no mem_handle with id %" PRIu64,
                hdr->handle_id);
        }

        na_zmq_addr_decref(source_addr);
        break;
    }

    case NA_ZMQ_GET_RESP: {
        /* Match in pending_rma_queue by local_handle_id */
        bool found = false;

        hg_thread_mutex_lock(&priv->queue_lock);
        STAILQ_FOREACH(op, &priv->pending_rma_queue, entry) {
            if (op->local_handle &&
                op->local_handle->handle_id == hdr->local_handle_id &&
                (uint64_t) op->local_offset == hdr->local_offset) {
                STAILQ_REMOVE(&priv->pending_rma_queue, op,
                    na_zmq_op_id, entry);
                hg_thread_mutex_unlock(&priv->queue_lock);

                /* Copy data into local handle */
                if (op->local_handle->buf && payload_size > 0 && payload) {
                    size_t copy_size = payload_size < op->rma_length ?
                        payload_size : op->rma_length;
                    memcpy((char *) op->local_handle->buf + op->local_offset,
                        payload, copy_size);
                }

                na_zmq_complete_op(op, NA_SUCCESS);
                found = true;
                break;
            }
        }

        if (!found) {
            hg_thread_mutex_unlock(&priv->queue_lock);
            NA_LOG_WARNING("GET_RESP: no matching pending get");
        }

        na_zmq_addr_decref(source_addr);
        break;
    }

    default:
        NA_LOG_WARNING("Unknown message type: %u", hdr->type);
        na_zmq_addr_decref(source_addr);
        break;
    }

    return NA_SUCCESS;
}

/* Drain all available ZMQ messages. Caller must hold socket_lock. */
static void
na_zmq_progress(struct na_zmq_class *priv)
{
    for (;;) {
        zmq_msg_t id_frame, hdr_frame, payload_frame;
        int rc;
        int more;
        size_t more_size = sizeof(more);

        zmq_msg_init(&id_frame);
        rc = zmq_msg_recv(&id_frame, priv->zmq_socket, ZMQ_DONTWAIT);
        if (rc < 0) {
            zmq_msg_close(&id_frame);
            break; /* No more messages */
        }

        /* Check for more frames */
        zmq_getsockopt(priv->zmq_socket, ZMQ_RCVMORE, &more, &more_size);
        if (!more) {
            zmq_msg_close(&id_frame);
            continue; /* Malformed — skip */
        }

        /* Receive header frame */
        zmq_msg_init(&hdr_frame);
        rc = zmq_msg_recv(&hdr_frame, priv->zmq_socket, 0);
        if (rc < 0) {
            zmq_msg_close(&id_frame);
            zmq_msg_close(&hdr_frame);
            break;
        }

        if ((size_t) zmq_msg_size(&hdr_frame) < sizeof(struct na_zmq_msg_hdr)) {
            zmq_msg_close(&id_frame);
            zmq_msg_close(&hdr_frame);
            /* Drain remaining frames */
            zmq_getsockopt(priv->zmq_socket, ZMQ_RCVMORE, &more, &more_size);
            if (more) {
                zmq_msg_t discard;
                zmq_msg_init(&discard);
                zmq_msg_recv(&discard, priv->zmq_socket, 0);
                zmq_msg_close(&discard);
            }
            continue;
        }

        struct na_zmq_msg_hdr *hdr =
            (struct na_zmq_msg_hdr *) zmq_msg_data(&hdr_frame);

        /* Check for payload frame */
        zmq_getsockopt(priv->zmq_socket, ZMQ_RCVMORE, &more, &more_size);

        zmq_msg_init(&payload_frame);
        if (more) {
            rc = zmq_msg_recv(&payload_frame, priv->zmq_socket, 0);
            if (rc < 0) {
                zmq_msg_close(&id_frame);
                zmq_msg_close(&hdr_frame);
                zmq_msg_close(&payload_frame);
                break;
            }
        }

        /* Process the message */
        na_zmq_process_msg(priv,
            zmq_msg_data(&id_frame), zmq_msg_size(&id_frame),
            hdr,
            more ? zmq_msg_data(&payload_frame) : NULL,
            more ? zmq_msg_size(&payload_frame) : 0);

        zmq_msg_close(&id_frame);
        zmq_msg_close(&hdr_frame);
        zmq_msg_close(&payload_frame);
    }
}

static na_return_t
na_zmq_poll(na_class_t *na_class, na_context_t NA_UNUSED *context,
    unsigned int *count_p)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);

    if (hg_thread_mutex_try_lock(&priv->socket_lock) == HG_UTIL_SUCCESS) {
        na_zmq_progress(priv);
        hg_thread_mutex_unlock(&priv->socket_lock);
    }

    if (count_p != NULL)
        *count_p = 0;
    return NA_SUCCESS;
}

static na_return_t
na_zmq_poll_wait(na_class_t *na_class, na_context_t NA_UNUSED *context,
    unsigned int timeout_ms, unsigned int *count_p)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);
    struct pollfd pfd;
    int fd = -1;
    size_t fd_size = sizeof(fd);
    int events = 0;
    size_t events_size = sizeof(events);
    bool has_data = false;
    int rc;

    /* Get ZMQ socket FD */
    zmq_getsockopt(priv->zmq_socket, ZMQ_FD, &fd, &fd_size);

    /* Check ZMQ_EVENTS to re-arm edge-triggered FD */
    zmq_getsockopt(priv->zmq_socket, ZMQ_EVENTS, &events, &events_size);
    if (events & ZMQ_POLLIN)
        has_data = true;

    /* Also check stashed message queues */
    if (!has_data &&
        (!STAILQ_EMPTY(&priv->unexpected_msg_queue) ||
         !STAILQ_EMPTY(&priv->expected_msg_queue)))
        has_data = true;

    if (has_data) {
        /* Data available - process immediately */
        hg_thread_mutex_lock(&priv->socket_lock);
        na_zmq_progress(priv);
        hg_thread_mutex_unlock(&priv->socket_lock);

        if (count_p != NULL)
            *count_p = 0;
        return NA_SUCCESS;
    }

    if (fd < 0) {
        /* No FD available, just try progress */
        hg_thread_mutex_lock(&priv->socket_lock);
        na_zmq_progress(priv);
        hg_thread_mutex_unlock(&priv->socket_lock);

        if (count_p != NULL)
            *count_p = 0;
        return NA_TIMEOUT;
    }

    /* Wait for activity on ZMQ socket FD */
    pfd.fd = fd;
    pfd.events = POLLIN;
    pfd.revents = 0;

    rc = poll(&pfd, 1, (int) timeout_ms);
    if (rc < 0 && errno != EINTR)
        return NA_IO_ERROR;

    /* Process regardless (ZMQ edge-triggered FD semantics) */
    hg_thread_mutex_lock(&priv->socket_lock);
    na_zmq_progress(priv);
    hg_thread_mutex_unlock(&priv->socket_lock);

    if (count_p != NULL)
        *count_p = 0;

    if (rc == 0)
        return NA_TIMEOUT;

    return NA_SUCCESS;
}

/*---------------------------------------------------------------------------*/
/* Cancel                                                                    */
/*---------------------------------------------------------------------------*/

static na_return_t
na_zmq_cancel(na_class_t *na_class, na_context_t *context NA_UNUSED,
    na_op_id_t *op_id)
{
    struct na_zmq_class *priv = na_zmq_class(na_class);
    struct na_zmq_op_id *op = (struct na_zmq_op_id *) op_id;
    struct na_zmq_op_id *iter;
    int32_t status;

    status = hg_atomic_get32(&op->status);
    if (status & NA_ZMQ_OP_COMPLETED)
        return NA_SUCCESS;

    hg_thread_mutex_lock(&priv->queue_lock);

    /* Try to remove from unexpected_op_queue */
    STAILQ_FOREACH(iter, &priv->unexpected_op_queue, entry) {
        if (iter == op) {
            STAILQ_REMOVE(&priv->unexpected_op_queue, op,
                na_zmq_op_id, entry);
            goto canceled;
        }
    }

    /* Try expected_op_queue */
    STAILQ_FOREACH(iter, &priv->expected_op_queue, entry) {
        if (iter == op) {
            STAILQ_REMOVE(&priv->expected_op_queue, op,
                na_zmq_op_id, entry);

            if (op->addr) {
                na_zmq_addr_decref(op->addr);
                op->addr = NULL;
            }
            goto canceled;
        }
    }

    /* Try pending_rma_queue */
    STAILQ_FOREACH(iter, &priv->pending_rma_queue, entry) {
        if (iter == op) {
            STAILQ_REMOVE(&priv->pending_rma_queue, op,
                na_zmq_op_id, entry);
            goto canceled;
        }
    }

    hg_thread_mutex_unlock(&priv->queue_lock);

    /* Not found in any queue — already completed or invalid */
    return NA_SUCCESS;

canceled:
    hg_thread_mutex_unlock(&priv->queue_lock);
    na_zmq_complete_op(op, NA_CANCELED);
    return NA_SUCCESS;
}

/*---------------------------------------------------------------------------*/
/* Ops table                                                                 */
/*---------------------------------------------------------------------------*/

NA_PLUGIN const struct na_class_ops NA_PLUGIN_OPS(zmq) = {
    "zmq",                              /* class_name */
    na_zmq_get_protocol_info,           /* get_protocol_info */
    na_zmq_check_protocol,              /* check_protocol */
    na_zmq_initialize,                  /* initialize */
    na_zmq_finalize,                    /* finalize */
    na_zmq_cleanup,                     /* cleanup */
    NULL,                               /* has_opt_feature */
    na_zmq_context_create,              /* context_create */
    na_zmq_context_destroy,             /* context_destroy */
    na_zmq_op_create,                   /* op_create */
    na_zmq_op_destroy,                  /* op_destroy */
    na_zmq_addr_lookup,                 /* addr_lookup */
    na_zmq_addr_free,                   /* addr_free */
    NULL,                               /* addr_set_remove */
    na_zmq_addr_self,                   /* addr_self */
    na_zmq_addr_dup,                    /* addr_dup */
    na_zmq_addr_cmp,                    /* addr_cmp */
    na_zmq_addr_is_self,                /* addr_is_self */
    na_zmq_addr_to_string,              /* addr_to_string */
    na_zmq_addr_get_serialize_size,     /* addr_get_serialize_size */
    na_zmq_addr_serialize,              /* addr_serialize */
    na_zmq_addr_deserialize,            /* addr_deserialize */
    na_zmq_msg_get_max_unexpected_size, /* msg_get_max_unexpected_size */
    na_zmq_msg_get_max_expected_size,   /* msg_get_max_expected_size */
    NULL,                               /* msg_get_unexpected_header_size */
    NULL,                               /* msg_get_expected_header_size */
    na_zmq_msg_get_max_tag,             /* msg_get_max_tag */
    NULL,                               /* msg_buf_alloc */
    NULL,                               /* msg_buf_free */
    NULL,                               /* msg_init_unexpected */
    na_zmq_msg_send_unexpected,         /* msg_send_unexpected */
    na_zmq_msg_recv_unexpected,         /* msg_recv_unexpected */
    NULL,                               /* msg_multi_recv_unexpected */
    NULL,                               /* msg_init_expected */
    na_zmq_msg_send_expected,           /* msg_send_expected */
    na_zmq_msg_recv_expected,           /* msg_recv_expected */
    na_zmq_mem_handle_create,           /* mem_handle_create */
    NULL,                               /* mem_handle_create_segments */
    na_zmq_mem_handle_free,             /* mem_handle_free */
    NULL,                               /* mem_handle_get_max_segments */
    NULL,                               /* mem_register */
    NULL,                               /* mem_deregister */
    na_zmq_mem_handle_get_serialize_size, /* mem_handle_get_serialize_size */
    na_zmq_mem_handle_serialize,        /* mem_handle_serialize */
    na_zmq_mem_handle_deserialize,      /* mem_handle_deserialize */
    na_zmq_put,                         /* put */
    na_zmq_get,                         /* get */
    na_zmq_poll_get_fd,                 /* poll_get_fd */
    na_zmq_poll_try_wait,               /* poll_try_wait */
    na_zmq_poll,                        /* poll */
    na_zmq_poll_wait,                   /* poll_wait */
    na_zmq_cancel                       /* cancel */
};
