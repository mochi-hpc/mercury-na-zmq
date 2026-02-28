# mercury-na-zmq

ZeroMQ ROUTER-based NA transport plugin for Mercury. Provides a portable,
TCP-based NA backend useful for development, testing, and environments
without RDMA.

## Requirements

- Mercury (with dynamic plugin support)
- libzmq >= 4.3
- toml11 >= 4.0 (optional, for the relay service)

## Building

```bash
mkdir build && cd build
cmake .. \
  -Dmercury_DIR=<mercury-install>/share/cmake/mercury \
  -DCMAKE_PREFIX_PATH=<zmq-prefix>
make
```

## Usage

Set `NA_PLUGIN_PATH` to include the directory containing `libna_plugin_zmq.so`:

```bash
export NA_PLUGIN_PATH=/path/to/lib
```

### Address format

```
zmq+tcp://hostname:port#identity
```

- **Listener**: `NA_Initialize("zmq://", true)` binds to an ephemeral port
- **Client**: `NA_Initialize("zmq://", false)` creates a socket for outbound connections
- **Lookup**: `NA_Addr_lookup(class, "zmq+tcp://host:port#id")` connects to a peer

## Running tests

```bash
cd build
NA_PLUGIN_PATH=. ./test/run_tests.sh .
```

## Cross-cluster relay

When processes live on separate networks (different clusters), they cannot
reach each other directly. The relay service bridges clusters: a message from
process A on cluster "alpha" to process B on cluster "beta" travels
**A -> relay-alpha -> relay-beta -> B**.

### Building the relay

The relay is built automatically when toml11 is found. If toml11 is
installed in a non-standard location, pass its prefix to CMake:

```bash
cmake .. -DCMAKE_PREFIX_PATH="<zmq-prefix>;<toml11-prefix>" ...
```

The resulting binary is `relay/na_zmq_relay`.

### Configuration

All relays share a single TOML configuration file. Each entry in the
`[peers]` table describes one relay (address to bind and ZMQ routing
identity):

```toml
[peers.alpha]
address = "tcp://login-alpha.example.com:5555"
identity = "relay-alpha"

[peers.beta]
address = "tcp://login-beta.example.com:5555"
identity = "relay-beta"

[peers.gamma]
address = "tcp://login-gamma.example.com:5555"
identity = "relay-gamma"
```

### Starting a relay

Each relay is started with the shared config file and its own cluster name:

```bash
na_zmq_relay relays.toml alpha   # on login-alpha
na_zmq_relay relays.toml beta    # on login-beta
na_zmq_relay relays.toml gamma   # on login-gamma
```

The relay looks up its own entry in `[peers]` to determine which address to
bind. It connects to every other entry as a remote peer.

### Configuring Mercury processes

Two environment variables enable relay-based routing in the NA plugin:

- **`NA_ZMQ_CLUSTER_NAME`** -- prefixed to the process identity
  (e.g. `alpha/a1b2c3d4`). Used to determine whether a destination is local
  or cross-cluster.
- **`NA_ZMQ_RELAY_ADDRESS`** -- address of the local relay in
  `tcp://host:port#identity` format. Cross-cluster messages are forwarded
  through this relay.

```bash
# On cluster alpha
export NA_ZMQ_CLUSTER_NAME=alpha
export NA_ZMQ_RELAY_ADDRESS="tcp://login-alpha.example.com:5555#relay-alpha"
my_mercury_app ...

# On cluster beta
export NA_ZMQ_CLUSTER_NAME=beta
export NA_ZMQ_RELAY_ADDRESS="tcp://login-beta.example.com:5555#relay-beta"
my_mercury_app ...
```

When a process sends a message to a destination whose cluster prefix differs
from its own, the plugin automatically routes the message through the relay
instead of attempting a direct connection. Same-cluster messages are sent
directly as before.

### Wire protocol: envelope frame

Cross-cluster messages use a 4-frame format with an envelope inserted between
the routing ID and the header:

| Frame | Contents |
|-------|----------|
| 0. Routing ID | Next hop (relay identity or final destination) |
| 1. Envelope | `0xFF` magic + source/destination identity strings |
| 2. Header | `struct na_zmq_msg_hdr` (unchanged) |
| 3. Payload | Application data (optional) |

The envelope's first byte (`0xFF`) distinguishes it from a direct header
(whose type field is 1--5). The relay treats the header and payload as opaque
data -- it only reads the envelope to determine the next hop.

### Message flow example

Process A (`alpha/a1b2c3d4`) sends a message to process B (`beta/e5f6g7h8`):

```
1. A sees different cluster prefix ("alpha" != "beta")
2. A builds envelope {src="alpha/a1b2c3d4", dst="beta/e5f6g7h8"}
3. A sends to local relay:     [relay-alpha][envelope][hdr][payload]
4. relay-alpha extracts dst cluster = "beta", forwards to peer:
                                [relay-beta] [envelope][hdr][payload]
5. relay-beta extracts dst cluster = "beta" (local), forwards to B:
                                [beta/e5f6g7h8][envelope][hdr][payload]
6. B detects envelope (0xFF), reads src = "alpha/a1b2c3d4"
7. B processes the message normally with source = "alpha/a1b2c3d4"
```

Response messages (including RMA GET responses) follow the reverse path.

## Architecture

### Overview

The plugin implements Mercury's `na_class_ops` callback table (~35 callbacks) using
a single ZMQ ROUTER socket per `na_class`. ROUTER sockets provide bidirectional,
identity-addressed, multi-peer communication over TCP. Because ZMQ is a pure
message-passing library with no RDMA support, all RMA operations (put and get)
are emulated with request/response messages.

### Socket and identity model

Each `na_class` owns one ZMQ context and one ROUTER socket. During
initialization the socket is assigned a unique routing identity of the form
`zmq-<hostname>-<pid>-<counter>` via `ZMQ_ROUTING_ID`, then bound to a TCP
endpoint. Ephemeral ports are supported: binding to `tcp://*:*` lets ZMQ
choose a port, which is queried back with `ZMQ_LAST_ENDPOINT`.

Both listeners and clients bind their socket (every endpoint is a server).
Outbound connectivity is established with `zmq_connect()` on first use (lazy
connect). `ZMQ_ROUTER_MANDATORY` is enabled so that sends to peers whose TCP
connection has not yet completed fail immediately with `EHOSTUNREACH` rather
than silently dropping the message.

### Addressing

An address encodes two pieces of information:

```
zmq+tcp://host:port#zmq-identity-string
```

- **Endpoint** (`tcp://host:port`): used by `zmq_connect()` to establish the
  TCP connection.
- **Identity** (`zmq-hostname-pid-counter`): used as the ZMQ routing ID in
  the first frame of every outgoing message.

For inbound connections (a peer that connected to us), no endpoint is known;
the ROUTER socket can still send to such peers using their routing ID alone.

Addresses are reference-counted and cached in a per-class list. Serialization
writes `[id_len][identity][ep_len][endpoint]`, which is sufficient for another
process to reconstruct the address and connect.

### Wire protocol

Every message consists of three ZMQ frames:

| Frame | Contents |
|-------|----------|
| 1. Routing ID | Destination identity string (handled by ROUTER) |
| 2. Header | `struct na_zmq_msg_hdr` (type, tag, payload size, RMA fields) |
| 3. Payload | Application data (may be empty) |

Five message types are defined:

| Type | Direction | Purpose |
|------|-----------|---------|
| `NA_ZMQ_UNEXPECTED` | Sender -> Receiver | Unexpected (unmatched) message |
| `NA_ZMQ_EXPECTED` | Sender -> Receiver | Expected (pre-posted recv) message |
| `NA_ZMQ_PUT` | Initiator -> Target | RMA write: carries data to copy into remote handle |
| `NA_ZMQ_GET_REQ` | Initiator -> Target | RMA read request: asks target to send data back |
| `NA_ZMQ_GET_RESP` | Target -> Initiator | RMA read response: carries requested data |

The header carries RMA metadata (remote handle ID, offset, length, and for
GET operations the requester's local handle ID and offset) so that the
receiver can locate the correct memory region and the initiator can match
responses to pending operations.

### Connection model

Connections use a lazy-connect-with-retry pattern:

1. `zmq_connect()` is called on first send to a given peer. The call returns
   immediately but the TCP three-way handshake proceeds asynchronously.
2. With `ZMQ_ROUTER_MANDATORY` enabled, the subsequent `zmq_send()` may fail
   with `EHOSTUNREACH` if the handshake has not yet completed.
3. The send loop retries up to 50 times with a 10 ms backoff between
   attempts, allowing up to 500 ms for the connection to establish.

For peers that connected to us (inbound), no `zmq_connect()` is needed.
The ROUTER socket can send directly using the peer's routing ID.

### Message matching and stashing

Mercury's NA API distinguishes two receive modes:

- **Unexpected recv**: the receiver does not know the sender or tag in
  advance. Matching is first-come, first-served.
- **Expected recv**: the receiver posts a recv specifying the expected
  source address and tag. The message must match both.

Because messages may arrive before the corresponding recv is posted (or vice
versa), the plugin maintains stash queues:

- `unexpected_msg_queue`: holds unexpected messages that arrived before a
  recv was posted. When `msg_recv_unexpected` is called, this queue is
  checked first.
- `expected_msg_queue`: holds expected messages that arrived before a matching
  recv was posted. When `msg_recv_expected` is called, the queue is searched
  for a (source, tag) match.

Conversely, if a recv is posted before the message arrives, the recv
operation is queued:

- `unexpected_op_queue`: pending unexpected recvs. Incoming unexpected
  messages are matched against this queue first.
- `expected_op_queue`: pending expected recvs. Incoming expected messages are
  matched by (source address, tag).

### RMA emulation

Since ZMQ provides no RDMA primitives, RMA is emulated over messages.
Memory handles are represented as `(handle_id, buf, buf_size)` tuples
registered in a per-class list. The handle ID is a monotonically increasing
64-bit counter assigned at creation time.

**Put** (one-way):
1. The initiator sends an `NA_ZMQ_PUT` message containing the data, the
   remote handle ID, and the offset.
2. The target's progress loop receives the message, locates the handle by
   ID, and copies the data into the buffer at the specified offset.
3. The initiator's put operation completes immediately after sending.

**Get** (request/response):
1. The initiator queues the operation in `pending_rma_queue`, then sends an
   `NA_ZMQ_GET_REQ` with the remote handle ID, remote offset, requested
   length, and its own local handle ID and offset (for response matching).
2. The target receives the request, reads data from its local handle, and
   sends back an `NA_ZMQ_GET_RESP` with the data.
3. The initiator's progress loop receives the response, matches it to the
   pending operation by local handle ID and offset, copies the data into
   the local buffer, and completes the operation.

The GET operation is queued before the request is sent to prevent a race
where the response could arrive before the operation is registered.

### Progress and polling

The progress engine is built around a non-blocking drain loop:

- `na_zmq_progress()`: called with `socket_lock` held, loops calling
  `zmq_msg_recv()` with `ZMQ_DONTWAIT` until no more messages are available.
  Each received message is dispatched to `na_zmq_process_msg()` which handles
  it according to its type.
- `na_zmq_poll()`: attempts a non-blocking `try_lock` on `socket_lock` and
  calls `na_zmq_progress()` if the lock is acquired. Never blocks.
- `na_zmq_poll_wait()`: blocking variant with timeout. Checks `ZMQ_EVENTS`
  and the stash queues for ready data. If none, uses POSIX `poll()` on the
  ZMQ socket file descriptor with the specified timeout, then drains any
  available messages.
- `na_zmq_poll_try_wait()`: returns whether it is safe to enter a blocking
  wait (i.e., no stashed messages and no pending ZMQ events).
- `na_zmq_poll_get_fd()`: returns -1 to direct Mercury into its legacy
  progress path. ZMQ's file descriptor is edge-triggered and its semantics
  are not fully compatible with Mercury's `poll_set` mechanism.

### Locking

Two mutexes protect shared state:

- **`socket_lock`**: guards all ZMQ socket operations (`zmq_send`,
  `zmq_msg_recv`, `zmq_connect`, `zmq_getsockopt`). ZMQ sockets are not
  thread-safe, so all access must be serialized.
- **`queue_lock`**: guards the operation queues, message stash queues,
  memory handle list, and address list.

The two locks are independent and may be held in either order. In
`na_zmq_process_msg()`, which is called with `socket_lock` held (during
progress), `queue_lock` is acquired and released within each message-type
case as needed. This prevents holding both locks for longer than necessary.

### Operation lifecycle

Operation IDs (`na_zmq_op_id`) are allocated by Mercury via `op_create` and
reused across multiple operations. The lifecycle is:

1. **Create**: `op_create` allocates the structure, initializes status to
   `NA_ZMQ_OP_COMPLETED`, and sets a plugin release callback
   (`na_zmq_release`).
2. **Setup**: each operation (send, recv, put, get) clears the status to 0,
   fills in the context, callback, and operation-specific fields.
3. **Queue** (recv/get only): the operation is inserted into the appropriate
   queue with status `NA_ZMQ_OP_QUEUED`.
4. **Complete**: `na_zmq_complete_op()` sets status to `NA_ZMQ_OP_COMPLETED`,
   fills the return code, and pushes the completion data onto Mercury's
   atomic completion queue via `na_cb_completion_add()`.
5. **Trigger**: Mercury's `NA_Trigger` pops the completion, calls the plugin
   release callback (which resets status for reuse), then calls the user
   callback.
6. **Destroy**: `op_destroy` frees the structure.

### Cancellation

A pending operation can be cancelled by searching the three operation queues
(`unexpected_op_queue`, `expected_op_queue`, `pending_rma_queue`). If found,
the operation is removed from its queue and completed with `NA_CANCELED`. If
not found, the operation has already completed and cancellation is a no-op.

### Files

```
mercury-na-zmq/
  CMakeLists.txt           Build system (finds Mercury + ZMQ + toml11, builds all targets)
  src/
    na_zmq.h               Data structures, constants, wire protocol definitions
    na_zmq.c               All na_class_ops callbacks + internal helpers
  relay/
    CMakeLists.txt          Builds the na_zmq_relay executable
    relay_config.hpp        TOML config loading (header-only)
    relay.cpp               Relay service: ZMQ event loop + message forwarding
  test/
    CMakeLists.txt          Test executables and CTest definitions
    test_zmq_init.c         Init/finalize smoke test
    test_zmq_msg_server.c   Message test server (unexpected + expected)
    test_zmq_msg_client.c   Message test client
    test_relay_driver.sh    Cross-cluster relay integration test driver
    run_tests.sh            Test runner (server/client pairs + integration tests)
    ... (additional HG-level test infrastructure)
```
