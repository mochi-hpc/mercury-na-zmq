/**
 * NA ZMQ Relay Service
 *
 * Forwards messages between clusters. Single-threaded event loop using one
 * ZMQ ROUTER socket. Routes based on the envelope frame's destination
 * identity cluster prefix.
 *
 * Usage: na_zmq_relay <config.toml> <cluster>
 */

#include "relay_config.hpp"

#include <zmq.h>

#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

static volatile sig_atomic_t g_running = 1;

static void
signal_handler(int)
{
    g_running = 0;
}

/* Envelope binary format:
 * Byte 0:    0xFF (magic)
 * Bytes 1-2: src_id_len (uint16_t LE)
 * Bytes 3-4: dst_id_len (uint16_t LE)
 * Bytes 5..: source identity (src_id_len bytes)
 *            dest identity (dst_id_len bytes)
 */
static bool
parse_envelope(const void *data, size_t len,
    std::string &src_id, std::string &dst_id)
{
    auto p = static_cast<const uint8_t *>(data);

    if (len < 5 || p[0] != 0xFF)
        return false;

    uint16_t src_len, dst_len;
    std::memcpy(&src_len, p + 1, sizeof(src_len));
    std::memcpy(&dst_len, p + 3, sizeof(dst_len));

    if (5u + src_len + dst_len > len)
        return false;

    src_id.assign(reinterpret_cast<const char *>(p + 5), src_len);
    dst_id.assign(reinterpret_cast<const char *>(p + 5 + src_len), dst_len);
    return true;
}

/* Extract cluster prefix (everything before first '/') */
static std::string
get_cluster(const std::string &identity)
{
    auto pos = identity.find('/');
    if (pos == std::string::npos)
        return {};
    return identity.substr(0, pos);
}

/* Receive all frames of a multi-part message */
static bool
recv_multipart(void *socket, std::vector<zmq_msg_t> &frames)
{
    for (;;) {
        zmq_msg_t msg;
        zmq_msg_init(&msg);
        if (zmq_msg_recv(&msg, socket, 0) < 0) {
            zmq_msg_close(&msg);
            return false;
        }
        frames.push_back(std::move(msg));

        int more = 0;
        size_t more_size = sizeof(more);
        zmq_getsockopt(socket, ZMQ_RCVMORE, &more, &more_size);
        if (!more)
            break;
    }
    return true;
}

/* Send multi-part message: new_routing_id + frames[1..] (envelope onward) */
static bool
forward_message(void *socket, const std::string &routing_id,
    std::vector<zmq_msg_t> &frames, size_t start_frame)
{
    /* Frame 0: new routing ID */
    if (zmq_send(socket, routing_id.data(), routing_id.size(),
            ZMQ_SNDMORE) < 0) {
        if (errno == EHOSTUNREACH) {
            std::fprintf(stderr, "relay: peer %s unreachable (EHOSTUNREACH)\n",
                routing_id.c_str());
        } else {
            std::fprintf(stderr, "relay: send routing_id failed: %s\n",
                zmq_strerror(errno));
        }
        return false;
    }

    /* Forward remaining frames */
    for (size_t i = start_frame; i < frames.size(); i++) {
        int flags = (i + 1 < frames.size()) ? ZMQ_SNDMORE : 0;
        zmq_msg_t copy;
        zmq_msg_init(&copy);
        zmq_msg_copy(&copy, &frames[i]);
        if (zmq_msg_send(&copy, socket, flags) < 0) {
            std::fprintf(stderr, "relay: send frame %zu failed: %s\n",
                i, zmq_strerror(errno));
            zmq_msg_close(&copy);
            return false;
        }
    }

    return true;
}

int
main(int argc, char *argv[])
{
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <config.toml> <cluster>\n", argv[0]);
        return 1;
    }

    RelayConfig cfg;
    try {
        cfg = load_config(argv[1], argv[2]);
    } catch (const std::exception &e) {
        std::fprintf(stderr, "Failed to load config: %s\n", e.what());
        return 1;
    }

    std::fprintf(stderr, "relay: cluster=%s bind=%s identity=%s peers=%zu\n",
        cfg.cluster.c_str(), cfg.bind_address.c_str(),
        cfg.identity.c_str(), cfg.peers.size());

    /* Set up signal handlers */
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    /* Create ZMQ context and ROUTER socket */
    void *ctx = zmq_ctx_new();
    if (!ctx) {
        std::fprintf(stderr, "zmq_ctx_new failed\n");
        return 1;
    }

    void *socket = zmq_socket(ctx, ZMQ_ROUTER);
    if (!socket) {
        std::fprintf(stderr, "zmq_socket failed: %s\n", zmq_strerror(errno));
        zmq_ctx_destroy(ctx);
        return 1;
    }

    /* Configure socket */
    int linger = 0;
    zmq_setsockopt(socket, ZMQ_LINGER, &linger, sizeof(linger));

    int mandatory = 1;
    zmq_setsockopt(socket, ZMQ_ROUTER_MANDATORY, &mandatory, sizeof(mandatory));

    zmq_setsockopt(socket, ZMQ_ROUTING_ID,
        cfg.identity.data(), cfg.identity.size());

    /* Bind */
    if (zmq_bind(socket, cfg.bind_address.c_str()) != 0) {
        std::fprintf(stderr, "zmq_bind(%s) failed: %s\n",
            cfg.bind_address.c_str(), zmq_strerror(errno));
        zmq_close(socket);
        zmq_ctx_destroy(ctx);
        return 1;
    }

    /* Connect to peer relays.
     *
     * With ROUTER-to-ROUTER, each relay both binds and would normally
     * connect to every peer.  But if both sides connect, the same routing
     * identity appears twice on a socket (once from the inbound accept,
     * once from the outbound connect), and ZMQ silently drops messages.
     *
     * Fix: only connect to peers whose cluster name is lexicographically
     * greater than ours.  The peer with the smaller name accepts the
     * inbound connection instead.  This guarantees exactly one TCP link
     * between each relay pair.
     */
    for (const auto &[name, peer] : cfg.peers) {
        if (name <= cfg.cluster) {
            std::fprintf(stderr,
                "relay: skipping connect to peer %s (will accept inbound)\n",
                name.c_str());
            continue;
        }
        if (zmq_connect(socket, peer.address.c_str()) != 0) {
            std::fprintf(stderr, "zmq_connect(%s) for peer %s failed: %s\n",
                peer.address.c_str(), name.c_str(), zmq_strerror(errno));
            zmq_close(socket);
            zmq_ctx_destroy(ctx);
            return 1;
        }
        std::fprintf(stderr, "relay: connected to peer %s at %s\n",
            name.c_str(), peer.address.c_str());
    }

    std::fprintf(stderr, "relay: ready\n");

    /* Main event loop */
    zmq_pollitem_t poll_item = {socket, 0, ZMQ_POLLIN, 0};
    unsigned long msg_count = 0;

    while (g_running) {
        int rc = zmq_poll(&poll_item, 1, 1000 /* 1s timeout */);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            std::fprintf(stderr, "zmq_poll failed: %s\n",
                zmq_strerror(errno));
            break;
        }

        if (rc == 0)
            continue; /* Timeout — check g_running */

        if (!(poll_item.revents & ZMQ_POLLIN))
            continue;

        /* Receive all frames */
        std::vector<zmq_msg_t> frames;
        if (!recv_multipart(socket, frames)) {
            std::fprintf(stderr, "relay: recv_multipart failed\n");
            continue;
        }

        /* Need at least 3 frames: [sender_id][envelope][header] */
        if (frames.size() < 3) {
            std::fprintf(stderr, "relay: dropping short message (%zu frames)\n",
                frames.size());
            for (auto &f : frames)
                zmq_msg_close(&f);
            continue;
        }

        /* Parse envelope (frame 1) */
        std::string src_id, dst_id;
        if (!parse_envelope(zmq_msg_data(&frames[1]),
                zmq_msg_size(&frames[1]), src_id, dst_id)) {
            std::fprintf(stderr, "relay: dropping message with invalid "
                "envelope\n");
            for (auto &f : frames)
                zmq_msg_close(&f);
            continue;
        }

        std::string dst_cluster = get_cluster(dst_id);

        if (dst_cluster == cfg.cluster || dst_cluster.empty()) {
            /* Local cluster — forward directly to destination process */
            forward_message(socket, dst_id, frames, 1);
        } else {
            /* Remote cluster — forward to peer relay */
            auto it = cfg.peers.find(dst_cluster);
            if (it != cfg.peers.end()) {
                forward_message(socket, it->second.identity, frames, 1);
            } else {
                std::fprintf(stderr,
                    "relay: no peer for cluster '%s', dropping message "
                    "(src=%s dst=%s)\n",
                    dst_cluster.c_str(), src_id.c_str(), dst_id.c_str());
            }
        }

        msg_count++;
        for (auto &f : frames)
            zmq_msg_close(&f);
    }

    std::fprintf(stderr, "relay: shutting down (forwarded %lu messages)\n",
        msg_count);

    zmq_close(socket);
    zmq_ctx_destroy(ctx);

    return 0;
}
