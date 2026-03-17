#!/bin/bash
#
# Cross-cluster relay integration test.
#
# Spawns two relay services (cluster-a, cluster-b), a Mercury server in
# cluster-a, and a Mercury client in cluster-b.  The client sends RPCs with
# bulk transfers through the relay chain:
#
#   client (cluster-b) → relay-b → relay-a → server (cluster-a)
#
# The server's GET_RESP messages travel the reverse path:
#
#   server (cluster-a) → relay-a → relay-b → client (cluster-b)
#
# Usage: test_relay_driver.sh <relay_bin> <server_bin> <client_bin> [client_args...]
#
set -e

if [ $# -lt 3 ]; then
    echo "Usage: $0 <relay_bin> <server_bin> <client_bin> [client_args...]"
    exit 1
fi

RELAY_BIN="$1"
SERVER_BIN="$2"
CLIENT_BIN="$3"
shift 3
CLIENT_EXTRA_ARGS=("$@")

TMPDIR=$(mktemp -d /tmp/zmq_relay_test.XXXXXX)

cleanup() {
    # Print relay logs before removing temp dir (invaluable on timeout)
    if [ -f "$TMPDIR/relay-a.log" ]; then
        echo "# relay-a log:"
        cat "$TMPDIR/relay-a.log"
    fi
    if [ -f "$TMPDIR/relay-b.log" ]; then
        echo "# relay-b log:"
        cat "$TMPDIR/relay-b.log"
    fi
    # Kill children and wait so ports are released before the next test
    kill $RELAY_A_PID $RELAY_B_PID $SERVER_PID 2>/dev/null
    wait $RELAY_A_PID $RELAY_B_PID $SERVER_PID 2>/dev/null
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

RELAY_A_PORT=15550
RELAY_B_PORT=15551

# ── Generate shared TOML config ──────────────────────────────────────────

cat > "$TMPDIR/relays.toml" <<EOF
[peers.cluster-a]
address = "tcp://127.0.0.1:${RELAY_A_PORT}"
# identity defaults to "cluster-a"

[peers.cluster-b]
address = "tcp://127.0.0.1:${RELAY_B_PORT}"
# identity defaults to "cluster-b"
EOF

# ── Start relays ─────────────────────────────────────────────────────────

echo "# Starting relay-a on port $RELAY_A_PORT"
"$RELAY_BIN" "$TMPDIR/relays.toml" cluster-a 2>"$TMPDIR/relay-a.log" &
RELAY_A_PID=$!

echo "# Starting relay-b on port $RELAY_B_PORT"
"$RELAY_BIN" "$TMPDIR/relays.toml" cluster-b 2>"$TMPDIR/relay-b.log" &
RELAY_B_PID=$!

# Give relays time to bind and connect to each other
sleep 2

for pid_name in "relay-a:$RELAY_A_PID" "relay-b:$RELAY_B_PID"; do
    name="${pid_name%%:*}"
    pid="${pid_name##*:}"
    if ! kill -0 "$pid" 2>/dev/null; then
        echo "FAILED: $name exited early"
        cat "$TMPDIR/${name}.log"
        exit 1
    fi
done

# ── Start server in cluster-a ────────────────────────────────────────────

rm -f /tmp/port.cfg

echo "# Starting server in cluster-a"
NA_ZMQ_CLUSTER_NAME=cluster-a \
NA_ZMQ_RELAY_ADDRESS="tcp://127.0.0.1:${RELAY_A_PORT}#cluster-a" \
"$SERVER_BIN" -p zmq &
SERVER_PID=$!

# Wait for server to write /tmp/port.cfg
for i in $(seq 1 30); do
    if [ -f /tmp/port.cfg ] && [ -s /tmp/port.cfg ]; then
        break
    fi
    sleep 0.2
done

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "FAILED: server exited early"
    exit 1
fi

if [ ! -s /tmp/port.cfg ]; then
    echo "FAILED: server did not write /tmp/port.cfg"
    exit 1
fi

echo "# Server address: $(cat /tmp/port.cfg)"

# ── Run client in cluster-b ──────────────────────────────────────────────

echo "# Starting client in cluster-b"
CLIENT_RC=0
NA_ZMQ_CLUSTER_NAME=cluster-b \
NA_ZMQ_RELAY_ADDRESS="tcp://127.0.0.1:${RELAY_B_PORT}#cluster-b" \
"$CLIENT_BIN" -p zmq "${CLIENT_EXTRA_ARGS[@]}" || CLIENT_RC=$?

# ── Wait for server ──────────────────────────────────────────────────────

SERVER_RC=0
wait "$SERVER_PID" || SERVER_RC=$?

# ── Print relay logs ─────────────────────────────────────────────────────

echo ""
echo "# relay-a log:"
cat "$TMPDIR/relay-a.log"
echo "# relay-b log:"
cat "$TMPDIR/relay-b.log"

# ── Result ───────────────────────────────────────────────────────────────

if [ $SERVER_RC -ne 0 ] || [ $CLIENT_RC -ne 0 ]; then
    echo "FAILED (server=$SERVER_RC client=$CLIENT_RC)"
    exit 1
fi

echo "PASSED"
exit 0
