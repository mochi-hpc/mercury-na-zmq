#!/bin/bash
#
# Simple test driver for server+client test pairs.
# Usage: test_driver.sh <server_cmd> [server_args...] -- <client_cmd> [client_args...]
#
set -e

TIMEOUT=30

# Split arguments at "--"
SERVER_ARGS=()
CLIENT_ARGS=()
found_sep=false
for arg in "$@"; do
    if [ "$arg" = "--" ]; then
        found_sep=true
        continue
    fi
    if $found_sep; then
        CLIENT_ARGS+=("$arg")
    else
        SERVER_ARGS+=("$arg")
    fi
done

if [ ${#SERVER_ARGS[@]} -eq 0 ] || [ ${#CLIENT_ARGS[@]} -eq 0 ]; then
    echo "Usage: $0 <server_cmd> [args...] -- <client_cmd> [args...]"
    exit 1
fi

# Clean up stale config
rm -f /tmp/port.cfg

# Start server in background
"${SERVER_ARGS[@]}" &
SERVER_PID=$!

# Wait for server to be ready
sleep 1
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "Server exited early"
    exit 1
fi

# Run client with timeout
CLIENT_RC=0
timeout "$TIMEOUT" "${CLIENT_ARGS[@]}" || CLIENT_RC=$?

if [ $CLIENT_RC -eq 124 ]; then
    echo "FAILED: client timed out after ${TIMEOUT}s"
fi

# Wait for server to finish (give it a moment, then force-kill)
timeout 5 bash -c "wait $SERVER_PID" 2>/dev/null || true
kill $SERVER_PID 2>/dev/null || true
SERVER_RC=0
wait $SERVER_PID 2>/dev/null || SERVER_RC=$?

if [ $SERVER_RC -ne 0 ] || [ $CLIENT_RC -ne 0 ]; then
    echo "FAILED (server=$SERVER_RC client=$CLIENT_RC)"
    exit 1
fi
