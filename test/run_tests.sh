#!/bin/bash
#
# Run the ZMQ NA plugin tests.
# Usage: ./run_tests.sh [build_dir]
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="${1:-${SCRIPT_DIR}/../build}"

# Find the plugin .so
PLUGIN_DIR="${BUILD_DIR}"
if [ -f "${BUILD_DIR}/libna_plugin_zmq.so" ]; then
    PLUGIN_DIR="${BUILD_DIR}"
elif [ -f "${BUILD_DIR}/src/libna_plugin_zmq.so" ]; then
    PLUGIN_DIR="${BUILD_DIR}/src"
fi

export NA_PLUGIN_PATH="${PLUGIN_DIR}"
echo "NA_PLUGIN_PATH=${NA_PLUGIN_PATH}"

PASS=0
FAIL=0

run_test() {
    local test_name="$1"
    shift
    echo ""
    echo "=========================================="
    echo "Running: ${test_name}"
    echo "=========================================="
    if "$@"; then
        echo "PASSED: ${test_name}"
        PASS=$((PASS + 1))
    else
        echo "FAILED: ${test_name}"
        FAIL=$((FAIL + 1))
    fi
}

run_server_client() {
    local test_name="$1"
    local server_cmd="$2"
    shift 2
    local client_cmd="$@"

    echo ""
    echo "=========================================="
    echo "Running: ${test_name}"
    echo "=========================================="

    # Clean up old config file
    rm -f /tmp/port.cfg

    # Start server in background
    ${server_cmd} &
    SERVER_PID=$!

    # Give server time to start and write config
    sleep 1

    # Check server is still running
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "FAILED: ${test_name} (server exited early)"
        FAIL=$((FAIL + 1))
        return
    fi

    # Start client
    ${client_cmd}
    CLIENT_RC=$?

    # Wait for server
    wait $SERVER_PID
    SERVER_RC=$?

    if [ $SERVER_RC -eq 0 ] && [ $CLIENT_RC -eq 0 ]; then
        echo "PASSED: ${test_name}"
        PASS=$((PASS + 1))
    else
        echo "FAILED: ${test_name} (server=$SERVER_RC client=$CLIENT_RC)"
        FAIL=$((FAIL + 1))
    fi
}

# ==========================================
# Test 1: Init/finalize smoke test
# ==========================================
run_test "init test" "${BUILD_DIR}/test/test_zmq_init"

# ==========================================
# Test 2: Basic message test (server + client)
# ==========================================
# Clean up old address file
rm -f zmq_test_addr.txt
run_server_client "basic message test" \
    "${BUILD_DIR}/test/test_zmq_msg_server" \
    "${BUILD_DIR}/test/test_zmq_msg_client"

# ==========================================
# Test 3: Integration lookup test (adapted from Mercury Testing)
# ==========================================
run_server_client "integration lookup test" \
    "${BUILD_DIR}/test/test_lookup_server -p zmq" \
    "${BUILD_DIR}/test/test_lookup -p zmq"

# ==========================================
# Test 4: HG Proc test (standalone, no server needed)
# ==========================================
run_test "HG proc test" "${BUILD_DIR}/test/test_proc"

# ==========================================
# Test 5: HG RPC test (server + client)
# ==========================================
run_server_client "HG RPC test" \
    "${BUILD_DIR}/test/test_hg_server -p zmq" \
    "${BUILD_DIR}/test/test_rpc -p zmq"

# ==========================================
# Test 6: HG Bulk test (server + client)
# ==========================================
run_server_client "HG bulk test" \
    "${BUILD_DIR}/test/test_hg_server -p zmq" \
    "${BUILD_DIR}/test/test_bulk -p zmq"

# ==========================================
# Summary
# ==========================================
echo ""
echo "=========================================="
echo "Test Summary: ${PASS} passed, ${FAIL} failed"
echo "=========================================="

if [ $FAIL -ne 0 ]; then
    exit 1
fi
