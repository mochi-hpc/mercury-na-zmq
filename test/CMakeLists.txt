# Test executables for ZMQ NA plugin

# Make test headers available
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Common test library (adapted from Mercury's Testing infrastructure)
add_library(na_test_common_lib STATIC
    na_test_getopt.c
    na_test.c
    na_test_common.c
)
target_link_libraries(na_test_common_lib PRIVATE na mercury_util)

# Init/finalize smoke test
add_executable(test_zmq_init test_zmq_init.c)
target_link_libraries(test_zmq_init PRIVATE na)

# Message test - server
add_executable(test_zmq_msg_server test_zmq_msg_server.c)
target_link_libraries(test_zmq_msg_server PRIVATE na)

# Message test - client
add_executable(test_zmq_msg_client test_zmq_msg_client.c)
target_link_libraries(test_zmq_msg_client PRIVATE na)

# Integration tests (adapted from Mercury's Testing/unit/na/)
# Lookup test - server
add_executable(test_lookup_server test_lookup_server.c)
target_link_libraries(test_lookup_server PRIVATE na_test_common_lib na mercury_util)

# Lookup test - client
add_executable(test_lookup test_lookup.c)
target_link_libraries(test_lookup PRIVATE na_test_common_lib na mercury_util)

# ============================================================
# HG-level unit tests (adapted from Mercury's Testing/unit/hg/)
# ============================================================

# HG test common library (mercury_test.c + mercury_unit.c + mercury_rpc_cb.c)
add_library(hg_test_common_lib STATIC
    mercury_test.c
    mercury_unit.c
    mercury_rpc_cb.c
)
target_link_libraries(hg_test_common_lib PUBLIC na_test_common_lib mercury mercury_util)

# HG test server
add_executable(test_hg_server test_hg_server.c)
target_link_libraries(test_hg_server PRIVATE hg_test_common_lib)

# HG RPC test (client)
add_executable(test_rpc test_rpc.c)
target_link_libraries(test_rpc PRIVATE hg_test_common_lib)

# HG Bulk test (client)
add_executable(test_bulk test_bulk.c)
target_link_libraries(test_bulk PRIVATE hg_test_common_lib)

# HG Proc test (standalone, no server needed)
add_executable(test_proc test_proc.c)
target_link_libraries(test_proc PRIVATE hg_test_common_lib)
