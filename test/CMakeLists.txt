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

# ============================================================
# CTest definitions
# ============================================================

set(TEST_DRIVER ${CMAKE_CURRENT_SOURCE_DIR}/test_driver.sh)

# Standalone tests
add_test(NAME zmq_init COMMAND test_zmq_init)
add_test(NAME zmq_proc COMMAND test_proc)

# Server+client tests
add_test(NAME zmq_msg
    COMMAND ${TEST_DRIVER}
        $<TARGET_FILE:test_zmq_msg_server> --
        $<TARGET_FILE:test_zmq_msg_client>)

add_test(NAME zmq_lookup
    COMMAND ${TEST_DRIVER}
        $<TARGET_FILE:test_lookup_server> -p zmq --
        $<TARGET_FILE:test_lookup> -p zmq)

add_test(NAME zmq_rpc
    COMMAND ${TEST_DRIVER}
        $<TARGET_FILE:test_hg_server> -p zmq --
        $<TARGET_FILE:test_rpc> -p zmq)

add_test(NAME zmq_bulk
    COMMAND ${TEST_DRIVER}
        $<TARGET_FILE:test_hg_server> -p zmq --
        $<TARGET_FILE:test_bulk> -p zmq)

# All tests need NA_PLUGIN_PATH to find the plugin
set_tests_properties(zmq_init zmq_proc zmq_msg zmq_lookup zmq_rpc zmq_bulk
    PROPERTIES ENVIRONMENT "NA_PLUGIN_PATH=$<TARGET_FILE_DIR:na_plugin_zmq>")

# ============================================================
# Cross-cluster relay integration tests
# ============================================================

if(TARGET na_zmq_relay)
    set(RELAY_TEST_DRIVER ${CMAKE_CURRENT_SOURCE_DIR}/test_relay_driver.sh)

    # Relay RPC test (RPC through two relays between clusters)
    add_test(NAME zmq_relay_rpc
        COMMAND ${RELAY_TEST_DRIVER}
            $<TARGET_FILE:na_zmq_relay>
            $<TARGET_FILE:test_hg_server>
            $<TARGET_FILE:test_rpc>)
    set_tests_properties(zmq_relay_rpc
        PROPERTIES ENVIRONMENT "NA_PLUGIN_PATH=$<TARGET_FILE_DIR:na_plugin_zmq>"
                   TIMEOUT 60)

    # Relay bulk test (RPC + bulk transfer through two relays between clusters)
    add_test(NAME zmq_relay_bulk
        COMMAND ${RELAY_TEST_DRIVER}
            $<TARGET_FILE:na_zmq_relay>
            $<TARGET_FILE:test_hg_server>
            $<TARGET_FILE:test_bulk>)
    set_tests_properties(zmq_relay_bulk
        PROPERTIES ENVIRONMENT "NA_PLUGIN_PATH=$<TARGET_FILE_DIR:na_plugin_zmq>"
                   TIMEOUT 60)
endif()
