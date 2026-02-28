cmake_minimum_required(VERSION 3.15)
project(na_plugin_zmq C CXX)

# Find Mercury
find_package(mercury REQUIRED)

# Find ZMQ - try pkg-config first, then manual search
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(ZMQ QUIET libzmq)
endif()

if(NOT ZMQ_FOUND)
    # Manual find
    find_path(ZMQ_INCLUDE_DIRS zmq.h
        HINTS ${CMAKE_PREFIX_PATH}
        PATH_SUFFIXES include)
    find_library(ZMQ_LINK_LIBRARIES zmq
        HINTS ${CMAKE_PREFIX_PATH}
        PATH_SUFFIXES lib)
    if(ZMQ_INCLUDE_DIRS AND ZMQ_LINK_LIBRARIES)
        set(ZMQ_FOUND TRUE)
    else()
        message(FATAL_ERROR "Could not find libzmq. Set CMAKE_PREFIX_PATH.")
    endif()
endif()

message(STATUS "ZMQ include: ${ZMQ_INCLUDE_DIRS}")
message(STATUS "ZMQ library: ${ZMQ_LINK_LIBRARIES}")

# Find UUID (libuuid from util-linux)
if(PkgConfig_FOUND)
    pkg_check_modules(UUID REQUIRED uuid)
else()
    find_path(UUID_INCLUDE_DIRS uuid/uuid.h)
    find_library(UUID_LINK_LIBRARIES uuid)
    if(NOT UUID_INCLUDE_DIRS OR NOT UUID_LINK_LIBRARIES)
        message(FATAL_ERROR "Could not find libuuid. Install uuid-dev or set CMAKE_PREFIX_PATH.")
    endif()
endif()

message(STATUS "UUID include: ${UUID_INCLUDE_DIRS}")
message(STATUS "UUID library: ${UUID_LINK_LIBRARIES}")

# Build the plugin as a MODULE library
add_library(na_plugin_zmq MODULE src/na_zmq.c)

target_include_directories(na_plugin_zmq PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${ZMQ_INCLUDE_DIRS}
    ${UUID_INCLUDE_DIRS}
)

target_link_libraries(na_plugin_zmq PRIVATE mercury ${ZMQ_LINK_LIBRARIES} ${UUID_LINK_LIBRARIES})

set_target_properties(na_plugin_zmq PROPERTIES PREFIX "lib")

# Installation
if(NOT NA_PLUGIN_INSTALL_DIR)
    include(GNUInstallDirs)
    set(NA_PLUGIN_INSTALL_DIR ${CMAKE_INSTALL_LIBDIR})
endif()

install(TARGETS na_plugin_zmq
    LIBRARY DESTINATION ${NA_PLUGIN_INSTALL_DIR}
)

# Relay service (optional, requires toml11)
find_package(toml11 QUIET)
if(toml11_FOUND)
    add_subdirectory(relay)
else()
    message(STATUS "toml11 not found, relay service will not be built")
endif()

# Tests
enable_testing()
add_subdirectory(test)
