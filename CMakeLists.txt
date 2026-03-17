cmake_minimum_required (VERSION 3.15)
project (na_plugin_zmq C CXX)

option (ENABLE_COVERAGE "Enable code coverage" OFF)
option (ENABLE_ASAN "Enable address sanitizer" OFF)

if (ENABLE_COVERAGE)
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --coverage -O0")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -O0")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage -lgcov")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} --coverage -lgcov")
    set (CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} --coverage -lgcov")
endif ()

if (ENABLE_ASAN)
    set (CMAKE_C_FLAGS "-fno-omit-frame-pointer -g -Wall -fsanitize=address")
    set (CMAKE_CXX_FLAGS "-fno-omit-frame-pointer -g -Wall -fsanitize=address")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=address")
    set (CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -fsanitize=address")
endif ()

# Find Mercury
find_package (mercury REQUIRED)
message (STATUS "Mercury version: ${MERCURY_VERSION_FULL}")

# Mercury <= 2.4.1 does not install na_plugin.h / na_error.h which are
# required to build out-of-tree NA plugins.  Fetch them from the upstream
# repository when they are missing.
if (MERCURY_VERSION_FULL VERSION_LESS_EQUAL "2.4.1")
    set (_hg_tag "v${MERCURY_VERSION_FULL}")
    set (_hg_hdr_dir "${CMAKE_CURRENT_BINARY_DIR}/mercury_headers")
    set (_need_download FALSE)
    foreach (_hdr na_plugin.h na_error.h)
        if (NOT EXISTS "${_hg_hdr_dir}/${_hdr}")
            set (_need_download TRUE)
        endif ()
    endforeach ()
    if (_need_download)
        message (STATUS
            "Mercury ${MERCURY_VERSION_FULL} does not ship na_plugin.h / "
            "na_error.h – downloading from GitHub (tag ${_hg_tag})")
        file (MAKE_DIRECTORY "${_hg_hdr_dir}")
        foreach (_hdr na_plugin.h na_error.h)
            file (DOWNLOAD
                "https://raw.githubusercontent.com/mercury-hpc/mercury/${_hg_tag}/src/na/${_hdr}"
                "${_hg_hdr_dir}/${_hdr}"
                STATUS _dl_status
            )
            list (GET _dl_status 0 _dl_rc)
            if (NOT _dl_rc EQUAL 0)
                list (GET _dl_status 1 _dl_msg)
                message (FATAL_ERROR
                    "Failed to download ${_hdr}: ${_dl_msg}")
            endif ()
        endforeach ()
    endif ()
    set (MERCURY_EXTRA_INCLUDE_DIR "${_hg_hdr_dir}")
    message (STATUS "Using downloaded NA headers in ${_hg_hdr_dir}")
else ()
    set (MERCURY_EXTRA_INCLUDE_DIR "")
endif ()

# Find ZMQ - try pkg-config first, then manual search
find_package (PkgConfig QUIET)
if (PkgConfig_FOUND)
    pkg_check_modules (ZMQ QUIET libzmq)
endif ()

if (NOT ZMQ_FOUND)
    # Manual find
    find_path (ZMQ_INCLUDE_DIRS zmq.h
        HINTS ${CMAKE_PREFIX_PATH}
        PATH_SUFFIXES include)
    find_library (ZMQ_LINK_LIBRARIES zmq
        HINTS ${CMAKE_PREFIX_PATH}
        PATH_SUFFIXES lib)
    if (ZMQ_INCLUDE_DIRS AND ZMQ_LINK_LIBRARIES)
        set (ZMQ_FOUND TRUE)
    else ()
        message (FATAL_ERROR "Could not find libzmq. Set CMAKE_PREFIX_PATH.")
    endif ()
endif ()

message (STATUS "ZMQ include: ${ZMQ_INCLUDE_DIRS}")
message (STATUS "ZMQ library: ${ZMQ_LINK_LIBRARIES}")

# Find UUID (libuuid from e.g. util-linux)
if (PkgConfig_FOUND)
    pkg_check_modules(UUID REQUIRED uuid)
else ()
    find_path (UUID_INCLUDE_DIRS uuid/uuid.h)
    find_library (UUID_LINK_LIBRARIES uuid)
    if (NOT UUID_INCLUDE_DIRS OR NOT UUID_LINK_LIBRARIES)
        message (FATAL_ERROR "Could not find libuuid. Install uuid-dev or set CMAKE_PREFIX_PATH.")
    endif ()
endif ()

message (STATUS "UUID include: ${UUID_INCLUDE_DIRS}")
message (STATUS "UUID library: ${UUID_LINK_LIBRARIES}")

# Build the plugin as a MODULE library
add_library (na_plugin_zmq MODULE src/na_zmq.c)

target_include_directories (na_plugin_zmq PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${ZMQ_INCLUDE_DIRS}
    ${UUID_INCLUDE_DIRS}
    ${MERCURY_EXTRA_INCLUDE_DIR}
)

target_link_libraries (na_plugin_zmq PRIVATE mercury ${ZMQ_LINK_LIBRARIES} ${UUID_LINK_LIBRARIES})

set_target_properties (na_plugin_zmq PROPERTIES PREFIX "lib")

# Installation
if (NOT NA_PLUGIN_INSTALL_DIR)
    include (GNUInstallDirs)
    set (NA_PLUGIN_INSTALL_DIR ${CMAKE_INSTALL_LIBDIR})
endif ()

install (TARGETS na_plugin_zmq
    LIBRARY DESTINATION ${NA_PLUGIN_INSTALL_DIR}
)

find_package (toml11 REQUIRED)

add_subdirectory (relay)

# Tests
enable_testing ()
add_subdirectory (test)
