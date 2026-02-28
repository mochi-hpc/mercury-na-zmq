# Relay service for cross-cluster message forwarding

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(na_zmq_relay relay.cpp)
target_link_libraries(na_zmq_relay PRIVATE ${ZMQ_LINK_LIBRARIES} toml11::toml11)
target_include_directories(na_zmq_relay PRIVATE ${ZMQ_INCLUDE_DIRS})

install(TARGETS na_zmq_relay RUNTIME DESTINATION bin)
