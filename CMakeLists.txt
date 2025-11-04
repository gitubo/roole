cmake_minimum_required(VERSION 3.10)
project(roole C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -pthread -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -O0 -DDEBUG")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3 -march=native")

include_directories(include)

# ============================================================================
# CORE LIBRARY (RPC + Common utilities + Metrics)
# ============================================================================
add_library(roole_core STATIC
    src/core/rpc.c
    src/core/common.c
    src/core/config.c
    src/core/config_validator.c
    src/core/logger.c
    src/core/service_registry.c
    src/core/event_bus.c
    src/core/metrics.c
    src/core/metrics_server.c
)
target_link_libraries(roole_core pthread m)

# ============================================================================
# DAG LIBRARY (DAG catalog + execution engine)
# ============================================================================
add_library(roole_dag STATIC
    src/dag/dag_catalog.c
    src/dag/dag_executor.c
)
target_link_libraries(roole_dag roole_core)

# ============================================================================
# CLUSTER LIBRARY (Membership + failure detection)
# ============================================================================
add_library(roole_cluster STATIC
    src/cluster/membership.c
    src/cluster/gossip_engine.c        
    src/cluster/gossip_protocol.c      
    src/cluster/gossip_transport.c     
)
target_link_libraries(roole_cluster roole_core)

# ============================================================================
# NODE LIBRARY (Unified - all node functionality)
# ============================================================================
add_library(roole_node STATIC
    src/node/peer_pool.c
    src/node/message_queue.c
    src/node/execution_tracker.c
    src/node/node_metrics.c
    src/node/node_rpc.c
    src/node/node_handlers.c
    src/node/node_executor.c
    src/node/node_capabilities.c
    src/node/node_core.c
    src/node/node_bootstrap.c
    src/node/node_state.c
    src/node/peer_pool.c
)
target_link_libraries(roole_node 
    PUBLIC roole_core roole_dag roole_cluster
    PRIVATE pthread m
)

# ============================================================================
# EXECUTABLES
# ============================================================================

# Unified node binary (main entry point)
add_executable(node src/node/node_main.c)
target_link_libraries(node roole_node)
set_target_properties(node PROPERTIES
    OUTPUT_NAME "roole-node"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# Client binary (testing tool - stays in test/)
add_executable(client test/client.c)
target_link_libraries(client roole_core)
set_target_properties(client PROPERTIES
    OUTPUT_NAME "roole-client"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# ============================================================================
# INSTALL TARGETS
# ============================================================================
install(TARGETS node client
    RUNTIME DESTINATION bin
)

# Install with proper names
install(PROGRAMS 
    "${CMAKE_BINARY_DIR}/bin/roole-node"
    "${CMAKE_BINARY_DIR}/bin/roole-client"
    DESTINATION bin
    OPTIONAL
)

install(DIRECTORY include/roole
    DESTINATION include
)

# ============================================================================
# OPTIONAL: Tests
# ============================================================================
option(BUILD_TESTS "Build test executables" OFF)

if(BUILD_TESTS)
    add_executable(test_dag_catalog test/test_dag.c)
    target_link_libraries(test_dag_catalog roole_dag)
    
    add_executable(test_cluster test/test_cluster.c)
    target_link_libraries(test_cluster roole_cluster)
endif()

# ============================================================================
# SUMMARY
# ============================================================================
message(STATUS "")
message(STATUS "Binaries:")
message(STATUS "  - roole-node   (unified node binary) [src/node/node_main.c]")
message(STATUS "  - roole-client (test client) [test/client.c]")
message(STATUS "")
message(STATUS "Output: ${CMAKE_BINARY_DIR}/bin/")
message(STATUS "")