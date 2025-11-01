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
)
target_link_libraries(roole_node 
    PUBLIC roole_core roole_dag roole_cluster
    PRIVATE pthread m
)

# ============================================================================
# EXECUTABLES
# ============================================================================

# Unified node binary
add_executable(node test/node.c)
target_link_libraries(node roole_node)

# Client binary
add_executable(client test/client.c)
target_link_libraries(client roole_core)

# ============================================================================
# INSTALL TARGETS
# ============================================================================
install(TARGETS node client
    RUNTIME DESTINATION bin
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
message(STATUS "==============================================")
message(STATUS "Roole Build Configuration")
message(STATUS "==============================================")
message(STATUS "Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C Compiler: ${CMAKE_C_COMPILER}")
message(STATUS "C Flags: ${CMAKE_C_FLAGS}")
message(STATUS "Install Prefix: ${CMAKE_INSTALL_PREFIX}")
message(STATUS "Build Tests: ${BUILD_TESTS}")
message(STATUS "")
message(STATUS "Binaries:")
message(STATUS "  - node   (unified node binary)")
message(STATUS "  - client (test client)")
message(STATUS "")
message(STATUS "Metrics: Built-in (no external dependencies)")
message(STATUS "==============================================")