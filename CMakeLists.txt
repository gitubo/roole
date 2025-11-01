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
# NODE LIBRARY (Unified - replaces router + worker)
# ============================================================================
add_library(roole_node STATIC
    # Core node functionality
    src/node/peer_pool.c
    src/node/node_metrics.c
    src/node/node_rpc.c
    src/node/node_handlers.c
    src/node/node_executor.c
    src/node/node_capabilities.c
    src/node/node_core.c
    src/node/node_bootstrap.c
    
    # Moved from worker library
    src/worker/message_queue.c
    
    # Moved from router library
    src/router/execution_tracker.c
)
target_link_libraries(roole_node 
    PUBLIC roole_core roole_dag roole_cluster
    PRIVATE pthread m
)

# ============================================================================
# LEGACY LIBRARIES (For backward compatibility with old test binaries)
# ============================================================================
option(BUILD_LEGACY_BINARIES "Build deprecated router/worker binaries" OFF)

if(BUILD_LEGACY_BINARIES)
    message(WARNING "Building LEGACY binaries - these are DEPRECATED")
    message(WARNING "Use -DBUILD_LEGACY_BINARIES=OFF to disable")
    
    # Legacy router library (minimal wrapper around roole_node)
    add_library(roole_router STATIC
        src/router/router_core.c
        src/router/load_balancer.c
        src/router/message_handler.c
        src/router/rpc_handler.c
    )
    target_link_libraries(roole_router roole_node)
    
    # Legacy worker library (minimal wrapper around roole_node)
    add_library(roole_worker STATIC
        src/worker/worker_core.c
        src/worker/message_handler.c
        src/worker/rpc_handler.c
    )
    target_link_libraries(roole_worker roole_node)
endif()

# ============================================================================
# EXECUTABLES
# ============================================================================

# Unified node binary (RECOMMENDED)
add_executable(node test/node.c)
target_link_libraries(node roole_node)

# Client binary
add_executable(client test/client.c)
target_link_libraries(client roole_core)

# Legacy binaries (DEPRECATED)
if(BUILD_LEGACY_BINARIES)
    add_executable(router test/router.c)
    target_link_libraries(router roole_router roole_node)
    
    add_executable(worker test/worker.c)
    target_link_libraries(worker roole_worker roole_node)
    
    install(TARGETS router worker
        RUNTIME DESTINATION bin
    )
endif()

# ============================================================================
# INSTALL TARGETS
# ============================================================================
install(TARGETS node client
    RUNTIME DESTINATION bin
)

install(DIRECTORY include/roole
    DESTINATION include
    PATTERN "router.h" EXCLUDE
    PATTERN "worker.h" EXCLUDE
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
message(STATUS "Roole Configuration Summary")
message(STATUS "==============================================")
message(STATUS "Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C Compiler: ${CMAKE_C_COMPILER}")
message(STATUS "C Flags: ${CMAKE_C_FLAGS}")
message(STATUS "Install Prefix: ${CMAKE_INSTALL_PREFIX}")
message(STATUS "Build Tests: ${BUILD_TESTS}")
message(STATUS "Build Legacy Binaries: ${BUILD_LEGACY_BINARIES}")
if(BUILD_LEGACY_BINARIES)
    message(STATUS "")
    message(STATUS "⚠️  WARNING: Legacy binaries enabled")
    message(STATUS "   These are DEPRECATED and will be removed in v0.3.0")
    message(STATUS "   Use -DBUILD_LEGACY_BINARIES=OFF to disable")
endif()
message(STATUS "Metrics: Built-in (no external dependencies)")
message(STATUS "==============================================")