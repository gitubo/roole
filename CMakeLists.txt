cmake_minimum_required(VERSION 3.10)
project(roole C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -pthread -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -O0 -DDEBUG")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3 -march=native")

include_directories(include)

# ============================================================================
# CORE LIBRARY (RPC + Common utilities)
# ============================================================================
add_library(roole_core STATIC
    src/core/rpc.c
    src/core/common.c
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
    src/cluster/failure_detector.c
)
target_link_libraries(roole_cluster roole_core)

# ============================================================================
# ROUTER LIBRARY
# ============================================================================
add_library(roole_router STATIC
    src/router/router_core.c
    src/router/load_balancer.c
    src/router/execution_tracker.c
    src/router/message_handler.c
    src/router/rpc_handler.c
)
target_link_libraries(roole_router roole_core roole_dag roole_cluster)

# ============================================================================
# WORKER LIBRARY
# ============================================================================
add_library(roole_worker STATIC
    src/worker/worker_core.c
    src/worker/message_queue.c
    src/worker/message_handler.c
    src/worker/rpc_handler.c
)
target_link_libraries(roole_worker roole_core roole_dag roole_cluster)

# ============================================================================
# EXECUTABLES
# ============================================================================

# Router binary
add_executable(router test/router.c)
target_link_libraries(router roole_router)

# Worker binary
add_executable(worker test/worker.c)
target_link_libraries(worker roole_worker)

# Client binary
add_executable(client test/client.c)
target_link_libraries(client roole_core)

# ============================================================================
# INSTALL TARGETS
# ============================================================================
install(TARGETS router worker client
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
message(STATUS "Roole Configuration Summary")
message(STATUS "==============================================")
message(STATUS "Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C Compiler: ${CMAKE_C_COMPILER}")
message(STATUS "C Flags: ${CMAKE_C_FLAGS}")
message(STATUS "Install Prefix: ${CMAKE_INSTALL_PREFIX}")
message(STATUS "Build Tests: ${BUILD_TESTS}")
message(STATUS "==============================================")