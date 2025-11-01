# Roole

**Roole** is a distributed execution engine for DAG (Directed Acyclic Graph) workflows.  
It provides a lightweight, capability-driven runtime for coordinating distributed task execution across a cluster of nodes using a decentralized gossip-based membership protocol.

Written in **C11**, Roole focuses on performance, scalability, and minimal dependencies.

---

## Overview

Roole enables distributed execution of task graphs (DAGs) across a dynamic cluster of nodes.  
Unlike traditional router/worker architectures, **all Roole nodes are identical at compile-time** and differentiate themselves via **runtime capabilities**:

- **Ingress Capability**: Accept client requests (like traditional "routers")
- **Execution Capability**: Process DAG tasks (like traditional "workers")  
- **Routing Capability**: Forward messages to other nodes

A node's capabilities are **detected automatically from configuration**, allowing flexible deployment:
- **Pure Ingress Node**: `ingress_addr` configured, no executor threads
- **Pure Compute Node**: No `ingress_addr`, multiple executor threads
- **Hybrid Node**: Both `ingress_addr` and executor threads enabled

---

## Architecture
```
┌──────────────────────────────────────────────┐
│          Unified Node (Capability-Driven)    │
│  ┌────────────┐  ┌─────────────┐            │
│  │  Ingress   │  │  Execution  │            │
│  │  (Optional)│  │  (Optional) │            │
│  └────────────┘  └─────────────┘            │
│         │                │                   │
│         └────────┬───────┘                   │
│                  │                           │
│         ┌────────▼─────────┐                │
│         │   RPC Subsystem  │                │
│         │  (DATA channel)  │                │
│         └────────┬─────────┘                │
│                  │                           │
│         ┌────────▼─────────┐                │
│         │  Gossip Engine   │                │
│         │  (SWIM Protocol) │                │
│         └──────────────────┘                │
└──────────────────────────────────────────────┘
```

### Key Components:

1. **Unified Node**: Single binary, capability-based role differentiation
2. **DAG Catalog**: Shared workflow definitions across cluster
3. **Execution Tracker**: Monitors task execution state and retries
4. **Peer Pool**: Maintains connections to cluster members
5. **Gossip Engine**: SWIM-based failure detection and cluster membership
6. **RPC Subsystem**: Asynchronous message passing between nodes
7. **Metrics System**: Dependency-free Prometheus exporter

---

## Build

### Requirements:
- GCC or Clang (C11 standard)
- CMake ≥ 3.10
- POSIX environment (Linux/macOS)
- pthreads

### Build Instructions:
```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

Produces:
- `./node` - Unified node binary (recommended)
- `./client` - Test client for submitting tasks

---

## Configuration

Nodes are configured via INI files. Capabilities are auto-detected:

### Example: Ingress Node (Client-Facing)
```ini
# config/ingress_node.ini
[Cluster]
name = production_cluster
routers = 10.0.1.10:7000;10.0.1.11:7000  # Seeds for bootstrap

[Node]
id = 1
type = ROUTER  # Legacy label (ignored by unified binary)
gossip_addr = 0.0.0.0:7000
data_addr = 0.0.0.0:7001
ingress_addr = 0.0.0.0:8081      # ← Enables ingress capability
metrics_addr = 0.0.0.0:9090

[Logging]
level = INFO
```

### Example: Compute Node (Execution-Only)
```ini
# config/compute_node.ini
[Cluster]
name = production_cluster
routers = 10.0.1.1:7000  # Seed node for discovery

[Node]
id = 100
type = WORKER  # Legacy label (ignored)
gossip_addr = 0.0.0.0:7100
data_addr = 0.0.0.0:7101
# No ingress_addr = No client-facing capability
metrics_addr = 0.0.0.0:9100

[Logging]
level = INFO
```

---

## Running

### Quick Start - Unified Node Deployment

Start a 3-node cluster using the unified binary:
```bash
# Use the automated test script:
./test/test_unified.sh

# Or manually:

# Terminal 1: Ingress node (accepts client requests)
./node config/router.ini

# Terminal 2: Compute node 1
./node config/worker_100.ini 4

# Terminal 3: Compute node 2
./node config/worker_200.ini 4

# Terminal 4: Send test message
./client 127.0.0.1 8081 "Hello Roole"
```

### Legacy Binary Support (Deprecated)

The separate `router` and `worker` binaries are still available but deprecated:
```bash
# ⚠️ DEPRECATED - Will be removed in v0.3.0
./router config/router.ini
./worker config/worker_100.ini 4

# ✅ RECOMMENDED - Use unified binary instead
./node config/router.ini
./node config/worker_100.ini 4
```

To disable building legacy binaries:
```bash
cmake -DBUILD_LEGACY_BINARIES=OFF ..
```

### Monitoring

Access Prometheus metrics from any node:
```bash
# Ingress node metrics
curl http://localhost:7002/metrics

# Compute node metrics
curl http://localhost:7102/metrics
curl http://localhost:7202/metrics
```

All metrics include standard labels:
- `cluster_name="my_test_cluster"`
- `node_id="1"` (or 100, 200, etc.)
- `node_type="router"` (ingress) or `"worker"` (compute)

## Code Structure
```
include/roole/          # Public API headers
  ├── node.h            # Unified node (replaces router.h + worker.h)
  ├── cluster.h         # SWIM gossip protocol
  ├── dag.h             # DAG definitions
  ├── rpc.h             # Async RPC subsystem
  ├── metrics.h         # Prometheus exporter
  └── config.h          # INI parser

src/
  ├── core/             # Common utilities (RPC, metrics, logging)
  ├── cluster/          # SWIM gossip implementation
  ├── dag/              # DAG catalog and executor
  └── node/             # Unified node implementation
      ├── node_core.c         # Initialization & lifecycle
      ├── node_rpc.c          # RPC service table builder
      ├── node_handlers.c     # RPC request handlers
      ├── node_executor.c     # DAG execution threads
      ├── node_bootstrap.c    # Cluster join protocol
      ├── node_capabilities.c # Capability detection
      ├── node_metrics.c      # Metrics initialization
      └── peer_pool.c         # Peer connection management

test/
  ├── node.c            # Unified node binary (USE THIS)
  ├── client.c          # Test client
  └── *.ini             # Example configurations
```

---

## Design Philosophy

### Capability-Based Architecture:
- **No hardcoded roles**: "Router" and "Worker" are legacy concepts
- **Runtime flexibility**: Same binary, different configurations
- **Graceful degradation**: Cluster adapts to node failures

### SWIM Gossip Protocol:
- **Scalable failure detection**: O(log N) message complexity
- **Eventual consistency**: Cluster state converges within seconds
- **Partition tolerance**: Nodes can rejoin after network splits

### Zero-Dependency Metrics:
- **No Prometheus client library**: Direct text format export
- **Minimal overhead**: Lock-free counters, atomic gauges
- **Standard labels**: All metrics include `cluster_name`, `node_id`, `node_type`

---

## Metrics Reference

| Metric | Type | Description |
|--------|------|-------------|
| `messages_processed_total` | Counter | Successfully completed tasks |
| `messages_failed_total` | Counter | Failed task executions |
| `messages_routed_total` | Counter | Tasks forwarded to other nodes |
| `messages_queue_size` | Gauge | Current queue depth |
| `active_executions` | Gauge | Tasks currently executing |
| `cluster_members_total` | Gauge | Known cluster members |
| `cluster_members_active` | Gauge | ALIVE members (ready for work) |
| `cluster_members_suspect` | Gauge | SUSPECT members (pending failure) |
| `cluster_members_dead` | Gauge | DEAD members (excluded from routing) |
| `uptime_seconds` | Gauge | Node uptime since start |

**Standard Labels** (present on all metrics):
- `cluster_name`: Cluster identifier
- `node_id`: Unique node ID
- `node_type`: `"router"` (ingress) or `"worker"` (compute-only)

---

## Migration from Legacy Binaries

If you were using separate `router` and `worker` binaries:

### Before (Legacy):
```bash
./router config/router.ini
./worker config/worker_100.ini 4
```

### After (Unified):
```bash
./node config/router.ini      # Automatically detected as ingress node
./node config/worker_100.ini 4  # Automatically detected as compute node
```

**No configuration changes required** - capability detection is automatic based on `ingress_addr` presence.

---

## Testing
```bash
# Run full cluster test:
./test/test_unified.sh

# Check logs:
tail -f logs/*.log
```

---

## Performance

Typical performance on commodity hardware:
- **Throughput**: 10K+ messages/sec per node (8 threads)
- **Latency**: <10ms p99 (local cluster)
- **Failure Detection**: <5s (SWIM protocol)
- **Memory**: ~50MB baseline per node

---

## License

MIT License. See `LICENSE` file for details.

---

## Roadmap

- [x] Unified node binary (v0.2.0)
- [x] SWIM gossip protocol (v0.2.0)
- [x] Dependency-free metrics (v0.2.0)
- [ ] DAG versioning and hot-reload (v0.3.0)
- [ ] Persistent execution state (v0.3.0)
- [ ] Raft consensus for catalog (v0.4.0)
- [ ] Python DAG SDK (v0.5.0)

---

## Contributing

Issues and PRs welcome via GitHub.

**Contact**: [gitubo](https://github.com/gitubo)