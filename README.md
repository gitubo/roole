
# Roole

**Roole** is a distributed execution engine for DAG (Directed Acyclic Graph) workflows.  
It provides a lightweight runtime capable of coordinating multiple worker nodes through a decentralized gossip-based cluster membership system.  
Roole is written in **C11** and focuses on performance, scalability, and minimal dependencies.

---

## Overview

Roole enables the distributed execution of task graphs (DAGs) across a cluster of nodes.  
It provides:

- **Router layer** — Handles job submission, scheduling, and dispatching tasks to workers.  
- **Worker layer** — Executes DAG tasks, reports metrics, and handles retries or failures.  
- **Cluster membership** — Implements a gossip protocol for node discovery and failure detection.  
- **RPC layer** — Lightweight transport for inter-node communication.  
- **Configuration module** — Parses INI-style configuration files.  
- **Metrics and monitoring** — Workers expose runtime metrics for debugging and performance tuning.

---

## Architecture

```
+-------------------+
|  Router Node(s)   |  <- Receives jobs and DAG definitions
|-------------------|
|  Load Balancer    |
|  Execution Tracker|
|  RPC Handler      |
+-------------------+
          |
          v
+-------------------+
|   Worker Node(s)  |  <- Executes individual DAG tasks
|-------------------|
|  Message Queue    |
|  Worker Core      |
|  Metrics Module   |
+-------------------+
          |
          v
+-------------------+
|  Cluster Gossip   |  <- Membership and failure detection
+-------------------+
```

The cluster layer uses a **gossip protocol** (`src/cluster/`) for peer discovery and status propagation.  
Routers communicate with workers using the internal **RPC subsystem** (`src/core/rpc.c`).  
DAG execution is managed by the **executor** and **catalog** components under `src/dag/`.

---

## Build

### Requirements
- GCC or Clang (C11)
- CMake ≥ 3.16
- POSIX environment (Linux recommended)
- pthreads

### Build Instructions
```bash
mkdir build && cd build
cmake ..
make -j
```

This produces the `roole` binaries and test executables under `build/`.

---

## Running

Example: start a 3-node cluster (1 router + 2 workers)

```bash
# From project root
./examples/cluster_3nodes.sh
```

Configuration files are under `config/`:
- `router.ini` — defines router parameters (port, peers)
- `worker_100.ini`, `worker_200.ini` — define worker nodes

---

## Code Structure

```
include/roole/   # Public headers
src/core/        # Common utilities, RPC, configuration
src/cluster/     # Gossip protocol, membership
src/dag/         # DAG catalog and executor
src/router/      # Router core, load balancing, tracking
src/worker/      # Worker core, message queue, metrics
test/            # Integration and gossip tests
examples/        # Demo scripts
```

---

## License

MIT License.  
See `LICENSE` file for details.

---

## Contact

Maintainer: [gitubo](https://github.com/gitubo)  
Contributions and bug reports are welcome via GitHub Issues and Pull Requests.
