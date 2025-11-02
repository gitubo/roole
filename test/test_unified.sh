#!/bin/bash

# test/test_unified.sh - Test unified node deployment

set -e

echo "=========================================="
echo "Roole Unified Node Cluster Test"
echo "=========================================="
echo ""

# Cleanup old processes
echo "Cleaning up old processes..."
pkill -9 node router worker 2>/dev/null || true
sleep 1

# Create log directory
mkdir -p logs

# Function to wait for node to be ready
wait_for_node() {
    local port=$1
    local max_attempts=10
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -s http://localhost:$port/metrics > /dev/null 2>&1; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    return 1
}

echo "Starting 3-node cluster (1 ingress + 2 compute)..."
echo ""

# Start ingress node (seed)
echo "[1/3] Starting INGRESS NODE (client-facing)..."
../build/bin/roole-node config/router.ini > logs/ingress_node.log 2>&1 &
INGRESS_PID=$!
echo "      PID: $INGRESS_PID"

if wait_for_node 7002; then
    echo "      ✓ Ingress node ready (metrics on :7002)"
else
    echo "      ✗ Ingress node failed to start"
    kill $INGRESS_PID 2>/dev/null
    exit 1
fi

# Start compute node 1
echo "[2/3] Starting COMPUTE NODE 100..."
../build/bin/roole-node config/worker_100.ini 4 > logs/compute_100.log 2>&1 &
COMPUTE1_PID=$!
echo "      PID: $COMPUTE1_PID"

if wait_for_node 7102; then
    echo "      ✓ Compute node 100 ready (metrics on :7102)"
else
    echo "      ✗ Compute node 100 failed to start"
    kill $INGRESS_PID $COMPUTE1_PID 2>/dev/null
    exit 1
fi

# Start compute node 2
echo "[3/3] Starting COMPUTE NODE 200..."
../build/bin/roole-node config/worker_200.ini 4 > logs/compute_200.log 2>&1 &
COMPUTE2_PID=$!
echo "      PID: $COMPUTE2_PID"

if wait_for_node 7202; then
    echo "      ✓ Compute node 200 ready (metrics on :7202)"
else
    echo "      ✗ Compute node 200 failed to start"
    kill $INGRESS_PID $COMPUTE1_PID $COMPUTE2_PID 2>/dev/null
    exit 1
fi

echo ""
echo "=========================================="
echo "Cluster Status"
echo "=========================================="
echo "✓ Ingress Node:  PID $INGRESS_PID"
echo "  - Client API:  http://localhost:8081"
echo "  - Metrics:     http://localhost:7002/metrics"
echo ""
echo "✓ Compute 100:   PID $COMPUTE1_PID"
echo "  - Metrics:     http://localhost:7102/metrics"
echo ""
echo "✓ Compute 200:   PID $COMPUTE2_PID"
echo "  - Metrics:     http://localhost:7202/metrics"
echo ""
echo "Logs: logs/*.log"
echo "=========================================="
echo ""

# Wait for cluster to stabilize
echo "Waiting for cluster to stabilize (5s)..."
sleep 5

# Send test message
echo "Sending test message to cluster..."
../build/bin/roole-client 127.0.0.1 8081 "Hello Unified Cluster" || {
    echo "✗ Test message failed"
    kill $INGRESS_PID $COMPUTE1_PID $COMPUTE2_PID 2>/dev/null
    exit 1
}

echo ""
echo "✓ Test message sent successfully"
echo ""
echo "=========================================="
echo "Cluster Running Successfully"
echo "=========================================="
echo ""
echo "Available commands:"
echo "  - Send message:    ../build/bin/roole-client 127.0.0.1 8081 'Your message'"
echo "  - View metrics:    curl http://localhost:7002/metrics"
echo "  - View logs:       tail -f logs/ingress_node.log"
echo "  - Stop cluster:    kill $INGRESS_PID $COMPUTE1_PID $COMPUTE2_PID"
echo ""
echo "Press Ctrl+C to stop all nodes and exit"
echo ""

# Trap to cleanup on exit
cleanup() {
    echo ""
    echo "Stopping cluster..."
    kill $INGRESS_PID $COMPUTE1_PID $COMPUTE2_PID 2>/dev/null
    echo "Cluster stopped"
    exit 0
}

trap cleanup INT TERM

# Keep script running
while true; do
    sleep 1
    # Check if nodes are still alive
    if ! kill -0 $INGRESS_PID 2>/dev/null; then
        echo "ERROR: Ingress node crashed"
        cleanup
    fi
    if ! kill -0 $COMPUTE1_PID 2>/dev/null; then
        echo "ERROR: Compute node 100 crashed"
        cleanup
    fi
    if ! kill -0 $COMPUTE2_PID 2>/dev/null; then
        echo "ERROR: Compute node 200 crashed"
        cleanup
    fi
done