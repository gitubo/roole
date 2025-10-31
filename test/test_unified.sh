#!/bin/bash

# test/test_unified.sh - Test unified node deployment

set -e

echo "=========================================="
echo "Roole Unified Node Test"
echo "=========================================="
echo ""

# Cleanup
echo "Cleaning up old processes..."
pkill -9 node router worker 2>/dev/null || true
sleep 1

# Create test directories
mkdir -p logs

echo "Starting test cluster with unified nodes..."
echo ""

# Start seed node (ingress-capable)
echo "1. Starting SEED NODE (ingress + execution)..."
./node config/router.ini > logs/seed_node.log 2>&1 &
SEED_PID=$!
echo "   PID: $SEED_PID"
sleep 2

# Start compute node 1
echo "2. Starting COMPUTE NODE 100 (execution only)..."
./node config/worker_100.ini 4 > logs/compute_100.log 2>&1 &
COMPUTE1_PID=$!
echo "   PID: $COMPUTE1_PID"
sleep 2

# Start compute node 2
echo "3. Starting COMPUTE NODE 200 (execution only)..."
./node config/worker_200.ini 4 > logs/compute_200.log 2>&1 &
COMPUTE2_PID=$!
echo "   PID: $COMPUTE2_PID"
sleep 2

# Start hybrid node (if config exists)
if [ -f config/hybrid_node.ini ]; then
    echo "4. Starting HYBRID NODE (ingress + execution)..."
    ./node config/hybrid_node.ini 2 > logs/hybrid_node.log 2>&1 &
    HYBRID_PID=$!
    echo "   PID: $HYBRID_PID"
    sleep 2
fi

echo ""
echo "=========================================="
echo "Cluster Started"
echo "=========================================="
echo "Seed Node:     PID $SEED_PID (ingress on 8081)"
echo "Compute 100:   PID $COMPUTE1_PID"
echo "Compute 200:   PID $COMPUTE2_PID"
if [ ! -z "$HYBRID_PID" ]; then
    echo "Hybrid Node:   PID $HYBRID_PID (ingress on 8082)"
fi
echo ""
echo "Logs: logs/*.log"
echo "=========================================="
echo ""

# Send test message
sleep 3
echo "Sending test message..."
./client 127.0.0.1 8081 "Hello Unified Cluster"

echo ""
echo "Cluster running. Press Ctrl+C to stop."
echo ""

# Wait for interrupt
trap "echo ''; echo 'Stopping cluster...'; kill $SEED_PID $COMPUTE1_PID $COMPUTE2_PID $HYBRID_PID 2>/dev/null; exit 0" INT TERM

wait