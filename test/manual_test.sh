#!/bin/bash
# test/manual_test.sh - Manual testing helper

set -e

echo "=========================================="
echo "Roole Manual Test Helper"
echo "=========================================="
echo ""
echo "This script will:"
echo "  1. Start router node (node 1)"
echo "  2. Start worker node (node 100)"
echo "  3. Wait for cluster formation"
echo "  4. Register test DAG"
echo "  5. Send test message"
echo ""
echo "Press Ctrl+C to stop all nodes"
echo "=========================================="
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo "Shutting down nodes..."
    pkill -f "roole-node.*router.ini" 2>/dev/null || true
    pkill -f "roole-node.*worker_100.ini" 2>/dev/null || true
    sleep 1
    echo "Done"
}

trap cleanup EXIT INT TERM

# Start router
echo "[1/5] Starting router node..."
./build/bin/roole-node config/router.ini 2 > logs/manual_router.log 2>&1 &
ROUTER_PID=$!
sleep 3

# Check if router started
if ! kill -0 $ROUTER_PID 2>/dev/null; then
    echo "ERROR: Router failed to start"
    cat logs/manual_router.log
    exit 1
fi
echo "  ✓ Router started (PID: $ROUTER_PID)"

# Start worker
echo "[2/5] Starting worker node..."
./build/bin/roole-node config/worker_100.ini 1 > logs/manual_worker.log 2>&1 &
WORKER_PID=$!
sleep 3

# Check if worker started
if ! kill -0 $WORKER_PID 2>/dev/null; then
    echo "ERROR: Worker failed to start"
    cat logs/manual_worker.log
    exit 1
fi
echo "  ✓ Worker started (PID: $WORKER_PID)"

# Wait for cluster formation
echo "[3/5] Waiting for cluster formation..."
sleep 5

# Check cluster state
METRICS=$(curl -s http://localhost:7002/metrics)
ACTIVE=$(echo "$METRICS" | grep "cluster_members_active{" | grep -v "^#" | awk '{print $2}')

if [ "$ACTIVE" = "2" ]; then
    echo "  ✓ Cluster formed: 2 active members"
else
    echo "  ✗ Cluster formation incomplete (active: $ACTIVE)"
    echo ""
    echo "Router log:"
    tail -20 logs/manual_router.log
    echo ""
    echo "Worker log:"
    tail -20 logs/manual_worker.log
    exit 1
fi

# Register DAG
echo "[4/5] Registering test DAG..."
./build/bin/roole-dag-register 127.0.0.1 8081 1 "test_dag" 3
if [ $? -eq 0 ]; then
    echo "  ✓ DAG registered"
else
    echo "  ✗ DAG registration failed"
    exit 1
fi

# Send test message
echo "[5/5] Sending test message..."
./build/bin/roole-client 127.0.0.1 8081 "Hello from manual test!"
if [ $? -eq 0 ]; then
    echo "  ✓ Message sent"
else
    echo "  ✗ Message send failed"
    exit 1
fi

echo ""
echo "=========================================="
echo "Manual Test Complete!"
echo "=========================================="
echo ""
echo "Nodes are still running. You can:"
echo "  - Send more messages: ./build/bin/roole-client 127.0.0.1 8081 \"your message\""
echo "  - Check metrics: curl http://localhost:7002/metrics"
echo "  - View router log: tail -f logs/manual_router.log"
echo "  - View worker log: tail -f logs/manual_worker.log"
echo ""
echo "Press Ctrl+C to stop all nodes"
echo ""

# Keep running
while true; do
    sleep 1
done