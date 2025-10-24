#!/bin/bash

# Kill any existing processes
pkill -9 router worker

echo "=========================================="
echo "Testing SWIM Gossip Protocol"
echo "=========================================="

# Start router (node 1)
echo "Starting Router (node 1)..."
./router 1 6000 6001 6002 &
ROUTER_PID=$!
sleep 2

# Start worker 1 (node 100)
echo "Starting Worker 100..."
./worker 100 5000 5001 1 127.0.0.1 6000 6001 9090 &
WORKER1_PID=$!
sleep 2

# Start worker 2 (node 101)
echo "Starting Worker 101..."
./worker 101 5010 5011 1 127.0.0.1 6000 6001 9091 &
WORKER2_PID=$!
sleep 2

echo ""
echo "=========================================="
echo "Cluster Started"
echo "  Router: PID $ROUTER_PID (gossip on 7000)"
echo "  Worker 100: PID $WORKER1_PID (gossip on 6000)"
echo "  Worker 101: PID $WORKER2_PID (gossip on 6010)"
echo "=========================================="
echo ""
echo "Watching gossip traffic for 30 seconds..."
echo "Look for: PING/ACK messages, cluster state updates"
echo ""

sleep 30

echo ""
echo "=========================================="
echo "Testing Failure Detection"
echo "=========================================="
echo "Killing Worker 100 to test failure detection..."

kill -9 $WORKER1_PID

echo "Worker 100 killed. Watching for failure detection (15 seconds)..."
echo "Expected: Router detects failure via gossip timeout"
echo ""

sleep 15

echo ""
echo "=========================================="
echo "Sending test message to surviving worker..."
./client 127.0.0.1 6002 "Test message after failure"

echo ""
echo "Test complete. Press Ctrl+C to stop remaining processes."
echo ""

# Wait for user interrupt
wait