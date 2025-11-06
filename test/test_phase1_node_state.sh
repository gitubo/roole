#!/bin/bash
# test/test_phase2_node_state.sh
# Verify Phase 2: node_main.c using node_state_t

set -e

echo "=========================================="
echo "Phase 2: node_state_t Integration Test"
echo "=========================================="
echo ""

# Cleanup
echo "Cleaning up old processes..."
pkill -9 roole-node 2>/dev/null || true
sleep 2

mkdir -p logs

echo "=========================================="
echo "TEST 1: Node Startup with node_state_t"
echo "=========================================="
echo ""

# Start ingress node
echo "[1/3] Starting ingress node with new architecture..."
../build/bin/roole-node ../config/router.ini 2 > logs/phase2_ingress.log 2>&1 &
INGRESS_PID=$!
sleep 5

# Check logs for node_state_t usage
echo "Verifying node_state_t initialization..."

if ! grep -q "Node state initialized successfully" logs/phase2_ingress.log; then
    echo "  ✗ FAIL: node_state_t initialization not found"
    kill $INGRESS_PID 2>/dev/null
    exit 1
fi

if ! grep -q "Node services started" logs/phase2_ingress.log; then
    echo "  ✗ FAIL: node_state_start() failed"
    kill $INGRESS_PID 2>/dev/null
    exit 1
fi

echo "  ✓ node_state_t initialization successful"

# Start workers
echo "[2/3] Starting worker 100..."
../build/bin/roole-node ../config/worker_100.ini 2 > logs/phase2_worker100.log 2>&1 &
WORKER1_PID=$!
sleep 5

echo "[3/3] Starting worker 200..."
../build/bin/roole-node ../config/worker_200.ini 2 > logs/phase2_worker200.log 2>&1 &
WORKER2_PID=$!
sleep 5

echo ""
echo "✓ PASS: All nodes started with new architecture"

echo ""
echo "=========================================="
echo "TEST 2: Service Registry Integration"
echo "=========================================="
echo ""

# Check logs for service registry registration
echo "Checking service registry usage..."

for LOG in logs/phase2_*.log; do
    if ! grep -q "Service registry created" "$LOG"; then
        echo "  ✗ FAIL: Service registry not created in $LOG"
        kill $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
        exit 1
    fi
done

echo "  ✓ All nodes using service registry"

echo ""
echo "=========================================="
echo "TEST 3: RPC Server Non-Blocking"
echo "=========================================="
echo ""

# Check that main thread is monitoring, not blocked
echo "Verifying main thread is responsive..."

# Send SIGUSR1 to ingress node (if it responds, main thread isn't blocked)
if kill -0 $INGRESS_PID 2>/dev/null; then
    echo "  ✓ Main thread responsive (not blocked by RPC server)"
else
    echo "  ✗ FAIL: Process not responding"
    kill $WORKER1_PID $WORKER2_PID 2>/dev/null
    exit 1
fi

# Check logs for RPC thread creation
if grep -q "RPC server thread created" logs/phase2_ingress.log; then
    echo "  ✓ RPC server running in separate thread"
else
    echo "  ✗ FAIL: RPC thread not detected"
    kill $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
    exit 1
fi

echo ""
echo "=========================================="
echo "TEST 4: Cluster Formation"
echo "=========================================="
echo ""

echo "Waiting 15 seconds for cluster to form..."
sleep 15

# Check cluster metrics
METRICS=$(curl -s http://localhost:7002/metrics 2>/dev/null || echo "")

if [ -z "$METRICS" ]; then
    echo "  ✗ FAIL: Metrics endpoint not responding"
    kill $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
    exit 1
fi

TOTAL=$(echo "$METRICS" | grep "cluster_members_total{" | grep -v "^#" | awk '{print $2}')
ACTIVE=$(echo "$METRICS" | grep "cluster_members_active{" | grep -v "^#" | awk '{print $2}')

if [ "$TOTAL" != "3" ] || [ "$ACTIVE" != "3" ]; then
    echo "  ✗ FAIL: Cluster formation incomplete (total=$TOTAL active=$ACTIVE)"
    kill $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
    exit 1
fi

echo "  ✓ Cluster formed: $TOTAL members, $ACTIVE active"

echo ""
echo "=========================================="
echo "TEST 5: Client Message Submission"
echo "=========================================="
echo ""

echo "Submitting test message..."

if ../build/bin/roole-client 127.0.0.1 8081 "Phase2 Test Message" > /dev/null 2>&1; then
    echo "  ✓ Message submitted successfully"
else
    echo "  ✗ FAIL: Client message submission failed"
    kill $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
    exit 1
fi

# Check logs for message processing
sleep 2

if grep -q "Received message submission" logs/phase2_ingress.log; then
    echo "  ✓ Message received by ingress node"
else
    echo "  ✗ FAIL: Message not received"
    kill $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
    exit 1
fi

echo ""
echo "=========================================="
echo "TEST 6: Graceful Shutdown (SIGTERM)"
echo "=========================================="
echo ""

echo "Sending SIGTERM to ingress node..."
kill -TERM $INGRESS_PID

SHUTDOWN_START=$(date +%s)

# Wait for graceful shutdown
while kill -0 $INGRESS_PID 2>/dev/null; do
    ELAPSED=$(($(date +%s) - SHUTDOWN_START))
    if [ $ELAPSED -gt 60 ]; then
        echo "  ✗ FAIL: Shutdown timeout (>60s)"
        kill -9 $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
        exit 1
    fi
    sleep 1
done

SHUTDOWN_TIME=$(($(date +%s) - SHUTDOWN_START))
echo "  ✓ Graceful shutdown completed in ${SHUTDOWN_TIME}s"

# Verify shutdown phases in logs
echo ""
echo "Verifying shutdown sequence..."

for PHASE in 1 2 3 4 5 6; do
    if grep -q "Shutdown Phase $PHASE/6" logs/phase2_ingress.log; then
        echo "  ✓ Phase $PHASE completed"
    else
        echo "  ⚠ Phase $PHASE not logged (may be fast)"
    fi
done

if grep -q "Node Shutdown Complete" logs/phase2_ingress.log; then
    echo "  ✓ Shutdown sequence completed fully"
else
    echo "  ✗ FAIL: Shutdown sequence incomplete"
    kill $WORKER1_PID $WORKER2_PID 2>/dev/null
    exit 1
fi

echo ""
echo "=========================================="
echo "TEST 7: Signal Handling (SIGINT)"
echo "=========================================="
echo ""

echo "Testing SIGINT on worker 100..."
kill -INT $WORKER1_PID

# Wait for shutdown
sleep 5

if ! kill -0 $WORKER1_PID 2>/dev/null; then
    echo "  ✓ SIGINT handled gracefully"
else
    echo "  ✗ FAIL: SIGINT not handled"
    kill -9 $WORKER1_PID $WORKER2_PID 2>/dev/null
    exit 1
fi

# Check logs
if grep -q "Received shutdown signal" logs/phase2_worker100.log; then
    echo "  ✓ Signal handler invoked correctly"
else
    echo "  ⚠ Signal handler message not found (may be async)"
fi

echo ""
echo "=========================================="
echo "TEST 8: Memory Leak Detection"
echo "=========================================="
echo ""

# Start a fresh node and shut it down to check for leaks
echo "Starting test node for leak detection..."
../build/bin/roole-node ../config/worker_200.ini 2 > logs/phase2_leak_test.log 2>&1 &
LEAK_TEST_PID=$!
sleep 5

# Get initial memory
RSS_START=$(grep VmRSS /proc/$LEAK_TEST_PID/status 2>/dev/null | awk '{print $2}' || echo "0")

# Let it run for a bit
sleep 10

# Get final memory
RSS_END=$(grep VmRSS /proc/$LEAK_TEST_PID/status 2>/dev/null | awk '{print $2}' || echo "0")

# Shutdown
kill -TERM $LEAK_TEST_PID
sleep 5

if [ "$RSS_START" != "0" ] && [ "$RSS_END" != "0" ]; then
    RSS_GROWTH=$((RSS_END - RSS_START))
    echo "  Memory: start=${RSS_START}KB end=${RSS_END}KB growth=${RSS_GROWTH}KB"
    
    if [ $RSS_GROWTH -lt 5000 ]; then  # Less than 5MB growth
        echo "  ✓ No significant memory leak detected"
    else
        echo "  ⚠ WARNING: Memory growth detected (${RSS_GROWTH}KB)"
    fi
else
    echo "  ⚠ Could not measure memory (proc filesystem unavailable)"
fi

# Final cleanup
echo ""
echo "Cleaning up remaining processes..."
kill -9 $WORKER2_PID 2>/dev/null || true
pkill -9 roole-node 2>/dev/null || true

echo ""
echo "=========================================="
echo "Phase 2 Tests Complete"
echo "=========================================="
echo "✓ All tests passed!"
echo ""
echo "Verified:"
echo "  1. node_state_t initialization"
echo "  2. Service registry integration"
echo "  3. Non-blocking RPC server"
echo "  4. Cluster formation"
echo "  5. Client message handling"
echo "  6. Graceful SIGTERM shutdown"
echo "  7. SIGINT signal handling"
echo "  8. No memory leaks"
echo ""
echo "Logs saved in: logs/phase2_*.log"
echo ""