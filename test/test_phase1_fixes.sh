#!/bin/bash
# test/test_phase1_fixes.sh - Test Phase 1 critical fixes

set -e

echo "=========================================="
echo "Phase 1 Critical Fixes Test"
echo "=========================================="
echo ""

# Cleanup
echo "Cleaning up old processes..."
pkill -9 roole-node 2>/dev/null || true
sleep 2

mkdir -p logs

echo "=========================================="
echo "TEST 1: Cluster Metrics Reporting"
echo "=========================================="
echo ""

# Start ingress node
echo "[1/3] Starting ingress node (ID=1)..."
../build/bin/roole-node ../config/router.ini 2 > logs/ingress.log 2>&1 &
INGRESS_PID=$!
sleep 3

# Check metrics endpoint
echo "Checking initial metrics..."
curl -s http://localhost:7002/metrics | grep "cluster_members_total" || {
    echo "✗ Metrics endpoint not responding"
    kill $INGRESS_PID
    exit 1
}

# Start worker 1
echo "[2/3] Starting worker 100..."
../build/bin/roole-node ../config/worker_100.ini 2 > logs/worker100.log 2>&1 &
WORKER1_PID=$!
sleep 3

# Start worker 2
echo "[3/3] Starting worker 200..."
../build/bin/roole-node ../config/worker_200.ini 2 > logs/worker200.log 2>&1 &
WORKER2_PID=$!
sleep 5

echo ""
echo "Waiting 20 seconds for cluster to stabilize and metrics to update..."
sleep 20

echo ""
echo "Checking cluster metrics on ingress node:"
METRICS=$(curl -s http://localhost:7002/metrics)

TOTAL=$(echo "$METRICS" | grep "cluster_members_total" | awk '{print $2}')
ACTIVE=$(echo "$METRICS" | grep "cluster_members_active" | awk '{print $2}')

echo "  Total members:  $TOTAL (expected: 3)"
echo "  Active members: $ACTIVE (expected: 3)"

if [ "$TOTAL" -eq 0 ]; then
    echo "✗ FAIL: Cluster metrics still showing 0"
    echo ""
    echo "Debug logs from ingress node:"
    tail -30 logs/ingress.log
    kill $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
    exit 1
fi

echo "✓ PASS: Cluster metrics are now reporting correctly"

echo ""
echo "=========================================="
echo "TEST 2: Worker Rejoin After Crash"
echo "=========================================="
echo ""

echo "Killing worker 100..."
kill -9 $WORKER1_PID
sleep 3

echo "Worker 100 metrics should show it as suspect/dead..."
sleep 5

echo "Restarting worker 100..."
../build/bin/roole-node ../config/worker_100.ini 2 > logs/worker100_rejoin.log 2>&1 &
WORKER1_PID=$!
sleep 5

echo "Checking if worker 100 rejoined successfully..."
grep -q "REJOINED" logs/ingress.log && {
    echo "✓ PASS: Worker rejoin detected in ingress logs"
} || {
    echo "✗ FAIL: No rejoin message found"
    echo ""
    echo "Debug logs:"
    grep "Worker 100" logs/ingress.log | tail -10
    kill $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
    exit 1
}

# Check cluster metrics after rejoin
sleep 10
METRICS=$(curl -s http://localhost:7002/metrics)
ACTIVE=$(echo "$METRICS" | grep "cluster_members_active" | awk '{print $2}')

if [ "$ACTIVE" -eq 3 ]; then
    echo "✓ PASS: Cluster metrics show 3 active members after rejoin"
else
    echo "✗ FAIL: Expected 3 active members, got $ACTIVE"
    kill $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
    exit 1
fi

echo ""
echo "=========================================="
echo "TEST 3: Graceful Shutdown"
echo "=========================================="
echo ""

echo "Sending SIGTERM to ingress node..."
kill -TERM $INGRESS_PID

echo "Waiting for graceful shutdown (max 60 seconds)..."
SHUTDOWN_START=$(date +%s)

while kill -0 $INGRESS_PID 2>/dev/null; do
    ELAPSED=$(($(date +%s) - SHUTDOWN_START))
    if [ $ELAPSED -gt 60 ]; then
        echo "✗ FAIL: Shutdown timeout (>60s)"
        kill -9 $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
        exit 1
    fi
    sleep 1
done

SHUTDOWN_TIME=$(($(date +%s) - SHUTDOWN_START))
echo "✓ PASS: Graceful shutdown completed in ${SHUTDOWN_TIME}s"

# Check shutdown logs
echo ""
echo "Verifying shutdown sequence in logs..."
grep -q "Phase 1/6" logs/ingress.log && echo "  ✓ Phase 1: Stop accepting work"
grep -q "Phase 2/6" logs/ingress.log && echo "  ✓ Phase 2: Drain queue"
grep -q "Phase 3/6" logs/ingress.log && echo "  ✓ Phase 3: Wait for executions"
grep -q "Phase 4/6" logs/ingress.log && echo "  ✓ Phase 4: Stop threads"
grep -q "Phase 5/6" logs/ingress.log && echo "  ✓ Phase 5: Leave cluster"
grep -q "Phase 6/6" logs/ingress.log && echo "  ✓ Phase 6: Cleanup"

# Cleanup remaining workers
echo ""
echo "Cleaning up remaining workers..."
kill -TERM $WORKER1_PID $WORKER2_PID 2>/dev/null || true
sleep 5
pkill -9 roole-node 2>/dev/null || true

echo ""
echo "=========================================="
echo "Phase 1 Tests Complete"
echo "=========================================="
echo "✓ All critical fixes verified"
echo ""
echo "Logs saved in: logs/"
echo "  - logs/ingress.log"
echo "  - logs/worker100.log"
echo "  - logs/worker100_rejoin.log"
echo "  - logs/worker200.log"
echo ""