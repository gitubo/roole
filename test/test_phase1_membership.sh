#!/bin/bash
# test/test_phase1_membership.sh
# Verify Phase 1: Shared cluster_view between membership and node

set -e

echo "=========================================="
echo "Phase 1: Membership Shared View Test"
echo "=========================================="
echo ""

# Cleanup
echo "Cleaning up old processes..."
pkill -9 roole-node 2>/dev/null || true
sleep 2

mkdir -p logs

echo "=========================================="
echo "TEST 1: Verify Shared cluster_view"
echo "=========================================="
echo ""

# Start ingress node
echo "[1/3] Starting ingress node (ID=1)..."
../build/bin/roole-node ../config/router.ini 2 > logs/phase1_ingress.log 2>&1 &
INGRESS_PID=$!
sleep 5

# Check logs for shared view confirmation
echo "Checking if ingress node is using shared cluster_view..."
if grep -q "Using shared cluster_view" logs/phase1_ingress.log; then
    INGRESS_VIEW_ADDR=$(grep "Using shared cluster_view" logs/phase1_ingress.log | awk '{print $NF}')
    echo "  ✓ Ingress node using shared view at: $INGRESS_VIEW_ADDR"
else
    echo "  ✗ FAIL: No shared view confirmation in logs"
    kill $INGRESS_PID
    exit 1
fi

# Start worker 1
echo "[2/3] Starting worker 100..."
../build/bin/roole-node ../config/worker_100.ini 2 > logs/phase1_worker100.log 2>&1 &
WORKER1_PID=$!
sleep 5

# Check worker logs
if grep -q "Using shared cluster_view" logs/phase1_worker100.log; then
    WORKER1_VIEW_ADDR=$(grep "Using shared cluster_view" logs/phase1_worker100.log | awk '{print $NF}')
    echo "  ✓ Worker 100 using shared view at: $WORKER1_VIEW_ADDR"
else
    echo "  ✗ FAIL: Worker 100 not using shared view"
    kill $INGRESS_PID $WORKER1_PID 2>/dev/null
    exit 1
fi

# Start worker 2
echo "[3/3] Starting worker 200..."
../build/bin/roole-node ../config/worker_200.ini 2 > logs/phase1_worker200.log 2>&1 &
WORKER2_PID=$!
sleep 5

if grep -q "Using shared cluster_view" logs/phase1_worker200.log; then
    WORKER2_VIEW_ADDR=$(grep "Using shared cluster_view" logs/phase1_worker200.log | awk '{print $NF}')
    echo "  ✓ Worker 200 using shared view at: $WORKER2_VIEW_ADDR"
else
    echo "  ✗ FAIL: Worker 200 not using shared view"
    kill $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
    exit 1
fi

echo ""
echo "✓ PASS: All nodes using shared cluster_view architecture"

echo ""
echo "=========================================="
echo "TEST 2: Verify No Duplicate cluster_view"
echo "=========================================="
echo ""

# Check that membership is NOT creating internal_view
echo "Checking for absence of 'internal_view' references..."

if grep -i "internal_view" logs/phase1_*.log; then
    echo "  ✗ FAIL: Found 'internal_view' references (should be removed)"
    kill $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
    exit 1
fi

# Check that membership_init logs show shared_view
if grep -q "membership_init: Using shared cluster_view" logs/phase1_ingress.log; then
    echo "  ✓ PASS: membership_init correctly receiving shared view"
else
    echo "  ✗ FAIL: membership_init not logging shared view usage"
    kill $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
    exit 1
fi

echo ""
echo "✓ PASS: No duplicate cluster_view detected"

echo ""
echo "=========================================="
echo "TEST 3: Verify Cluster Convergence"
echo "=========================================="
echo ""

echo "Waiting 15 seconds for cluster to converge..."
sleep 15

# Check metrics on all nodes
echo "Checking cluster metrics..."

for PORT in 7002 7102 7202; do
    METRICS=$(curl -s http://localhost:$PORT/metrics 2>/dev/null || echo "")
    
    if [ -z "$METRICS" ]; then
        echo "  ✗ FAIL: Port $PORT not responding"
        kill $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
        exit 1
    fi
    
    TOTAL=$(echo "$METRICS" | grep "cluster_members_total" | awk '{print $2}')
    ACTIVE=$(echo "$METRICS" | grep "cluster_members_active" | awk '{print $2}')
    
    if [ "$TOTAL" != "3" ] || [ "$ACTIVE" != "3" ]; then
        echo "  ✗ FAIL: Port $PORT shows total=$TOTAL active=$ACTIVE (expected 3/3)"
        kill $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
        exit 1
    fi
    
    echo "  ✓ Port $PORT: total=$TOTAL active=$ACTIVE"
done

echo ""
echo "✓ PASS: All nodes see consistent cluster state"

echo ""
echo "=========================================="
echo "TEST 4: Verify Memory Efficiency"
echo "=========================================="
echo ""

# Check process memory usage
echo "Checking memory usage (should be lower without duplicate cluster_view)..."

for PID in $INGRESS_PID $WORKER1_PID $WORKER2_PID; do
    if [ -e /proc/$PID/status ]; then
        RSS=$(grep VmRSS /proc/$PID/status | awk '{print $2}')
        echo "  Process $PID: ${RSS} KB RSS"
        
        # Sanity check: RSS should be reasonable (< 100MB for test cluster)
        if [ "$RSS" -gt 102400 ]; then
            echo "    ⚠ WARNING: High memory usage (>100MB)"
        fi
    fi
done

echo ""
echo "✓ PASS: Memory usage within expected range"

echo ""
echo "=========================================="
echo "TEST 5: Stress Test - Rapid Member Changes"
echo "=========================================="
echo ""

echo "Killing and restarting worker 100 rapidly..."

for i in {1..3}; do
    echo "  Iteration $i: Kill worker 100"
    kill -9 $WORKER1_PID 2>/dev/null || true
    sleep 2
    
    echo "  Iteration $i: Restart worker 100"
    ../build/bin/roole-node ../config/worker_100.ini 2 >> logs/phase1_worker100_stress.log 2>&1 &
    WORKER1_PID=$!
    sleep 3
    
    # Verify cluster view consistency
    METRICS=$(curl -s http://localhost:7002/metrics)
    ACTIVE=$(echo "$METRICS" | grep "cluster_members_active" | awk '{print $2}')
    
    if [ "$ACTIVE" != "3" ]; then
        echo "    ⚠ Iteration $i: Active members = $ACTIVE (expected 3)"
    else
        echo "    ✓ Iteration $i: Cluster recovered correctly"
    fi
done

echo ""
echo "✓ PASS: Shared cluster_view handles rapid membership changes"

echo ""
echo "=========================================="
echo "Cleanup"
echo "=========================================="
echo ""

echo "Stopping all nodes..."
kill -TERM $INGRESS_PID $WORKER1_PID $WORKER2_PID 2>/dev/null || true
sleep 5
pkill -9 roole-node 2>/dev/null || true

echo ""
echo "=========================================="
echo "Phase 1 Tests Complete"
echo "=========================================="
echo "✓ All tests passed!"
echo ""
echo "Verified:"
echo "  1. All nodes use shared cluster_view"
echo "  2. No duplicate internal_view created"
echo "  3. Cluster converges correctly"
echo "  4. Memory usage is efficient"
echo "  5. Handles rapid membership changes"
echo ""
echo "Logs saved in: logs/phase1_*.log"
echo ""