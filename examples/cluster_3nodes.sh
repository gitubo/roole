#!/bin/bash
# examples/cluster_3nodes.sh
# Start a local 3-node cluster (1 router + 2 workers)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if binaries exist
if [ ! -f "../build/router" ] || [ ! -f "../build/worker" ]; then
    echo -e "${RED}Error: Binaries not found. Please run 'cmake .. && make' first.${NC}"
    exit 1
fi

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Shutting down cluster...${NC}"
    pkill -P $$ || true
    wait
    echo -e "${GREEN}Cluster stopped.${NC}"
}

trap cleanup EXIT INT TERM

# Start router
echo -e "${GREEN}Starting Router (ID=1, Port=5000)${NC}"
../build/router 1 5000 > logs/router_1.log 2>&1 &
ROUTER_PID=$!
echo "Router PID: $ROUTER_PID"

# Wait for router to initialize
sleep 2

# Start worker 1
echo -e "${GREEN}Starting Worker 1 (ID=100, Port=6000, 4 threads)${NC}"
../build/worker 100 6000 4 > logs/worker_100.log 2>&1 &
WORKER1_PID=$!
echo "Worker 1 PID: $WORKER1_PID"

# Start worker 2
echo -e "${GREEN}Starting Worker 2 (ID=101, Port=6001, 4 threads)${NC}"
../build/worker 101 6001 4 > logs/worker_101.log 2>&1 &
WORKER2_PID=$!
echo "Worker 2 PID: $WORKER2_PID"

echo -e "\n${GREEN}Cluster started successfully!${NC}"
echo -e "${YELLOW}==============================================\n"
echo -e "Router:   PID $ROUTER_PID (Port 5000)"
echo -e "Worker 1: PID $WORKER1_PID (Port 6000)"
echo -e "Worker 2: PID $WORKER2_PID (Port 6001)"
echo -e "\n==============================================\n${NC}"
echo -e "Logs are in logs/ directory"
echo -e "Press Ctrl+C to stop the cluster\n"

# Tail logs in real-time (optional)
if command -v multitail &> /dev/null; then
    echo -e "${YELLOW}Streaming logs with multitail...${NC}"
    multitail logs/router_1.log logs/worker_100.log logs/worker_101.log
else
    echo -e "${YELLOW}Install 'multitail' to view logs in real-time${NC}"
    echo -e "Waiting for shutdown signal...\n"
    wait
fi