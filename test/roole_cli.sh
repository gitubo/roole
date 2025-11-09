#!/bin/bash
# roole-interactive-cli.sh - Interactive Menu for Roole Manual Testing

# --- Configuration ---
ROUTER_CONFIG="./config/router.ini"
WORKER_CONFIG="./config/worker_100.ini"
ROUTER_LOG="./logs/manual_router.log"
WORKER_LOG="./logs/manual_worker.log"
METRICS_PORT="7002"
CLIENT_HOST="127.0.0.1"
CLIENT_PORT="8081"
TEST_DAG_ID="1"
TEST_DAG_NAME="test_dag"
TEST_DAG_NODES="3"

# --- Helper Functions ---

# Function to safely kill nodes
cleanup() {
    echo ""
    echo "--- Shutting down nodes ---"
    # Use pkill with specific names to ensure only roole nodes are targeted
    pkill -f "roole-node.*router.ini" 2>/dev/null || true
    pkill -f "roole-node.*worker_100.ini" 2>/dev/null || true
    sleep 1
    echo "Done."
}

# Function to check if a process is running
check_process() {
    local config_file=$1
    if pgrep -f "roole-node.*$config_file" > /dev/null; then
        return 0 # Running
    else
        return 1 # Not running
    fi
}

# --- Command Functions ---

# 🚀 Start the router node in the background
start_router() {
    if check_process $ROUTER_CONFIG; then
        echo "Router is already running."
        return
    fi
    echo "🚀 Starting Router Node..."
    ./build/bin/roole-node $ROUTER_CONFIG 2 > $ROUTER_LOG 2>&1 &
    sleep 3
    if check_process $ROUTER_CONFIG; then
        echo "  ✓ Router started (PID: $(pgrep -f "roole-node.*$ROUTER_CONFIG"))"
    else
        echo "  ✗ ERROR: Router failed to start. Check $ROUTER_LOG"
    fi
}

# 🏭 Start the worker node in the background
start_worker() {
    if check_process $WORKER_CONFIG; then
        echo "Worker is already running."
        return
    fi
    echo "🏭 Starting Worker Node..."
    ./build/bin/roole-node $WORKER_CONFIG 1 > $WORKER_LOG 2>&1 &
    sleep 3
    if check_process $WORKER_CONFIG; then
        echo "  ✓ Worker started (PID: $(pgrep -f "roole-node.*$WORKER_CONFIG"))"
    else
        echo "  ✗ ERROR: Worker failed to start. Check $WORKER_LOG"
    fi
}

# 🔗 Wait for cluster formation and check state (synchronous)
check_cluster() {
    echo "🔗 Waiting for cluster formation (5s)..."
    sleep 5
    
    METRICS=$(curl -s http://localhost:$METRICS_PORT/metrics)
    ACTIVE=$(echo "$METRICS" | grep "cluster_members_active{" | grep -v "^#" | awk '{print $2}')

    if [ "$ACTIVE" = "2" ]; then
        echo "  ✓ Cluster formed: $ACTIVE active members."
    else
        echo "  ✗ Cluster formation incomplete (active: $ACTIVE). Check logs."
        return 1
    fi
}

# 📝 Register the test DAG (synchronous)
register_dag() {
    if ! check_process $ROUTER_CONFIG; then
        echo "✗ Cannot register DAG: Router is not running."
        return 1
    fi
    echo "📝 Registering Test DAG ($TEST_DAG_NAME)..."
    ./build/bin/roole-dag-register $CLIENT_HOST $CLIENT_PORT $TEST_DAG_ID $TEST_DAG_NAME $TEST_DAG_NODES
    
    if [ $? -eq 0 ]; then
        echo "  ✓ DAG registered successfully."
    else
        echo "  ✗ DAG registration failed."
        return 1
    fi
}

# ✉️ Send a test message (synchronous)
send_message() {
    if ! check_process $ROUTER_CONFIG; then
        echo "✗ Cannot send message: Router is not running."
        return 1
    fi

    read -p "Enter message to send (default: 'Hello from manual test!'): " message
    local message="${message:-Hello from manual test!}"
    
    echo "✉️ Sending message: \"$message\""
    ./build/bin/roole-client $CLIENT_HOST $CLIENT_PORT "$message"
    
    if [ $? -eq 0 ]; then
        echo "  ✓ Message sent successfully."
    else
        echo "  ✗ Message send failed."
        return 1
    fi
}

# 🔍 Display current status
show_status() {
    echo "--- Current Roole Status ---"
    if check_process $ROUTER_CONFIG; then
        echo "  🚀 Router: RUNNING (PID: $(pgrep -f "roole-node.*$ROUTER_CONFIG"))"
    else
        echo "  🚀 Router: STOPPED"
    fi

    if check_process $WORKER_CONFIG; then
        echo "  🏭 Worker: RUNNING (PID: $(pgrep -f "roole-node.*$WORKER_CONFIG"))"
    else
        echo "  🏭 Worker: STOPPED"
    fi
    
    echo ""
    # Check cluster status without the 5s sleep
    check_cluster || true
}

# --- Menu Logic ---

# Array of menu options
MENU_OPTIONS=(
    "Start Router"
    "Start Worker"
    "Start All & Check Cluster"
    "Register DAG"
    "Send Message"
    "Status Check"
    "Stop All Nodes"
    "Exit"
)

# Function to display the menu and handle user selection
menu_loop() {
    PS3='Select action: '
    
    select opt in "${MENU_OPTIONS[@]}"
    do
        case $opt in
            "Start Router")
                start_router
                ;;
            "Start Worker")
                start_worker
                ;;
            "Start All & Check Cluster")
                start_router
                start_worker
                check_cluster
                ;;
            "Register DAG")
                register_dag
                ;;
            "Send Message")
                send_message
                ;;
            "Status Check")
                show_status
                ;;
            "Stop All Nodes")
                cleanup
                ;;
            "Exit")
                echo "Exiting Roole CLI. Goodbye!"
                cleanup
                break
                ;;
            *)
                echo "Invalid option $REPLY"
                ;;
        esac
        echo "" # Add a blank line after action completion
    done
}

# --- Execution ---

# Ensure cleanup runs on script interrupt
trap cleanup EXIT INT TERM

echo "=========================================="
echo "      Roole Interactive Testing CLI       "
echo "=========================================="
echo ""

# Start the interactive loop
menu_loop