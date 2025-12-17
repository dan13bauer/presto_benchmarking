#!/bin/bash

###############################################################################
# Chaos Monkey Script for Presto Worker Disruption Testing
#
# This script runs TPC-H queries while randomly stopping and starting workers
# to test the impact of worker failures on running queries.
#
# Usage: ./chaos_monkey.sh [OPTIONS]
#
# Options:
#   --presto-client PATH        Path to presto client (default: ~/presto_client)
#   --query-list FILE           Query list file (default: ./tpch_all_queries.txt)
#   --coordinator-port PORT     Coordinator port (default: 19001)
#   --schema SCHEMA             Schema name (default: sf100_nvidia)
#   --num-iterations N          Number of chaos iterations (default: 3)
#   --stop-probability PCT      Probability of stopping a worker 0-100 (default: 40)
#   --start-probability PCT     Probability of starting a worker 0-100 (default: 30)
#   --chaos-interval SECS       Seconds between chaos actions (default: 10)
#   --worker-range START END    Range of worker IDs to target (e.g., 4 5, default: all workers)
#   --container-prefix PREFIX   Container name prefix (default: sro_worker)
#   --output-dir DIR            Output directory for logs (default: ./chaos_results)
#   --help                      Show this help message
#
###############################################################################

# Default values
PRESTO_CLIENT="$HOME/presto_client"
QUERY_LIST="./tpch_all_queries.txt"
COORDINATOR_PORT="19001"
SCHEMA="sf100_nvidia"
NUM_ITERATIONS="1"
STOP_PROBABILITY="30"
START_PROBABILITY="60"
CHAOS_INTERVAL="10"
WORKER_RANGE_START=""
WORKER_RANGE_END=""
CONTAINER_PREFIX="sro_worker"
OUTPUT_DIR="./chaos_results/$(date +%Y%m%d_%H%M%S)"
HELP=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --presto-client)
            PRESTO_CLIENT="$2"
            shift 2
            ;;
        --query-list)
            QUERY_LIST="$2"
            shift 2
            ;;
        --coordinator-port)
            COORDINATOR_PORT="$2"
            shift 2
            ;;
        --schema)
            SCHEMA="$2"
            shift 2
            ;;
        --num-iterations)
            NUM_ITERATIONS="$2"
            shift 2
            ;;
        --stop-probability)
            STOP_PROBABILITY="$2"
            shift 2
            ;;
        --start-probability)
            START_PROBABILITY="$2"
            shift 2
            ;;
        --chaos-interval)
            CHAOS_INTERVAL="$2"
            shift 2
            ;;
        --worker-range)
            WORKER_RANGE_START="$2"
            WORKER_RANGE_END="$3"
            shift 3
            ;;
        --container-prefix)
            CONTAINER_PREFIX="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --help)
            HELP=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            HELP=true
            shift
            ;;
    esac
done

# Show help if requested
if [ "$HELP" = true ]; then
    grep "^#" "$0" | grep -v "^#!/" | sed 's/^# //'
    exit 0
fi

# Validate required files
if [ ! -d "$PRESTO_CLIENT" ]; then
    echo "ERROR: Presto client directory not found: $PRESTO_CLIENT"
    exit 1
fi

if [ ! -f "$QUERY_LIST" ]; then
    echo "ERROR: Query list file not found: $QUERY_LIST"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Logging functions
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" 
}

log_event() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] EVENT: $1" 
}

log_query_result() {
    echo "$1" >> "$OUTPUT_DIR/query_results.log"
}

# Get currently running workers
get_running_workers() {
    local workers=$(docker ps --filter "name=${CONTAINER_PREFIX}_" --format "{{.Names}}" | sed "s/${CONTAINER_PREFIX}_//" | sort -n)

    if [ -n "$WORKER_RANGE_START" ] && [ -n "$WORKER_RANGE_END" ]; then
        echo "$workers" | awk -v start="$WORKER_RANGE_START" -v end="$WORKER_RANGE_END" '$0 >= start && $0 <= end'
    else
        echo "$workers"
    fi
}

# Get stopped workers
get_stopped_workers() {
    local workers=$(docker ps -a --filter "name=${CONTAINER_PREFIX}_" --format "{{.Names}}" | while read container; do
        if ! docker ps --format "{{.Names}}" | grep -q "^${container}$"; then
            echo "$container" | sed "s/${CONTAINER_PREFIX}_//"
        fi
    done | sort -n)

    if [ -n "$WORKER_RANGE_START" ] && [ -n "$WORKER_RANGE_END" ]; then
        echo "$workers" | awk -v start="$WORKER_RANGE_START" -v end="$WORKER_RANGE_END" '$0 >= start && $0 <= end'
    else
        echo "$workers"
    fi
}

# Stop a random worker
stop_random_worker() {
    local running_workers=($(get_running_workers))

    if [ ${#running_workers[@]} -eq 0 ]; then
        log_event "No running workers to stop"
        return 1
    fi

    local random_idx=$((RANDOM % ${#running_workers[@]}))
    local worker_id="${running_workers[$random_idx]}"

    log_event "Stopping worker $worker_id"
    docker stop "${CONTAINER_PREFIX}_$worker_id" > /dev/null 2>&1

    if [ $? -eq 0 ]; then
        log_event "Successfully stopped worker $worker_id"
        return 0
    else
        log_event "Failed to stop worker $worker_id"
        return 1
    fi
}

# Start a random stopped worker
start_random_worker() {
    local stopped_workers=($(get_stopped_workers))

    if [ ${#stopped_workers[@]} -eq 0 ]; then
        log_event "No stopped workers to start"
        return 1
    fi

    local random_idx=$((RANDOM % ${#stopped_workers[@]}))
    local worker_id="${stopped_workers[$random_idx]}"

    log_event "Starting worker $worker_id"

    # Remove the stopped container first to avoid naming conflicts
    docker rm "${CONTAINER_PREFIX}_$worker_id" > /dev/null 2>&1

    # Start the worker using start_worker.sh
    bash ./docker/start_worker.sh "$worker_id" "$CONTAINER_PREFIX" > /dev/null 2>&1

    if [ $? -eq 0 ]; then
        log_event "Successfully started worker $worker_id"
        return 0
    else
        log_event "Failed to start worker $worker_id"
        return 1
    fi
}

# Chaos loop - runs in background and randomly stops/starts workers
chaos_loop() {
    local iteration=$1
    local chaos_pid_file="$OUTPUT_DIR/chaos_${iteration}.pid"

    log "Chaos loop $iteration started (PID: $$)"
    echo $$ > "$chaos_pid_file"

    while true; do
        # Check if test_tpch.sh process still exists
        if ! kill -0 "$TEST_TPCH_PID" 2>/dev/null; then
            log_event "Query execution process ended, stopping chaos loop"
            break
        fi

	sleep "$CHAOS_INTERVAL"
	
        # Randomly decide to stop a worker
        if [ $((RANDOM % 100)) -lt $STOP_PROBABILITY ]; then
            stop_random_worker
        fi

        # Randomly decide to start a worker
        if [ $((RANDOM % 100)) -lt $START_PROBABILITY ]; then
            start_random_worker
        fi

        # Log current worker status
        local running=$(get_running_workers | tr '\n' ',' | sed 's/,$//')
        local stopped=$(get_stopped_workers | tr '\n' ',' | sed 's/,$//')
        log_event "Worker status - Running: [$running] | Stopped: [$stopped]"

    done

    rm -f "$chaos_pid_file"
}

# Main execution
main() {
    log "=========================================="
    log "Chaos Monkey Test Starting"
    log "=========================================="
    log "Configuration:"
    log "  Presto Client: $PRESTO_CLIENT"
    log "  Query List: $QUERY_LIST"
    log "  Coordinator Port: $COORDINATOR_PORT"
    log "  Schema: $SCHEMA"
    log "  Iterations: $NUM_ITERATIONS"
    log "  Stop Probability: $STOP_PROBABILITY%"
    log "  Start Probability: $START_PROBABILITY%"
    log "  Chaos Interval: ${CHAOS_INTERVAL}s"
    log "  Container Prefix: $CONTAINER_PREFIX"
    if [ -n "$WORKER_RANGE_START" ] && [ -n "$WORKER_RANGE_END" ]; then
        log "  Worker Range: $WORKER_RANGE_START - $WORKER_RANGE_END"
    else
        log "  Worker Range: All workers"
    fi
    log "  Output Directory: $OUTPUT_DIR"
    log "=========================================="
    
    # Create results header
    log_query_result "ITERATION,QUERY,START_TIME,END_TIME,DURATION_SECS,STATUS,ERROR"

    for iteration in $(seq 1 $NUM_ITERATIONS); do
        log ""
        log "========== ITERATION $iteration / $NUM_ITERATIONS =========="

        # Start all workers

	log "Stopping all dead workers..."
        bash ./docker/stop_all_workers.sh

	
        log "Starting all workers..."
        bash ./docker/start_all_workers.sh
        sleep 5

        # Log initial worker status
        local running=$(get_running_workers | tr '\n' ',' | sed 's/,$//')
        log_event "Initial worker status - Running: [$running]"

        # Create iteration-specific test script to capture timing
        local iteration_test_script="$OUTPUT_DIR/test_tpch_iteration_$iteration.sh"
        cat > "$iteration_test_script" << 'ITERATION_EOF'
#!/bin/bash
PRESTO_CLIENT=$1
QUERY_LIST=$2
COORDINATOR_PORT=$3
SF_SCHEMA=$4
OUTPUT_DIR=$5
ITERATION=$6
QUERY_DIR=./tpch_queries

if [[ "$SF_SCHEMA" == *"_nvidia" ]]; then
    QUERY_DIR=./tpch_queries_nvidia
fi

for query in $(shuf $QUERY_LIST); do
    file=$QUERY_DIR/$query
    if [[ -f "$file" ]]; then
        START_TIME=$(date '+%Y-%m-%d %H:%M:%S')
        START_EPOCH=$(date '+%s%N')

        echo "*** Executing query $file on schema $SF_SCHEMA"

        # Run query and capture output
        QUERY_OUTPUT=$($PRESTO_CLI_DIR/presto --server localhost:$COORDINATOR_PORT --catalog hive --schema $SF_SCHEMA --session single_node_execution_enabled=false -f $file 2>&1)
        QUERY_STATUS=$?

        END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
        END_EPOCH=$(date '+%s%N')

        # Calculate duration in seconds with nanosecond precision
        DURATION_MS=$(( (END_EPOCH - START_EPOCH) / 1000000 ))
        DURATION_SECS=$(echo "scale=3; $DURATION_MS / 1000" | bc)

        # Determine status
        if [ $QUERY_STATUS -eq 0 ]; then
            STATUS="SUCCESS"
            ERROR=""
        else
            STATUS="FAILED"
            ERROR="Exit code: $QUERY_STATUS"
        fi

        # Log result (using OUTPUT_DIR for proper path)
       echo "${ITERATION},${query},${START_TIME},${END_TIME},${DURATION_SECS},${STATUS},${ERROR}" >> "${OUTPUT_DIR}/query_results.log"

        echo "Query $query completed with status $STATUS in ${DURATION_SECS}s"
	echo "Output: $QUERY_OUTPUT"
    fi
    sleep 3
done
ITERATION_EOF
        chmod +x "$iteration_test_script"

        # Run test_tpch in background and capture PID
        log "Starting query execution in background ..."
        PRESTO_CLI_DIR="$PRESTO_CLIENT" \
        OUTPUT_DIR="$OUTPUT_DIR" \
        bash "$iteration_test_script" "$PRESTO_CLIENT" "$QUERY_LIST" "$COORDINATOR_PORT" "$SCHEMA" "$OUTPUT_DIR" "$iteration" > "$OUTPUT_DIR/query_execution_$iteration.log" 2>&1 &
        TEST_TPCH_PID=$!
        log "Query execution PID: $TEST_TPCH_PID"

        # Start chaos loop in background
        log "Starting chaos loop in background..."
        chaos_loop "$iteration" &
        CHAOS_PID=$!
        log "Chaos loop PID: $CHAOS_PID"

        # Wait for query execution to complete
        log "Waiting for query execution to complete..."
        wait $TEST_TPCH_PID
        QUERY_STATUS=$?
        log "Query execution completed with status: $QUERY_STATUS"

        # Wait a bit for chaos loop to exit
        sleep 2

        # Kill chaos loop if still running
        if kill -0 $CHAOS_PID 2>/dev/null; then
            kill $CHAOS_PID 2>/dev/null
            wait $CHAOS_PID 2>/dev/null
        fi

        log_event "Iteration $iteration completed"

    done

    log ""
    log "=========================================="
    log "Chaos Monkey Test Completed"
    log "=========================================="
    log "Results saved to: $OUTPUT_DIR"
    log "  - chaos_monkey.log: Main log file"
    log "  - chaos_events.log: Worker start/stop events"
    log "  - query_results.log: Query execution results (CSV format)"
    log "  - query_execution_N.log: Per-iteration query output"
    log "  - worker_control_N.log: Worker start/stop output"
    log "=========================================="

    # Print summary
    echo ""
    echo "Query Results Summary:"
    if [ -f "$OUTPUT_DIR/query_results.log" ]; then
        tail -n +2 "$OUTPUT_DIR/query_results.log" | awk -F',' '{
            if ($6 == "SUCCESS") success++; else failed++;
            total_duration += $5;
        }
        END {
            print "  Total queries: " success + failed;
            print "  Successful: " success;
            print "  Failed: " failed;
            printf "  Total duration: %.2f seconds\n", total_duration;
        }'
    fi
}

# Run main
main
