#!/bin/bash

# Presto Worker Configuration Generator
# Version: 1.0.0

VERSION="1.0.0"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_DIR="${SCRIPT_DIR}/templates"

# Default values
NODE_ID_PREFIX="prestissimo_local"
METASTORE_URI="thrift://sally.zuvela.ibm.com:9083"

# Function to display usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Generate Presto worker configurations from templates.

OPTIONS:
    Mode A: Command-line arguments
    --num-workers N              Number of workers to generate (required)
    --target-ip IP               Target node IP address (required)
    --coordinator-uri URI        Discovery URI (required)
    --metastore-uri URI          Hive metastore URI (optional, default: thrift://sally.zuvela.ibm.com:9083)
    --output-dir DIR             Output directory name (required)
    --start-http-port PORT       Starting HTTP port (required)
    --port-increment N           Port increment between workers (required)
    --max-drivers-per-task N     Max drivers per task (required)
    --node-id-prefix PREFIX      Node ID prefix (optional, default: prestissimo_local)

    Mode B: Parameter file
    --config-file FILE           Path to parameter file (CONFIG_SUMMARY.txt format)

    Other:
    --help                       Display this help message
    --version                    Display version information

EXAMPLE:
    $0 --num-workers 8 --target-ip 9.4.249.65 \\
       --coordinator-uri http://sally.zuvela.ibm.com:19300 \\
       --metastore-uri thrift://sally.zuvela.ibm.com:9083 \\
       --output-dir wrk_cfg_sally --start-http-port 13013 \\
       --port-increment 100 --max-drivers-per-task 4

    $0 --config-file wrk_cfg_sally/CONFIG_SUMMARY.txt

EOF
    exit 1
}

# Function to parse parameter file
parse_config_file() {
    local config_file="$1"

    if [[ ! -f "$config_file" ]]; then
        echo "Error: Config file not found: $config_file"
        exit 1
    fi

    # Parse the config file
    while IFS='=' read -r key value; do
        # Skip comments and empty lines
        [[ "$key" =~ ^#.*$ ]] && continue
        [[ -z "$key" ]] && continue
        [[ "$key" =~ ^\[.*\]$ ]] && continue  # Skip section headers

        # Trim whitespace
        key=$(echo "$key" | xargs)
        value=$(echo "$value" | xargs)

        case "$key" in
            num_workers) NUM_WORKERS="$value" ;;
            output_dir) OUTPUT_DIR="$value" ;;
            target_ip) TARGET_IP="$value" ;;
            coordinator_uri) COORDINATOR_URI="$value" ;;
            metastore_uri) METASTORE_URI="$value" ;;
            start_http_port) START_HTTP_PORT="$value" ;;
            port_increment) PORT_INCREMENT="$value" ;;
            max_drivers_per_task) MAX_DRIVERS_PER_TASK="$value" ;;
            node_id_prefix) NODE_ID_PREFIX="$value" ;;
        esac
    done < "$config_file"
}

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --num-workers)
            NUM_WORKERS="$2"
            shift 2
            ;;
        --target-ip)
            TARGET_IP="$2"
            shift 2
            ;;
        --coordinator-uri)
            COORDINATOR_URI="$2"
            shift 2
            ;;
        --metastore-uri)
            METASTORE_URI="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --start-http-port)
            START_HTTP_PORT="$2"
            shift 2
            ;;
        --port-increment)
            PORT_INCREMENT="$2"
            shift 2
            ;;
        --max-drivers-per-task)
            MAX_DRIVERS_PER_TASK="$2"
            shift 2
            ;;
        --node-id-prefix)
            NODE_ID_PREFIX="$2"
            shift 2
            ;;
        --config-file)
            CONFIG_FILE="$2"
            parse_config_file "$CONFIG_FILE"
            shift 2
            ;;
        --help)
            usage
            ;;
        --version)
            echo "Presto Worker Configuration Generator v${VERSION}"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate required parameters
if [[ -z "$NUM_WORKERS" || -z "$TARGET_IP" || -z "$COORDINATOR_URI" || \
      -z "$OUTPUT_DIR" || -z "$START_HTTP_PORT" || -z "$PORT_INCREMENT" || \
      -z "$MAX_DRIVERS_PER_TASK" ]]; then
    echo "Error: Missing required parameters"
    usage
fi

# Validate numeric parameters
if ! [[ "$NUM_WORKERS" =~ ^[0-9]+$ ]] || [ "$NUM_WORKERS" -lt 1 ]; then
    echo "Error: num-workers must be a positive integer"
    exit 1
fi

if ! [[ "$START_HTTP_PORT" =~ ^[0-9]+$ ]] || [ "$START_HTTP_PORT" -lt 1 ] || [ "$START_HTTP_PORT" -gt 65535 ]; then
    echo "Error: start-http-port must be between 1 and 65535"
    exit 1
fi

if ! [[ "$PORT_INCREMENT" =~ ^[0-9]+$ ]]; then
    echo "Error: port-increment must be a non-negative integer"
    exit 1
fi

if ! [[ "$MAX_DRIVERS_PER_TASK" =~ ^[0-9]+$ ]] || [ "$MAX_DRIVERS_PER_TASK" -lt 1 ]; then
    echo "Error: max-drivers-per-task must be a positive integer"
    exit 1
fi

# Check if template directory exists
if [[ ! -d "$TEMPLATE_DIR" ]]; then
    echo "Error: Template directory not found: $TEMPLATE_DIR"
    exit 1
fi

# Check if all template files exist
for template in config.properties.template node.properties.template hive.properties.template; do
    if [[ ! -f "${TEMPLATE_DIR}/${template}" ]]; then
        echo "Error: Template file not found: ${TEMPLATE_DIR}/${template}"
        exit 1
    fi
done

# Create output directory
if [[ -d "$OUTPUT_DIR" ]]; then
    echo "Warning: Output directory already exists: $OUTPUT_DIR"
    read -p "Do you want to overwrite it? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 1
    fi
    rm -rf "$OUTPUT_DIR"
fi

mkdir -p "$OUTPUT_DIR"

echo "Generating Presto worker configurations..."
echo "Output directory: $OUTPUT_DIR"
echo "Number of workers: $NUM_WORKERS"
echo

# Generate CONFIG_SUMMARY.txt
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
cat > "${OUTPUT_DIR}/CONFIG_SUMMARY.txt" << EOF
# Presto Worker Configuration
# Generated by: generate_worker_configs.sh v${VERSION}
# Timestamp: ${TIMESTAMP}

[GENERAL]
num_workers = ${NUM_WORKERS}
output_dir = ${OUTPUT_DIR}

[NETWORK]
target_ip = ${TARGET_IP}
coordinator_uri = ${COORDINATOR_URI}
metastore_uri = ${METASTORE_URI}

[PORTS]
start_http_port = ${START_HTTP_PORT}
port_increment = ${PORT_INCREMENT}

[WORKER_SETTINGS]
max_drivers_per_task = ${MAX_DRIVERS_PER_TASK}
node_id_prefix = ${NODE_ID_PREFIX}

[WORKER_MATRIX]
# Worker | HTTP Port | CUDF Port | Node ID
EOF

# Generate worker configurations
for ((i=1; i<=NUM_WORKERS; i++)); do
    WORKER_DIR="${OUTPUT_DIR}/worker_${i}"
    WORKER_ETC_DIR="${WORKER_DIR}/etc"
    WORKER_CATALOG_DIR="${WORKER_ETC_DIR}/catalog"

    # Create directory structure
    mkdir -p "$WORKER_CATALOG_DIR"

    # Calculate ports
    HTTP_PORT=$((START_HTTP_PORT + (i - 1) * PORT_INCREMENT))
    CUDF_PORT=$((HTTP_PORT + 3))

    # Generate node ID
    NODE_ID="${NODE_ID_PREFIX}${i}"

    # Generate config.properties
    sed -e "s|{{COORDINATOR_URI}}|${COORDINATOR_URI}|g" \
        -e "s|{{HTTP_PORT}}|${HTTP_PORT}|g" \
        -e "s|{{CUDF_PORT}}|${CUDF_PORT}|g" \
        -e "s|{{MAX_DRIVERS_PER_TASK}}|${MAX_DRIVERS_PER_TASK}|g" \
        "${TEMPLATE_DIR}/config.properties.template" > "${WORKER_ETC_DIR}/config.properties"

    # Generate node.properties
    sed -e "s|{{TARGET_IP}}|${TARGET_IP}|g" \
        -e "s|{{NODE_ID}}|${NODE_ID}|g" \
        "${TEMPLATE_DIR}/node.properties.template" > "${WORKER_ETC_DIR}/node.properties"

    # Generate hive.properties
    sed -e "s|{{METASTORE_URI}}|${METASTORE_URI}|g" \
        "${TEMPLATE_DIR}/hive.properties.template" > "${WORKER_CATALOG_DIR}/hive.properties"

    # Append to worker matrix in CONFIG_SUMMARY.txt
    printf "worker_%d | %d | %d | %s\n" "$i" "$HTTP_PORT" "$CUDF_PORT" "$NODE_ID" >> "${OUTPUT_DIR}/CONFIG_SUMMARY.txt"

    echo "  Generated worker_${i}: HTTP=${HTTP_PORT}, CUDF=${CUDF_PORT}, Node ID=${NODE_ID}"
done

echo
echo "Configuration generation complete!"
echo "Output directory: $OUTPUT_DIR"
echo "Summary file: ${OUTPUT_DIR}/CONFIG_SUMMARY.txt"
