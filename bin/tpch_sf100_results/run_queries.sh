#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <output_directory>"
    exit 1
fi

OUT_DIR=$1

if [ ! -d "$OUT_DIR" ]; then
    echo "Error: '$OUT_DIR' is not a directory."
    exit 1
fi

for q in query_*sql; do
~/presto_run_zrl/presto --server localhost:19000 --catalog hive --schema sf100_nvidia --session single_node_execution_enabled=false -f $q >& ./$OUT_DIR/${q//sql/res}
done
