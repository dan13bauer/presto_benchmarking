#!/bin/bash

if [ $# -eq 0 ]; then
    QUERY=query_05.sql
else
    if [ ${1} -lt 10 ]; then
	QUERY=query_0${1}.sql
    else
	QUERY=query_${1}.sql
    fi
fi

~/presto_run_zrl/presto --server sally:19300 --catalog hive --schema sf1000_nvidia --session single_node_execution_enabled=false -f ~/presto_benchmarking/bin/tpch_queries_nvidia/${QUERY}
