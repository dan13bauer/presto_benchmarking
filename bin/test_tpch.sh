if [ $# -ne 4 ]; then
    echo "echo syntax <PRESTO_CLI_DIR> <QUERY_LIST> <COORDINATOR_PORT> <SF_SCHEMA>"
    echo "e.g. ./test_tpch.sh ~/presto_client ./tpch_sf1.txt 19001 sf1_parquet"
    exit
fi

PRESTO_CLI_DIR=$1
QUERY_LIST=$2
COORDINATOR_PORT=$3
SF_SCHEMA=$4
QUERY_DIR=./tpch_queries

if [[ "$SF_SCHEMA" == *"_nvidia" ]]; then
    echo "using nvidia queries !"
    QUERY_DIR=./tpch_queries_nvidia
fi

for query in $(cat $QUERY_LIST); do


    file=$QUERY_DIR/$query    
    if [[ -f "$file" ]]; then
        echo "*** Executing query $file on schema $SF_SCHEMA"
	$PRESTO_CLI_DIR/presto --server localhost:$COORDINATOR_PORT --catalog hive --schema $SF_SCHEMA --session single_node_execution_enabled=false -f $file 
    fi
    sleep 3
done

