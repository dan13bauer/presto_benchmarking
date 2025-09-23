#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Usage: $0 <BUILD_DIR> <WORKERS_DIR>"
    echo "Example $0 ./_build/release"
    exit 1
fi

BUILD_DIR=$1
WORKERS_DIR=$2

export LD_LIBRARY_PATH=/opt/rh/gcc-toolset-12/root/usr/lib64:/opt/rh/gcc-toolset-12/root/usr/lib:/usr/local/lib


for GPU  in $(seq 0 7); do
    #UCX_PARAMS="UCX_TCP_CM_REUSEADDR=y UCX_TLS=^ib UCX_LOG_LEVEL=error UCX_TCP_KEEPINTVL=1ms UCX_KEEPALIVE_INTERVAL=1ms"
    #CUDA_PARAMS="CUDA_VISIBLE_DEVICES=$GPU LIBCUDF_USE_DEBUG_STREAM_POOL=ON GLOG_logtostderr=1"
    #UCX_PARAMS="UCX_TCP_CM_REUSEADDR=y UCX_LOG_LEVEL=error UCX_TCP_KEEPINTVL=1ms UCX_KEEPALIVE_INTERVAL=1ms"
    UCX_PARAMS="UCX_TCP_CM_REUSEADDR=y UCX_PROTO_INFO=y UCX_LOG_LEVEL=error UCX_RNDV_PIPELINE_ERROR_HANDLING=y"
    CUDA_PARAMS="CUDA_VISIBLE_DEVICES=$GPU GLOG_logtostderr=1"
    #VELOX_ARGS="-velox_cudf_debug=true -velox_cudf_table_scan=false -velox_cudf_enabled=false -velox_cudf_exchange=false -velox_cudf_memory_resource=pool -v=3"
    VELOX_ARGS="-velox_cudf_debug=true -velox_cudf_table_scan=true -velox_cudf_enabled=true -velox_cudf_exchange=true -velox_cudf_memory_resource=async -v=3"
    WORKER_ID=$((GPU+1))
    
    env $UCX_PARAMS env $CUDA_PARAMS $BUILD_DIR/presto_cpp/main/presto_server  -etc_dir $WORKERS_DIR/worker_$WORKER_ID/etc $VELOX_ARGS &
    sleep 3
done
