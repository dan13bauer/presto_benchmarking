#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Usage: $0 <WORKER_ID>"
    echo "Example $0 1"
    exit 1
else

WORKER_ID=$1
GPU_ID=$((WORKER_ID-1))
IMG_BASE=presto/unified-76f7902
#IMG_BASE=471112500371.dkr.ecr.us-west-2.amazonaws.com/presto/prestissimo-runtime-centos9
#IMG_VER=ucx1.19.0-latest-exchange
#IMG_VER=ucx1.19.0-latest-exchange-mem-cudf2510
#IMG_VER=latest-rapidsai-exchange
IMG_VER=centos

IMG=${IMG_BASE}:${IMG_VER}
DEV_ARG=\"device=${GPU_ID}\"

#V_ARGS="-velox_cudf_debug=true -velox_cudf_table_scan=true -velox_cudf_enabled=true -velox_cudf_exchange=true -velox_cudf_memory_resource=async -v=3"

#V_ARGS="-velox_cudf_debug=true -velox_cudf_table_scan=true -velox_cudf_enabled=true -velox_cudf_exchange=true -velox_cudf_memory_resource=async -velox_cudf_memory_percentage=90 -v=3"
#V_ARGS="-velox_cudf_debug=false -velox_cudf_table_scan=false -velox_cudf_enabled=false -v=3"
#V_ARGS="-velox_cudf_debug=false -velox_cudf_enabled=true -velox_cudf_memory_resource=async  -v=3"

#V_ARGS="-velox_cudf_debug=true -velox_cudf_enabled=true -velox_cudf_exchange=false -velox_cudf_memory_resource=async -v=3"
#V_ARGS="-velox_cudf_debug=true -velox_cudf_enabled=true -velox_cudf_exchange=true -velox_cudf_memory_resource=async -v=3"
V_ARGS="-v=3"

docker run --rm -d  --gpus all -it --network=host --cap-add=IPC_LOCK --shm-size=1g --ulimit memlock=-1 --ulimit stack=67108864 --pid host \
       --runtime=nvidia \
       --device /dev/infiniband/rdma_cm  --device=/dev/infiniband/uverbs0 --device=/dev/infiniband/uverbs1 --device=/dev/infiniband/uverbs2 --device=/dev/infiniband/uverbs3 --device=/dev/infiniband/uverbs4 --device=/dev/infiniband/uverbs5 --device=/dev/infiniband/uverbs6 --device=/dev/infiniband/uverbs7 --device=/dev/infiniband/uverbs8 --device=/dev/infiniband/uverbs9 \
       -e KVIKIO_COMPAT_MODE=OFF \
       -e KVIKIO_NTHREADS=16 \
       -e NVIDIA_GDS=enabled \
       -v /etc/cufile.json:/etc/cufile.json:ro \
       -e UCX_LOG_LEVEL=info \
       -e CUDA_VISIBLE_DEVICES=${GPU_ID} \
       -e UCX_TCP_CM_REUSEADDR=y \
       -e UCX_PROTO_INFO=y \
       -e UCX_RNDV_PIPELINE_ERROR_HANDLING=y \
       -v ~/tmp/presto-root/:/tmp/presto-root/ \
       -v ${HOME}/presto_workers/worker_${WORKER_ID}/etc:/opt/presto-server/etc \
       -v ${HOME}/presto_workers/dummy_config.sh:/root/.aws/config.sh \
       -v /gpfs/zc2:/gpfs/zc2 \
       --name sro_worker_${WORKER_ID} \
       ${IMG} \
       ${V_ARGS}
fi

#       -e UCX_TLS=tcp,cuda_copy \

