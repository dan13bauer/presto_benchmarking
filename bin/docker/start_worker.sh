#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Usage: $0 <WORKER_ID> [CONTAINER_PREFIX]"
    echo "Example: $0 1"
    echo "Example: $0 1 my_worker"
    exit 1
else

WORKER_ID=$1
CONTAINER_PREFIX="${2:-sro_worker}"
GPU_ID=$((WORKER_ID-1))
IMG_BASE=presto-a3141860dc/velox-93c21a2b6
#IMG_BASE=presto/main-dec2-drv-q9-fix
#IMG_BASE=471112500371.dkr.ecr.us-west-2.amazonaws.com/presto/prestissimo-runtime-centos9
#IMG_VER=ucx1.19.0-latest-exchange
#IMG_VER=ucx1.19.0-latest-exchange-mem-cudf2510
#IMG_VER=latest-rapidsai-exchange
#IMG_VER=9d198e0
IMG_VER=centos

IMG=${IMG_BASE}:${IMG_VER}
DEV_ARG=\"device=${GPU_ID}\"

V_ARGS="-v=3"

#RM_CMD="--rm"
RM_CMD=""

docker run $RM_CMD -d  --gpus all -it --network=host --cap-add=IPC_LOCK --shm-size=1g --ulimit memlock=-1 --ulimit stack=67108864 --pid host \
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
       --name ${CONTAINER_PREFIX}_${WORKER_ID} \
       ${IMG} \
       ${V_ARGS}
fi

#       -e UCX_TLS=tcp,cuda_copy \

