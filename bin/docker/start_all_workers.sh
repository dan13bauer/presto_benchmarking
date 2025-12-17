#!/bin/bash

CONTAINER_PREFIX="${1:-sro_worker}"

for i in $(seq 4 5); do
    bash docker/start_worker.sh $i "$CONTAINER_PREFIX"
done
