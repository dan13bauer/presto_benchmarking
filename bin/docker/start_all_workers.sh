#!/bin/bash

CONTAINER_PREFIX="${1:-dnb_worker}"

for i in $(seq 2 6); do
    bash docker/start_worker.sh $i "$CONTAINER_PREFIX"
done
