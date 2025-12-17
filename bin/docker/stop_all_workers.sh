#!/bin/bash

CONTAINER_PREFIX="${1:-sro_worker}"

for i in $(seq 1 8); do
    docker stop ${CONTAINER_PREFIX}_$i
    docker rm ${CONTAINER_PREFIX}_$i
done
