#!/bin/bash

for i in $(seq 1 8); do
    docker stop sro_worker_$i
    docker rm sro_worker_$i
done
