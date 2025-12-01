#!/bin/bash

for i in $(seq 5 6); do
    bash ~/perf_docker/start_worker.sh $i
done
