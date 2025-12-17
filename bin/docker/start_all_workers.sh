#!/bin/bash

for i in $(seq 4 5); do
    bash docker/start_worker.sh $i
done
