#!/usr/bin/env bash

for qn in 0{1..9} {10..22} ; do
  echo "-- TPCH Q${qn}" > tmp/query_${qn}.sql
  cat query_${qn}.sql >> tmp/query_${qn}.sql
done
