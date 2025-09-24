#!/usr/bin/env bash

# Input JSON file
json_file="queries.json"

# Iterate over all keys in the JSON
for key in $(jq -r 'keys[]' "$json_file"); do
  # Extract the value for the current key
  value=$(jq -r --arg k "$key" '.[$k]' "$json_file")
  n=${key/Q/}
  qn=$(printf %02d $n)
  # Write the value into a file named after the key
  echo "$value" > "query_${qn}.sql"
done
