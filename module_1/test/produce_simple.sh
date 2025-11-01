#!/bin/bash

APP_CONTAINER="module_1-app-1"

# Generate payload
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
file_path=$(python $SCRIPT_DIR/messages/generate_simple.py)
echo "Got messages from: $file_path"

cat $file_path | while read line; do
    docker exec module_1-app-1 python /app/cli.py producer -t test_topic -v "$line"
done

rm -f $file_path