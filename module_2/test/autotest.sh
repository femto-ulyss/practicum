#!/usr/bin/bash
TEST_DATA=$1

echo "Running test for \`app.message_processor\`"

while read -r line; do
    topic=$(echo "$line" | jq -r '.topic')
    message=$(echo "$line" | jq -c '.message')
    
    docker exec kafka1 bash -c "echo '$message' | kafka-console-producer --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094 --topic $topic"
    echo -n "."
done < "$TEST_DATA"

echo "DONE"