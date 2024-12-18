#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

docker compose up -d

echo "Waiting for Kafka to launch on 9092..."
while ! nc -z kafka 9092; do   
  sleep 1.0
  echo "Kafka not yet ready..."
done 
echo "Kafka is now ready!"

kafka-topics --create \
    --bootstrap-server kafka:9092 \
    --topic vehicle-positions \
    --partitions 6 \
    --replication-factor 1

docker container run -d \
    --name producer \
    --net advanced-topics_confluent \
    --platform linux/amd64 \
    cnfltraining/vp-producer:v2
