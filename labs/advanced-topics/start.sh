#! /bin/bash

docker-compose up -d

echo "Waiting for Kafka to launch on 9092..."
while ! nc -z kafka 9092; do
  sleep 1.0
  echo "Kafka not yet ready..."
done
echo "Kafka is now ready!"

docker exec advanced-topics-kafka-1  kafka-topics --bootstrap-server kafka:9092 \
    --topic vehicle-positions \
    --create \
    --partitions 6 \
    --replication-factor 1

docker container run -d \
    --name producer \
    --net advanced-topics_confluent \
    cnfltraining/vp-producer:v2

echo "Opening shell in docker container..."
docker exec -it advanced-topics-kafka-1 /bin/bash
