#! /bin/bash

docker-compose up -d

echo "Waiting for Kafka to launch on 9092..."
while ! nc -z kafka 9092; do   
  sleep 1.0
  echo "Kafka not yet ready..."
done 
echo "Kafka is now ready!"
