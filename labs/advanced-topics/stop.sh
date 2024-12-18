#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

# Remove the producer
docker container rm -f producer
# remove the Kafka cluster
docker compose down -v
# remove all consumer containers
docker container ls | grep sample-consumer | awk '{ print $1 }' | xargs docker container rm -f
