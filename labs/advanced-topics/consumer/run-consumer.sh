#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

docker container run -d \
  --net advanced-topics_confluent \
  sample-consumer:1.0
