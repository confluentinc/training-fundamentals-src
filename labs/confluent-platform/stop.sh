#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

docker container rm -f producer
docker compose down -v
