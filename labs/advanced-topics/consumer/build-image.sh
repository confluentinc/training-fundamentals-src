#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

docker image build -t sample-consumer:1.0 .
