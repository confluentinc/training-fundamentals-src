#! /bin/sh
#
# Copyright (c) 2019-2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Script safety options
set -o errexit
set -o nounset

# Launch Kafka
docker compose up -d

echo "Waiting for Kafka to launch on 9092..."
while ! kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1 ; do
  sleep 1.0
  echo "Kafka not yet ready..."
done

cat << EOF
Kafka is now ready!

Creating 'vehicle-positions' topic...
EOF

kafka-topics --bootstrap-server kafka:9092 \
    --topic vehicle-positions \
    --create \
    --partitions 6 \
    --replication-factor 1

cat << EOF
Topic created.

Building and running producer...
EOF

docker container run -d \
    --name producer \
    --net exploring_confluent \
    cnfltraining/vp-producer:v2

echo "Producer up and running!"

