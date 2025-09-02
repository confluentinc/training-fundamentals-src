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

echo "Stopping containers and destroying data volumes..."
docker container rm -f producer
docker container rm -f kafka
docker compose down -v
echo "Lab successfully stopped"
