#!/bin/bash

# Exit on any error
set -e

echo "STARTING DEPLOY DOCKER-COMPOSE"

bazel run --execution_log_json_file=events.json //main:store_image

docker-compose up --force-recreate -d store store-profile



