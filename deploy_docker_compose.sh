#!/bin/bash

# Exit on any error
set -e

bazel run //store:store_image

docker-compose up --force-recreate -d store



