#!/bin/bash

# Exit on any error
set -e

bazel run //main:store_image

docker-compose up --force-recreate -d store store-profile



