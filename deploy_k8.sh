#!/bin/bash

# Exit on any error
set -e

bazel run //store:image_push

kubectl apply -f ./resources/store.yaml

kubectl rollout restart statefulset store



