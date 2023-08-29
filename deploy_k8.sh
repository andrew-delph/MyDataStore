#!/bin/bash

# Exit on any error
set -e

bazel run //store:image_push
kubectl rollout restart statefulset store



