#!/bin/bash

# Exit on any error
set -e

eval $(minikube docker-env)

bazel run //store:store_image

kubectl delete statefulset store --ignore-not-found=true --wait=true

kubectl apply -f ./resources/store.yaml

kubectl rollout restart statefulset store

kubectl rollout status statefulset/store -w --timeout=30s


