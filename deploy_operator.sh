#!/bin/bash

# Exit on any error
set -e

eval $(minikube docker-env)
IMG=docker.io/andrew-delph/operator:operator_image
bazel run //operator:operator_image
(cd operator/config/manager && kustomize edit set image controller=$IMG)
(cd operator && kustomize build config/default | kubectl apply -f -)







