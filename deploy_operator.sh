#!/bin/bash

# eval $(minikube docker-env)


TAG=$(date +%s)
IMG=ghcr.io/andrew-delph/operator:$TAG
bazel run //operator:image_push -- -dst=$IMG
(cd operator && make deploy IMG=$IMG)
(cd operator && kustomize build config/rbac | kubectl apply -f -  || true)







