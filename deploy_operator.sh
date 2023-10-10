#!/bin/bash

# eval $(minikube docker-env)


TAG=$(date +%s)
IMG=docker.io/andrew-delph/operator:$TAG
bazel run //operator:image_push
docker tag docker.io/andrew-delph/operator:operator_image $IMG
(cd operator && make deploy IMG=$IMG)
(cd operator && kustomize build config/rbac | kubectl apply -f -  || true)







