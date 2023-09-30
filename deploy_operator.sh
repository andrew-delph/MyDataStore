#!/bin/bash



eval $(minikube docker-env)
IMG=docker.io/andrew-delph/operator:operator_image
bazel run //operator:operator_image
(cd operator && make undeploy || true)
(cd operator && make deploy || true)
(cd operator && kustomize build config/default | kubectl apply -f - || true)
(cd operator && kustomize build config/rbac | kubectl apply -f - || true)







