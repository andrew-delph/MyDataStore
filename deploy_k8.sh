#!/bin/bash

# Exit on any error
set -e

STATEFULSET_NAME="store"
NAMESPACE="default"

eval $(minikube docker-env)

bazel run //store:store_image


# kubectl delete statefulset store --ignore-not-found=true --wait=true


ROLL_OUT_FLAG=0
# Use getopts to check for the -r (rollout) flag
while getopts ":r" opt; do
  case $opt in
    r)
      ROLL_OUT_FLAG=1
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done

# Check if the rollout flag was set
if [ $ROLL_OUT_FLAG -eq 0 ]; then
    echo "The 'rollout' flag is not set. Setting up and exiting."
    kubectl create -f ./resources/store.yaml
    exit 1
fi



# kubectl patch statefulset $STATEFULSET_NAME -n $NAMESPACE --type='json' -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/readinessProbe/httpGet/path", "value": "/health"}]'
# exit 1

kubectl rollout restart statefulset/store

kubectl rollout status statefulset/store -w 

echo "Rollout complete!"



