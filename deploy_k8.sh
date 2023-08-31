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


SECONDS=0  # Reset the SECONDS variable


if [ $ROLL_OUT_FLAG -eq 0 ]; then
    echo "The 'rollout' flag is not set. Setting up."
    kubectl create -f ./resources/store.yaml
else
    echo "The 'rollout' flag is set."
    kubectl rollout restart statefulset/store
fi

# kubectl rollout status statefulset/store -w 

# echo "Rollout complete!"
# echo "[$(date +"%Y-%m-%d %H:%M")] Elapsed time: $SECONDS seconds" | tee -a rollout.txt




