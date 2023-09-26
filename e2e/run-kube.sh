#!/bin/bash


set -e

if [ $# -lt 1 ]; then
    echo "Usage: ./run-kube.sh <RESOURCE_FILENAME>"
    exit 1
fi


if [ "$2" == "--external" ]; 
then
    echo "USING EXTERNAL"
    WS_HOST="ws:\/\/ws.andrewdelph.com\/socket.io\/\?EIO\=4\&transport\=websocket"
    OPTIONS_HOST="https:\/\/options.andrewdelph.com"
else
    echo "USING INTERNAL"
    WS_HOST="ws:\/\/socketio-service.default.svc.cluster.local\/socket.io\/\?EIO\=4\&transport\=websocket"
    OPTIONS_HOST="http:\/\/options-service.default.svc.cluster.local"
fi

RESOURCE_FILENAME=$1
RESOURCE_NAME="$(basename -s .yaml $RESOURCE_FILENAME)"
# Inspect the YAML to find the actual test-script name...
SCRIPT_NAME="$(yq -r '.spec.script.configMap.file' $RESOURCE_FILENAME)"
# Clean up the script name and append the unique timestamp
TAG_PREFIX="$(basename -s .js $SCRIPT_NAME)"
TAG_NAME="test-$(date +%m-%d-%H-%M-%S)"


# Replacement doesn't seem to trigger, so we need to delete any previous execution
kubectl --context minikube delete --ignore-not-found=true --wait=true -f $RESOURCE_FILENAME -n k6

# Update '--tag testid=...' to include the test-script name and timestamp for uniqueness, then apply
sed -e "s/testid\=${RESOURCE_NAME}/testid\=${TAG_NAME}/g" -e "s/WS_HOST_VALUE/${WS_HOST}/g" -e "s/OPTIONS_HOST_VALUE/${OPTIONS_HOST}/g" $RESOURCE_FILENAME | kubectl --context minikube apply  -f - -n k6

# sed -e "s/testid\=${RESOURCE_NAME}/testid\=${TAG_NAME}/g" -e "s/WS_HOST_VALUE/${WS_HOST}/g" -e "s/OPTIONS_HOST_VALUE/${OPTIONS_HOST}/g" $RESOURCE_FILENAME

echo "WS_HOST:" $WS_HOST

echo "OPTIONS_HOST:" $OPTIONS_HOST

echo "RESOURCE_NAME:" $RESOURCE_NAME

echo "SCRIPT_NAME:" $SCRIPT_NAME

echo "test id:" $TAG_NAME

