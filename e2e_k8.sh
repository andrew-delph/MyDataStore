#!/bin/bash

# Define the service and namespace
SERVICE_NAME=store
NAMESPACE=default
LOCAL_PORT=8080
REMOTE_PORT=8080

# Start kubectl port-forward in the background
kubectl port-forward -n $NAMESPACE svc/$SERVICE_NAME $LOCAL_PORT:$REMOTE_PORT &
# Get the PID of kubectl port-forward
PORT_FORWARD_PID=$!

# Function to kill the port-forward process when the script exits
function cleanup {
    echo "Cleaning up..."
    kill -9 $PORT_FORWARD_PID
}
trap cleanup EXIT

# Wait for the connection to be open
echo "Waiting for port-forward to be ready..."
sleep 5

# Run your k6 script
k6 run e2e/test.js