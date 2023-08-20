#!/bin/bash

# Check if an argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <number_of_replicas>"
    exit 1
fi

# Check if the argument is a number
if ! [[ "$1" =~ ^[0-9]+$ ]]; then
    echo "Error: The argument must be a positive integer."
    exit 1
fi

# Scale the service
docker-compose up -d --scale store=$1

# Print the status
if [ $? -eq 0 ]; then
    echo "Successfully scaled the 'store' service to $1 replicas."
else
    echo "Failed to scale the 'store' service."
    exit 1
fi
