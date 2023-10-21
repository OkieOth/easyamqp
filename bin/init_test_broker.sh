#!/bin/bash

if [ -z "$RABBIT_SERVER" ]; then
    RABBIT_SERVER=127.0.0.1
fi

# Define the command and arguments
RABBITMQ_ADMIN_CMD="rabbitmqadmin"
RABBITMQ_ADMIN_ARGS="--host $RABBIT_SERVER --username guest --password guest declare exchange name=first type=topic"

# Define the maximum number of attempts
MAX_ATTEMPTS=5

# Initialize variables
attempt=1
sleep_duration=2  # Initial sleep duration in seconds

while [ $attempt -le $MAX_ATTEMPTS ]; do
    echo "Attempt $attempt: Running $RABBITMQ_ADMIN_CMD $RABBITMQ_ADMIN_ARGS..."
    if $RABBITMQ_ADMIN_CMD $RABBITMQ_ADMIN_ARGS; then
        echo "Command successful. Exiting with status 0."
        exit 0
    else
        echo "Command failed. Retrying in $sleep_duration seconds..."
        sleep $sleep_duration
        attempt=$((attempt + 1))
        sleep_duration=$((sleep_duration * 2))
    fi
done

# If we reach this point, all attempts failed
echo "Maximum number of attempts reached. Exiting with status 1."
exit 1
