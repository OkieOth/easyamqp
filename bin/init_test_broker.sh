#!/bin/bash

if [ -z "$RABBIT_SERVER" ]; then
    RABBIT_SERVER=127.0.0.1
fi

# Define the command and arguments
RABBITMQ_ADMIN_CMD="rabbitmqadmin"
RABBIT_AUTH="--host $RABBIT_SERVER --username guest --password guest"
RABBITMQ_ADMIN_ARGS_EXCHANGE="$RABBIT_AUTH declare exchange name=first type=topic"
RABBITMQ_ADMIN_ARGS_QUEUE="$RABBIT_AUTH declare queue name=first_queue"
RABBITMQ_ADMIN_ARGS_BINDING="$RABBIT_AUTH declare binding source=first destination=first_queue routing_key='*'"

# Define the maximum number of attempts
MAX_ATTEMPTS=5

# Initialize variables
attempt=1
sleep_duration=2  # Initial sleep duration in seconds

while [ $attempt -le $MAX_ATTEMPTS ]; do
    echo "Attempt $attempt: Running $RABBITMQ_ADMIN_CMD $RABBITMQ_ADMIN_ARGS_EXCHANGE ..."
    if $RABBITMQ_ADMIN_CMD $RABBITMQ_ADMIN_ARGS_EXCHANGE; then
        echo "Exchange created successful"
        echo "Attempt $attempt: Running $RABBITMQ_ADMIN_CMD $RABBITMQ_ADMIN_ARGS_QUEUE ..."
        if $RABBITMQ_ADMIN_CMD $RABBITMQ_ADMIN_ARGS_QUEUE; then
            echo "Queue created successful"
            echo "Attempt $attempt: Running $RABBITMQ_ADMIN_CMD $RABBITMQ_ADMIN_ARGS_BINDING ..."
            if $RABBITMQ_ADMIN_CMD $RABBITMQ_ADMIN_ARGS_BINDING; then
                echo "Binding created successful"
                exit 0
            else
                echo "Command failed. Retrying in $sleep_duration seconds..."
                sleep $sleep_duration
                attempt=$((attempt + 1))
                sleep_duration=$((sleep_duration * 2))
            fi
        else
            echo "Command failed. Retrying in $sleep_duration seconds..."
            sleep $sleep_duration
            attempt=$((attempt + 1))
            sleep_duration=$((sleep_duration * 2))
        fi
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
