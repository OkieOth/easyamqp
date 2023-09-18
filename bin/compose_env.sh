#!/bin/bash

scriptPos=${0%/*}

COMPOSE_FILE=$scriptPos/../compose_env.yaml

function start() {
  echo "Starting Docker Compose environment..."
  docker compose -f $COMPOSE_FILE up -d
}

function stop() {
  echo "Stopping Docker Compose environment..."
  docker compose -f $COMPOSE_FILE down
}

function destroy() {
  echo "Destroying Docker Compose environment..."
  docker compose -f $COMPOSE_FILE down -v
}

case "$1" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  destroy)
    destroy
    ;;
  *)
    echo "Usage: $0 {start|stop|destroy}"
    exit 1
    ;;
esac

exit 0
