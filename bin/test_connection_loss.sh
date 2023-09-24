#!/bin/bash

scriptPos=${0%/*}

COMPOSE_FILE=$scriptPos/../test_connection_loss.yaml

function run_test() {
  echo "Run Docker Compose based integration tests ..."
  if ! docker compose  -f $COMPOSE_FILE up --build --abort-on-container-exit --exit-code-from test_runner; then
    exit 1
  else
    exit 0
  fi
}

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
  test)
    run_test
    ;;
  *)
    echo "Usage: $0 {start|stop|destroy}"
    exit 1
    ;;
esac

exit 0
