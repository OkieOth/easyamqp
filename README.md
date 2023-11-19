# TL;DR;

Attempt to write a simple rabbitmq driver wrapper, that supports connection
auto healing and multithreading.

**This repo is still work in progress.**

... But I hope, over time it develops over time to an professional project x-D

# Main Principle of the API
* Easy to use API
* Full async/await
* Multithreading secure
* Emphazise the topology used for the broker communication (Exchanges, Queues, ...)


# Project content
* easyamqp - library crate
* examples/con_test - example binary that plays with the lib for connection tests

# Usage

```bash
# start the dev env
./bin/compose_env.sh start

# between this call, you can for instance run the integration tests
# against this test env
cargo test -- --ignored --show-output

# stop the dev env
./bin/compose_env.sh stop

# run all integration tests in a closed docker compose env
./bin/test_easyamqp.sh test

# running tests in docker
docker run --privileged -u podman:podman \
    -v $(pwd):/project \
    --rm \
    -it \
    mgoltzsche/podman:minimal

```

# Requirements

## Test Requirements
```bash
# Ubuntu
sudo apt install libssl-dev pkg-config
```