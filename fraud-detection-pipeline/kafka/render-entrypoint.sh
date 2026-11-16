#!/bin/sh
set -eu

if [ -n "${RENDER_INTERNAL_HOSTNAME:-}" ]; then
    export KAFKA_ADVERTISED_LISTENERS="INTERNAL://${RENDER_INTERNAL_HOSTNAME}:9092"
    export KAFKA_CONTROLLER_QUORUM_VOTERS="1@localhost:9093"
fi

exec "$@"
