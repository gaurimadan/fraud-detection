#!/bin/bash
set -euo pipefail

if [ -n "${RENDER_INTERNAL_HOSTNAME:-}" ]; then
    export KAFKA_ADVERTISED_LISTENERS="INTERNAL://${RENDER_INTERNAL_HOSTNAME}:9092"
    export KAFKA_CONTROLLER_QUORUM_VOTERS="1@localhost:9093"
fi

if [ -z "${KAFKA_INITIAL_TOPIC:-}" ]; then
    exec "$@"
fi

"$@" &
broker_pid=$!
trap 'kill -TERM "$broker_pid" 2>/dev/null || true' TERM INT

for ((attempt = 1; attempt <= 60; attempt++)); do
    if ! kill -0 "$broker_pid" 2>/dev/null; then
        wait "$broker_pid"
        exit $?
    fi
    if /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --create \
        --if-not-exists \
        --topic "$KAFKA_INITIAL_TOPIC" \
        --partitions 1 \
        --replication-factor 1 >/dev/null 2>&1; then
        echo "Kafka topic '$KAFKA_INITIAL_TOPIC' is ready."
        wait "$broker_pid"
        exit $?
    fi
    sleep 2
done

echo "Kafka topic '$KAFKA_INITIAL_TOPIC' was not ready after 120 seconds." >&2
kill -TERM "$broker_pid" 2>/dev/null || true
wait "$broker_pid" || true
exit 1
