#!/bin/sh
set -e

# Wait for RabbitMQ to be ready
echo "Waiting for RabbitMQ to be ready..."
while ! nc -z ${AMQP_HOST:-rabbitmq} ${AMQP_PORT:-5672}; do
  sleep 1
done

echo "RabbitMQ is ready!"

# Run AMQP client based on environment
if [ "$AMQP_MODE" = "producer" ]; then
    exec /usr/local/bin/amqp --client-producer \
        --host ${AMQP_HOST:-rabbitmq} \
        --port ${AMQP_PORT:-5672} \
        --user ${AMQP_USER:-guest} \
        --password ${AMQP_PASSWORD:-guest} \
        --vhost ${AMQP_VHOST:-/} \
        --exchange ${AMQP_EXCHANGE:-test_exchange} \
        --topic ${AMQP_TOPIC:-test.key} \
        --test-mode \
        --send-count ${AMQP_SEND_COUNT:-1000}
elif [ "$AMQP_MODE" = "consumer" ]; then
    exec /usr/local/bin/amqp --client-consumer \
        --host ${AMQP_HOST:-rabbitmq} \
        --port ${AMQP_PORT:-5672} \
        --user ${AMQP_USER:-guest} \
        --password ${AMQP_PASSWORD:-guest} \
        --vhost ${AMQP_VHOST:-/} \
        --queue ${AMQP_QUEUE:-test_queue} \
        --test-mode \
        --receive-count ${AMQP_RECEIVE_COUNT:-1000}
else
    # Interactive mode
    echo "Starting interactive AMQP client..."
    echo "Use: /usr/local/bin/amqp --help for options"
    exec /bin/sh
fi
