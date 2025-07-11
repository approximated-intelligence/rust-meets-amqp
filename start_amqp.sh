#!/bin/sh
set -e

# Wait for RabbitMQ to be ready
echo "Waiting for RabbitMQ to be ready..."
while ! nc -z ${AMQP_HOST:-rabbitmq} ${AMQP_PORT:-5672}; do
  sleep 1
done

echo "RabbitMQ is ready!"

# Run AMQP client based on environment
if [ "$1" = "producer" ]; then
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
elif [ "$1" = "consumer" ]; then
    exec /usr/local/bin/amqp --client-consumer \
        --host ${AMQP_HOST:-rabbitmq} \
        --port ${AMQP_PORT:-5672} \
        --user ${AMQP_USER:-guest} \
        --password ${AMQP_PASSWORD:-guest} \
        --vhost ${AMQP_VHOST:-/} \
        --queue ${AMQP_QUEUE:-test_queue} \
        --test-mode \
        --receive-count ${AMQP_RECEIVE_COUNT:-1000}
elif [ "$1" = "both" ]; then
    echo "Starting both producer and consumer..."
    
    # Start producer in background
    /usr/local/bin/amqp --client-producer \
        --host ${AMQP_HOST:-rabbitmq} \
        --port ${AMQP_PORT:-5672} \
        --user ${AMQP_USER:-guest} \
        --password ${AMQP_PASSWORD:-guest} \
        --vhost ${AMQP_VHOST:-/} \
        --queue ${AMQP_QUEUE:-test_queue} \
        --test-mode \
        --sent-count ${AMQP_RECEIVE_COUNT:-1000} &
    
    PRODUCER_PID=$!
    echo "Started producer with PID: $PRODUCER_PID"
    
    # Wait a moment for producer to initialize
    sleep 2
    
    # Start consumer in foreground
    /usr/local/bin/amqp --client-consumer \
        --host ${AMQP_HOST:-rabbitmq} \
        --port ${AMQP_PORT:-5672} \
        --user ${AMQP_USER:-guest} \
        --password ${AMQP_PASSWORD:-guest} \
        --vhost ${AMQP_VHOST:-/} \
        --exchange ${AMQP_EXCHANGE:-test_exchange} \
        --topic ${AMQP_TOPIC:-test.key} \
        --test-mode \
        --receive-count ${AMQP_SEND_COUNT:-1000} &
    
    CONSUMER_PID=$!
    echo "Started consumer with PID: $CONSUMER_PID"
    
    # Function to cleanup on exit
    cleanup() {
        echo "Cleaning up processes..."
        kill $CONSUMER_PID $PRODUCER_PID 2>/dev/null || true
        wait $CONSUMER_PID $PRODUCER_PID 2>/dev/null || true
        echo "Cleanup complete"
    }
    
    # Set trap for cleanup
    trap cleanup EXIT INT TERM
    
    # Wait for both processes to complete
    wait $CONSUMER_PID $PRODUCER_PID
    
    echo "Both producer and consumer completed"
else
    # Interactive mode
    echo "Starting interactive AMQP client..."
    echo "Use: /usr/local/bin/amqp --help for options"
    echo "Available modes: producer, consumer, both"
    exec /bin/sh
fi
