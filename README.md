# Rust 🤝 AMQP

<img src="https://repository-images.githubusercontent.com/1017573697/35d14245-6fd9-40c7-b5a6-0fdc570a3d06" style="width:100%;height:auto;">
## Vibecoding an AMQP-Broker in Rust

A complete implementation of the AMQP 0.9.1 protocol in Rust, providing both server and client functionality for message queuing and pub/sub patterns.

## Features

- **Still Broken**: Borrower is issue might point to an architecture problem :)
- **Full AMQP 0.9.1 Protocol Support**: Complete implementation of the AMQP specification
- **Server Mode**: Run as a standalone AMQP broker with support for exchanges, queues, and bindings
- **Client Mode**: Producer and consumer clients with full protocol support
- **Heartbeat Support**: Automatic connection health monitoring
- **Multiple Exchange Types**: Direct, fanout, topic, and headers exchanges
- **Configurable Reliability**: Fire-and-forget, confirm, and transactional modes
- **Test Mode**: Built-in testing with message validation and statistics
- **Concurrent Operations**: Support for multiple producers and consumers
- **Secure Authentication**: Password-based authentication with secure input
- **Resource Management**: Configurable limits and resource controls

## Installation

```bash
git clone https://github.com/yourusername/amqp-rust
cd amqp-rust
cargo build --release
```

## Quick Start

### Start AMQP Server

```bash
# Start server on default port 5672
cargo run -- --server

# Start server with custom settings
cargo run -- --server --host 0.0.0.0 --port 5673 --debug
```

### Producer Client

```bash
# Send 100 messages in test mode
cargo run -- --client-producer --test-mode --send-count 100

# Interactive producer mode
cargo run -- --client-producer --exchange my_exchange --topic routing.key

# Multiple concurrent producers
cargo run -- --client-producer --num-producers 5 --send-count 1000
```

### Consumer Client

```bash
# Receive 100 messages in test mode
cargo run -- --client-consumer --test-mode --receive-count 100

# Interactive consumer mode
cargo run -- --client-consumer --queue my_queue --validate-messages

# Multiple concurrent consumers
cargo run -- --client-consumer --num-consumers 3 --prefetch-count 10
```

## Configuration Options

### Connection Settings
- `--host`: Server hostname (default: localhost)
- `--port`: Server port (default: 5672)
- `--user`: Username (default: guest)
- `--password`: Password (prompts if not provided)
- `--vhost`: Virtual host (default: /)

### Exchange Configuration
- `--exchange`: Exchange name
- `--exchange-type`: direct, fanout, topic, headers
- `--exchange-durability`: durable, transient
- `--exchange-auto-delete`: persistent, auto-delete

### Queue Configuration
- `--queue`: Queue name
- `--queue-type`: classic, quorum, stream
- `--queue-durability`: durable, transient
- `--queue-exclusivity`: shared, exclusive

### Message Settings
- `--delivery-mode`: transient, persistent
- `--ack-mode`: auto, manual
- `--prefetch-count`: Number of unacked messages
- `--reliability`: fire, confirm, transactional

### Test Mode
- `--test-mode`: Enable testing with automatic validation
- `--send-count`: Number of messages to send
- `--receive-count`: Number of messages to receive
- `--validate-messages`: Validate message content and sequence

