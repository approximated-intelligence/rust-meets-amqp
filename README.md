# Rust 🤝 AMQP

<img src="https://repository-images.githubusercontent.com/1017573697/35d14245-6fd9-40c7-b5a6-0fdc570a3d06" style="width:100%;height:auto;">

## Vibecoding an AMQP-Broker in Rust

A complete AMQP 0.9.1 server and client implementation written in pure Rust using Tokio. This project will provide both a functional message broker and client library with comprehensive protocol support.

## Features

### 🚀 **Core Functionality**
- **Full AMQP 0.9.1 Protocol** - Complete wire protocol implementation
- **Server Mode** - Standalone message broker with virtual hosts
- **Client Mode** - Producer and consumer implementations
- **Heartbeat Support** - Connection health monitoring
- **Message Validation** - Content verification and sequencing

### 📊 Definitely not **Production Ready** :)
- **Connection Management** - Multi-client support with resource limits
- **Exchange Types** - Direct, fanout, topic, and headers
- **Queue Operations** - Durable, exclusive, auto-delete configurations
- **Reliability Modes** - Fire-and-forget, confirm, transactional
- **QoS Control** - Prefetch limits and acknowledgment modes

### 🔧 **Developer Features**
- **Test Mode** - Automated testing with message counting
- **Interactive Mode** - Real-time message publishing/consuming
- **Comprehensive Logging** - Debug, trace, and quiet modes
- **Concurrent Operations** - Multiple producers/consumers
- **Resource Monitoring** - Connection and message statistics

## Quick Start

### Build
```bash
cargo build --release
```

### Start Server
```bash
# Start AMQP server on default port 5672
./target/release/amqp --server

# Custom configuration
./target/release/amqp --server --host 0.0.0.0 --port 5673 --debug
```

### Producer Client
```bash
# Interactive mode
./target/release/amqp --client-producer --exchange my_exchange --topic my.routing.key

# Test mode - send 1000 messages
./target/release/amqp --client-producer --test-mode --send-count 1000 --send-interval-ms 100
```

### Consumer Client
```bash
# Interactive mode
./target/release/amqp --client-consumer --queue my_queue --ack-mode manual

# Test mode - receive 1000 messages with validation
./target/release/amqp --client-consumer --test-mode --receive-count 1000 --validate-messages
```

## ☸️ Kubernetes Deployment

```bash
kubectl apply -f amqp-chart.yaml
```

## 🐳 Docker Deployment

This project supports multiple deployment strategies for both development and production environments.

### Docker Compose (Recommended for Development)
```bash
# Start RabbitMQ + AMQP client
docker-compose up -d

# View logs
docker-compose logs -f amqp-client

# Interactive shell
docker-compose exec amqp-client sh

# Test producer/consumer
docker-compose exec amqp-client /usr/local/bin/amqp --client-producer --host rabbitmq
docker-compose exec amqp-client /usr/local/bin/amqp --client-consumer --host rabbitmq
```

### Build Docker Image
```bash
# Build the static binary image
docker build -t amqp-client:latest .

# Multi-platform build
docker buildx build --platform linux/amd64,linux/arm64 -t amqp-client:latest .
```

### Standalone Docker
```bash
# Run RabbitMQ
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=guest \
  -e RABBITMQ_DEFAULT_PASS=guest \
  rabbitmq:3-management

# Run AMQP client
docker run -it --rm \
  --link rabbitmq \
  amqp-client:latest \
  /usr/local/bin/amqp --client-producer --host rabbitmq
```

## 🔧 Configuration Options

### Environment Variables
```bash
# RabbitMQ Configuration
RABBITMQ_DEFAULT_USER=guest
RABBITMQ_DEFAULT_PASS=guest
RABBITMQ_DEFAULT_VHOST=/

# AMQP Client Configuration
AMQP_HOST=rabbitmq-service
AMQP_PORT=5672
AMQP_USER=guest
AMQP_PASSWORD=guest
AMQP_VHOST=/
```

## 🔧 Troubleshooting

### Common Issues
```bash
# Check RabbitMQ is running
kubectl get pods -l app=rabbitmq
kubectl logs -f deployment/rabbitmq

# Check AMQP client connectivity
kubectl exec -it deployment/amqp-client -- /usr/local/bin/amqp --host rabbitmq-service --debug

# View RabbitMQ management UI
kubectl port-forward svc/rabbitmq-service 15672:15672
# Open http://localhost:15672 (guest/guest)

# Check network connectivity
kubectl exec -it deployment/amqp-client -- nc -zv rabbitmq-service 5672
```

### Debug Commands
```bash
# Test connection
kubectl run debug --rm -i --tty --image=amqp-client:latest -- /bin/sh
/usr/local/bin/amqp --host rabbitmq-service --client-producer --debug

# Check DNS resolution
kubectl run debug --rm -i --tty --image=alpine -- nslookup rabbitmq-service

# Check resource usage
kubectl top pods
kubectl describe pod <pod-name>
```

## Architecture

### Protocol Implementation
- **Pure Rust** - No external AMQP libraries, built from AMQP 0.9.1 specification
- **Async/Await** - Tokio-based for high concurrency
- **Zero-Copy** - Efficient frame parsing and construction
- **Type Safety** - Strong typing for AMQP values and methods

## Configuration

### Connection Settings
```bash
--host localhost          # Server hostname
--port 5672               # Server port
--vhost /                 # Virtual host
--user guest              # Username
--password guest          # Password (or prompt if empty)
--heartbeat-interval 60   # Heartbeat seconds
--frame-max 131072        # Maximum frame size
```

### Exchange Configuration
```bash
--exchange my_exchange           # Exchange name
--exchange-type direct           # direct|fanout|topic|headers
--exchange-durability durable    # durable|transient
--exchange-auto-delete persistent # persistent|auto-delete
--exchange-internal external     # external|internal
```

### Queue Configuration
```bash
--queue my_queue                    # Queue name
--queue-type classic                # classic|quorum|stream
--queue-durability durable          # durable|transient
--queue-exclusivity shared          # shared|exclusive
--queue-auto-delete persistent      # persistent|auto-delete
```

### Message Handling
```bash
--delivery-mode persistent    # transient|persistent
--ack-mode manual            # auto|manual
--prefetch-count 1           # QoS prefetch limit
--reliability fire           # fire|confirm|transactional
```

## Examples

### Simple Producer/Consumer Test
```bash
# Terminal 1 - Start server
./target/release/amqp --server --debug

# Terminal 2 - Start consumer
./target/release/amqp --client-consumer --queue test_queue --debug

# Terminal 3 - Send messages
./target/release/amqp --client-producer --exchange test_exchange --topic test.key --debug
```

### Load Testing
```bash
# Multiple producers sending 10,000 messages each
./target/release/amqp --client-producer --test-mode --send-count 10000 --num-producers 5 --send-interval-ms 0

# Multiple consumers with validation
./target/release/amqp --client-consumer --test-mode --receive-count 50000 --num-consumers 3 --validate-messages
```

### Advanced Configuration
```bash
# High-throughput producer with confirms
./target/release/amqp --client-producer \
  --exchange high_volume \
  --exchange-type fanout \
  --reliability confirm \
  --send-interval-ms 10 \
  --frame-max 1048576

# Consumer with custom QoS
./target/release/amqp --client-consumer \
  --queue priority_queue \
  --prefetch-count 100 \
  --ack-mode manual \
  --qos-scope per-channel
```

## Development

### Project Structure
```
src/
├── main.rs              # Main entry point and CLI
```

### Building for Production
```bash
# Optimized release build
cargo build --release --target x86_64-unknown-linux-gnu

# With specific CPU features
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

## Performance

### Optimization Tips
- Use `--send-interval-ms 0` for maximum throughput
- Increase `--prefetch-count` for batch processing
- Set `--frame-max` to larger values for big messages
- Use `--reliability fire` for lowest latency

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

---

*Built with ❤️ in Rust*
