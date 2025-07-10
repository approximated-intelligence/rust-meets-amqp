# AMQP 0.9.1 Server Design Criteria

## 🔧 Core Functional Rust Principles

### 1. Crash-Only Software ⚡

- **Crash early and loudly** with maximum diagnostic information
- **No error recovery** - let external systems handle restarts
- **Fail fast on protocol violations, logic errors, configuration issues**
- **Retry only for transient network issues** with exponential backoff

**Decision Framework:**
```
Protocol violations → CRASH (with frame analysis)
Network issues → RETRY (exponential backoff)  
Unknown errors → CRASH (fail-safe default)
Configuration errors → CRASH (startup validation)
```

### 2. Pure Functional Programming

- **No structs with `impl` blocks** - enums and pure functions only
- **No mutable state** - data flows through transformations
- **Atomic operations** - everything done at once
- **Functional composition** - chain operations: `outer(parser(receiver(source)))`

### 3. Enum-Attached Strings ✨

- **Single source of truth** - `ExchangeType::Direct.wire_name()` → `"direct"`
- **Type-safe conversion** - no repeated strings across CLI/protocol/display
- **Consistency guarantee** - enum methods ensure protocol compliance

```rust
ExchangeType::Direct.wire_name()    // "direct"
ExchangeType::Direct.display_name() // "direct"  
DeliveryMode::Persistent.mode_value() // 2
```

### 4. Meaningful Return Values

- **Never return bare `Ok(())`** - always return useful information
- **Composable results** - `PublishResult`, `ConnectionInfo`, `ProducerStats`
- **Operation metrics** - bytes sent/received, message counts, delivery confirmations

## 🎯 AMQP 0.9.1 Server Architecture

### 5. Event-Driven Coroutine System

```rust
enum ServerCoroutine {
    AwaitingAuth(Box<dyn FnOnce(ConnectionStartOk) -> Result<AuthResult>>),
    ProcessingMethod(Box<dyn FnOnce(MethodFrame) -> Result<MethodResponse>>),
    // Dynamic function generation with closures
}
```

**Core Pattern:**
- **Capture state with closures** - dynamic function generation
- **Pair with coroutines** - correct handler gets called at packet reception
- **Event-driven execution** - no polling or busy waiting

### 6. Concurrency Model

- **tokio::select! with mpsc channels** for single-threaded, many-coroutine design
- **One task per connection** - scales to 1000+ concurrent connections
- **No global state management** - each connection independent
- **Dynamic function creation using enums**

```rust
// Per-connection isolation
async fn handle_connection(mut conn: ConnectionHandler) -> Result<()> {
    loop {
        tokio::select! {
            frame = receive_frame(&mut conn.socket) => {
                // Route to appropriate coroutine based on connection state
            }
            cmd = conn.server_rx.recv() => {
                // Handle server commands
            }
        }
    }
}
```

### 7. Event Sourcing with SQLite

- **Crash-only persistence** - rely on SQLite's crash safety
- **Event sourcing schema** - all state changes as events
- **Periodic checkpointing** to squash events for faster recovery
- **Persistent across server crashes** - metadata and delivery mode 2 messages
- **VHost foreign keys** in all relations for complete isolation

```sql
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    vhost TEXT NOT NULL,
    event_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    data TEXT NOT NULL -- JSON
);
```

## 📊 AMQP Compliance Requirements

### 8. MUST Implement (AMQP 0.9.1 Spec)

**Core Protocol:**
- **Channels, queues, exchanges, bindings, basic routing**
- **Direct exchanges** (minimum), all exchange types (goal)
- **Delivery modes** (1=transient, 2=persistent)
- **Frame types**: method, header, body, heartbeat
- **Basic.ack, Basic.reject, Basic.nack**
- **Fragmented messages** across frames
- **SASL authentication** (PLAIN)
- **Minimum frame size** 4096 bytes
- **Channel.Flow** for flow control (mandatory to implement)

### 9. Resource Limits & Management

**Configurable Limits:**
- **Max channels per connection**: `--server-max-channels-per-connection` (default: 2047)
- **Max connections**: Hard limit enforced
- **Max queues/exchanges per vhost**: Configurable limits
- **Max bindings per queue**: SHOULD support ≥4, configurable
- **Max consumers per queue**: SHOULD support ≥16, configurable

**Naming & Size Limits:**
- **Queue names**: Up to 255 bytes UTF-8
- **"amq." prefix reserved** for server-defined entities
- **Frame max negotiation**: Minimum 512 bytes, configurable maximum

### 10. Exchange Types & Routing

**Direct Exchange:**
```rust
fn route_direct(routing_key: &str, queues: &HashMap<String, QueueInfo>) -> Vec<String>
```
- Exact routing key match to bound queues

**Fanout Exchange:**
```rust
fn route_fanout(exchange: &str, queues: &HashMap<String, QueueInfo>) -> Vec<String>
```
- Broadcast to all bound queues (ignore routing key)

**Topic Exchange:**
```rust
fn route_topic(routing_key: &str, exchange: &str, queues: &HashMap<String, QueueInfo>) -> Vec<String>
```
- Wildcard pattern matching: `*` = one word, `#` = zero or more words
- Example: `stock.*.nyse` matches `stock.ibm.nyse` but not `stock.ibm.nasdaq`

**Headers Exchange:**
```rust
fn route_headers(message_headers: &HashMap<String, String>, exchange: &str, queues: &HashMap<String, QueueInfo>) -> Vec<String>
```
- Attribute-based routing with `x-match` argument:
  - `x-match: all` = all specified headers must match
  - `x-match: any` = at least one header must match

**Default Exchange:**
```rust
fn route_default(routing_key: &str, queues: &HashMap<String, QueueInfo>) -> Vec<String>
```
- Route directly to queue with same name as routing key

### 11. Virtual Host Isolation

**Complete Separation:**
- **VHost specification**: `--vhost-list "/:/host1:/host2"`
- **Foreign key relations** for complete resource isolation
- **Independent authentication** per vhost
- **Separate resource limits** per vhost

```rust
struct VirtualHost {
    name: String,
    exchanges: HashMap<String, ExchangeInfo>,
    queues: HashMap<String, QueueInfo>,
}
```

## 🛡️ Reliability & Configuration

### 12. Authentication & Security

**Password File Format:**
```
username:sha256_hash:salt:vhost1,vhost2,vhost3
guest:5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8:defaultsalt:/
```

**SASL PLAIN Authentication:**
- Format: `\0username\0password`
- Salted SHA-256 password hashing
- Per-user vhost access control

### 13. Production Features

**Reliability Modes:**
- **Publisher Confirms**: Async acknowledgment of published messages
- **Transactions**: Atomic commit/rollback of message operations
- **Flow Control**: Channel.Flow to pause/resume delivery

**Monitoring & Management:**
- **HTTP management interface** on configurable port (default: 15672)
- **Queue/exchange inspection endpoints**
- **Connection and channel statistics**
- **Resource usage metrics**

### 14. Resource Exhaustion Handling

```rust
// Graceful limit enforcement
if vhost_state.exchanges.len() >= self.config.max_exchanges_per_vhost as usize {
    return Err(AmqpError::Configuration(format!(
        "Exchange limit reached for vhost '{}'", vhost
    )));
}
```

**Configurable Limits:**
- Connection-level: channel count, frame size
- VHost-level: queues, exchanges, total memory
- Queue-level: bindings, consumers, message depth
- Message-level: size limits, TTL, priorities

## 🔥 Implementation Patterns

### 15. Memory Management

- **Use `Vec<u8>` not `BytesMut`** - immutable operations
- **Slice destructuring** for protocol parsing
- **Zero-allocation parsing** where possible
- **Borrow checker friendly** - store async results when borrowing

```rust
match payload {
    [class_hi, class_lo, method_hi, method_lo, args @ ..] => {
        let class = u16::from_be_bytes([*class_hi, *class_lo]);
        let method = u16::from_be_bytes([*method_hi, *method_lo]);
        parse_method_args(args)
    }
    _ => Err(AmqpError::Protocol("Invalid method frame".into())),
}
```

### 16. Error Classification & Handling

**Crash Immediately:**
- Protocol violations (unexpected frame sequences, malformed data)
- Programming errors (configuration incompatibilities, logic bugs)
- Resource corruption (content size mismatches, invalid state)
- Security issues (authentication failures, permission denied)

**Retry with Backoff:**
- Network issues (connection refused, timeouts, resets)
- Broker unavailability (temporary overload, resource locks)
- Resource contention (specific transient cases only)

**Never Retry:**
- Configuration errors (invalid CLI arguments, bad parameters)
- Protocol specification violations (unknown methods, invalid classes)
- Data corruption (message validation failures)

### 17. Separation of Concerns

**Pure Functions by Category:**

```rust
// Pure construction (no I/O)
fn build_connection_start_frame() -> Vec<u8>
fn build_basic_publish_frame(exchange: &str, routing_key: &str) -> Vec<u8>

// Pure I/O (no business logic)
async fn send_frame(stream: &mut TcpStream, frame: Vec<u8>) -> Result<usize>
async fn receive_frame(stream: &mut TcpStream) -> Result<Vec<u8>>

// Pure parsing (no I/O)
fn parse_frame(data: &[u8]) -> Result<(u8, u16, &[u8])>
fn parse_method_frame(payload: &[u8]) -> Result<(u16, u16, &[u8])>

// Composed operations (combine pure functions)
async fn send_and_expect_response(
    stream: &mut TcpStream, 
    frame: Vec<u8>, 
    expected_class: u16, 
    expected_method: u16
) -> Result<Vec<u8>>
```

## 🎖️ Design Philosophy Summary

**"Event-driven functional purity with crash-only reliability. Build a production-ready AMQP broker using coroutines and closures for dynamic state management, SQLite for crash-safe persistence, and functional composition over object-oriented abstractions. When things go wrong, crash immediately with full context for rapid diagnosis and restart."**

### Core Values:

1. **Predictability** - Functional purity makes behavior deterministic
2. **Testability** - Pure functions are easy to unit test
3. **Debuggability** - Crash-only design provides excellent diagnostics
4. **Operational Simplicity** - External orchestration handles complexity
5. **Correctness over Availability** - Better to fail fast than corrupt slowly
6. **Performance through Simplicity** - Event-driven single-threaded model scales

### Target Environment:

- **Container orchestration** (Kubernetes, Docker Swarm)
- **External monitoring** (Prometheus, health checks)
- **Fast restart capability** (< 5 second recovery time)
- **Persistent storage** (networked volumes, local SSDs)
- **High connection count** (1000+ concurrent clients)

This approach prioritizes **operational excellence** and **debugging ease** over traditional high-availability patterns, making it ideal for modern container-based deployments where external systems handle service lifecycle management.

## 📋 Implementation Checklist

When implementing new server features:

- [ ] Does this function return meaningful information?
- [ ] Are all string values defined via enum methods?
- [ ] Is error handling specific and contextual?
- [ ] Does this crash appropriately on undefined states?
- [ ] Is retry logic applied only to transient issues?
- [ ] Are all configuration options exposed via CLI?
- [ ] Is logging appropriately leveled and controllable?
- [ ] Are slice destructuring patterns used for parsing?
- [ ] Does this compose cleanly with other functions?
- [ ] Is the separation of concerns maintained?
- [ ] Are resource limits properly enforced?
- [ ] Is event sourcing used for persistent state changes?
- [ ] Are coroutines used for connection state management?
- [ ] Is virtual host isolation maintained?
- [ ] Are AMQP 0.9.1 compliance requirements met?
