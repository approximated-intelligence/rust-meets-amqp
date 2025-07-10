# Functional Rust AMQP Client - Design Guidelines

## 🔧 Functional Rust Design Principles

### 1. Crash-Only Software ⚡

- **Crash early and loudly** - Never continue in undefined/unexpected states
- **Maximum diagnostic information** - Include all relevant context when crashing
- **Fail fast, fail clearly** - Better to crash with good info than corrupt data silently
- **No error recovery** - Let external systems (systemd, Kubernetes, etc.) handle restarts
- **Structured crash information** - Parse and display unexpected frames in readable format
- **Context preservation** - Always include operation context in crash messages

**Core Principle**: *"When in doubt, crash with context. It's better to have a system that fails predictably and provides excellent diagnostics than one that limps along in an undefined state."*

#### When to Crash vs When to Retry:

- **Transient issues** (network, resource contention) → **RETRY with exponential backoff**
- **Logic/protocol/security issues** → **CRASH IMMEDIATELY**  
- **Unknown errors** → **CRASH** (fail-safe default)

**Decision Framework:**
1. Is this an external system issue? → **RETRY**
2. Is this a programming bug? → **CRASH**
3. Is this a protocol violation? → **CRASH**
4. Is this a configuration error? → **CRASH**
5. Is this data corruption? → **CRASH**
6. Is this a transient network issue? → **RETRY**

### 2. Pure Functional Programming

- **No structs with `impl` blocks** - Use enums and pure functions only
- **No mutable state** - Data flows through transformations
- **No intermediate variables** unless used multiple times
- **Atomic operations** - Everything done at once, not incrementally
- **Functional composition** - Chain operations: `outer(parser(receiver(source)))`

### 3. Enum-Attached Strings ✨

- Use enum methods like `wire_name()`, `display_name()` for all string representations
- **Single source of truth** - Never repeat strings across CLI args, settings, and protocol
- **Type-safe conversion** - Enum methods ensure consistency

```rust
ExchangeType::Direct.wire_name()    // "direct"
ExchangeType::Direct.display_name() // "direct"  
DeliveryMode::Persistent.mode_value() // 2
```

### 4. Meaningful Return Values ✨

- **Never return bare `Ok(())`** - Always return useful information
- Return operation details: bytes sent, message counts, connection info
- **Composable results** - Return types that can be easily combined
- Examples: `PublishResult`, `ProducerStats`, `ConnectionInfo`

### 5. Enhanced Error Handling ✨

- **Specific error variants** - `Authentication`, `Connection`, `Timeout`, `Protocol`, `Configuration`
- **Use `.map_err()` throughout** - Provide context at every boundary
- **Structured error messages** - Include relevant details for debugging
- **Crash with full frame analysis** - Parse unexpected frames for debugging

### 6. Slice Destructuring for Protocol Parsing

- **Pattern match on slices** - Destructure buffers directly in match arms
- **Atomic field extraction** - Get all needed fields at once
- **Declarative parsing** - Wire format visible in destructuring patterns
- **Zero-allocation parsing** - Use slice references during parsing

```rust
match payload {
    [class_hi, class_lo, method_hi, method_lo, args @ ..] => {
        let class = u16::from_be_bytes([*class_hi, *class_lo]);
        let method = u16::from_be_bytes([*method_hi, *method_lo]);
        // Process args...
    }
    _ => Err(AmqpError::Protocol("Invalid method frame".into())),
}
```

### 7. Memory Management

- **Use `Vec<u8>` not `BytesMut`** - Immutable operations
- **Work with slices** - Use `&[u8]` until final assembly
- **Borrow checker management** - Store async results when borrowing

### 8. Separation of Concerns

- **Pure construction** - Build data with no I/O (`build_*_frame` functions)
- **Pure I/O** - Handle input/output with no business logic (`send_frame`, `receive_frame`)
- **Pure parsing** - Extract data with no I/O (`parse_*` functions)
- **Composed operations** - Combine pure functions cleanly (`send_and_expect_response`)

## 🎯 Configuration Management

### 9. Total Configurability

- **All AMQP components configurable** - Exchanges, queues, channels, delivery modes
- **CLI-driven configuration** - Everything controllable via command line
- **Sane defaults** - Reasonable defaults for all options
- **Configuration validation** - Incompatible combinations cause startup errors

### 10. Delivery Pattern Support

- **At-most-once**: `--ack-mode auto --delivery-mode transient --reliability fire`
- **At-least-once**: `--ack-mode manual --delivery-mode persistent --reliability confirm`
- **Exactly-once**: `--ack-mode manual --delivery-mode persistent --reliability tx`

## 🧪 Testing and Automation

### 11. Testable Design

- **Automated testing support** - `--send-count` and `--receive-count` for deterministic tests
- **External timeout handling** - Tests use `timeout` command to prevent hanging
- **Comprehensive test scenarios** - Cover all major AMQP use cases
- **Validation logic** - Tests verify message counts and completion status

## 📊 Observability and Debugging

### 12. Layered Logging

- **Error**: Failures with context and suggested actions (always shown)
- **Info**: Essential user-facing status changes (controllable with `--quiet`)
- **Debug**: Operational details (shown with `--debug`)
- **Trace**: Low-level protocol details (shown with `--trace`)

**Logging Control:**
- Default: Error + Info
- `--quiet`: Error only
- `--debug`: Error + Info + Debug  
- `--trace`: Error + Info + Debug + Trace

### 13. Meaningful Statistics

- **Operation metrics** - Bytes sent/received, message counts, error counts
- **Performance data** - Message sizes, transmission rates
- **Completion status** - Success/failure with detailed breakdown

## ⚡ Key Anti-Patterns to Avoid

- ❌ Nested Result types (`Ok(Ok())`)
- ❌ Borrowing from temporary values
- ❌ Bare `Ok()` returns
- ❌ Repeated string literals
- ❌ Complex borrowing relationships
- ❌ Missing trait derivations (`Clone`, `Debug`)
- ❌ Silent error handling
- ❌ Hardcoded configuration values
- ❌ Continuing execution in undefined states
- ❌ Swallowing errors without context

## 🔥 Retry Logic Implementation

```rust
// Exponential backoff with jitter
async fn connect_with_retry(host: &str, port: u16, max_retries: u32) -> Result<TcpStream> {
    let base_delay = Duration::from_millis(100);
    let max_delay = Duration::from_secs(30);
    
    for attempt in 1..=max_retries {
        match TcpStream::connect((host, port)).await {
            Ok(stream) => return Ok(stream),
            Err(e) if is_retryable(&e) => {
                if attempt == max_retries {
                    return Err(AmqpError::Connection(
                        format!("Failed after {} attempts: {}", max_retries, e)
                    ));
                }
                let jitter = random_duration(Duration::from_millis(50));
                let delay = std::cmp::min(
                    base_delay * 2_u32.pow(attempt - 1) + jitter,
                    max_delay
                );
                tokio::time::sleep(delay).await;
            }
            Err(e) => return Err(AmqpError::Connection(
                format!("Non-retryable error: {}", e)
            )),
        }
    }
    unreachable!()
}

fn is_retryable(error: &std::io::Error) -> bool {
    match error.kind() {
        std::io::ErrorKind::ConnectionRefused => true,
        std::io::ErrorKind::ConnectionReset => true,
        std::io::ErrorKind::ConnectionAborted => true,
        std::io::ErrorKind::TimedOut => true,
        std::io::ErrorKind::Interrupted => true,
        _ => false,
    }
}
```

## 🛡️ Error Classification

### Crash Immediately:
- **Protocol Violations**: Unexpected frame sequences, malformed data
- **Programming Errors**: Configuration incompatibilities, logic bugs
- **Resource Corruption**: Content size mismatches, invalid state
- **Security Issues**: Authentication failures, permission denied

### Retry with Backoff:
- **Network Issues**: Connection refused, timeouts, resets
- **Broker Unavailability**: Temporary overload, resource locks
- **Resource Contention**: Queue conflicts (specific cases only)

### Never Retry:
- **Configuration Errors**: Invalid CLI arguments, bad parameters
- **Protocol Specification Violations**: Unknown methods, invalid classes
- **Data Corruption**: Message validation failures

## 🎖️ Design Philosophy Summary

**"Functional purity, explicit data flow, and composability over object-oriented abstractions. Build complex operations by composing simple functions rather than using mutable state. Every function has a single, obvious purpose. When things go wrong, crash immediately with enough context to diagnose the root cause."**

This results in code that's **predictable, testable, debuggable, and easy to reason about** - functional programming principles applied to systems programming with excellent error handling and comprehensive configurability.

The system prioritizes **correctness over availability** and **debuggability over resilience**, making it ideal for environments where:
- External orchestration handles service restarts
- Fast restart is better than slow corruption  
- Clear failure modes are easier to debug than mysterious behavior
- Operational simplicity reduces complexity and maintenance burden

## 📋 Implementation Checklist

When implementing new features:

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
