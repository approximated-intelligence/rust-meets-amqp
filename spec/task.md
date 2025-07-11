# Add Message Faucet/Drain and Test Mode Implementation

## Task Overview
Add a **Message Faucet** (EPOCH timestamp generator, one per second) and **Message Drain** to the existing code to create a **Test mode** (`--test`) for AMQP consumers and producers.

## Requirements

### Test Mode Functionality
- **Command line flag**: `--test` enables test mode
- **Message count**: `--test-count` specifies number of messages to send/receive
- **Success criteria**:
  - Correct number of messages sent and received
  - All messages received in full with valid checksum
  - Message format: `text:sha256(text)`
  - All messages handled according to delivery settings

### Implementation Notes
- Add any missing method implementations to AmqpClient or AmqpServer as needed
- Message Source should generate current EPOCH timestamp once per second
- Message Drain should validate the checksum format

### Logging Requirements
- **Levels**: Trace, Debug, Info, Warn (≈ Error), Error (often Fatal)
- **Control flags**:
  - Default: Error + Info
  - `--quiet`: Error only
  - `--debug`: Error + Info + Debug
  - `--trace`: Error + Info + Debug + Trace
- **Output**: All logs to stderr
- **Functions**: `log_trace()`, `log_debug()`, `log_info()`, `log_warn()`, `log_error()`, `fatal()`

## Core Design Principles (as referenced in the original requirements)

### Crash-Only Software
- **Principle**: This is crash-only software - undefined states and malformed input are considered Errors
- **Transient issues** (network, resource contention) → RETRY with exponential backoff
- **Logic/protocol/security issues** → CRASH IMMEDIATELY
- **Unknown errors** → CRASH (fail-safe default)
- **Distinction**: Warn ≈ Error, and Error is often Fatal (crash coroutine vs whole program)

### Functional Programming Approach
- No structs with `impl` blocks - use enums and pure functions only
- No mutable state - data flows through transformations
- Atomic operations - everything done at once
- Functional composition - chain operations

### Key Implementation Patterns
- **Enum-attached strings**: Use enum methods for string representations (single source of truth)
- **Meaningful return values**: Never return bare `Ok()` - always return useful information
- **Slice destructuring**: Use for protocol parsing
- **Separation of concerns**: Pure construction, pure I/O, pure parsing, composed operations
- **Error context**: Use `.map_err()` to provide context at every boundary

### Configuration Philosophy
- Total configurability via CLI arguments
- Delivery pattern support:
  - At-most-once: `--ack-mode auto --delivery-mode transient --reliability fire`
  - At-least-once: `--ack-mode manual --delivery-mode persistent --reliability confirm`
  - Exactly-once: `--ack-mode manual --delivery-mode persistent --reliability tx`

## Implementation Checklist
- [ ] Message Source generates EPOCH timestamps (one per second)
- [ ] Message Drain validates format `text:sha256(text)` and checksum
- [ ] Test mode exits with success/failure based on criteria
- [ ] Missing AmqpClient/AmqpServer methods added as discovered
- [ ] Logging functions implemented with appropriate stderr output
- [ ] Command line flags `--test` and `--test-count` added
- [ ] Functions return meaningful information (not bare `Ok()`)
- [ ] Error handling follows crash/retry framework
- [ ] Informational output uses `log_info()` (suppressible with `--quiet`)
- [ ] AmqpServer implementation is preserved in all its functionalty, though you are allowed to fix errors you come across
