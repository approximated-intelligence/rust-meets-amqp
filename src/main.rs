use clap::{Parser, ValueEnum};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

// === LOGGING INFRASTRUCTURE ===

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogLevel {
    Error,
    Info,
    Debug,
    Trace,
}

#[derive(Debug, Clone)]
pub struct LogConfig {
    pub debug: bool,
    pub trace: bool,
    pub quiet: bool,
}

impl LogConfig {
    pub fn should_log(&self, level: LogLevel) -> bool {
        match level {
            LogLevel::Error => true, // Always log errors
            LogLevel::Info => !self.quiet,
            LogLevel::Debug => self.debug || self.trace,
            LogLevel::Trace => self.trace,
        }
    }
}

// Pure logging functions
pub fn log_error(config: &LogConfig, message: &str) {
    if config.should_log(LogLevel::Error) {
        eprintln!("[ERROR] {}", message);
    }
}

pub fn log_info(config: &LogConfig, message: &str) {
    if config.should_log(LogLevel::Info) {
        eprintln!("[INFO] {}", message);
    }
}

pub fn log_debug(config: &LogConfig, message: &str) {
    if config.should_log(LogLevel::Debug) {
        eprintln!("[DEBUG] {}", message);
    }
}

pub fn log_trace(config: &LogConfig, message: &str) {
    if config.should_log(LogLevel::Trace) {
        eprintln!("[TRACE] {}", message);
    }
}

// === MESSAGE SOURCE AND DRAIN ===

#[derive(Debug, Clone)]
pub struct MessageSource {
    pub prefix: String,
    pub include_timestamp: bool,
}

impl MessageSource {
    pub fn generate_message(&self, sequence: usize) -> Vec<u8> {
        let timestamp = if self.include_timestamp {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
        } else {
            0
        };

        format!("{} {} @ {}", self.prefix, sequence, timestamp).into_bytes()
    }
}

#[derive(Debug, Clone)]
pub struct MessageDrain {
    pub expected_prefix: Option<String>,
    pub validate_sequence: bool,
    pub last_sequence: Option<usize>,
}

impl MessageDrain {
    pub fn validate_message(&self, body: &[u8]) -> Result<MessageValidation> {
        let message = String::from_utf8_lossy(body);
        let parts: Vec<&str> = message.split(' ').collect();

        // Validate prefix if expected
        if let Some(expected) = &self.expected_prefix {
            if parts.is_empty() || parts[0] != expected {
                return Ok(MessageValidation {
                    valid: false,
                    sequence: None,
                    timestamp: None,
                    error: Some(format!(
                        "Expected prefix '{}', got '{}'",
                        expected,
                        parts.get(0).unwrap_or(&"")
                    )),
                });
            }
        }

        // Extract sequence if validating
        let sequence = if self.validate_sequence && parts.len() > 1 {
            parts[1].parse::<usize>().ok()
        } else {
            None
        };

        // Extract timestamp
        let timestamp = if parts.len() > 3 && parts[2] == "@" {
            parts[3].parse::<u128>().ok()
        } else {
            None
        };

        Ok(MessageValidation {
            valid: true,
            sequence,
            timestamp,
            error: None,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MessageValidation {
    pub valid: bool,
    pub sequence: Option<usize>,
    pub timestamp: Option<u128>,
    pub error: Option<String>,
}

// === SHARED AMQP LIBRARY ===

#[derive(Error, Debug)]
pub enum AmqpError {
    #[error("IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("Protocol: {0}")]
    Protocol(String),
    #[error("Timeout: {0}")]
    Timeout(String),
    #[error("Authentication: {0}")]
    Authentication(String),
    #[error("Connection: {0}")]
    Connection(String),
    #[error("Heartbeat: {0}")]
    Heartbeat(String),
    #[error("Configuration: {0}")]
    Configuration(String),
    #[error("Channel: {0}")]
    Channel(String),
    #[error("Resource: {0}")]
    Resource(String),
}

type Result<T> = std::result::Result<T, AmqpError>;

// === OPERATION RESULTS (Meaningful Return Values) ===
#[derive(Debug, Clone)]
pub struct ConnectionResult {
    pub client_id: String,
    pub server_addr: SocketAddr,
    pub negotiated_params: ConnectionParams,
    pub connection_time: Duration,
}

#[derive(Debug, Clone)]
pub struct PublishResult {
    pub bytes_sent: usize,
    pub delivery_tag: Option<u64>,
    pub confirmed: bool,
}

#[derive(Debug, Clone)]
pub struct ConsumeResult {
    pub consumer_tag: String,
    pub queue_name: String,
    pub prefetch_count: u16,
}

#[derive(Debug, Clone)]
pub struct ServerStartResult {
    pub server_id: String,
    pub bind_address: SocketAddr,
    pub resource_limits: ResourceLimits,
}

// === AMQP PROTOCOL CONSTANTS ===
mod constants {
    pub const AMQP_PROTOCOL_HEADER: &[u8] = b"AMQP\x00\x00\x09\x01";
    pub const FRAME_END_BYTE: u8 = 0xCE;
    pub const FRAME_METHOD: u8 = 1;
    pub const FRAME_CONTENT_HEADER: u8 = 2;
    pub const FRAME_CONTENT_BODY: u8 = 3;
    pub const FRAME_HEARTBEAT: u8 = 8;

    pub const CONNECTION_CLASS: u16 = 10;
    pub const CONNECTION_START: u16 = 10;
    pub const CONNECTION_START_OK: u16 = 11;
    pub const CONNECTION_TUNE: u16 = 30;
    pub const CONNECTION_TUNE_OK: u16 = 31;
    pub const CONNECTION_OPEN: u16 = 40;
    pub const CONNECTION_OPEN_OK: u16 = 41;
    pub const CONNECTION_CLOSE: u16 = 50;
    pub const CONNECTION_CLOSE_OK: u16 = 51;

    pub const CHANNEL_CLASS: u16 = 20;
    pub const CHANNEL_OPEN: u16 = 10;
    pub const CHANNEL_OPEN_OK: u16 = 11;
    pub const CHANNEL_CLOSE: u16 = 40;
    pub const CHANNEL_CLOSE_OK: u16 = 41;

    pub const EXCHANGE_CLASS: u16 = 40;
    pub const EXCHANGE_DECLARE: u16 = 10;
    pub const EXCHANGE_DECLARE_OK: u16 = 11;

    pub const QUEUE_CLASS: u16 = 50;
    pub const QUEUE_DECLARE: u16 = 10;
    pub const QUEUE_DECLARE_OK: u16 = 11;
    pub const QUEUE_BIND: u16 = 20;
    pub const QUEUE_BIND_OK: u16 = 21;

    pub const BASIC_CLASS: u16 = 60;
    pub const BASIC_QOS: u16 = 10;
    pub const BASIC_QOS_OK: u16 = 11;
    pub const BASIC_CONSUME: u16 = 20;
    pub const BASIC_CONSUME_OK: u16 = 21;
    pub const BASIC_PUBLISH: u16 = 40;
    pub const BASIC_DELIVER: u16 = 60;
    pub const BASIC_ACK: u16 = 80;
    pub const BASIC_REJECT: u16 = 90;
    pub const BASIC_NACK: u16 = 120;

    pub const CONFIRM_CLASS: u16 = 85;
    pub const CONFIRM_SELECT: u16 = 10;
    pub const CONFIRM_SELECT_OK: u16 = 11;
}

// === SHARED VALUE SYSTEM ===
#[derive(Debug, Clone)]
pub enum Value {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    ShortString(String),
    LongString(Vec<u8>),
    Flags(Vec<bool>),
    Table(HashMap<String, String>),
}

impl Value {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::U8(v) => vec![*v],
            Value::U16(v) => v.to_be_bytes().to_vec(),
            Value::U32(v) => v.to_be_bytes().to_vec(),
            Value::U64(v) => v.to_be_bytes().to_vec(),
            Value::ShortString(s) => [vec![s.len() as u8], s.as_bytes().to_vec()].concat(),
            Value::LongString(data) => {
                [(data.len() as u32).to_be_bytes().to_vec(), data.clone()].concat()
            }
            Value::Flags(flags) => vec![flags
                .iter()
                .take(8)
                .enumerate()
                .fold(0u8, |acc, (i, &flag)| acc | ((flag as u8) << i))],
            Value::Table(_) => vec![0, 0, 0, 0], // Empty table for simplicity
        }
    }
}

// === SHARED CONNECTION STATES ===
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    Initial,
    StartReceived,
    StartOkSent,
    TuneReceived,
    TuneOkSent,
    OpenSent,
    Open,
    Closing,
    Closed,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChannelState {
    Closed,
    Opening,
    Open,
    Closing,
}

// === ENUM-ATTACHED STRINGS ===
#[derive(Debug, Clone, ValueEnum)]
pub enum ExchangeType {
    Direct,
    Fanout,
    Topic,
    Headers,
}

impl ExchangeType {
    pub fn wire_name(&self) -> &'static str {
        match self {
            ExchangeType::Direct => "direct",
            ExchangeType::Fanout => "fanout",
            ExchangeType::Topic => "topic",
            ExchangeType::Headers => "headers",
        }
    }

    pub fn display_name(&self) -> &'static str {
        self.wire_name()
    }
}

#[derive(Debug, Clone, ValueEnum)]
pub enum DeliveryMode {
    Transient,
    Persistent,
}

impl DeliveryMode {
    pub fn mode_value(&self) -> u8 {
        match self {
            DeliveryMode::Transient => 1,
            DeliveryMode::Persistent => 2,
        }
    }

    pub fn wire_name(&self) -> &'static str {
        match self {
            DeliveryMode::Transient => "transient",
            DeliveryMode::Persistent => "persistent",
        }
    }
}

#[derive(Debug, Clone, ValueEnum)]
pub enum AckMode {
    Auto,
    Manual,
}

impl AckMode {
    pub fn wire_name(&self) -> &'static str {
        match self {
            AckMode::Auto => "auto",
            AckMode::Manual => "manual",
        }
    }

    pub fn is_no_ack(&self) -> bool {
        matches!(self, AckMode::Auto)
    }
}

// === SHARED HEARTBEAT STATE ===
#[derive(Debug, Clone)]
pub struct HeartbeatState {
    interval: Duration,
    last_received: Instant,
    last_sent: Instant,
}

impl HeartbeatState {
    pub fn new(interval_seconds: u16) -> Self {
        let now = Instant::now();
        Self {
            interval: Duration::from_secs(interval_seconds as u64),
            last_received: now,
            last_sent: now,
        }
    }

    pub fn is_enabled(&self) -> bool {
        !self.interval.is_zero()
    }

    pub fn update_received(&mut self) {
        self.last_received = Instant::now();
    }

    pub fn update_sent(&mut self) {
        self.last_sent = Instant::now();
    }

    pub fn should_send_heartbeat(&self) -> bool {
        self.is_enabled() && self.last_sent.elapsed() >= self.interval
    }

    pub fn is_connection_dead(&self) -> bool {
        self.is_enabled() && self.last_received.elapsed() > self.interval * 2
    }
}

// === SHARED CONNECTION TYPES ===
#[derive(Debug, Clone)]
pub struct ConnectionParams {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

impl Default for ConnectionParams {
    fn default() -> Self {
        Self {
            channel_max: 2047,
            frame_max: 131072,
            heartbeat: 60,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub host: String,
    pub port: u16,
    pub vhost: String,
    pub username: String,
    pub params: ConnectionParams,
}

// === SHARED MESSAGE TYPES ===
#[derive(Debug, Clone)]
pub struct DeliveryInfo {
    pub delivery_tag: u64,
    pub consumer_tag: String,
    pub exchange: String,
    pub routing_key: String,
    pub redelivered: bool,
}

#[derive(Debug, Clone)]
pub struct ContentHeader {
    pub class_id: u16,
    pub weight: u16,
    pub body_size: u64,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub delivery_info: DeliveryInfo,
    pub content_header: ContentHeader,
    pub body: Vec<u8>,
}

// === SHARED RESOURCE LIMITS ===
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    pub max_connections: usize,
    pub max_channels_per_connection: u16,
    pub max_queues_per_vhost: usize,
    pub max_exchanges_per_vhost: usize,
    pub max_bindings_per_queue: usize,
    pub max_consumers_per_queue: usize,
    pub max_frame_size: u32,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_connections: 1000,
            max_channels_per_connection: 2047,
            max_queues_per_vhost: 10000,
            max_exchanges_per_vhost: 1000,
            max_bindings_per_queue: 100,
            max_consumers_per_queue: 100,
            max_frame_size: 131072,
        }
    }
}

// === SHARED AMQP LIBRARY ===
pub struct AmqpLib;

impl AmqpLib {
    // === FRAME I/O ===
    pub async fn send_frame(
        stream: &mut TcpStream,
        frame: Vec<u8>,
        log_config: &LogConfig,
    ) -> Result<usize> {
        log_trace(log_config, &format!("Sending frame: {} bytes", frame.len()));
        let bytes_written = frame.len();
        stream.write_all(&frame).await.map_err(AmqpError::Io)?;
        stream.flush().await.map_err(AmqpError::Io)?;
        Ok(bytes_written)
    }

    pub async fn receive_frame(stream: &mut TcpStream, log_config: &LogConfig) -> Result<Vec<u8>> {
        let mut header = [0u8; 7];
        stream
            .read_exact(&mut header)
            .await
            .map_err(AmqpError::Io)?;

        let payload_len = u32::from_be_bytes([header[3], header[4], header[5], header[6]]) as usize;
        let mut payload = vec![0u8; payload_len + 1];
        stream
            .read_exact(&mut payload)
            .await
            .map_err(AmqpError::Io)?;

        log_trace(
            log_config,
            &format!(
                "Received frame: type={}, channel={}, payload_len={}",
                header[0],
                u16::from_be_bytes([header[1], header[2]]),
                payload_len
            ),
        );

        Ok([&header[..], &payload].concat())
    }

    pub async fn send_frame_with_heartbeat(
        stream: &mut TcpStream,
        frame: Vec<u8>,
        heartbeat_state: &mut HeartbeatState,
        log_config: &LogConfig,
    ) -> Result<usize> {
        let bytes_sent = Self::send_frame(stream, frame, log_config).await?;
        heartbeat_state.update_sent();
        Ok(bytes_sent)
    }

    pub async fn receive_frame_with_heartbeat(
        stream: &mut TcpStream,
        heartbeat_state: &mut HeartbeatState,
        log_config: &LogConfig,
    ) -> Result<Vec<u8>> {
        loop {
            if heartbeat_state.is_connection_dead() {
                return Err(AmqpError::Heartbeat(format!(
                    "Connection timeout - no traffic received for {}s",
                    heartbeat_state.last_received.elapsed().as_secs()
                )));
            }

            let timeout_duration = if heartbeat_state.is_enabled() {
                heartbeat_state.interval / 2
            } else {
                Duration::from_secs(30)
            };

            match tokio::time::timeout(timeout_duration, Self::receive_frame(stream, log_config))
                .await
            {
                Ok(frame_result) => {
                    let frame = frame_result?;
                    let (frame_type, channel, _payload) = Self::parse_frame(&frame)?;

                    heartbeat_state.update_received();

                    match frame_type {
                        constants::FRAME_HEARTBEAT => {
                            if channel != 0 {
                                return Err(AmqpError::Protocol(format!(
                                    "Invalid heartbeat frame: channel must be 0, got {}",
                                    channel
                                )));
                            }
                            log_trace(log_config, "Received heartbeat frame");
                        }
                        _ => {
                            return Ok(frame);
                        }
                    }
                }
                Err(_) => {
                    if heartbeat_state.should_send_heartbeat() {
                        log_trace(log_config, "Sending heartbeat frame");
                        Self::send_frame(stream, Self::build_heartbeat_frame(), log_config).await?;
                        heartbeat_state.update_sent();
                    }
                }
            }
        }
    }

    // === FRAME PARSING ===
    pub fn parse_frame(data: &[u8]) -> Result<(u8, u16, &[u8])> {
        match data {
            [frame_type, ch_hi, ch_lo, len_a, len_b, len_c, len_d, payload @ .., constants::FRAME_END_BYTE] =>
            {
                let channel = u16::from_be_bytes([*ch_hi, *ch_lo]);
                let payload_len = u32::from_be_bytes([*len_a, *len_b, *len_c, *len_d]) as usize;

                if payload.len() == payload_len {
                    Ok((*frame_type, channel, &payload[..payload_len]))
                } else {
                    Err(AmqpError::Protocol("Payload length mismatch".into()))
                }
            }
            _ => Err(AmqpError::Protocol("Invalid frame format".into())),
        }
    }

    pub fn parse_method_frame(payload: &[u8]) -> Result<(u16, u16, &[u8])> {
        match payload {
            [class_hi, class_lo, method_hi, method_lo, args @ ..] => Ok((
                u16::from_be_bytes([*class_hi, *class_lo]),
                u16::from_be_bytes([*method_hi, *method_lo]),
                args,
            )),
            _ => Err(AmqpError::Protocol("Invalid method frame".into())),
        }
    }

    // === FRAME BUILDERS ===
    pub fn build_raw_frame(frame_type: u8, channel: u16, payload: &[u8]) -> Vec<u8> {
        [
            &[frame_type],
            &channel.to_be_bytes()[..],
            &(payload.len() as u32).to_be_bytes(),
            payload,
            &[constants::FRAME_END_BYTE],
        ]
        .concat()
    }

    pub fn build_method_frame(channel: u16, class: u16, method: u16, args: &[Value]) -> Vec<u8> {
        Self::build_raw_frame(
            constants::FRAME_METHOD,
            channel,
            &[
                class.to_be_bytes().to_vec(),
                method.to_be_bytes().to_vec(),
                args.iter().flat_map(|v| v.to_bytes()).collect(),
            ]
            .concat(),
        )
    }

    pub fn build_heartbeat_frame() -> Vec<u8> {
        Self::build_raw_frame(constants::FRAME_HEARTBEAT, 0, &[])
    }

    // === CONNECTION FRAMES ===
    pub fn build_connection_start_frame() -> Vec<u8> {
        Self::build_method_frame(
            0,
            constants::CONNECTION_CLASS,
            constants::CONNECTION_START,
            &[
                Value::U8(0),                         // version_major
                Value::U8(9),                         // version_minor
                Value::Table(HashMap::new()),         // server_properties
                Value::LongString(b"PLAIN".to_vec()), // mechanisms
                Value::LongString(b"en_US".to_vec()), // locales
            ],
        )
    }

    pub fn build_connection_start_ok_frame(username: &str, password: &str) -> Vec<u8> {
        Self::build_method_frame(
            0,
            constants::CONNECTION_CLASS,
            constants::CONNECTION_START_OK,
            &[
                Value::Table(HashMap::new()),
                Value::ShortString("PLAIN".to_string()),
                Value::LongString(format!("\x00{}\x00{}", username, password).into_bytes()),
                Value::ShortString("en_US".to_string()),
            ],
        )
    }

    pub fn build_connection_tune_frame(
        channel_max: u16,
        frame_max: u32,
        heartbeat: u16,
    ) -> Vec<u8> {
        Self::build_method_frame(
            0,
            constants::CONNECTION_CLASS,
            constants::CONNECTION_TUNE,
            &[
                Value::U16(channel_max),
                Value::U32(frame_max),
                Value::U16(heartbeat),
            ],
        )
    }

    pub fn build_connection_tune_ok_frame(
        channel_max: u16,
        frame_max: u32,
        heartbeat: u16,
    ) -> Vec<u8> {
        Self::build_method_frame(
            0,
            constants::CONNECTION_CLASS,
            constants::CONNECTION_TUNE_OK,
            &[
                Value::U16(channel_max),
                Value::U32(frame_max),
                Value::U16(heartbeat),
            ],
        )
    }

    pub fn build_connection_open_frame(virtual_host: &str) -> Vec<u8> {
        Self::build_method_frame(
            0,
            constants::CONNECTION_CLASS,
            constants::CONNECTION_OPEN,
            &[
                Value::ShortString(virtual_host.to_string()),
                Value::ShortString("".to_string()),
                Value::U8(0),
            ],
        )
    }

    pub fn build_connection_open_ok_frame() -> Vec<u8> {
        Self::build_method_frame(
            0,
            constants::CONNECTION_CLASS,
            constants::CONNECTION_OPEN_OK,
            &[Value::ShortString("".to_string())],
        )
    }

    // === CHANNEL FRAMES ===
    pub fn build_channel_open_frame(channel: u16) -> Vec<u8> {
        Self::build_method_frame(
            channel,
            constants::CHANNEL_CLASS,
            constants::CHANNEL_OPEN,
            &[Value::ShortString("".to_string())],
        )
    }

    pub fn build_channel_open_ok_frame(channel: u16) -> Vec<u8> {
        Self::build_method_frame(
            channel,
            constants::CHANNEL_CLASS,
            constants::CHANNEL_OPEN_OK,
            &[
                Value::LongString(vec![]), // reserved
            ],
        )
    }

    // === EXCHANGE FRAMES ===
    pub fn build_exchange_declare_frame(
        channel: u16,
        exchange: &str,
        exchange_type: &str,
        durable: bool,
        auto_delete: bool,
        internal: bool,
    ) -> Vec<u8> {
        let flags = vec![false, durable, auto_delete, internal, false]; // passive, durable, auto_delete, internal, no_wait
        Self::build_method_frame(
            channel,
            constants::EXCHANGE_CLASS,
            constants::EXCHANGE_DECLARE,
            &[
                Value::U16(0), // reserved
                Value::ShortString(exchange.to_string()),
                Value::ShortString(exchange_type.to_string()),
                Value::Flags(flags),
                Value::Table(HashMap::new()), // arguments
            ],
        )
    }

    pub fn build_exchange_declare_ok_frame(channel: u16) -> Vec<u8> {
        Self::build_method_frame(
            channel,
            constants::EXCHANGE_CLASS,
            constants::EXCHANGE_DECLARE_OK,
            &[],
        )
    }

    // === QUEUE FRAMES ===
    pub fn build_queue_declare_frame(
        channel: u16,
        queue: &str,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
    ) -> Vec<u8> {
        let flags = vec![false, durable, exclusive, auto_delete, false]; // passive, durable, exclusive, auto_delete, no_wait
        Self::build_method_frame(
            channel,
            constants::QUEUE_CLASS,
            constants::QUEUE_DECLARE,
            &[
                Value::U16(0), // reserved
                Value::ShortString(queue.to_string()),
                Value::Flags(flags),
                Value::Table(HashMap::new()), // arguments
            ],
        )
    }

    pub fn build_queue_declare_ok_frame(
        channel: u16,
        queue: &str,
        message_count: u32,
        consumer_count: u32,
    ) -> Vec<u8> {
        Self::build_method_frame(
            channel,
            constants::QUEUE_CLASS,
            constants::QUEUE_DECLARE_OK,
            &[
                Value::ShortString(queue.to_string()),
                Value::U32(message_count),
                Value::U32(consumer_count),
            ],
        )
    }

    pub fn build_queue_bind_frame(
        channel: u16,
        queue: &str,
        exchange: &str,
        routing_key: &str,
    ) -> Vec<u8> {
        Self::build_method_frame(
            channel,
            constants::QUEUE_CLASS,
            constants::QUEUE_BIND,
            &[
                Value::U16(0), // reserved
                Value::ShortString(queue.to_string()),
                Value::ShortString(exchange.to_string()),
                Value::ShortString(routing_key.to_string()),
                Value::U8(0),                 // no_wait
                Value::Table(HashMap::new()), // arguments
            ],
        )
    }

    pub fn build_queue_bind_ok_frame(channel: u16) -> Vec<u8> {
        Self::build_method_frame(
            channel,
            constants::QUEUE_CLASS,
            constants::QUEUE_BIND_OK,
            &[],
        )
    }

    // === BASIC FRAMES ===
    pub fn build_basic_qos_frame(
        channel: u16,
        prefetch_size: u32,
        prefetch_count: u16,
        global: bool,
    ) -> Vec<u8> {
        Self::build_method_frame(
            channel,
            constants::BASIC_CLASS,
            constants::BASIC_QOS,
            &[
                Value::U32(prefetch_size),
                Value::U16(prefetch_count),
                Value::U8(if global { 1 } else { 0 }),
            ],
        )
    }

    pub fn build_basic_qos_ok_frame(channel: u16) -> Vec<u8> {
        Self::build_method_frame(
            channel,
            constants::BASIC_CLASS,
            constants::BASIC_QOS_OK,
            &[],
        )
    }

    pub fn build_basic_consume_frame(
        channel: u16,
        queue: &str,
        consumer_tag: &str,
        no_local: bool,
        no_ack: bool,
        exclusive: bool,
    ) -> Vec<u8> {
        let flags = vec![no_local, no_ack, exclusive, false]; // no_local, no_ack, exclusive, no_wait
        Self::build_method_frame(
            channel,
            constants::BASIC_CLASS,
            constants::BASIC_CONSUME,
            &[
                Value::U16(0), // reserved
                Value::ShortString(queue.to_string()),
                Value::ShortString(consumer_tag.to_string()),
                Value::Flags(flags),
                Value::Table(HashMap::new()), // arguments
            ],
        )
    }

    pub fn build_basic_consume_ok_frame(channel: u16, consumer_tag: &str) -> Vec<u8> {
        Self::build_method_frame(
            channel,
            constants::BASIC_CLASS,
            constants::BASIC_CONSUME_OK,
            &[Value::ShortString(consumer_tag.to_string())],
        )
    }

    pub fn build_basic_publish_frame(
        channel: u16,
        exchange: &str,
        routing_key: &str,
        mandatory: bool,
        immediate: bool,
    ) -> Vec<u8> {
        let flags = vec![mandatory, immediate];
        Self::build_method_frame(
            channel,
            constants::BASIC_CLASS,
            constants::BASIC_PUBLISH,
            &[
                Value::U16(0), // reserved
                Value::ShortString(exchange.to_string()),
                Value::ShortString(routing_key.to_string()),
                Value::Flags(flags),
            ],
        )
    }

    pub fn build_basic_deliver_frame(
        channel: u16,
        consumer_tag: &str,
        delivery_tag: u64,
        redelivered: bool,
        exchange: &str,
        routing_key: &str,
    ) -> Vec<u8> {
        Self::build_method_frame(
            channel,
            constants::BASIC_CLASS,
            constants::BASIC_DELIVER,
            &[
                Value::ShortString(consumer_tag.to_string()),
                Value::U64(delivery_tag),
                Value::U8(if redelivered { 1 } else { 0 }),
                Value::ShortString(exchange.to_string()),
                Value::ShortString(routing_key.to_string()),
            ],
        )
    }

    pub fn build_basic_ack_frame(channel: u16, delivery_tag: u64, multiple: bool) -> Vec<u8> {
        Self::build_method_frame(
            channel,
            constants::BASIC_CLASS,
            constants::BASIC_ACK,
            &[
                Value::U64(delivery_tag),
                Value::U8(if multiple { 1 } else { 0 }),
            ],
        )
    }

    pub fn build_basic_reject_frame(channel: u16, delivery_tag: u64, requeue: bool) -> Vec<u8> {
        Self::build_method_frame(
            channel,
            constants::BASIC_CLASS,
            constants::BASIC_REJECT,
            &[
                Value::U64(delivery_tag),
                Value::U8(if requeue { 1 } else { 0 }),
            ],
        )
    }

    pub fn build_basic_nack_frame(
        channel: u16,
        delivery_tag: u64,
        multiple: bool,
        requeue: bool,
    ) -> Vec<u8> {
        Self::build_method_frame(
            channel,
            constants::BASIC_CLASS,
            constants::BASIC_NACK,
            &[
                Value::U64(delivery_tag),
                Value::U8(if multiple { 1 } else { 0 }),
                Value::U8(if requeue { 1 } else { 0 }),
            ],
        )
    }

    // === CONTENT FRAMES ===
    pub fn build_content_header_frame(channel: u16, class_id: u16, body_size: u64) -> Vec<u8> {
        Self::build_raw_frame(
            constants::FRAME_CONTENT_HEADER,
            channel,
            &[
                class_id.to_be_bytes().to_vec(),
                0u16.to_be_bytes().to_vec(), // weight
                body_size.to_be_bytes().to_vec(),
                vec![0, 0], // property flags (empty)
            ]
            .concat(),
        )
    }

    pub fn build_content_body_frame(channel: u16, body: &[u8]) -> Vec<u8> {
        Self::build_raw_frame(constants::FRAME_CONTENT_BODY, channel, body)
    }

    // === PROTOCOL PARSERS ===
    pub fn parse_connection_tune(args: &[u8]) -> Result<ConnectionParams> {
        match args {
            [ch_hi, ch_lo, fr_a, fr_b, fr_c, fr_d, hb_hi, hb_lo, ..] => Ok(ConnectionParams {
                channel_max: u16::from_be_bytes([*ch_hi, *ch_lo]),
                frame_max: u32::from_be_bytes([*fr_a, *fr_b, *fr_c, *fr_d]),
                heartbeat: u16::from_be_bytes([*hb_hi, *hb_lo]),
            }),
            _ => Err(AmqpError::Protocol("Invalid Connection.Tune args".into())),
        }
    }

    pub fn parse_short_string(data: &[u8]) -> Result<(String, &[u8])> {
        match data {
            [len, rest @ ..] if rest.len() >= *len as usize => {
                let string = String::from_utf8_lossy(&rest[..*len as usize]).to_string();
                Ok((string, &rest[*len as usize..]))
            }
            _ => Err(AmqpError::Protocol("Invalid short string".into())),
        }
    }

    // === PROTOCOL WORKFLOWS ===
    pub async fn send_and_expect_response(
        stream: &mut TcpStream,
        frame: Vec<u8>,
        expected_class: u16,
        expected_method: u16,
        heartbeat_state: &mut HeartbeatState,
        log_config: &LogConfig,
    ) -> Result<Vec<u8>> {
        log_debug(
            log_config,
            &format!(
                "Sending frame and expecting {}.{}",
                expected_class, expected_method
            ),
        );
        Self::send_frame_with_heartbeat(stream, frame, heartbeat_state, log_config).await?;
        let received_frame =
            Self::receive_frame_with_heartbeat(stream, heartbeat_state, log_config).await?;
        let (frame_type, _channel, payload) = Self::parse_frame(&received_frame)?;

        if frame_type != constants::FRAME_METHOD {
            return Err(AmqpError::Protocol("Expected method response".into()));
        }

        let (class, method, args) = Self::parse_method_frame(payload)?;

        if class != expected_class || method != expected_method {
            return Err(AmqpError::Protocol(format!(
                "Expected {}.{}, got {}.{}",
                expected_class, expected_method, class, method
            )));
        }

        Ok(args.to_vec())
    }
}

// === SHARED MESSAGE PROCESSOR ===
#[derive(Debug, Clone)]
pub struct MessageStats {
    pub messages_sent: usize,
    pub messages_received: usize,
    pub bytes_sent: usize,
    pub bytes_received: usize,
    pub errors: usize,
    pub messages_validated: usize,
    pub validation_failures: usize,
}

impl MessageStats {
    pub fn new() -> Self {
        Self {
            messages_sent: 0,
            messages_received: 0,
            bytes_sent: 0,
            bytes_received: 0,
            errors: 0,
            messages_validated: 0,
            validation_failures: 0,
        }
    }

    pub fn print_summary(&self, log_config: &LogConfig) {
        log_info(log_config, "=== Message Statistics ===");
        log_info(
            log_config,
            &format!("Messages sent: {}", self.messages_sent),
        );
        log_info(
            log_config,
            &format!("Messages received: {}", self.messages_received),
        );
        log_info(log_config, &format!("Bytes sent: {}", self.bytes_sent));
        log_info(
            log_config,
            &format!("Bytes received: {}", self.bytes_received),
        );
        log_info(
            log_config,
            &format!("Messages validated: {}", self.messages_validated),
        );
        log_info(
            log_config,
            &format!("Validation failures: {}", self.validation_failures),
        );
        log_info(log_config, &format!("Errors: {}", self.errors));
    }
}

// === AMQP CLIENT CONNECTION ===
pub async fn establish_connection(
    client_id: String,
    host: &str,
    port: u16,
    vhost: &str,
    username: &str,
    password: &str,
    heartbeat_interval: u16,
    frame_max: u32,
    log_config: &LogConfig,
) -> Result<(TcpStream, ConnectionResult, HeartbeatState)> {
    let start_time = Instant::now();

    log_info(log_config, &format!("Connecting to {}:{}", host, port));
    let mut stream = TcpStream::connect((host, port)).await.map_err(|e| {
        AmqpError::Connection(format!("Failed to connect to {}:{}: {}", host, port, e))
    })?;

    let server_addr = stream
        .peer_addr()
        .map_err(|e| AmqpError::Connection(format!("Failed to get peer address: {}", e)))?;

    // Send AMQP protocol header
    log_debug(log_config, "Sending AMQP protocol header");
    stream
        .write_all(constants::AMQP_PROTOCOL_HEADER)
        .await
        .map_err(|e| {
            AmqpError::Connection(format!("Failed to send AMQP protocol header: {}", e))
        })?;

    let mut temp_heartbeat_state = HeartbeatState::new(0);

    // Wait for Connection.Start
    log_debug(log_config, "Waiting for Connection.Start");
    let received_frame =
        AmqpLib::receive_frame_with_heartbeat(&mut stream, &mut temp_heartbeat_state, log_config)
            .await?;
    let (frame_type, _channel, payload) = AmqpLib::parse_frame(&received_frame)?;

    if frame_type != constants::FRAME_METHOD {
        return Err(AmqpError::Protocol("Expected Connection.Start".into()));
    }

    let (class, method, _args) = AmqpLib::parse_method_frame(payload)?;
    if class != constants::CONNECTION_CLASS || method != constants::CONNECTION_START {
        return Err(AmqpError::Protocol("Expected Connection.Start".into()));
    }

    // Send Connection.StartOk and wait for Connection.Tune
    log_debug(log_config, "Sending Connection.StartOk");
    let tune_args = AmqpLib::send_and_expect_response(
        &mut stream,
        AmqpLib::build_connection_start_ok_frame(username, password),
        constants::CONNECTION_CLASS,
        constants::CONNECTION_TUNE,
        &mut temp_heartbeat_state,
        log_config,
    )
    .await?;

    let server_params = AmqpLib::parse_connection_tune(&tune_args)?;

    // Negotiate parameters
    let negotiated_params = ConnectionParams {
        channel_max: server_params.channel_max.min(2047),
        frame_max: server_params.frame_max.min(frame_max),
        heartbeat: server_params.heartbeat.min(heartbeat_interval),
    };

    log_debug(
        log_config,
        &format!(
            "Negotiated params: channel_max={}, frame_max={}, heartbeat={}",
            negotiated_params.channel_max, negotiated_params.frame_max, negotiated_params.heartbeat
        ),
    );

    // Send Connection.TuneOk
    AmqpLib::send_frame(
        &mut stream,
        AmqpLib::build_connection_tune_ok_frame(
            negotiated_params.channel_max,
            negotiated_params.frame_max,
            negotiated_params.heartbeat,
        ),
        log_config,
    )
    .await?;

    // Initialize heartbeat with negotiated interval
    let mut heartbeat_state = HeartbeatState::new(negotiated_params.heartbeat);

    // Send Connection.Open and wait for Connection.OpenOk
    log_debug(log_config, &format!("Opening vhost '{}'", vhost));
    let _ = AmqpLib::send_and_expect_response(
        &mut stream,
        AmqpLib::build_connection_open_frame(vhost),
        constants::CONNECTION_CLASS,
        constants::CONNECTION_OPEN_OK,
        &mut heartbeat_state,
        log_config,
    )
    .await?;

    log_info(
        log_config,
        &format!("Connected to {} as {}", server_addr, client_id),
    );

    Ok((
        stream,
        ConnectionResult {
            client_id,
            server_addr,
            negotiated_params,
            connection_time: start_time.elapsed(),
        },
        heartbeat_state,
    ))
}

// === CHANNEL OPERATIONS ===
pub async fn open_channel(
    stream: &mut TcpStream,
    channel_id: u16,
    heartbeat_state: &mut HeartbeatState,
    log_config: &LogConfig,
) -> Result<u16> {
    log_debug(log_config, &format!("Opening channel {}", channel_id));

    let _ = AmqpLib::send_and_expect_response(
        stream,
        AmqpLib::build_channel_open_frame(channel_id),
        constants::CHANNEL_CLASS,
        constants::CHANNEL_OPEN_OK,
        heartbeat_state,
        log_config,
    )
    .await?;

    log_debug(log_config, &format!("Channel {} opened", channel_id));
    Ok(channel_id)
}

// === EXCHANGE OPERATIONS ===
pub async fn declare_exchange(
    stream: &mut TcpStream,
    channel_id: u16,
    exchange: &str,
    exchange_type: &ExchangeType,
    durable: bool,
    auto_delete: bool,
    internal: bool,
    heartbeat_state: &mut HeartbeatState,
    log_config: &LogConfig,
) -> Result<()> {
    log_debug(
        log_config,
        &format!(
            "Declaring exchange '{}' of type '{}'",
            exchange,
            exchange_type.wire_name()
        ),
    );

    let _ = AmqpLib::send_and_expect_response(
        stream,
        AmqpLib::build_exchange_declare_frame(
            channel_id,
            exchange,
            exchange_type.wire_name(),
            durable,
            auto_delete,
            internal,
        ),
        constants::EXCHANGE_CLASS,
        constants::EXCHANGE_DECLARE_OK,
        heartbeat_state,
        log_config,
    )
    .await?;

    Ok(())
}

// === QUEUE OPERATIONS ===
pub async fn declare_queue(
    stream: &mut TcpStream,
    channel_id: u16,
    queue: &str,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    heartbeat_state: &mut HeartbeatState,
    log_config: &LogConfig,
) -> Result<String> {
    log_debug(log_config, &format!("Declaring queue '{}'", queue));

    let response = AmqpLib::send_and_expect_response(
        stream,
        AmqpLib::build_queue_declare_frame(channel_id, queue, durable, exclusive, auto_delete),
        constants::QUEUE_CLASS,
        constants::QUEUE_DECLARE_OK,
        heartbeat_state,
        log_config,
    )
    .await?;

    let (queue_name, _) = AmqpLib::parse_short_string(&response)?;
    log_debug(log_config, &format!("Queue declared: '{}'", queue_name));
    Ok(queue_name)
}

pub async fn bind_queue(
    stream: &mut TcpStream,
    channel_id: u16,
    queue: &str,
    exchange: &str,
    routing_key: &str,
    heartbeat_state: &mut HeartbeatState,
    log_config: &LogConfig,
) -> Result<()> {
    log_debug(
        log_config,
        &format!(
            "Binding queue '{}' to exchange '{}' with key '{}'",
            queue, exchange, routing_key
        ),
    );

    let _ = AmqpLib::send_and_expect_response(
        stream,
        AmqpLib::build_queue_bind_frame(channel_id, queue, exchange, routing_key),
        constants::QUEUE_CLASS,
        constants::QUEUE_BIND_OK,
        heartbeat_state,
        log_config,
    )
    .await?;

    Ok(())
}

// === PUBLISHING OPERATIONS ===
pub async fn publish_message(
    stream: &mut TcpStream,
    channel_id: u16,
    exchange: &str,
    routing_key: &str,
    body: &[u8],
    mandatory: bool,
    immediate: bool,
    heartbeat_state: &mut HeartbeatState,
    log_config: &LogConfig,
) -> Result<PublishResult> {
    log_trace(
        log_config,
        &format!(
            "Publishing to exchange '{}' with key '{}'",
            exchange, routing_key
        ),
    );

    // Send Basic.Publish
    let publish_bytes = AmqpLib::send_frame_with_heartbeat(
        stream,
        AmqpLib::build_basic_publish_frame(channel_id, exchange, routing_key, mandatory, immediate),
        heartbeat_state,
        log_config,
    )
    .await?;

    // Send Content Header
    let header_bytes = AmqpLib::send_frame_with_heartbeat(
        stream,
        AmqpLib::build_content_header_frame(channel_id, constants::BASIC_CLASS, body.len() as u64),
        heartbeat_state,
        log_config,
    )
    .await?;

    // Send Content Body
    let body_bytes = AmqpLib::send_frame_with_heartbeat(
        stream,
        AmqpLib::build_content_body_frame(channel_id, body),
        heartbeat_state,
        log_config,
    )
    .await?;

    let total_bytes = publish_bytes + header_bytes + body_bytes;

    Ok(PublishResult {
        bytes_sent: total_bytes,
        delivery_tag: None,
        confirmed: false,
    })
}

// === CONSUMING OPERATIONS ===
pub async fn start_consuming(
    stream: &mut TcpStream,
    channel_id: u16,
    queue: &str,
    consumer_tag: &str,
    no_local: bool,
    no_ack: bool,
    exclusive: bool,
    heartbeat_state: &mut HeartbeatState,
    log_config: &LogConfig,
) -> Result<ConsumeResult> {
    log_debug(
        log_config,
        &format!("Starting consumer '{}' on queue '{}'", consumer_tag, queue),
    );

    let response = AmqpLib::send_and_expect_response(
        stream,
        AmqpLib::build_basic_consume_frame(
            channel_id,
            queue,
            consumer_tag,
            no_local,
            no_ack,
            exclusive,
        ),
        constants::BASIC_CLASS,
        constants::BASIC_CONSUME_OK,
        heartbeat_state,
        log_config,
    )
    .await?;

    let (actual_consumer_tag, _) = AmqpLib::parse_short_string(&response)?;

    Ok(ConsumeResult {
        consumer_tag: actual_consumer_tag,
        queue_name: queue.to_string(),
        prefetch_count: 0,
    })
}

pub async fn receive_message(
    stream: &mut TcpStream,
    heartbeat_state: &mut HeartbeatState,
    log_config: &LogConfig,
) -> Result<Option<Message>> {
    let frame = AmqpLib::receive_frame_with_heartbeat(stream, heartbeat_state, log_config).await?;
    let (frame_type, _channel, payload) = AmqpLib::parse_frame(&frame)?;

    match frame_type {
        constants::FRAME_METHOD => {
            let (class, method, args) = AmqpLib::parse_method_frame(payload)?;

            if class == constants::BASIC_CLASS && method == constants::BASIC_DELIVER {
                // Parse Basic.Deliver
                let (consumer_tag, remaining) = AmqpLib::parse_short_string(args)?;
                let delivery_tag = u64::from_be_bytes(remaining[0..8].try_into().unwrap());
                let redelivered = remaining[8] != 0;
                let (exchange, remaining) = AmqpLib::parse_short_string(&remaining[9..])?;
                let (routing_key, _) = AmqpLib::parse_short_string(remaining)?;

                let delivery_info = DeliveryInfo {
                    delivery_tag,
                    consumer_tag,
                    exchange,
                    routing_key,
                    redelivered,
                };

                // Receive Content Header
                let header_frame =
                    AmqpLib::receive_frame_with_heartbeat(stream, heartbeat_state, log_config)
                        .await?;
                let (header_type, _header_channel, header_payload) =
                    AmqpLib::parse_frame(&header_frame)?;

                if header_type != constants::FRAME_CONTENT_HEADER {
                    return Err(AmqpError::Protocol("Expected content header".into()));
                }

                let class_id = u16::from_be_bytes(header_payload[0..2].try_into().unwrap());
                let body_size = u64::from_be_bytes(header_payload[4..12].try_into().unwrap());

                let content_header = ContentHeader {
                    class_id,
                    weight: 0,
                    body_size,
                    properties: HashMap::new(),
                };

                // Receive Content Body
                let body_frame =
                    AmqpLib::receive_frame_with_heartbeat(stream, heartbeat_state, log_config)
                        .await?;
                let (body_type, _body_channel, body_payload) = AmqpLib::parse_frame(&body_frame)?;

                if body_type != constants::FRAME_CONTENT_BODY {
                    return Err(AmqpError::Protocol("Expected content body".into()));
                }

                let message = Message {
                    delivery_info,
                    content_header,
                    body: body_payload.to_vec(),
                };

                log_trace(
                    log_config,
                    &format!("Received message with delivery tag {}", delivery_tag),
                );

                Ok(Some(message))
            } else {
                // Handle other methods
                log_trace(log_config, &format!("Received method {}.{}", class, method));
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

pub async fn ack_message(
    stream: &mut TcpStream,
    channel_id: u16,
    delivery_tag: u64,
    multiple: bool,
    heartbeat_state: &mut HeartbeatState,
    log_config: &LogConfig,
) -> Result<usize> {
    log_trace(
        log_config,
        &format!("Acknowledging delivery tag {}", delivery_tag),
    );
    let bytes_sent = AmqpLib::send_frame_with_heartbeat(
        stream,
        AmqpLib::build_basic_ack_frame(channel_id, delivery_tag, multiple),
        heartbeat_state,
        log_config,
    )
    .await?;
    Ok(bytes_sent)
}

// === PRODUCER WORKFLOW ===
pub async fn run_producer_workflow(
    connection_result: ConnectionResult,
    config: ProducerConfig,
    mut stream: TcpStream,
    mut heartbeat_state: HeartbeatState,
    log_config: &LogConfig,
) -> Result<ProducerStats> {
    let start_time = Instant::now();
    let mut stats = MessageStats::new();

    // Open channel
    let channel_id = open_channel(&mut stream, 1, &mut heartbeat_state, log_config).await?;

    // Setup exchange and queue
    declare_exchange(
        &mut stream,
        channel_id,
        &config.exchange,
        &config.exchange_type,
        config.durable,
        config.auto_delete,
        config.internal,
        &mut heartbeat_state,
        log_config,
    )
    .await?;

    let queue_name = declare_queue(
        &mut stream,
        channel_id,
        &config.queue,
        config.durable,
        config.exclusive,
        config.auto_delete,
        &mut heartbeat_state,
        log_config,
    )
    .await?;

    bind_queue(
        &mut stream,
        channel_id,
        &queue_name,
        &config.exchange,
        &config.routing_key,
        &mut heartbeat_state,
        log_config,
    )
    .await?;

    let message_source = MessageSource {
        prefix: format!("Message from {}", connection_result.client_id),
        include_timestamp: true,
    };

    let mut final_delivery_tag = None;

    match config.count {
        Some(count) => {
            // Test mode - send specific number of messages
            log_info(
                log_config,
                &format!(
                    "Producer {} sending {} messages",
                    connection_result.client_id, count
                ),
            );

            for i in 1..=count {
                let message = message_source.generate_message(i);

                match publish_message(
                    &mut stream,
                    channel_id,
                    &config.exchange,
                    &config.routing_key,
                    &message,
                    false,
                    false,
                    &mut heartbeat_state,
                    log_config,
                )
                .await
                {
                    Ok(publish_result) => {
                        stats.messages_sent += 1;
                        stats.bytes_sent += publish_result.bytes_sent;
                        final_delivery_tag = publish_result.delivery_tag;

                        if i % 100 == 0 {
                            log_debug(
                                log_config,
                                &format!(
                                    "Producer {} sent {} messages",
                                    connection_result.client_id, i
                                ),
                            );
                        }
                    }
                    Err(e) => {
                        stats.errors += 1;
                        log_error(
                            log_config,
                            &format!("Failed to publish message {}: {}", i, e),
                        );
                        return Err(e);
                    }
                }

                if let Some(interval) = config.send_interval {
                    tokio::time::sleep(interval).await;
                }
            }

            log_info(
                log_config,
                &format!(
                    "Producer {} successfully sent all {} messages",
                    connection_result.client_id, count
                ),
            );
        }
        None => {
            // Interactive mode
            let stdin = tokio::io::stdin();
            let mut reader = BufReader::new(stdin);
            let mut line = String::new();

            log_info(
                log_config,
                &format!(
                    "Producer {} ready. Type messages (Ctrl+C to quit):",
                    connection_result.client_id
                ),
            );

            let mut seq = 1;
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        let user_message = line.trim();
                        if !user_message.is_empty() {
                            let message = if user_message == "." {
                                // Use generated message
                                message_source.generate_message(seq)
                            } else {
                                // Use user input
                                user_message.as_bytes().to_vec()
                            };

                            match publish_message(
                                &mut stream,
                                channel_id,
                                &config.exchange,
                                &config.routing_key,
                                &message,
                                false,
                                false,
                                &mut heartbeat_state,
                                log_config,
                            )
                            .await
                            {
                                Ok(publish_result) => {
                                    stats.messages_sent += 1;
                                    stats.bytes_sent += publish_result.bytes_sent;
                                    final_delivery_tag = publish_result.delivery_tag;
                                    seq += 1;
                                }
                                Err(e) => {
                                    stats.errors += 1;
                                    log_error(log_config, &format!("Failed to publish: {}", e));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log_error(log_config, &format!("Read error: {}", e));
                        break;
                    }
                }
            }
        }
    }

    stats.print_summary(log_config);

    Ok(ProducerStats {
        client_id: connection_result.client_id,
        messages_sent: stats.messages_sent,
        bytes_sent: stats.bytes_sent,
        errors: stats.errors,
        duration: start_time.elapsed(),
        final_delivery_tag,
    })
}

// === CONSUMER WORKFLOW ===
pub async fn run_consumer_workflow(
    connection_result: ConnectionResult,
    config: ConsumerConfig,
    mut stream: TcpStream,
    mut heartbeat_state: HeartbeatState,
    log_config: &LogConfig,
) -> Result<ConsumerStats> {
    let start_time = Instant::now();
    let mut stats = MessageStats::new();

    // Open channel
    let channel_id = open_channel(&mut stream, 1, &mut heartbeat_state, log_config).await?;

    // Setup QoS
    let _ = AmqpLib::send_and_expect_response(
        &mut stream,
        AmqpLib::build_basic_qos_frame(channel_id, 0, config.prefetch_count, false),
        constants::BASIC_CLASS,
        constants::BASIC_QOS_OK,
        &mut heartbeat_state,
        log_config,
    )
    .await?;

    // Start consuming
    let consume_result = start_consuming(
        &mut stream,
        channel_id,
        &config.queue,
        &format!("consumer-{}", connection_result.client_id),
        false,
        config.no_ack,
        false,
        &mut heartbeat_state,
        log_config,
    )
    .await?;

    let message_drain = MessageDrain {
        expected_prefix: if config.validate_messages {
            Some("Message".to_string())
        } else {
            None
        },
        validate_sequence: config.validate_messages,
        last_sequence: None,
    };

    log_info(
        log_config,
        &format!(
            "Consumer {} ready with tag '{}'. Waiting for messages...",
            connection_result.client_id, consume_result.consumer_tag
        ),
    );

    let mut final_delivery_tag = None;

    match config.count {
        Some(target_count) => {
            // Test mode - receive specific number of messages
            log_info(
                log_config,
                &format!(
                    "Consumer {} waiting for {} messages",
                    connection_result.client_id, target_count
                ),
            );

            let mut received_count = 0;

            while received_count < target_count {
                match receive_message(&mut stream, &mut heartbeat_state, log_config).await {
                    Ok(Some(message)) => {
                        received_count += 1;
                        stats.messages_received += 1;
                        stats.bytes_received += message.body.len();
                        final_delivery_tag = Some(message.delivery_info.delivery_tag);

                        // Validate message if configured
                        if config.validate_messages {
                            match message_drain.validate_message(&message.body) {
                                Ok(validation) => {
                                    if validation.valid {
                                        stats.messages_validated += 1;
                                        log_trace(
                                            log_config,
                                            &format!("Message {} validated", received_count),
                                        );
                                    } else {
                                        stats.validation_failures += 1;
                                        log_error(
                                            log_config,
                                            &format!(
                                                "Message {} validation failed: {}",
                                                received_count,
                                                validation.error.unwrap_or_default()
                                            ),
                                        );
                                    }
                                }
                                Err(e) => {
                                    stats.validation_failures += 1;
                                    log_error(
                                        log_config,
                                        &format!("Message validation error: {}", e),
                                    );
                                }
                            }
                        }

                        if received_count % 100 == 0 {
                            log_debug(
                                log_config,
                                &format!(
                                    "Consumer {} received {} messages",
                                    connection_result.client_id, received_count
                                ),
                            );
                        }

                        // Acknowledge message if not in no-ack mode
                        if !config.no_ack {
                            match config.ack_mode {
                                AckMode::Manual | AckMode::Auto => {
                                    match ack_message(
                                        &mut stream,
                                        channel_id,
                                        message.delivery_info.delivery_tag,
                                        false,
                                        &mut heartbeat_state,
                                        log_config,
                                    )
                                    .await
                                    {
                                        Ok(_) => {}
                                        Err(e) => {
                                            stats.errors += 1;
                                            log_error(
                                                log_config,
                                                &format!("Failed to ack message: {}", e),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        // No message received, continue
                    }
                    Err(e) => {
                        stats.errors += 1;
                        log_error(log_config, &format!("Failed to receive message: {}", e));
                        return Err(e);
                    }
                }
            }

            log_info(
                log_config,
                &format!(
                    "Consumer {} successfully received all {} messages",
                    connection_result.client_id, target_count
                ),
            );
        }
        None => {
            // Interactive mode - receive until interrupted
            log_info(
                log_config,
                "Consumer in interactive mode. Press Ctrl+C to stop.",
            );

            loop {
                match receive_message(&mut stream, &mut heartbeat_state, log_config).await {
                    Ok(Some(message)) => {
                        stats.messages_received += 1;
                        stats.bytes_received += message.body.len();
                        final_delivery_tag = Some(message.delivery_info.delivery_tag);

                        log_info(
                            log_config,
                            &format!(
                                "Received message {}: {}",
                                stats.messages_received,
                                String::from_utf8_lossy(&message.body)
                            ),
                        );

                        // Acknowledge message if not in no-ack mode
                        if !config.no_ack {
                            match ack_message(
                                &mut stream,
                                channel_id,
                                message.delivery_info.delivery_tag,
                                false,
                                &mut heartbeat_state,
                                log_config,
                            )
                            .await
                            {
                                Ok(_) => {}
                                Err(e) => {
                                    stats.errors += 1;
                                    log_error(log_config, &format!("Failed to ack message: {}", e));
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        // No message received, continue
                    }
                    Err(e) => {
                        stats.errors += 1;
                        log_error(log_config, &format!("Error receiving message: {}", e));
                    }
                }
            }
        }
    }

    stats.print_summary(log_config);

    Ok(ConsumerStats {
        client_id: connection_result.client_id,
        messages_received: stats.messages_received,
        bytes_received: stats.bytes_received,
        errors: stats.errors,
        duration: start_time.elapsed(),
        final_delivery_tag,
    })
}

// === AMQP SERVER ===

// Pure data structures for server state
#[derive(Debug, Clone)]
struct Exchange {
    name: String,
    exchange_type: String,
    durable: bool,
    auto_delete: bool,
    internal: bool,
}

#[derive(Debug, Clone)]
struct Queue {
    name: String,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    messages: VecDeque<Message>,
    consumers: Vec<String>,
}

#[derive(Debug, Clone)]
struct Binding {
    queue: String,
    exchange: String,
    routing_key: String,
}

#[derive(Debug, Clone)]
struct VirtualHost {
    name: String,
    exchanges: HashMap<String, Exchange>,
    queues: HashMap<String, Queue>,
    bindings: Vec<Binding>,
}

#[derive(Debug)]
struct ServerConnection {
    id: String,
    stream: TcpStream,
    addr: SocketAddr,
    state: ConnectionState,
    heartbeat_state: HeartbeatState,
    channels: HashMap<u16, ChannelState>,
    params: ConnectionParams,
    vhost: String,
    username: String,
}

pub struct AmqpServer {
    server_id: String,
    listener: TcpListener,
    connections: HashMap<String, ServerConnection>,
    limits: ResourceLimits,
    stats: MessageStats,
    virtual_hosts: HashMap<String, VirtualHost>,
    log_config: LogConfig,
}

impl AmqpServer {
    pub async fn bind(
        server_id: String,
        host: &str,
        port: u16,
        limits: ResourceLimits,
        log_config: LogConfig,
    ) -> Result<ServerStartResult> {
        log_info(
            &log_config,
            &format!("Binding server {} to {}:{}", server_id, host, port),
        );

        let listener = TcpListener::bind((host, port)).await.map_err(|e| {
            AmqpError::Connection(format!("Failed to bind to {}:{}: {}", host, port, e))
        })?;

        let bind_address = listener
            .local_addr()
            .map_err(|e| AmqpError::Connection(format!("Failed to get local address: {}", e)))?;

        let mut virtual_hosts = HashMap::new();
        virtual_hosts.insert("/".to_string(), Self::create_default_vhost("/"));

        let _server = Self {
            server_id: server_id.clone(),
            listener,
            connections: HashMap::new(),
            limits: limits.clone(),
            stats: MessageStats::new(),
            virtual_hosts,
            log_config,
        };

        Ok(ServerStartResult {
            server_id,
            bind_address,
            resource_limits: limits,
        })
    }

    fn create_default_vhost(name: &str) -> VirtualHost {
        let mut exchanges = HashMap::new();

        // Create default exchange
        exchanges.insert(
            "".to_string(),
            Exchange {
                name: "".to_string(),
                exchange_type: "direct".to_string(),
                durable: true,
                auto_delete: false,
                internal: false,
            },
        );

        VirtualHost {
            name: name.to_string(),
            exchanges,
            queues: HashMap::new(),
            bindings: Vec::new(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        log_info(
            &self.log_config,
            &format!(
                "Server {} listening on {:?}",
                self.server_id,
                self.listener.local_addr()
            ),
        );

        loop {
            // Accept new connections
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    if self.connections.len() >= self.limits.max_connections {
                        log_error(
                            &self.log_config,
                            &format!(
                                "Server {} rejected connection from {} - max connections reached",
                                self.server_id, addr
                            ),
                        );
                        continue;
                    }

                    let conn_id = format!("conn-{}-{}", addr, Uuid::new_v4());
                    log_info(
                        &self.log_config,
                        &format!(
                            "Server {} accepted connection {} from {}",
                            self.server_id, conn_id, addr
                        ),
                    );

                    let connection = ServerConnection {
                        id: conn_id.clone(),
                        stream,
                        addr,
                        state: ConnectionState::Initial,
                        heartbeat_state: HeartbeatState::new(0),
                        channels: HashMap::new(),
                        params: ConnectionParams::default(),
                        vhost: "/".to_string(),
                        username: "guest".to_string(),
                    };

                    self.connections.insert(conn_id.clone(), connection);
                }
                Err(e) => {
                    log_error(
                        &self.log_config,
                        &format!("Server {} accept error: {}", self.server_id, e),
                    );
                }
            }

            // Handle existing connections
            self.handle_connections().await?;
        }
    }

    async fn handle_connections(&mut self) -> Result<()> {
        let mut connections_to_remove = Vec::new();
        let connection_ids: Vec<String> = self.connections.keys().cloned().collect();

        for conn_id in connection_ids {
            let should_continue = match self.connections.get_mut(&conn_id) {
                Some(connection) => {
                    // The borrow is released at the end of this block
                    match self.handle_connection_frame(connection).await {
                        Ok(continue_flag) => continue_flag,
                        Err(e) => {
                            log_error(
                                &self.log_config,
                                &format!(
                                    "Server {} connection {} error: {}",
                                    self.server_id, conn_id, e
                                ),
                            );
                            false
                        }
                    }
                }
                None => false,
            };

            if !should_continue {
                connections_to_remove.push(conn_id);
            }
        }

        for conn_id in connections_to_remove {
            self.connections.remove(&conn_id);
            log_info(
                &self.log_config,
                &format!("Server {} removed connection {}", self.server_id, conn_id),
            );
        }

        Ok(())
    }

    async fn handle_connection_frame(&mut self, connection: &mut ServerConnection) -> Result<bool> {
        // Non-blocking frame receive
        let frame = match tokio::time::timeout(
            Duration::from_millis(10),
            AmqpLib::receive_frame_with_heartbeat(
                &mut connection.stream,
                &mut connection.heartbeat_state,
                &self.log_config,
            ),
        )
        .await
        {
            Ok(Ok(frame)) => frame,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Ok(true), // Timeout, continue
        };

        let (frame_type, channel, payload) = AmqpLib::parse_frame(&frame)?;

        match frame_type {
            constants::FRAME_METHOD => {
                let (class, method, args) = AmqpLib::parse_method_frame(payload)?;
                self.handle_method_frame(connection, channel, class, method, args)
                    .await?;
            }
            constants::FRAME_CONTENT_HEADER => {
                log_trace(&self.log_config, "Received content header frame");
            }
            constants::FRAME_CONTENT_BODY => {
                log_trace(&self.log_config, "Received content body frame");
            }
            _ => {
                return Err(AmqpError::Protocol(format!(
                    "Unhandled frame type: {}",
                    frame_type
                )));
            }
        }

        Ok(true)
    }

    async fn handle_method_frame(
        &mut self,
        connection: &mut ServerConnection,
        channel: u16,
        class: u16,
        method: u16,
        args: &[u8],
    ) -> Result<()> {
        log_trace(
            &self.log_config,
            &format!(
                "Handling method {}.{} on channel {}",
                class, method, channel
            ),
        );

        match (class, method) {
            (constants::CONNECTION_CLASS, constants::CONNECTION_START_OK) => {
                // Send Connection.Tune
                let tune_frame = AmqpLib::build_connection_tune_frame(
                    self.limits.max_channels_per_connection,
                    self.limits.max_frame_size,
                    60,
                );
                AmqpLib::send_frame_with_heartbeat(
                    &mut connection.stream,
                    tune_frame,
                    &mut connection.heartbeat_state,
                    &self.log_config,
                )
                .await?;
                connection.state = ConnectionState::TuneReceived;
            }

            (constants::CONNECTION_CLASS, constants::CONNECTION_TUNE_OK) => {
                let params = AmqpLib::parse_connection_tune(args)?;
                connection.params = params;
                connection.heartbeat_state = HeartbeatState::new(connection.params.heartbeat);
                connection.state = ConnectionState::TuneOkSent;
            }

            (constants::CONNECTION_CLASS, constants::CONNECTION_OPEN) => {
                let (vhost, _) = AmqpLib::parse_short_string(args)?;
                connection.vhost = vhost;

                // Send Connection.OpenOk
                let open_ok_frame = AmqpLib::build_connection_open_ok_frame();
                AmqpLib::send_frame_with_heartbeat(
                    &mut connection.stream,
                    open_ok_frame,
                    &mut connection.heartbeat_state,
                    &self.log_config,
                )
                .await?;
                connection.state = ConnectionState::Open;
                log_info(
                    &self.log_config,
                    &format!(
                        "Connection {} opened vhost '{}'",
                        connection.id, connection.vhost
                    ),
                );
            }

            (constants::CHANNEL_CLASS, constants::CHANNEL_OPEN) => {
                if connection.channels.len() >= self.limits.max_channels_per_connection as usize {
                    return Err(AmqpError::Resource(
                        "Maximum channels per connection exceeded".into(),
                    ));
                }

                connection.channels.insert(channel, ChannelState::Open);

                // Send Channel.OpenOk
                let channel_ok_frame = AmqpLib::build_channel_open_ok_frame(channel);
                AmqpLib::send_frame_with_heartbeat(
                    &mut connection.stream,
                    channel_ok_frame,
                    &mut connection.heartbeat_state,
                    &self.log_config,
                )
                .await?;
                log_debug(
                    &self.log_config,
                    &format!("Channel {} opened on connection {}", channel, connection.id),
                );
            }

            (constants::EXCHANGE_CLASS, constants::EXCHANGE_DECLARE) => {
                self.handle_exchange_declare(connection, channel, args)
                    .await?;
            }

            (constants::QUEUE_CLASS, constants::QUEUE_DECLARE) => {
                self.handle_queue_declare(connection, channel, args).await?;
            }

            (constants::QUEUE_CLASS, constants::QUEUE_BIND) => {
                self.handle_queue_bind(connection, channel, args).await?;
            }

            (constants::BASIC_CLASS, constants::BASIC_QOS) => {
                // Send Basic.QosOk
                let qos_ok_frame = AmqpLib::build_basic_qos_ok_frame(channel);
                AmqpLib::send_frame_with_heartbeat(
                    &mut connection.stream,
                    qos_ok_frame,
                    &mut connection.heartbeat_state,
                    &self.log_config,
                )
                .await?;
            }

            (constants::BASIC_CLASS, constants::BASIC_CONSUME) => {
                self.handle_basic_consume(connection, channel, args).await?;
            }

            (constants::BASIC_CLASS, constants::BASIC_PUBLISH) => {
                self.handle_basic_publish(connection, channel, args).await?;
            }

            (constants::BASIC_CLASS, constants::BASIC_ACK) => {
                log_trace(&self.log_config, "Received Basic.Ack");
            }

            _ => {
                log_debug(
                    &self.log_config,
                    &format!(
                        "Server {} - Unhandled method {}.{} on channel {}",
                        self.server_id, class, method, channel
                    ),
                );
            }
        }

        Ok(())
    }

    async fn handle_exchange_declare(
        &mut self,
        connection: &mut ServerConnection,
        channel: u16,
        args: &[u8],
    ) -> Result<()> {
        let mut pos = 2; // Skip reserved
        let (exchange_name, remaining) = AmqpLib::parse_short_string(&args[pos..])?;
        pos += 1 + exchange_name.len();

        let (exchange_type, _) = AmqpLib::parse_short_string(&remaining)?;

        let exchange = Exchange {
            name: exchange_name.clone(),
            exchange_type,
            durable: true, // Simplified
            auto_delete: false,
            internal: false,
        };

        if let Some(vhost) = self.virtual_hosts.get_mut(&connection.vhost) {
            vhost.exchanges.insert(exchange_name.clone(), exchange);
            log_debug(
                &self.log_config,
                &format!(
                    "Exchange '{}' declared in vhost '{}'",
                    exchange_name, connection.vhost
                ),
            );
        }

        // Send Exchange.DeclareOk
        let declare_ok_frame = AmqpLib::build_exchange_declare_ok_frame(channel);
        AmqpLib::send_frame_with_heartbeat(
            &mut connection.stream,
            declare_ok_frame,
            &mut connection.heartbeat_state,
            &self.log_config,
        )
        .await?;

        Ok(())
    }

    async fn handle_queue_declare(
        &mut self,
        connection: &mut ServerConnection,
        channel: u16,
        args: &[u8],
    ) -> Result<()> {
        let pos = 2; // Skip reserved
        let (queue_name, _) = AmqpLib::parse_short_string(&args[pos..])?;

        let actual_queue_name = if queue_name.is_empty() {
            format!("amq.gen-{}", Uuid::new_v4())
        } else {
            queue_name
        };

        let queue = Queue {
            name: actual_queue_name.clone(),
            durable: true, // Simplified
            exclusive: false,
            auto_delete: false,
            messages: VecDeque::new(),
            consumers: Vec::new(),
        };

        if let Some(vhost) = self.virtual_hosts.get_mut(&connection.vhost) {
            vhost.queues.insert(actual_queue_name.clone(), queue);
            log_debug(
                &self.log_config,
                &format!(
                    "Queue '{}' declared in vhost '{}'",
                    actual_queue_name, connection.vhost
                ),
            );
        }

        // Send Queue.DeclareOk
        let declare_ok_frame =
            AmqpLib::build_queue_declare_ok_frame(channel, &actual_queue_name, 0, 0);
        AmqpLib::send_frame_with_heartbeat(
            &mut connection.stream,
            declare_ok_frame,
            &mut connection.heartbeat_state,
            &self.log_config,
        )
        .await?;

        Ok(())
    }

    async fn handle_queue_bind(
        &mut self,
        connection: &mut ServerConnection,
        channel: u16,
        args: &[u8],
    ) -> Result<()> {
        let mut pos = 2; // Skip reserved
        let (queue_name, remaining) = AmqpLib::parse_short_string(&args[pos..])?;
        pos = 1 + queue_name.len();

        let (exchange_name, remaining) = AmqpLib::parse_short_string(&remaining)?;
        pos = 1 + exchange_name.len();

        let (routing_key, _) = AmqpLib::parse_short_string(&remaining)?;

        let binding = Binding {
            queue: queue_name.clone(),
            exchange: exchange_name.clone(),
            routing_key: routing_key.clone(),
        };

        if let Some(vhost) = self.virtual_hosts.get_mut(&connection.vhost) {
            vhost.bindings.push(binding);
            log_debug(
                &self.log_config,
                &format!(
                    "Bound queue '{}' to exchange '{}' with key '{}'",
                    queue_name, exchange_name, routing_key
                ),
            );
        }

        // Send Queue.BindOk
        let bind_ok_frame = AmqpLib::build_queue_bind_ok_frame(channel);
        AmqpLib::send_frame_with_heartbeat(
            &mut connection.stream,
            bind_ok_frame,
            &mut connection.heartbeat_state,
            &self.log_config,
        )
        .await?;

        Ok(())
    }

    async fn handle_basic_consume(
        &mut self,
        connection: &mut ServerConnection,
        channel: u16,
        args: &[u8],
    ) -> Result<()> {
        let mut pos = 2; // Skip reserved
        let (queue_name, remaining) = AmqpLib::parse_short_string(&args[pos..])?;
        pos = 1 + queue_name.len();

        let (consumer_tag, _) = AmqpLib::parse_short_string(&remaining)?;

        let actual_consumer_tag = if consumer_tag.is_empty() {
            format!("consumer-{}", Uuid::new_v4())
        } else {
            consumer_tag
        };

        // Add consumer to queue
        if let Some(vhost) = self.virtual_hosts.get_mut(&connection.vhost) {
            if let Some(queue) = vhost.queues.get_mut(&queue_name) {
                queue.consumers.push(actual_consumer_tag.clone());
                log_debug(
                    &self.log_config,
                    &format!(
                        "Consumer '{}' added to queue '{}'",
                        actual_consumer_tag, queue_name
                    ),
                );
            }
        }

        // Send Basic.ConsumeOk
        let consume_ok_frame = AmqpLib::build_basic_consume_ok_frame(channel, &actual_consumer_tag);
        AmqpLib::send_frame_with_heartbeat(
            &mut connection.stream,
            consume_ok_frame,
            &mut connection.heartbeat_state,
            &self.log_config,
        )
        .await?;

        Ok(())
    }

    async fn handle_basic_publish(
        &mut self,
        connection: &mut ServerConnection,
        _channel: u16,
        args: &[u8],
    ) -> Result<()> {
        let mut pos = 2; // Skip reserved
        let (exchange_name, remaining) = AmqpLib::parse_short_string(&args[pos..])?;
        pos = 1 + exchange_name.len();

        let (routing_key, _) = AmqpLib::parse_short_string(&remaining)?;

        // Find queues bound to this exchange/routing key
        let matching_queues = if let Some(vhost) = self.virtual_hosts.get(&connection.vhost) {
            vhost
                .bindings
                .iter()
                .filter(|b| b.exchange == exchange_name && b.routing_key == routing_key)
                .map(|b| b.queue.clone())
                .collect::<Vec<String>>()
        } else {
            Vec::new()
        };

        // Note: In a real implementation, we would wait for content header and body frames
        log_debug(&self.log_config, &format!("Server {} published message to exchange '{}' with routing key '{}', matched {} queues",
               self.server_id, exchange_name, routing_key, matching_queues.len()));

        self.stats.messages_received += 1;

        Ok(())
    }
}

// === CONFIGURATION ENUMS ===
#[derive(Debug, Clone, ValueEnum)]
pub enum ReliabilityMode {
    Fire,
    Confirm,
    Transactional,
}

impl ReliabilityMode {
    pub fn wire_name(&self) -> &'static str {
        match self {
            ReliabilityMode::Fire => "fire",
            ReliabilityMode::Confirm => "confirm",
            ReliabilityMode::Transactional => "transactional",
        }
    }
}

#[derive(Debug, Clone, ValueEnum)]
pub enum DurabilityMode {
    Durable,
    Transient,
}

impl DurabilityMode {
    pub fn wire_name(&self) -> &'static str {
        match self {
            DurabilityMode::Durable => "durable",
            DurabilityMode::Transient => "transient",
        }
    }

    pub fn is_durable(&self) -> bool {
        matches!(self, DurabilityMode::Durable)
    }
}

#[derive(Debug, Clone, ValueEnum)]
pub enum AutoDeleteMode {
    Persistent,
    AutoDelete,
}

impl AutoDeleteMode {
    pub fn wire_name(&self) -> &'static str {
        match self {
            AutoDeleteMode::Persistent => "persistent",
            AutoDeleteMode::AutoDelete => "auto-delete",
        }
    }

    pub fn is_auto_delete(&self) -> bool {
        matches!(self, AutoDeleteMode::AutoDelete)
    }
}

#[derive(Debug, Clone, ValueEnum)]
pub enum ExclusivityMode {
    Shared,
    Exclusive,
}

impl ExclusivityMode {
    pub fn wire_name(&self) -> &'static str {
        match self {
            ExclusivityMode::Shared => "shared",
            ExclusivityMode::Exclusive => "exclusive",
        }
    }

    pub fn is_exclusive(&self) -> bool {
        matches!(self, ExclusivityMode::Exclusive)
    }
}

#[derive(Debug, Clone, ValueEnum)]
pub enum InternalMode {
    External,
    Internal,
}

impl InternalMode {
    pub fn wire_name(&self) -> &'static str {
        match self {
            InternalMode::External => "external",
            InternalMode::Internal => "internal",
        }
    }

    pub fn is_internal(&self) -> bool {
        matches!(self, InternalMode::Internal)
    }
}

#[derive(Debug, Clone, ValueEnum)]
pub enum QueueType {
    Classic,
    Quorum,
    Stream,
}

impl QueueType {
    pub fn wire_name(&self) -> &'static str {
        match self {
            QueueType::Classic => "classic",
            QueueType::Quorum => "quorum",
            QueueType::Stream => "stream",
        }
    }
}

#[derive(Debug, Clone, ValueEnum)]
pub enum ChannelMode {
    Regular,
    Confirm,
    Transactional,
}

impl ChannelMode {
    pub fn wire_name(&self) -> &'static str {
        match self {
            ChannelMode::Regular => "regular",
            ChannelMode::Confirm => "confirm",
            ChannelMode::Transactional => "transactional",
        }
    }
}

#[derive(Debug, Clone, ValueEnum)]
pub enum QosScope {
    PerConsumer,
    PerChannel,
}

impl QosScope {
    pub fn wire_name(&self) -> &'static str {
        match self {
            QosScope::PerConsumer => "per-consumer",
            QosScope::PerChannel => "per-channel",
        }
    }
}

#[derive(Debug, Clone, ValueEnum)]
pub enum PublishMode {
    Standard,
    Immediate,
    Mandatory,
}

impl PublishMode {
    pub fn wire_name(&self) -> &'static str {
        match self {
            PublishMode::Standard => "standard",
            PublishMode::Immediate => "immediate",
            PublishMode::Mandatory => "mandatory",
        }
    }
}

// === CONFIGURATION STRUCTS ===
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub exchange: String,
    pub routing_key: String,
    pub exchange_type: ExchangeType,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub queue: String,
    pub exclusive: bool,
    pub count: Option<usize>,
    pub send_interval: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub queue: String,
    pub prefetch_count: u16,
    pub no_ack: bool,
    pub ack_mode: AckMode,
    pub count: Option<usize>,
    pub validate_messages: bool,
}

// === COMMAND LINE INTERFACE ===
#[derive(Parser, Debug)]
#[command(name = "amqp")]
#[command(about = "AMQP 0.9.1 Server/Client Implementation")]
#[command(version = "1.0.0")]
pub struct Config {
    // === Common Arguments ===
    #[arg(
        short = 'H',
        long,
        default_value = "localhost",
        help = "Server hostname"
    )]
    pub host: String,

    #[arg(short, long, default_value = "5672", help = "Server port")]
    pub port: u16,

    #[arg(long, default_value = "/", help = "Virtual host")]
    pub vhost: String,

    #[arg(short, long, default_value = "guest", help = "Username")]
    pub user: String,

    #[arg(
        short,
        long,
        default_value = "",
        help = "Password (if empty, will prompt)"
    )]
    pub password: String,

    #[arg(short, long, help = "Enable debug output")]
    pub debug: bool,

    #[arg(long, help = "Enable detailed tracing")]
    pub trace: bool,

    #[arg(short, long, help = "Suppress info output")]
    pub quiet: bool,

    #[arg(long, default_value = "131072", help = "Maximum frame size")]
    pub frame_max: u32,

    #[arg(long, default_value = "60", help = "Heartbeat interval in seconds")]
    pub heartbeat_interval: u16,

    // === Mode Selection ===
    #[arg(long, help = "Start in AMQP server mode")]
    pub server: bool,

    #[arg(long, help = "Enable producer mode")]
    pub client_producer: bool,

    #[arg(long, help = "Enable consumer mode")]
    pub client_consumer: bool,

    // === Test Mode ===
    #[arg(long, help = "Enable test mode")]
    pub test_mode: bool,

    #[arg(long, help = "Validate received messages in test mode")]
    pub validate_messages: bool,

    // === Client Configuration ===
    #[arg(short, long, default_value = "test_exchange", help = "Exchange name")]
    pub exchange: String,

    #[arg(short, long, default_value = "test.key", help = "Routing key")]
    pub topic: String,

    #[arg(short, long, default_value = "test_queue", help = "Queue name")]
    pub queue: String,

    #[arg(short, long, default_value = "fire", help = "Reliability mode")]
    pub reliability: ReliabilityMode,

    #[arg(long, default_value = "direct", help = "Exchange type")]
    pub exchange_type: ExchangeType,

    #[arg(long, default_value = "durable", help = "Exchange durability")]
    pub exchange_durability: DurabilityMode,

    #[arg(long, default_value = "persistent", help = "Exchange auto-delete")]
    pub exchange_auto_delete: AutoDeleteMode,

    #[arg(long, default_value = "external", help = "Exchange internal")]
    pub exchange_internal: InternalMode,

    #[arg(long, default_value = "classic", help = "Queue type")]
    pub queue_type: QueueType,

    #[arg(long, default_value = "durable", help = "Queue durability")]
    pub queue_durability: DurabilityMode,

    #[arg(long, default_value = "shared", help = "Queue exclusivity")]
    pub queue_exclusivity: ExclusivityMode,

    #[arg(long, default_value = "persistent", help = "Queue auto-delete")]
    pub queue_auto_delete: AutoDeleteMode,

    #[arg(long, default_value = "regular", help = "Channel mode")]
    pub channel_mode: ChannelMode,

    #[arg(long, default_value = "persistent", help = "Delivery mode")]
    pub delivery_mode: DeliveryMode,

    #[arg(long, default_value = "manual", help = "Acknowledgment mode")]
    pub ack_mode: AckMode,

    #[arg(long, default_value = "1", help = "Prefetch count")]
    pub prefetch_count: u16,

    #[arg(long, default_value = "per-consumer", help = "QoS scope")]
    pub qos_scope: QosScope,

    #[arg(long, default_value = "standard", help = "Publish mode")]
    pub publish_mode: PublishMode,

    #[arg(long, help = "Messages to send (producer)")]
    pub send_count: Option<usize>,

    #[arg(long, help = "Messages to receive (consumer)")]
    pub receive_count: Option<usize>,

    #[arg(long, default_value = "1000", help = "Send interval in milliseconds")]
    pub send_interval_ms: u64,

    // === Server Configuration ===
    #[arg(long, default_value = "1000", help = "Maximum connections")]
    pub server_max_connections: usize,

    #[arg(long, default_value = "2047", help = "Max channels per connection")]
    pub server_max_channels_per_connection: u16,

    #[arg(long, default_value = "10000", help = "Max queues per vhost")]
    pub server_max_queues_per_vhost: usize,

    #[arg(long, default_value = "1000", help = "Max exchanges per vhost")]
    pub server_max_exchanges_per_vhost: usize,

    #[arg(long, default_value = "100", help = "Max bindings per queue")]
    pub server_max_bindings_per_queue: usize,

    #[arg(long, default_value = "100", help = "Max consumers per queue")]
    pub server_max_consumers_per_queue: usize,

    #[arg(long, default_value = "/", help = "Virtual host names")]
    pub server_vhost_list: String,

    #[arg(long, help = "Password file path")]
    pub server_password_file: Option<String>,

    #[arg(long, default_value = "amqp-server.db", help = "SQLite database path")]
    pub server_database_path: String,

    #[arg(long, default_value = "300", help = "Checkpoint interval in seconds")]
    pub server_checkpoint_interval_secs: u64,

    // === Concurrency ===
    #[arg(long, default_value = "1", help = "Number of concurrent producers")]
    pub num_producers: usize,

    #[arg(long, default_value = "1", help = "Number of concurrent consumers")]
    pub num_consumers: usize,
}

impl Config {
    pub fn to_producer_config(&self) -> ProducerConfig {
        ProducerConfig {
            exchange: self.exchange.clone(),
            routing_key: self.topic.clone(),
            exchange_type: self.exchange_type.clone(),
            durable: self.exchange_durability.is_durable(),
            auto_delete: self.exchange_auto_delete.is_auto_delete(),
            internal: self.exchange_internal.is_internal(),
            queue: self.queue.clone(),
            exclusive: self.queue_exclusivity.is_exclusive(),
            count: if self.test_mode {
                self.send_count
            } else {
                None
            },
            send_interval: if self.send_interval_ms > 0 {
                Some(Duration::from_millis(self.send_interval_ms))
            } else {
                None
            },
        }
    }

    pub fn to_consumer_config(&self) -> ConsumerConfig {
        ConsumerConfig {
            queue: self.queue.clone(),
            prefetch_count: self.prefetch_count,
            no_ack: self.ack_mode.is_no_ack(),
            ack_mode: self.ack_mode.clone(),
            count: if self.test_mode {
                self.receive_count
            } else {
                None
            },
            validate_messages: self.validate_messages,
        }
    }

    pub fn to_resource_limits(&self) -> ResourceLimits {
        ResourceLimits {
            max_connections: self.server_max_connections,
            max_channels_per_connection: self.server_max_channels_per_connection,
            max_queues_per_vhost: self.server_max_queues_per_vhost,
            max_exchanges_per_vhost: self.server_max_exchanges_per_vhost,
            max_bindings_per_queue: self.server_max_bindings_per_queue,
            max_consumers_per_queue: self.server_max_consumers_per_queue,
            max_frame_size: self.frame_max,
        }
    }

    pub fn to_log_config(&self) -> LogConfig {
        LogConfig {
            debug: self.debug,
            trace: self.trace,
            quiet: self.quiet,
        }
    }
}

// === PRODUCER/CONSUMER EXECUTION RESULTS ===
#[derive(Debug, Clone)]
pub struct ProducerStats {
    pub client_id: String,
    pub messages_sent: usize,
    pub bytes_sent: usize,
    pub errors: usize,
    pub duration: Duration,
    pub final_delivery_tag: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ConsumerStats {
    pub client_id: String,
    pub messages_received: usize,
    pub bytes_received: usize,
    pub errors: usize,
    pub duration: Duration,
    pub final_delivery_tag: Option<u64>,
}

// === PASSWORD HANDLING ===
async fn get_password(provided: Option<String>, log_config: &LogConfig) -> Result<String> {
    match provided {
        Some(password) if !password.is_empty() => Ok(password),
        _ => {
            // Read password from stdin
            eprint!("Password: ");

            #[cfg(unix)]
            {
                use std::os::unix::io::AsRawFd;
                let fd = std::io::stdin().as_raw_fd();
                let mut termios = unsafe { std::mem::zeroed() };
                unsafe {
                    libc::tcgetattr(fd, &mut termios);
                    termios.c_lflag &= !libc::ECHO;
                    libc::tcsetattr(fd, libc::TCSANOW, &termios);
                }

                let mut password = String::new();
                let read_result = std::io::stdin().read_line(&mut password);

                unsafe {
                    libc::tcgetattr(fd, &mut termios);
                    termios.c_lflag |= libc::ECHO;
                    libc::tcsetattr(fd, libc::TCSANOW, &termios);
                }

                println!();
                read_result.map_err(AmqpError::Io)?;
                Ok(password.trim().to_string())
            }

            #[cfg(not(unix))]
            {
                let mut password = String::new();
                std::io::stdin()
                    .read_line(&mut password)
                    .map_err(AmqpError::Io)?;
                Ok(password.trim().to_string())
            }

            // // For now, just read normally (in production, use a crate like rpassword)
            // // Note: Password will be visible when typing
            // use tokio::io::{AsyncBufReadExt, BufReader};
            // let stdin = tokio::io::stdin();
            // let mut reader = BufReader::new(stdin);
            // let mut password = String::new();

            // reader.read_line(&mut password).await
            //     .map_err(|e| AmqpError::Configuration(format!("Failed to read password: {}", e)))?;

            // Ok(password.trim().to_string())
        }
    }
}

// === MAIN FUNCTION ===
#[tokio::main]
async fn main() -> Result<()> {
    let mut config = Config::parse();
    let log_config = config.to_log_config();

    // Setup logging
    if config.debug {
        log_info(&log_config, "Debug mode enabled");
    }
    if config.trace {
        log_info(&log_config, "Trace mode enabled");
    }
    if config.test_mode {
        log_info(&log_config, "Test mode enabled");
    }

    // Get password securely if not provided
    if config.password.is_empty() {
        if config.user != "guest" {
            config.password = get_password(None, &log_config).await?;
        } else {
            config.password = "guest".to_string()
        };
    }

    if config.server {
        // Start server
        let limits = config.to_resource_limits();
        let server_result = AmqpServer::bind(
            "server-1".to_string(),
            &config.host,
            config.port,
            limits,
            log_config.clone(),
        )
        .await?;

        log_info(
            &log_config,
            &format!("Starting AMQP server on {}", server_result.bind_address),
        );
        log_info(
            &log_config,
            &format!(
                "Resource limits: max_connections={}, max_channels_per_connection={}",
                server_result.resource_limits.max_connections,
                server_result.resource_limits.max_channels_per_connection
            ),
        );

        // Create server instance and run
        let mut server = AmqpServer {
            server_id: server_result.server_id,
            listener: TcpListener::bind((config.host.as_str(), config.port))
                .await
                .map_err(|e| AmqpError::Connection(format!("Failed to bind: {}", e)))?,
            connections: HashMap::new(),
            limits: server_result.resource_limits,
            stats: MessageStats::new(),
            virtual_hosts: {
                let mut vh = HashMap::new();
                vh.insert("/".to_string(), AmqpServer::create_default_vhost("/"));
                vh
            },
            log_config: log_config.clone(),
        };

        server.run().await?;
    } else {
        let mut handles = Vec::new();

        // Start producers
        if config.client_producer {
            for i in 0..config.num_producers {
                let producer_config = config.clone();
                let producer_id = format!("producer-{}", i + 1);
                let log_config = log_config.clone();

                handles.push(tokio::spawn(async move {
                    match establish_connection(
                        producer_id.clone(),
                        &producer_config.host,
                        producer_config.port,
                        &producer_config.vhost,
                        &producer_config.user,
                        &producer_config.password,
                        producer_config.heartbeat_interval,
                        producer_config.frame_max,
                        &log_config,
                    ).await {
                        Ok((stream, connection_result, heartbeat_state)) => {
                            log_info(&log_config, &format!("Producer {} connected to {} in {:?}",
                                   connection_result.client_id,
                                   connection_result.server_addr,
                                   connection_result.connection_time));

                            // Run producer workflow
                            match run_producer_workflow(
                                connection_result,
                                producer_config.to_producer_config(),
                                stream,
                                heartbeat_state,
                                &log_config
                            ).await {
                                Ok(stats) => {
                                    log_info(&log_config, &format!(
                                        "Producer {} completed: {} messages, {} bytes, {} errors in {:?}",
                                        stats.client_id, stats.messages_sent, stats.bytes_sent,
                                        stats.errors, stats.duration
                                    ));

                                    // Exit with success if test mode and all messages sent
                                    if producer_config.test_mode {
                                        if let Some(expected) = producer_config.send_count {
                                            if stats.messages_sent == expected && stats.errors == 0 {
                                                log_info(&log_config, &format!("Producer {} test PASSED", stats.client_id));
                                            } else {
                                                log_error(&log_config, &format!(
                                                    "Producer {} test FAILED: expected {} messages, sent {}, errors {}",
                                                    stats.client_id, expected, stats.messages_sent, stats.errors
                                                ));
                                                std::process::exit(1);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    log_error(&log_config, &format!("Producer {} workflow failed: {}", producer_id, e));
                                    if producer_config.test_mode {
                                        std::process::exit(1);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            log_error(&log_config, &format!("Producer {} connection failed: {}", producer_id, e));
                            if producer_config.test_mode {
                                std::process::exit(1);
                            }
                        }
                    }
                }));
            }
        }

        // Start consumers
        if config.client_consumer {
            for i in 0..config.num_consumers {
                let consumer_config = config.clone();
                let consumer_id = format!("consumer-{}", i + 1);
                let log_config = log_config.clone();

                handles.push(tokio::spawn(async move {
                    match establish_connection(
                        consumer_id.clone(),
                        &consumer_config.host,
                        consumer_config.port,
                        &consumer_config.vhost,
                        &consumer_config.user,
                        &consumer_config.password,
                        consumer_config.heartbeat_interval,
                        consumer_config.frame_max,
                        &log_config,
                    ).await {
                        Ok((stream, connection_result, heartbeat_state)) => {
                            log_info(&log_config, &format!("Consumer {} connected to {} in {:?}",
                                   connection_result.client_id,
                                   connection_result.server_addr,
                                   connection_result.connection_time));

                            // Run consumer workflow
                            match run_consumer_workflow(
                                connection_result,
                                consumer_config.to_consumer_config(),
                                stream,
                                heartbeat_state,
                                &log_config
                            ).await {
                                Ok(stats) => {
                                    log_info(&log_config, &format!(
                                        "Consumer {} completed: {} messages, {} bytes, {} errors in {:?}",
                                        stats.client_id, stats.messages_received, stats.bytes_received,
                                        stats.errors, stats.duration
                                    ));

                                    // Exit with success if test mode and all messages received
                                    if consumer_config.test_mode {
                                        if let Some(expected) = consumer_config.receive_count {
                                            if stats.messages_received == expected && stats.errors == 0 {
                                                log_info(&log_config, &format!("Consumer {} test PASSED", stats.client_id));
                                            } else {
                                                log_error(&log_config, &format!(
                                                    "Consumer {} test FAILED: expected {} messages, received {}, errors {}",
                                                    stats.client_id, expected, stats.messages_received, stats.errors
                                                ));
                                                std::process::exit(1);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    log_error(&log_config, &format!("Consumer {} workflow failed: {}", consumer_id, e));
                                    if consumer_config.test_mode {
                                        std::process::exit(1);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            log_error(&log_config, &format!("Consumer {} connection failed: {}", consumer_id, e));
                            if consumer_config.test_mode {
                                std::process::exit(1);
                            }
                        }
                    }
                }));
            }
        }

        // If neither producer nor consumer is specified, show help
        if !config.client_producer && !config.client_consumer {
            log_error(
                &log_config,
                "Error: Must specify either --client-producer or --client-consumer or --server",
            );
            log_error(&log_config, "Use --help for more information");
            std::process::exit(1);
        }

        // Wait for all tasks to complete
        for handle in handles {
            if let Err(e) = handle.await {
                log_error(&log_config, &format!("Task error: {}", e));
                if config.test_mode {
                    std::process::exit(1);
                }
            }
        }

        log_info(&log_config, "All tasks completed");
    }

    Ok(())
}

// === UTILITY IMPLEMENTATIONS ===
impl Clone for Config {
    fn clone(&self) -> Self {
        Self {
            host: self.host.clone(),
            port: self.port,
            vhost: self.vhost.clone(),
            user: self.user.clone(),
            password: self.password.clone(),
            debug: self.debug,
            trace: self.trace,
            quiet: self.quiet,
            frame_max: self.frame_max,
            heartbeat_interval: self.heartbeat_interval,
            server: self.server,
            client_producer: self.client_producer,
            client_consumer: self.client_consumer,
            test_mode: self.test_mode,
            validate_messages: self.validate_messages,
            exchange: self.exchange.clone(),
            topic: self.topic.clone(),
            queue: self.queue.clone(),
            reliability: self.reliability.clone(),
            exchange_type: self.exchange_type.clone(),
            exchange_durability: self.exchange_durability.clone(),
            exchange_auto_delete: self.exchange_auto_delete.clone(),
            exchange_internal: self.exchange_internal.clone(),
            queue_type: self.queue_type.clone(),
            queue_durability: self.queue_durability.clone(),
            queue_exclusivity: self.queue_exclusivity.clone(),
            queue_auto_delete: self.queue_auto_delete.clone(),
            channel_mode: self.channel_mode.clone(),
            delivery_mode: self.delivery_mode.clone(),
            ack_mode: self.ack_mode.clone(),
            prefetch_count: self.prefetch_count,
            qos_scope: self.qos_scope.clone(),
            publish_mode: self.publish_mode.clone(),
            send_count: self.send_count,
            receive_count: self.receive_count,
            send_interval_ms: self.send_interval_ms,
            server_max_connections: self.server_max_connections,
            server_max_channels_per_connection: self.server_max_channels_per_connection,
            server_max_queues_per_vhost: self.server_max_queues_per_vhost,
            server_max_exchanges_per_vhost: self.server_max_exchanges_per_vhost,
            server_max_bindings_per_queue: self.server_max_bindings_per_queue,
            server_max_consumers_per_queue: self.server_max_consumers_per_queue,
            server_vhost_list: self.server_vhost_list.clone(),
            server_password_file: self.server_password_file.clone(),
            server_database_path: self.server_database_path.clone(),
            server_checkpoint_interval_secs: self.server_checkpoint_interval_secs,
            num_producers: self.num_producers,
            num_consumers: self.num_consumers,
        }
    }
}

// === TESTS ===
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_building() {
        let frame = AmqpLib::build_heartbeat_frame();
        assert_eq!(frame[0], constants::FRAME_HEARTBEAT);
        assert_eq!(frame[frame.len() - 1], constants::FRAME_END_BYTE);
    }

    #[test]
    fn test_frame_parsing() {
        let frame = AmqpLib::build_heartbeat_frame();
        let (frame_type, channel, payload) = AmqpLib::parse_frame(&frame).unwrap();
        assert_eq!(frame_type, constants::FRAME_HEARTBEAT);
        assert_eq!(channel, 0);
        assert_eq!(payload.len(), 0);
    }

    #[test]
    fn test_connection_params() {
        let params = ConnectionParams::default();
        assert_eq!(params.channel_max, 2047);
        assert_eq!(params.frame_max, 131072);
        assert_eq!(params.heartbeat, 60);
    }

    #[test]
    fn test_heartbeat_state() {
        let mut heartbeat = HeartbeatState::new(60);
        assert!(heartbeat.is_enabled());
        assert!(!heartbeat.should_send_heartbeat());
        assert!(!heartbeat.is_connection_dead());

        heartbeat.update_received();
        heartbeat.update_sent();
    }

    #[test]
    fn test_resource_limits() {
        let limits = ResourceLimits::default();
        assert_eq!(limits.max_connections, 1000);
        assert_eq!(limits.max_channels_per_connection, 2047);
        assert_eq!(limits.max_frame_size, 131072);
    }

    #[test]
    fn test_message_source() {
        let source = MessageSource {
            prefix: "Test".to_string(),
            include_timestamp: false,
        };

        let msg = source.generate_message(42);
        let expected = "Test 42 @ 0".as_bytes();
        assert_eq!(msg, expected);
    }

    #[test]
    fn test_message_drain() {
        let drain = MessageDrain {
            expected_prefix: Some("Test".to_string()),
            validate_sequence: true,
            last_sequence: None,
        };

        let msg = "Test 42 @ 12345".as_bytes();
        let validation = drain.validate_message(msg).unwrap();
        assert!(validation.valid);
        assert_eq!(validation.sequence, Some(42));
        assert_eq!(validation.timestamp, Some(12345));
    }

    #[test]
    fn test_log_config() {
        let config = LogConfig {
            debug: true,
            trace: false,
            quiet: false,
        };

        assert!(config.should_log(LogLevel::Error));
        assert!(config.should_log(LogLevel::Info));
        assert!(config.should_log(LogLevel::Debug));
        assert!(!config.should_log(LogLevel::Trace));
    }

    #[test]
    fn test_config_conversion() {
        let config = Config {
            exchange: "test_exchange".to_string(),
            topic: "test.key".to_string(),
            queue: "test_queue".to_string(),
            exchange_type: ExchangeType::Direct,
            exchange_durability: DurabilityMode::Durable,
            send_count: Some(10),
            test_mode: true,
            ..Default::default()
        };

        let producer_config = config.to_producer_config();
        assert_eq!(producer_config.exchange, "test_exchange");
        assert_eq!(producer_config.routing_key, "test.key");
        assert_eq!(producer_config.exchange_type.wire_name(), "direct");
        assert_eq!(producer_config.count, Some(10));
    }

    #[test]
    fn test_enum_wire_names() {
        assert_eq!(ExchangeType::Direct.wire_name(), "direct");
        assert_eq!(DeliveryMode::Persistent.mode_value(), 2);
        assert_eq!(AckMode::Manual.wire_name(), "manual");
        assert!(DurabilityMode::Durable.is_durable());
        assert!(!AutoDeleteMode::Persistent.is_auto_delete());
    }
}

// === DEFAULT IMPLEMENTATIONS ===
impl Default for Config {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5672,
            vhost: "/".to_string(),
            user: "guest".to_string(),
            password: "guest".to_string(),
            debug: false,
            trace: false,
            quiet: false,
            frame_max: 131072,
            heartbeat_interval: 60,
            server: false,
            client_producer: false,
            client_consumer: false,
            test_mode: false,
            validate_messages: false,
            exchange: "test_exchange".to_string(),
            topic: "test.key".to_string(),
            queue: "test_queue".to_string(),
            reliability: ReliabilityMode::Fire,
            exchange_type: ExchangeType::Direct,
            exchange_durability: DurabilityMode::Durable,
            exchange_auto_delete: AutoDeleteMode::Persistent,
            exchange_internal: InternalMode::External,
            queue_type: QueueType::Classic,
            queue_durability: DurabilityMode::Durable,
            queue_exclusivity: ExclusivityMode::Shared,
            queue_auto_delete: AutoDeleteMode::Persistent,
            channel_mode: ChannelMode::Regular,
            delivery_mode: DeliveryMode::Persistent,
            ack_mode: AckMode::Manual,
            prefetch_count: 1,
            qos_scope: QosScope::PerConsumer,
            publish_mode: PublishMode::Standard,
            send_count: None,
            receive_count: None,
            send_interval_ms: 1000,
            server_max_connections: 1000,
            server_max_channels_per_connection: 2047,
            server_max_queues_per_vhost: 10000,
            server_max_exchanges_per_vhost: 1000,
            server_max_bindings_per_queue: 100,
            server_max_consumers_per_queue: 100,
            server_vhost_list: "/".to_string(),
            server_password_file: None,
            server_database_path: "amqp-server.db".to_string(),
            server_checkpoint_interval_secs: 300,
            num_producers: 1,
            num_consumers: 1,
        }
    }
}
