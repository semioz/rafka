use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

const MAX_MESSAGE_SIZE: usize = 1024;
const SUPPORTED_VERSION_MIN: i16 = 0;
const SUPPORTED_VERSION_MAX: i16 = 4;
const API_KEY_API_VERSIONS: i16 = 18;

#[derive(Debug, Copy, Clone, PartialEq)]
enum KafkaErrorCode {
    None = 0,
    UnsupportedVersion = 35,
}

impl From<KafkaErrorCode> for i16 {
    fn from(error: KafkaErrorCode) -> i16 {
        error as i16
    }
}

#[derive(Debug)]
enum ServerError {
    IoError(std::io::Error),
    InvalidMessageSize(i32),
}

impl From<std::io::Error> for ServerError {
    fn from(error: std::io::Error) -> Self {
        ServerError::IoError(error)
    }
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::IoError(e) => write!(f, "IO error: {}", e),
            ServerError::InvalidMessageSize(size) => {
                write!(f, "Invalid message size: {} (max: {})", size, MAX_MESSAGE_SIZE)
            }
        }
    }
}

impl std::error::Error for ServerError {}

// Handles parsing of binary messages from TCP streams
struct MessageParser;

impl MessageParser {
    // Reads exactly `size` bytes from the stream
    fn read_exact_bytes(stream: &mut TcpStream, size: usize) -> Result<Vec<u8>, ServerError> {
        let mut buffer = vec![0; size];
        stream.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    // Reads a big-endian i32 from the stream
    fn read_i32(stream: &mut TcpStream) -> Result<i32, ServerError> {
        let buffer = Self::read_exact_bytes(stream, 4)?;
        Ok(i32::from_be_bytes(
            buffer.try_into()
                .map_err(|_| ServerError::InvalidMessageSize(-1))?
        ))
    }

    /// Reads a big-endian i16 from the stream
    fn read_i16(stream: &mut TcpStream) -> Result<i16, ServerError> {
        let buffer = Self::read_exact_bytes(stream, 2)?;
        Ok(i16::from_be_bytes(
            buffer.try_into()
                .map_err(|_| ServerError::InvalidMessageSize(-1))?
        ))
    }
}
macro_rules! kafka_response {
    (
        correlation_id: $correlation_id:expr,
        error_code: $error_code:expr,
        $(
            $field:ident($type:ident): $value:expr
        ),* $(,)?
    ) => {{
        let mut data = Vec::new();
        
        // Standard Kafka response header
        data.extend_from_slice(&($correlation_id as i32).to_be_bytes());
        data.extend_from_slice(&i16::from($error_code).to_be_bytes());
        
        // Custom fields with compile-time type checking
        $(
            kafka_response!(@write_field data, $type, $value, stringify!($field));
        )*
        
        // Wrap with message size
        let mut response = Vec::new();
        response.extend_from_slice(&(data.len() as i32).to_be_bytes());
        response.extend(data);
        response
    }};
    
    // Helper rule for writing different field types
    (@write_field $data:expr, i32, $value:expr, $field_name:expr) => {
        $data.extend_from_slice(&($value as i32).to_be_bytes());
    };
    (@write_field $data:expr, i16, $value:expr, $field_name:expr) => {
        $data.extend_from_slice(&($value as i16).to_be_bytes());
    };
    (@write_field $data:expr, i8, $value:expr, $field_name:expr) => {
        $data.push($value as u8);
    };
}

// that builds binary protocol responses for Kafka
struct ResponseBuilder;

impl ResponseBuilder {
    fn build_api_versions_response(correlation_id: i32, error_code: KafkaErrorCode) -> Vec<u8> {
        kafka_response! {
            correlation_id: correlation_id,
            error_code: error_code,
            
            // APIVersions response fields (Kafka Protocol v4)
            num_api_keys(i8): 2,           // Compact array: actual count + 1
            api_key(i16): API_KEY_API_VERSIONS,  // API_VERSIONS key
            min_version(i16): 0,           // Minimum supported version
            max_version(i16): 4,           // Maximum supported version
            tag_buffer_1(i8): 0,           // TAG_BUFFER for API key entry
            throttle_time(i32): 0,         // Throttle time in milliseconds
            tag_buffer_2(i8): 0,           // TAG_BUFFER for response
        }
    }
}

#[derive(Debug)]
struct KafkaRequest {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
}

struct KafkaServer {
    listener: TcpListener,
}

impl KafkaServer {
    fn new(address: &str) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind(address)?;
        println!("Server bound to {}", address);
        Ok(KafkaServer { listener })
    }

    fn validate_message_size(&self, size: i32) -> Result<(), ServerError> {
        if size <= 0 {
            return Err(ServerError::InvalidMessageSize(size));
        }
        if size as usize > MAX_MESSAGE_SIZE {
            return Err(ServerError::InvalidMessageSize(size));
        }
        Ok(())
    }

    fn read_request(&self, stream: &mut TcpStream) -> Result<KafkaRequest, ServerError> {
        let message_size = MessageParser::read_i32(stream)?;
        self.validate_message_size(message_size)?;

        let api_key = MessageParser::read_i16(stream)?;
        let api_version = MessageParser::read_i16(stream)?;
        let correlation_id = MessageParser::read_i32(stream)?;

        // Read and discard remaining bytes to consume the entire request
        let remaining_size = message_size as usize - 8; // 8 bytes already read
        if remaining_size > 0 {
            let mut remaining = vec![0; remaining_size];
            stream.read_exact(&mut remaining)?;
        }

        Ok(KafkaRequest {
            api_key,
            api_version,
            correlation_id,
        })
    }

    fn is_version_supported(&self, version: i16) -> bool {
        version >= SUPPORTED_VERSION_MIN && version <= SUPPORTED_VERSION_MAX
    }

    fn process_request(&self, request: &KafkaRequest) -> Vec<u8> {
        let error_code = if self.is_version_supported(request.api_version) {
            KafkaErrorCode::None
        } else {
            KafkaErrorCode::UnsupportedVersion
        };

        match request.api_key {
            API_KEY_API_VERSIONS => {
                ResponseBuilder::build_api_versions_response(request.correlation_id, error_code)
            }
            _ => {
                println!("Unsupported API key: {}", request.api_key);
                Vec::new() // Return empty response for unsupported APIs
            }
        }
    }

    // can handle multiple requests from a single client connection
    fn handle_client(&self, mut stream: TcpStream) -> Result<(), ServerError> {
        loop {
            match self.read_request(&mut stream) {
                Ok(request) => {
                    println!("Processing request: {:?}", request);

                    let response = self.process_request(&request);
                    
                    if !response.is_empty() {
                        stream.write_all(&response)?;
                        println!("Response sent for correlation ID: {}", request.correlation_id);
                    }
                }
                Err(ServerError::IoError(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    println!("Client disconnected gracefully");
                    break;
                }
                Err(e) => {
                    eprintln!("Error processing request: {}", e);
                    break;
                }
            }
        }
        Ok(())
    }

    fn run(&self) -> Result<(), std::io::Error> {
        println!("Starting Kafka server on port 9092");
        
        for stream_result in self.listener.incoming() {
            match stream_result {
                Ok(stream) => {
                    println!("New client connected from: {:?}", stream.peer_addr());
                    
                    if let Err(e) = self.handle_client(stream) {
                        eprintln!("Client handling error: {}", e);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                }
            }
        }
        
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = KafkaServer::new("127.0.0.1:9092")?;
    server.run()?;
    Ok(())
}