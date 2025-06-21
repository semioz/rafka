use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

const MAX_MESSAGE_SIZE: usize = 1024;
const SUPPORTED_VERSION_MIN: i16 = 0;
const SUPPORTED_VERSION_MAX: i16 = 4;

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

struct MessageParser;

impl MessageParser {
    fn read_exact_bytes(stream: &mut TcpStream, size: usize) -> Result<Vec<u8>, ServerError> {
        let mut buffer = vec![0; size];
        stream.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    fn read_i32(stream: &mut TcpStream) -> Result<i32, ServerError> {
        let buffer = Self::read_exact_bytes(stream, 4)?;
        Ok(i32::from_be_bytes(buffer.try_into().unwrap()))
    }

    fn read_i16(stream: &mut TcpStream) -> Result<i16, ServerError> {
        let buffer = Self::read_exact_bytes(stream, 2)?;
        Ok(i16::from_be_bytes(buffer.try_into().unwrap()))
    }
}

struct ResponseBuilder;

macro_rules! kafka_response {
    (
        correlation_id: $correlation_id:expr,
        error_code: $error_code:expr,
        $(
            $field:ident($type:ident): $value:expr
        ),* $(,)?
    ) => {{
        let mut data = Vec::new();
        
        // standard kafka response header
        data.extend_from_slice(&($correlation_id as i32).to_be_bytes());
        data.extend_from_slice(&(i16::from($error_code)).to_be_bytes());
        
        $(
            match stringify!($type) {
                "i32" => data.extend_from_slice(&($value as i32).to_be_bytes()),
                "i16" => data.extend_from_slice(&($value as i16).to_be_bytes()),
                "i8" => data.push($value as u8),
                _ => panic!("Unsupported type for {}: {}", stringify!($field), stringify!($type)),
            }
        )*
        
        // wrapping with message size
        let mut response = Vec::new();
        response.extend_from_slice(&(data.len() as i32).to_be_bytes());
        response.extend(data);
        response
    }};
}

impl ResponseBuilder {
    fn build_api_versions_response(correlation_id: i32, error_code: KafkaErrorCode) -> Vec<u8> {
    kafka_response! {
            correlation_id: correlation_id,
            error_code: error_code,
            
            num_api_keys(i8): 2,
            api_key(i16): 18,
            min_version(i16): 0,
            max_version(i16): 4,
            tag_buffer_1(i8): 0,
            throttle_time(i32): 220,
            tag_buffer_2(i8): 0,
        }
    }
}

struct KafkaServer {
    listener: TcpListener,
}

impl KafkaServer {
    fn new(address: &str) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind(address)?;
        Ok(KafkaServer { listener })
    }

    fn validate_message_size(&self, size: i32) -> Result<(), ServerError> {
        if size <= 0 || size as usize > MAX_MESSAGE_SIZE {
            return Err(ServerError::InvalidMessageSize(size));
        }
        Ok(())
    }

    fn read_request(&self, stream: &mut TcpStream) -> Result<(i16, i16, i32), ServerError> {
        let message_size = MessageParser::read_i32(stream)?;
        self.validate_message_size(message_size)?;

        let api_key = MessageParser::read_i16(stream)?;
        let api_version = MessageParser::read_i16(stream)?;
        let correlation_id = MessageParser::read_i32(stream)?;

        // Read remaining bytes
        let remaining_size = message_size as usize - 8; // 8 bytes already read (api_key + api_version + correlation_id)
        let mut remaining = vec![0; remaining_size];
        stream.read_exact(&mut remaining)?;

        Ok((api_key, api_version, correlation_id))
    }

fn handle_client(&self, mut stream: TcpStream) -> Result<(), ServerError> {
        // can handle multiple requests on the same connection
        loop {
            match self.read_request(&mut stream) {
                Ok((api_key, api_version, correlation_id)) => {
                    println!("Received request - API Key: {}, Version: {}, Correlation ID: {}", 
                             api_key, api_version, correlation_id);

                    let error_code = if api_version < SUPPORTED_VERSION_MIN || api_version > SUPPORTED_VERSION_MAX {
                        KafkaErrorCode::UnsupportedVersion
                    } else {
                        KafkaErrorCode::None
                    };

                    let response = if api_key == 18 { // API_VERSIONS
                        ResponseBuilder::build_api_versions_response(correlation_id, error_code)
                    } else {
                        // For unsupported API keys, return empty response or error
                        Vec::new()
                    };

                    if !response.is_empty() {
                        stream.write_all(&response)?;
                        println!("Response sent successfully for correlation ID: {}", correlation_id);
                    }
                }
                Err(ServerError::IoError(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    println!("Client disconnected");
                    break;
                }
                Err(e) => {
                    eprintln!("Error reading request: {:?}", e);
                    break;
                }
            }
        }
        Ok(())
    }

    fn run(&self) {
        println!("Starting server on port 9092");
        
        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    println!("Accepted new connection");
                    if let Err(e) = self.handle_client(stream) {
                        eprintln!("Error handling client: {:?}", e);
                    }
                }
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
        }
    }
}

fn main() {
    match KafkaServer::new("127.0.0.1:9092") {
        Ok(server) => server.run(),
        Err(e) => eprintln!("Failed to start server: {}", e),
    }
}