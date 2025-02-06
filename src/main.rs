use bytes::BufMut;
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
    MessageTooShort,
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

impl ResponseBuilder {
    fn build_api_versions_response(correlation_id: i32, error_code: KafkaErrorCode) -> Vec<u8> {
        let mut data = Vec::new();
        
        // Response body
        data.put_i32(correlation_id);
        data.put_i16(i16::from(error_code));
        data.put_i8(2); // num api key records + 1
        data.put_i16(18); // api key
        data.put_i16(0); // min version
        data.put_i16(4); // max version
        data.put_i8(0); // TAG_BUFFER length
        data.put_i32(420); // throttle time ms
        data.put_i8(0); // TAG_BUFFER length

        // Wrap with message size
        let mut response = Vec::new();
        response.put_i32(data.len() as i32);
        response.extend(data);
        
        response
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
        let (api_key, api_version, correlation_id) = self.read_request(&mut stream)?;

        let error_code = if api_version < SUPPORTED_VERSION_MIN || api_version > SUPPORTED_VERSION_MAX {
            KafkaErrorCode::UnsupportedVersion
        } else {
            KafkaErrorCode::None
        };

        let response = if api_key == 18 {
            ResponseBuilder::build_api_versions_response(correlation_id, error_code)
        } else {
            Vec::new() // Handle other API types if needed
        };

        if !response.is_empty() {
            stream.write_all(&response)?;
            println!("Response sent successfully");
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