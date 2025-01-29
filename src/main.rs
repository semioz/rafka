use std::{io::{Read, Write}, net::{TcpListener, TcpStream}};

const MAX_MESSAGE_SIZE: usize = 1024;
const SUPPORTED_VERSION_MIN: i16 = 0;
const SUPPORTED_VERSION_MAX: i16 = 4;

#[derive(Debug, Copy, Clone)]
enum KafkaErrorCode {
    None = 0,
    UnsupportedVersion = 35,
}

impl From<KafkaErrorCode> for i16 {
    fn from(error: KafkaErrorCode) -> i16 {
        error as i16
    }
}

struct RequestHeader {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
}

#[derive(Debug)]
struct ResponseHeader {
    correlation_id: i32,
    error_code: KafkaErrorCode,
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
    fn extract_helper(buffer: &[u8], start: usize, len: usize) -> Option<i32> {
        if buffer.len() < start + len {
            eprintln!("Request too short to extract field.");
            return None;
        }
        let mut bytes = [0; 4];
        bytes.copy_from_slice(&buffer[start..start + len]);
        Some(i32::from_be_bytes(bytes))
    }

    fn extract_corr_id(buffer: &[u8]) -> i32 {
        Self::extract_helper(buffer, 4, 4).unwrap_or(0)
    }

    fn extract_api_info(buffer: &[u8]) -> Result<(i16, i16), ServerError> {
        if buffer.len() < 4 {
            return Err(ServerError::MessageTooShort);
        }

        let api_key = i16::from_be_bytes([buffer[0], buffer[1]]);
        let api_version = i16::from_be_bytes([buffer[2], buffer[3]]);
        Ok((api_key, api_version))
    }
}

// api versions in req and resp are same --> Produce Request (Version: 3) and Produce Response (Version: 3)
// indicates that apiversions requested by the client is not supported by the broker. assuming broker only supports 0 to 4.
// broker must send must send an ApiVersions version 4 response with the error_code field set to 35(UNSUPPORTED_VERSION)
struct KafkaServer {
    listener: TcpListener,
}

impl KafkaServer {
    fn new(address: &str) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind(address)?;
        Ok(KafkaServer { listener })
    }

    fn read_message_size(&self, stream: &mut TcpStream) -> Result<i32, ServerError> {
        let mut size_buffer = [0; 4];
        // reading exact 4 bytes for message size
        stream.read_exact(&mut size_buffer)?;

        let message_size = i32::from_be_bytes(size_buffer);
        if message_size <= 0 || message_size as usize > MAX_MESSAGE_SIZE {
            return Err(ServerError::InvalidMessageSize(message_size));
        }
        Ok(message_size)
    }

    fn handle_client(&self, mut stream: TcpStream) -> Result<(), ServerError> {
        let message_size = self.read_message_size(&mut stream)?;

        let mut buffer = vec![0; message_size as usize];
        stream.read_exact(&mut buffer)?;

        let correlation_id = MessageParser::extract_corr_id(&buffer);
        let (_, api_version) = MessageParser::extract_api_info(&buffer)?;

        let error_code = if api_version < SUPPORTED_VERSION_MIN || api_version > SUPPORTED_VERSION_MAX {
            KafkaErrorCode::UnsupportedVersion
        } else {
            KafkaErrorCode::None
        };

        let response = ResponseHeader {
            correlation_id,
            error_code,
        };

        self.send_response(&mut stream, &response)?;
        println!("Response sent successfully");
        Ok(())
    }

    fn send_response(&self, stream: &mut TcpStream, response: &ResponseHeader) -> Result<(), ServerError> {
        let mut res = Vec::new();
        res.extend_from_slice(&[0, 0, 0, 0]); // message size placeholder
        res.extend_from_slice(&response.correlation_id.to_be_bytes());
        res.extend_from_slice(&i16::from(response.error_code).to_be_bytes());

        let message_size = (res.len() - 4) as i32;
        res[0..4].copy_from_slice(&message_size.to_be_bytes());

        stream.write_all(&res)?;
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