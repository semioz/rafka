use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

use crate::{
    constants::MAX_MESSAGE_SIZE,
    error::ServerError,
    protocol::{KafkaProtocolHandler, KafkaRequest},
    message::MessageParser,
};

pub struct KafkaServer {
    listener: TcpListener,
}

impl KafkaServer {
    pub fn new(address: &str) -> Result<Self, std::io::Error> {
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

    /// Handles multiple requests from a single client connection
    fn handle_client(&self, mut stream: TcpStream) -> Result<(), ServerError> {
        let peer_addr = stream.peer_addr().unwrap_or_else(|e| {
            eprintln!("Failed to get peer address: {}", e);
            "0.0.0.0:0".parse().unwrap()
        });

        loop {
            match self.read_request(&mut stream) {
                Ok(request) => {
                    println!("Processing request from {}: {:?}", peer_addr, request);

                    let response = KafkaProtocolHandler::process_request(&request);
                    
                    if !response.is_empty() {
                        stream.write_all(&response)?;
                        println!("Response sent to {} for correlation ID: {}", peer_addr, request.correlation_id);
                    }
                }
                Err(ServerError::IoError(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    println!("Client {} disconnected gracefully", peer_addr);
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

    pub fn run(&self) -> Result<(), std::io::Error> {
        println!("Starting Kafka server on port 9092");
        
        for stream_result in self.listener.incoming() {
            match stream_result {
                Ok(stream) => {
                    let peer_addr = stream.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap());
                    println!("New client connected from: {}", peer_addr);
                    
                    // Note: In a real implementation, you'd want to share the server instance
                    // using Arc<Mutex<T>> or similar. For now, we're creating a dummy listener
                    // just to satisfy the type system.
                    thread::spawn(move || {
                        let server = KafkaServer { 
                            listener: TcpListener::bind("127.0.0.1:0").unwrap() 
                        };
                        if let Err(e) = server.handle_client(stream) {
                            eprintln!("Client handling error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                }
            }
        }
        
        Ok(())
    }
}