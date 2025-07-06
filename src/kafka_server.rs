use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task;

use crate::{
    constants::MAX_MESSAGE_SIZE,
    error::ServerError,
    protocol::{KafkaProtocolHandler, KafkaRequest},
    message::MessageParser,
};

pub struct KafkaServer {
    address: String,
}

impl KafkaServer {
    pub fn new(address: &str) -> Result<Self, std::io::Error> {
        println!("Server bound to {}", address);
        Ok(KafkaServer { address: address.to_string() })
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

    async fn  read_request(&self, stream: &mut TcpStream) -> Result<KafkaRequest, ServerError> {
        let message_size = MessageParser::read_i32_async(stream).await?;
        self.validate_message_size(message_size)?;

        let api_key = MessageParser::read_i16_async(stream).await?;
        let api_version = MessageParser::read_i16_async(stream).await?;
        let correlation_id = MessageParser::read_i32_async(stream).await?;

        // Read and discard remaining bytes to consume the entire request
        let remaining_size = message_size as usize - 8; // 8 bytes already read
        if remaining_size > 0 {
            let mut remaining = vec![0; remaining_size];
            stream.read_exact(&mut remaining).await?;
        }

        Ok(KafkaRequest {
            api_key,
            api_version,
            correlation_id,
        })
    }

    // multiple requests from a single client connection
    async fn handle_client(&self, mut stream: TcpStream) -> Result<(), ServerError> {
        let peer_addr = stream.peer_addr().unwrap_or_else(|e| {
            eprintln!("Failed to get peer address: {}", e);
            "0.0.0.0:0".parse().unwrap()
        });

        loop {
            match self.read_request(&mut stream).await {
                Ok(request) => {
                    println!("Processing request from {}: {:?}", peer_addr, request);

                    let response = KafkaProtocolHandler::process_request(&request);
                    
                    if !response.is_empty() {
                        stream.write_all(&response).await?;
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

    pub async fn run(&self) -> Result<(), std::io::Error> {
        println!("Starting Kafka server on port 9092");

        let listener = TcpListener::bind(&self.address).await?;

        loop {
            let (stream, addr) = listener.accept().await?;
            println!("New client connected from: {}", addr);

            let server_clone = KafkaServer {
                address: self.address.clone(),
            };

            task::spawn(async move {
                if let Err(e) = server_clone.handle_client(stream).await {
                    eprintln!("Client handling error: {}", e);
                }
            });
        }
    }
}