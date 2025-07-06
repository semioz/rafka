use tokio::io::{AsyncReadExt};
use tokio::net::TcpStream;

use crate::error::ServerError;

pub struct MessageParser;

impl MessageParser {
    pub async fn read_exact_bytes_async(stream: &mut TcpStream, size: usize) -> Result<Vec<u8>, ServerError> {
        let mut buffer = vec![0; size];
        stream.read_exact(&mut buffer).await?;
        Ok(buffer)
    }

    pub async fn read_i32_async(stream: &mut TcpStream) -> Result<i32, ServerError> {
        let buffer = Self::read_exact_bytes_async(stream, 4).await?;
        Ok(i32::from_be_bytes(
            buffer.try_into()
                .map_err(|_| ServerError::InvalidMessageSize(-1))?
        ))
    }

    pub async fn read_i16_async(stream: &mut TcpStream) -> Result<i16, ServerError> {
        let buffer = Self::read_exact_bytes_async(stream, 2).await?;
        Ok(i16::from_be_bytes(
            buffer.try_into()
                .map_err(|_| ServerError::InvalidMessageSize(-1))?
        ))
    }
}