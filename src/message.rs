use std::{
    io::Read,
    net::TcpStream,
};

use crate::error::ServerError;

pub struct MessageParser;

impl MessageParser {
    /// Reads exactly size bytes from the stream
    pub fn read_exact_bytes(stream: &mut TcpStream, size: usize) -> Result<Vec<u8>, ServerError> {
        let mut buffer = vec![0; size];
        stream.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    /// Reads a big-endian i32 from the stream
    pub fn read_i32(stream: &mut TcpStream) -> Result<i32, ServerError> {
        let buffer = Self::read_exact_bytes(stream, 4)?;
        Ok(i32::from_be_bytes(
            buffer.try_into()
                .map_err(|_| ServerError::InvalidMessageSize(-1))?
        ))
    }

    /// Reads a big-endian i16 from the stream
    pub fn read_i16(stream: &mut TcpStream) -> Result<i16, ServerError> {
        let buffer = Self::read_exact_bytes(stream, 2)?;
        Ok(i16::from_be_bytes(
            buffer.try_into()
                .map_err(|_| ServerError::InvalidMessageSize(-1))?
        ))
    }
}