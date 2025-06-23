use crate::constants::MAX_MESSAGE_SIZE;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum KafkaErrorCode {
    None = 0,
    UnsupportedVersion = 35,
}

impl From<KafkaErrorCode> for i16 {
    fn from(error: KafkaErrorCode) -> i16 {
        error as i16
    }
}

#[derive(Debug)]
pub enum ServerError {
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