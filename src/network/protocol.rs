use crate::{
    constants::{API_KEY_API_VERSIONS, SUPPORTED_VERSION_MIN, SUPPORTED_VERSION_MAX, API_KEY_FETCH},
    error::KafkaErrorCode,
    network::api::ResponseBuilder,
};

#[derive(Debug)]
pub struct KafkaRequest {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
}

pub struct KafkaProtocolHandler;

impl KafkaProtocolHandler {
    pub fn is_version_supported(api_key: i16, api_version: i16) -> bool {
        match api_key {
            API_KEY_API_VERSIONS => api_version >= SUPPORTED_VERSION_MIN && api_version <= SUPPORTED_VERSION_MAX,
            API_KEY_FETCH => api_version == 16, // only version 16 supported for Fetch now
            _ => false,
        }
    }

    pub fn process_request(request: &KafkaRequest) -> Vec<u8> {
        let error_code = if Self::is_version_supported(request.api_key, request.api_version) {
            KafkaErrorCode::None
        } else {
            KafkaErrorCode::UnsupportedVersion
        };

        match request.api_key {
            API_KEY_API_VERSIONS => {
                ResponseBuilder::build_api_versions_response(request.correlation_id, error_code)
            }
            API_KEY_FETCH if request.api_version == 16 => {
                ResponseBuilder::build_fetch_response(request.correlation_id)
            }
            _ => {
                println!("Unsupported API key: {}", request.api_key);
                Vec::new() // Return empty response for unsupported APIs
            }
        }
    }
} 