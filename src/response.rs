use crate::{
    error::KafkaErrorCode,
    constants::{API_KEY_API_VERSIONS, API_KEY_FETCH},
};

#[macro_export]
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

pub struct ResponseBuilder;

impl ResponseBuilder {
    pub fn build_api_versions_response(correlation_id: i32, error_code: KafkaErrorCode) -> Vec<u8> {
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

            // fetch api entry
            api_key_fetch(i16): API_KEY_FETCH,     // API Key for Fetch
            min_version_fetch(i16): 0,             // Minimum version for Fetch API
            max_version_fetch(i16): 16             // Maximum version for Fetch API (at least 16)
        }
    }
}