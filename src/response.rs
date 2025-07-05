use crate::{
    error::KafkaErrorCode,
    constants::{API_KEY_API_VERSIONS, API_KEY_FETCH},
};

pub struct ResponseBuilder;

impl ResponseBuilder {
pub fn build_api_versions_response(correlation_id: i32, error_code: KafkaErrorCode) -> Vec<u8> {
    let mut body = Vec::new();

    // correlation_id
    body.extend_from_slice(&correlation_id.to_be_bytes());

    // error_code
    body.extend_from_slice(&(error_code as i16).to_be_bytes());

    // CompactArray length = 2 entries  encoded as 0x03 (2 + 1)
    body.push(0x03);

    // --- First ApiKey Entry (API_VERSIONS) ---
    body.extend_from_slice(&(API_KEY_API_VERSIONS as i16).to_be_bytes()); // api_key
    body.extend_from_slice(&0i16.to_be_bytes()); // min_version
    body.extend_from_slice(&4i16.to_be_bytes()); // max_version
    body.push(0x00); // tag_buffer (empty)

    // --- Second ApiKey Entry (FETCH) ---
    body.extend_from_slice(&(API_KEY_FETCH as i16).to_be_bytes()); // api_key
    body.extend_from_slice(&0i16.to_be_bytes()); // min_version
    body.extend_from_slice(&16i16.to_be_bytes()); // max_version
    body.push(0x00); // tag_buffer (empty)

    // throttle_time_ms
    body.extend_from_slice(&0i32.to_be_bytes());

    // tag_buffer for response
    body.push(0x00);

    // wrap in full response: prepend length
    let mut response = Vec::new();
    response.extend_from_slice(&(body.len() as i32).to_be_bytes());
    response.extend(body);

    response
}

}