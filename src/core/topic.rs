/*
use std::collections::HashMap;
use crate::core::partition::Partition;

#[derive(Debug)]
pub struct Topic {
    name: String,
    partitions: HashMap<i32, Partition>,
    replication_factor: i32,
    config: TopicConfig,
}

#[derive(Debug)]
pub struct TopicConfig {
    cleanup_policy: String,          // delete or compact
    retention_ms: i64,              // how long to keep messages
    max_message_bytes: i32,         // maximum size of a message
    min_insync_replicas: i32,       // minimum number of replicas that must acknowledge writes
}
*/