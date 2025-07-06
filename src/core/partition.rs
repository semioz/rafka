use std::collections::VecDeque;
use std::sync::Arc;

#[derive(Debug)]
pub struct Partition {
    id: i32,
    log: PartitionLog,
    replicas: Vec<i32>,  // broker IDs that host replicas
    isr: Vec<i32>,       // in-sync replicas
    leader: Option<i32>, // broker ID of the leader
}

#[derive(Debug)]
pub struct PartitionLog {
    messages: VecDeque<Arc<Message>>,
    base_offset: i64,
    next_offset: i64,
}

#[derive(Debug)]
pub struct Message {
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
}