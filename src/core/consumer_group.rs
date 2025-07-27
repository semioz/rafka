use std::collections::HashMap;
use std::time::{Duration, SystemTime};


// TODO
#[derive(Debug)]
pub struct ConsumerGroup {
    group_id: String,
    members: HashMap<String, GroupMember>,
    assignments: HashMap<String, Vec<TopicPartition>>,
    generation_id: i32,
    protocol_type: String,
    leader: Option<String>,
    state: GroupState,
}

#[derive(Debug)]
pub struct GroupMember {
    member_id: String,
    client_id: String,
    client_host: String,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    subscription: Vec<String>,  // list of subscribed topics
    last_heartbeat: SystemTime,
}

#[derive(Debug)]
pub struct TopicPartition {
    topic: String,
    partition: i32,
}

#[derive(Debug, PartialEq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
}