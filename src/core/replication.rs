use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::core::partition::Message;

#[derive(Debug)]
pub struct ReplicaManager {
    broker_id: i32,
    leader_partitions: HashMap<(String, i32), LeaderState>,    // (topic, partition) -> state
    follower_partitions: HashMap<(String, i32), FollowerState>, // (topic, partition) -> state
}

#[derive(Debug)]
pub struct LeaderState {
    topic: String,
    partition_id: i32,
    last_offset: i64,
    isr: Vec<i32>, // in-sync replicas
    followers: HashMap<i32, FollowerProgress>,
}

#[derive(Debug)]
pub struct FollowerState {
    topic: String,
    partition_id: i32,
    leader_id: i32,
    fetch_offset: i64,
    last_fetched_epoch: i32,
}

#[derive(Debug)]
pub struct FollowerProgress {
    broker_id: i32,
    last_fetched_offset: i64,
    last_fetch_timestamp: i64,
}

impl ReplicaManager {
}