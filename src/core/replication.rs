use std::collections::HashMap;
use std::sync::Arc;
use std::vec;
use tokio::sync::RwLock;
use tokio::fs::File;
use chrono::Utc;
use crate::storage::log::Log;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct PartitionMetadata {
    leader_offset: i64,
    isr: Vec<i32>,
    timestamp: i64,
}

#[derive(Debug)]
pub struct ReplicaManager {
    broker_id: i32,
    leader_partitions: Arc<RwLock<HashMap<(String, i32), LeaderState>>>,
    follower_partitions: Arc<RwLock<HashMap<(String, i32), FollowerState>>>,
    partition_logs: HashMap<(String, i32), Log>,
}

#[derive(Debug)]
pub struct LeaderState {
    topic: String,
    partition_id: i32,
    last_offset: i64,
    isr: Vec<i32>, // in-sync replicas
    followers: HashMap<i32, FollowerProgress>,
    last_update_timestamp: i64, // for detecting stale leader
}

#[derive(Debug)]
pub struct FollowerState {
    topic: String,
    partition_id: i32,
    leader_id: i32,
    fetch_offset: i64,
    last_fetched_epoch: i32,
    broker_id: i32,
}

#[derive(Debug)]
pub struct FollowerProgress {
    broker_id: i32,
    last_fetched_offset: i64,
    last_fetch_timestamp: i64,
}

impl ReplicaManager {
    pub fn new(broker_id: i32) -> Self {
        Self { 
            broker_id,
            leader_partitions: Arc::new(RwLock::new(HashMap::new())),
            follower_partitions: Arc::new(RwLock::new(HashMap::new())),
            partition_logs: HashMap::new(),
        }
    }

    pub async fn add_leader_partition(&mut self, topic:String, partition_id: i32) {
        let key = (topic.clone(), partition_id);
        let leader_state = LeaderState {
            topic,
            partition_id,
            last_offset:0, 
            isr: vec![self.broker_id], // assuming leader starts as in-sync
            followers: HashMap::new(),
            last_update_timestamp: Utc::now().timestamp_millis()
        };

        let mut leaders= self.leader_partitions.write().await;
        leaders.insert(key, leader_state);
    }

    pub async fn add_follower_partition(&mut self, topic: String, partition_id: i32, leader_id: i32, follower_id: i32, fetch_offset: i64) {
        let key = (topic.clone(), partition_id);
        let follower_state = FollowerState {
            broker_id: follower_id,
            topic,
            partition_id,
            leader_id,
            fetch_offset,
            last_fetched_epoch: 0,
        };

        let mut followers = self.follower_partitions.write().await;
        followers.insert(key, follower_state);
    }

    pub async fn update_leader_offset(&self, topic: String, partition_id: i32, offset: i64) {
        let key = (topic, partition_id);
        let mut leaders = self.leader_partitions.write().await;

        if let Some(leader) = leaders.get_mut(&key) {
            leader.last_offset = offset;
            leader.last_update_timestamp = Utc::now().timestamp_millis();
        }
    }

    pub async fn remove_leader_partition(&mut self, topic: String, partition_id: i32) {
        let key = (topic, partition_id);
        let mut leaders = self.leader_partitions.write().await;
        leaders.remove(&key);
    }

    pub async fn remove_follower_partition(&mut self, topic: String, partition_id: i32) {
        let key = (topic, partition_id);
        let mut followers = self.follower_partitions.write().await;
        followers.remove(&key);
    }

    pub async fn is_follower_in_isr(&mut self, topic:String, partition_id: i32, follower_id: i32) -> bool {
        let key = (topic, partition_id);
        let leaders = self.leader_partitions.read().await;
        if let Some(leader) = leaders.get(&key) {
            leader.isr.contains(&follower_id)
        } else {
            false
        }
    }

    pub async fn update_follower_fetch(&self, topic: String, partition_id: i32, follower_id: i32, fetch_offset: i64, fetch_epoch: i32) {
        let key = (topic.clone(), partition_id);
        let mut followers = self.follower_partitions.write().await;

        if let Some(follower) = followers.get_mut(&key) {
            if follower.broker_id == follower_id {
                follower.fetch_offset = fetch_offset;
                follower.last_fetched_epoch = fetch_epoch;
            }
        }
    }


    pub async fn update_follower_progress(
    &self,
    topic: String,
    partition_id: i32,
    follower_id: i32,
    offset: i64,
    ) {
        let key = (topic.clone(), partition_id);
        let mut leaders = self.leader_partitions.write().await;

        if let Some(leader) = leaders.get_mut(&key) {
            let progress = FollowerProgress {
                broker_id: follower_id,
                last_fetched_offset: offset,
                last_fetch_timestamp: Utc::now().timestamp_millis(),
            };
            leader.followers.insert(follower_id, progress);
        }
    }

    pub async fn flush_state(&self) {
    let leaders = self.leader_partitions.read().await;

    for ((topic, partition_id), state) in leaders.iter() {
        let metadata = PartitionMetadata {
            leader_offset: state.last_offset,
            isr: state.isr.clone(),
            timestamp: state.last_update_timestamp,
        };

        let path = format!("data/{}/{}-metadata.json", topic, partition_id);
        if let Some(parent) = std::path::Path::new(&path).parent() {
            std::fs::create_dir_all(parent).ok();
        }

        let mut file = match File::create(&path).await {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Failed to create metadata file: {}", e);
                continue;
            }
        };
        let json = match serde_json::to_string_pretty(&metadata) {
            Ok(j) => j, 
            Err(e) => {
                eprintln!("Failed to serialize metadata: {}", e);
                return;
            }
        };
        use tokio::io::AsyncWriteExt;
        if let Err(e) = file.write_all(json.as_bytes()).await {
            eprintln!("Write failed: {}", e);
        }
    }
}

}