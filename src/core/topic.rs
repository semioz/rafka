use std::{collections::HashMap, sync::Arc};
use crate::{core::partition::{Message, Partition}};
use tokio::sync::RwLock;
use thiserror::Error;

#[derive(Debug)]
pub struct Topic {
    name: String,
    partitions: RwLock<HashMap<i32, Arc<Partition>>>,
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

#[derive(Debug, Error)]
pub enum TopicError {
    #[error("Partition {0} not found")]
    PartitionNotFound(i32),

    #[error("Message too large")]
    MessageTooLarge,

    #[error("Unknown error")]
    Unknown,
}

impl Topic {
    pub fn new(name:String, replication_factor:i32, config: TopicConfig) -> Self {
        Topic { name, partitions: RwLock::new(HashMap::new()),  replication_factor, config }
    }

    pub async fn add_partition(&mut self, partition_id: i32, partition: Partition) {
        let mut partitions = self.partitions.write().await;
        partitions.insert(partition_id, Arc::new(partition));
    }

    pub async fn enforce_retention(&mut self, now_ms: i64) {
        let mut partitions = self.partitions.write().await;
        let cutoff = now_ms - self.config.retention_ms;

        for partition in partitions.values_mut() {
            match self.config.cleanup_policy.as_str() {
                // deletes old messages based on time or size.
                "delete" => {
                    let mut log = partition.log_write().await;
                    log.truncate_before_timestamp(cutoff);
                }
                // compacts log by keeping only the latest record for each key. useful for stateful data etc.
                "compact" => {
                    let mut log = partition.log_write().await;
                    log.compact();
                }
                _ => {
                    eprintln!("Unknown cleanup policy: {}", self.config.cleanup_policy);
                }
            }
        }
    }

    pub async fn append_message_to_partition(
        &self,
        partition_id: i32,
        message: Message,
    ) -> Result<(), TopicError> {
        if message.value.len() > self.config.max_message_bytes as usize {
            return Err(TopicError::MessageTooLarge);
        }

        let mut partitions = self.partitions.write().await;
        if let Some(partition) = partitions.get_mut(&partition_id) {
            partition.append_message(message).await;
            Ok(())
        } else {
            Err(TopicError::PartitionNotFound(partition_id))
        }
    }

    pub async fn get_partition(&self, partition_id: i32) -> Option<Arc<Partition>> {
        let partitions = self.partitions.read().await;
        partitions.get(&partition_id).cloned()
    }

    pub async fn num_partitions(&self) -> usize {
        let partitions = self.partitions.read().await;
        partitions.len()
    }

    pub async fn all_partitions(&self) -> Vec<i32> {
        let partitions = self.partitions.read().await;
        partitions.keys().cloned().collect()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn assign_replicas(&self, broker_ids: &[i32]) -> Vec<i32> {
        broker_ids
            .iter()
            .cloned()
            .take(self.replication_factor as usize)
            .collect()
    }

    pub async fn has_enough_replicas(&self, partition_id: i32) -> bool {
        let partitions = self.partitions.read().await;
        if let Some(partition) = partitions.get(&partition_id) {
            partition.isr_count().await as i32 >= self.config.min_insync_replicas
        } else {
            false
        }
    }
}

