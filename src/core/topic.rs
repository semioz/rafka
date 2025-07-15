use std::{collections::HashMap, hash::Hash};
use crate::core::{partition::{Partition, Message}, replication};

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

impl Topic {
    pub fn new(name:String, replication_factor:i32, config: TopicConfig) -> Self {
        Topic { name, partitions: HashMap::new(),  replication_factor, config }
    }

    pub fn add_partition(&mut self, partition_id: i32, partition: Partition) {
        self.partitions.insert(partition_id, partition);
    }

    pub fn get_partition(&mut self, partition_id: i32) -> Option<&Partition> {
        self.partitions.get(&partition_id)
    }

    pub fn get_partition_mut(&mut self, partition_id: i32) -> Option<&mut Partition> {
        self.partitions.get_mut(&partition_id)
    }

    pub fn num_partitions(&self) -> usize {
        self.partitions.len()
    }

    pub fn all_partitions(&self) -> Vec<i32> {
        self.partitions.keys().cloned().collect()
    }

    pub fn append_message_to_partition(&mut self, partition_id: i32, message: Message) -> Result<(), String> {
        if let Some(partition) = self.partitions.get_mut(&partition_id) {
            partition.append_message(message);
            Ok(())
        } else {
            Err(format!("Partition {} not found", partition_id))
        }
    }
}

