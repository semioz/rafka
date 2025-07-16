use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct Partition {
    id: i32,
    log: RwLock<PartitionLog>,   
    replicas: RwLock<Vec<i32>>,     
    isr: RwLock<Vec<i32>>,          
    leader: RwLock<Option<i32>>,
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

impl PartitionLog {
    pub fn new() -> Self {
        PartitionLog {
            messages: VecDeque::new(),
            base_offset: 0,
            next_offset: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// clears messages older than a given offset (for retention/cleanup)
    pub fn truncate_before(&mut self, offset: i64) {
        while let Some(front) = self.messages.front() {
            if front.offset >= offset {
                break;
            }
            if let Some(popped) = self.messages.pop_front() {
                self.base_offset = popped.offset;
            }
        }
    }

    pub fn truncate_before_timestamp(&mut self, cutoff: i64) {
        while let Some(front) = self.messages.front() {
            if front.timestamp >= cutoff {
                break;
            }
            if let Some(removed) = self.messages.pop_front() {
                self.base_offset = removed.offset;
            }
        }
    }

    pub fn compact(&mut self) {
        use std::collections::HashMap;
        let mut latest_by_key: HashMap<Vec<u8>, Arc<Message>> = HashMap::new();

        for msg in self.messages.iter() {
            if let Some(key) = &msg.key {
                latest_by_key.insert(key.clone(), Arc::clone(msg));
            }
        }

        // reconstruct log from compacted messages
        let mut compacted: Vec<_> = latest_by_key.into_values().collect();
        compacted.sort_by_key(|m| m.offset); // maintain order

        self.messages = compacted.into_iter().collect(); // into VecDeque
        if let Some(front) = self.messages.front() {
            self.base_offset = front.offset;
        }
    }

}

impl Partition {
    pub fn new(id: i32) -> Self {
        Partition {
            id,
            log: RwLock::new(PartitionLog::new()),
            replicas: RwLock::new(Vec::new()),
            isr: RwLock::new(Vec::new()),
            leader: RwLock::new(None),
        }
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub async fn append_message(&self, message: Message) {
        let mut log = self.log.write().await;
        // assign unique offset to new message
        let offset = log.next_offset;
        let arc_message = Arc::new(Message {
            offset,
            ..message
        });

        // each partition is an append-only log.
        log.messages.push_back(arc_message);
        log.next_offset += 1;
    }

    // pull messages starting from a specific offset
    pub async fn read_from(&self, offset: i64, max_num_of_messages: usize) -> Vec<Arc<Message>> {
        let log = self.log.read().await;
        log.messages
            .iter()
            .filter(|msg| msg.offset >= offset)
            .take(max_num_of_messages)
            .cloned()
            .collect()
    }

    pub async fn log_write(&self) -> tokio::sync::RwLockWriteGuard<'_, PartitionLog> {
        self.log.write().await
    }

    pub async fn log_read(&self) -> tokio::sync::RwLockReadGuard<'_, PartitionLog> {
        self.log.read().await
    }

    pub async fn clear_log(&self) {
        let mut log = self.log.write().await;
        log.messages.clear();
        log.base_offset = log.next_offset;
    }

    pub async fn get_high_watermark(&self) -> i64 {
        let log = self.log.read().await;
        log.next_offset
    }

    pub async fn set_leader(&self, broker_id: i32) {
        let mut leader = self.leader.write().await;
        *leader = Some(broker_id);
    }

    pub async fn add_replica(&self, broker_id: i32) {
        let mut replicas = self.replicas.write().await;
        if !replicas.contains(&broker_id) {
            replicas.push(broker_id);
        }
    }

    pub async fn update_isr(&self, isr_list: Vec<i32>) {
        let mut isr = self.isr.write().await;
        *isr = isr_list;
    }

    pub async fn is_leader(&self, broker_id: i32) -> bool {
        let leader = self.leader.read().await;
        *leader == Some(broker_id)
    }

    pub async fn isr_count(&self) -> usize {
        let isr = self.isr.read().await;
        isr.len()
    }

}