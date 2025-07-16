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
}

impl Partition {
    pub fn new(id: i32, log: PartitionLog) -> Self {
        Partition {
            id,
            log,
            replicas: Vec::new(),
            isr: Vec::new(),
            leader: None,
        }
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn append_message(&mut self, message: Message) {
        // assign unique offset to new message
        let offset = self.log.next_offset;
        let arc_message = Arc::new(Message {
            offset,
            ..message
        });

        // each partition is an append-only log.
        self.log.messages.push_back(arc_message);
        self.log.next_offset += 1;
    }

    // pull messages starting from a specific offset
    pub fn read_from(&self, offset: i64, max_num_of_messages: usize) -> Vec<Arc<Message>> {
        self.log
            .messages
            .iter()
            .filter(|msg| msg.offset >= offset)
            .take(max_num_of_messages)
            .cloned()
            .collect()
    }

    pub fn clear_log(&mut self) {
        self.log.messages.clear();
        self.log.base_offset = self.log.next_offset;
    }

    pub fn get_high_watermark(&self) -> i64 {
        self.log.next_offset
    }

    pub fn set_leader(&mut self, broker_id: i32) {
        self.leader = Some(broker_id);
    }

    /// adds a replica broker ID
    pub fn add_replica(&mut self, broker_id: i32) {
        if !self.replicas.contains(&broker_id) {
            self.replicas.push(broker_id);
        }
    }

    pub fn update_isr(&mut self, isr_list: Vec<i32>) {
        self.isr = isr_list;
    }

    pub fn is_leader(&self, broker_id: i32) -> bool {
        self.leader == Some(broker_id)
    }

}