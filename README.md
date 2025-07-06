# rafka

A minimal Rust implementation of Apache Kafka, focusing on core messaging functionality and broker operations.

## Project Structure

```
rafka/
  - src/
    - core/           # Core broker functionality
      - topic.rs      # Topic management
      - partition.rs  # Partition handling
      - consumer_group.rs # Consumer group coordination
      - replication.rs # Replication management
    - network/        # Network and protocol handling
      - api.rs       # API response builders
      - handler.rs   # Message parsing
      - protocol.rs  # Protocol implementation
      - server.rs    # TCP server
    - storage/        # Storage and persistence
      - log.rs       # Log segment management
      - index.rs     # Message indexing
      - segment.rs   # Segment handling
```

## Completed Components

### Network Layer
- TCP server implementation with async I/O - tokio
- Basic Kafka protocol handling
- Support for API versions request
- Support for basic Fetch request (v16)
- Message parsing and validation
- Response building for supported APIs

## In Progress

### Core Layer
- Topic management implementation
  - Topic creation/deletion
  - Partition management
  - Configuration handling

- Consumer Group Framework
  - Group membership
  - Partition assignment
  - Session management

- Replication System
  - Leader/follower mechanics
  - ISR tracking
  - Replication protocol

### Storage Layer
- Log-based storage system
  - Segment management
  - Message persistence
  - Offset handling

## TO:DO

### Core Features
1. Topic Management
   - Dynamic configuration
   - Multi-partition support
   - Topic deletion and cleanup

2. Consumer Groups
   - Rebalance protocol
   - Static membership
   - Sticky partition assignment
   - Session timeout handling

3. Replication
   - Leader election
   - Replica synchronization
   - Partition reassignment
   - ISR management

### Storage Layer
1. Index Implementation
   - Offset index for fast message lookup
   - Time-based index for time-based queries
   - Index compaction and cleanup

2. Segment Management
   - Segment compaction
   - Segment deletion based on retention
   - Recovery and validation
   - Hot backup support

### Network Layer
1. Additional Protocol Support
   - Produce requests
   - Offset management
   - Topic management APIs
   - Consumer group coordination

2. Security Features
   - Authentication
   - Authorization
   - SSL/TLS support

3. Monitoring and Metrics
   - Broker metrics
   - Topic metrics
   - Consumer group metrics
   - Replication metrics
