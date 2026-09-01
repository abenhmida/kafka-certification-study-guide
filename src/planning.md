# Kafka Certification Master Course

## Table of Contents

- [Target](#target)
  - [Developer](#developer)
  - [Administrator](#administrator)
- [Part I — Kafka Foundations](#part-i-kafka-foundations)
  - [Chapter 1 — Kafka Mental Model](#chapter-1-kafka-mental-model)
- [Part II — Kafka Architecture](#part-ii-kafka-architecture)
  - [Chapter 2 — Topics, Partitions and Offsets](#chapter-2-topics-partitions-and-offsets)
- [Part III — Kafka Producers](#part-iii-kafka-producers)
  - [Chapter 3 — Producer Architecture](#chapter-3-producer-architecture)
  - [Chapter 4 — Producer Partitioning](#chapter-4-producer-partitioning)
  - [Chapter 5 — Producer Reliability](#chapter-5-producer-reliability)

---

## Target

### Developer

You should become comfortable with:

- Kafka architecture
- Topics and partitions
- Producers
- Serializers
- Partitioning
- Producer reliability
- Idempotence
- Consumers
- Consumer groups
- Rebalancing
- Offset management
- Transactions
- Exactly-once semantics
- AdminClient
- Kafka Streams
- Kafka Connect
- Schema evolution
- Error handling
- Performance tuning
- Testing Kafka applications
-

### Administrator

You should become comfortable with:

- Cluster architecture
- Brokers
- KRaft
- Controllers
- Replication
- Leaders and followers
- ISR
- Partition management
- Topic configuration
- Broker configuration
- Dynamic configuration
- Consumer groups
- Reassignments
- Preferred leader election
- Storage
- Log segments
- Retention
- Compaction
- Security
- ACLs
- TLS/SASL
- Monitoring
- Metrics
- Consumer lag
- Failure diagnosis
- Capacity planning
- Disaster recovery
- MirrorMaker
- Production operations

## Part I — Kafka Foundations

### Chapter 1 — Kafka Mental Model

Study:

- Event streaming
- Publish/subscribe
- Kafka vs traditional queues
- Topics
- Partitions
- Records
- Offsets
- Producers
- Consumers
- Brokers
- Clusters
- Consumer groups
- Replication
  
Core model:

```text
                 KAFKA CLUSTER

       +--------------------------------+
       |              Broker 1          |
       |                                |
Producer ---> Topic: orders             |
       |       | P0 | P1 | P2 |         |
       +-------|----|----|----|---------+
               |    |    |
               v    v    v
             Consumers
                 |
          Consumer Group
```

You must be able to explain why Kafka is a distributed log rather than simply a message queue.

## Part II — Kafka Architecture

### Chapter 2 — Topics, Partitions and Offsets

Master

```text
Topic
 ├── Partition 0
 │    ├── Offset 0
 │    ├── Offset 1
 │    └── Offset 2
 │
 ├── Partition 1
 │    ├── Offset 0
 │    ├── Offset 1
 │    └── Offset 2
 │
 └── Partition 2
      ├── Offset 0
      ├── Offset 1
      └── Offset 2
```

Critical concepts:

- Ordering
- Partition key
- Partition count
- Offset
- Retention
- Log segments
- Replication

Certification rule

**Ordering is guaranteed within a partition, not across an entire topic.**

## Part III — Kafka Producers

### Chapter 3 — Producer Architecture

Study

```text
Application
     |
     v
KafkaProducer
     |
Serializer
     |
Partitioner
     |
RecordAccumulator
     |
Sender thread
     |
Network
     |
Broker
```

Master:

- `bootstrap.servers`
- `key.serializer`
- `value.serializer`
- `acks`
- `retries`
- `delivery.timeout.ms`
- `linger.ms`
- `batch.size`
- `buffer.memory`
- `compression.type`
- `max.in.flight.requests.per.connection`
- `request.timeout.ms`

The book dedicates Chapter 3 to producer construction, configuration, serializers, Avro, partitions, headers, interceptors, quotas and throttling.

### Chapter 4 — Producer Partitioning

Understand exactly how Kafka chooses a partition.

```text
ProducerRecord
      |
      +---- key?
      |
      v
   Partitioner
      |
      v
   Partition
      |
      v
    Broker
```

You should know:

```text
same key
   ↓
same partition
   ↓
ordered processing
```

when the relevant partitioning conditions remain unchanged.

Study:

- Keyed records
- Null keys
- Custom partitioners
- Partition count changes
- Load distribution
- Hot partitions

### Chapter 5 — Producer Reliability

This is a **high-priority certification chapter**.

Understand

```text
acks=0
acks=1
acks=all
```

`acks=0`

Producer does not wait for broker acknowledgment.

`acks=1`

Leader acknowledges the write.

`acks=all`

Leader waits according to the replication/in-sync requirements.

Then combine:

```text
acks
+
replication.factor
+
min.insync.replicas
+
retries
+
idempotence
```

This combination is extremely important.
