# Kafka Developer Certification — Mock Exam Book


## Table of Contents

- [How to Use This Mock Exam](#how-to-use-this-mock-exam)
  - [Suggested exam rules](#suggested-exam-rules)
  - [Suggested scoring](#suggested-scoring)
- [Section 1 — Kafka Fundamentals](#section-1-kafka-fundamentals)
  - [Question 1](#question-1)
  - [Question 2](#question-2)
  - [Question 3](#question-3)
  - [Question 4](#question-4)
  - [Question 5](#question-5)
- [Section 2 — Producers](#section-2-producers)
  - [Question 6](#question-6)
  - [Question 7](#question-7)
  - [Question 8](#question-8)
  - [Question 9](#question-9)
  - [Question 10](#question-10)
- [Section 3 — Consumers and Consumer Groups](#section-3-consumers-and-consumer-groups)
  - [Question 11](#question-11)
  - [Question 12](#question-12)
  - [Question 13](#question-13)
  - [Question 14](#question-14)
  - [Question 15](#question-15)
- [Section 4 — Offsets and Delivery Semantics](#section-4-offsets-and-delivery-semantics)
  - [Question 16](#question-16)
  - [Question 17](#question-17)
  - [Question 18](#question-18)
  - [Question 19](#question-19)
  - [Question 20](#question-20)
  - [Question 21](#question-21)
  - [Question 22](#question-22)
  - [Question 23](#question-23)
  - [Question 24](#question-24)
  - [Question 25](#question-25)
  - [Question 26](#question-26)
  - [Question 27](#question-27)
  - [Question 28](#question-28)
  - [Question 29](#question-29)
  - [Question 30](#question-30)
  - [Question 31](#question-31)
  - [Question 32](#question-32)
  - [Question 33](#question-33)
  - [Question 34](#question-34)
  - [Question 35](#question-35)
  - [Question 36](#question-36)
  - [Question 37](#question-37)
  - [Question 38](#question-38)
  - [Question 39](#question-39)
  - [Question 40](#question-40)
  - [Question 41](#question-41)
  - [Question 42](#question-42)
  - [Question 43](#question-43)
  - [Question 44](#question-44)
  - [Question 45](#question-45)
  - [Question 46](#question-46)
  - [Question 47](#question-47)
  - [Question 48](#question-48)
  - [Question 49](#question-49)
  - [Question 50](#question-50)
  - [Question 51](#question-51)
  - [Question 52](#question-52)
  - [Question 53](#question-53)
  - [Question 54](#question-54)
  - [Question 55](#question-55)
  - [Question 56](#question-56)
  - [Question 57](#question-57)
  - [Question 58](#question-58)
  - [Question 59](#question-59)
  - [Question 60](#question-60)
- [1. Partitions Are the Unit of Parallelism](#1-partitions-are-the-unit-of-parallelism)
- [2. Ordering](#2-ordering)
- [3. Producer Acknowledgments](#3-producer-acknowledgments)
- [4. Idempotent Producer](#4-idempotent-producer)
- [5. Consumer Groups](#5-consumer-groups)
- [6. Offsets](#6-offsets)
- [7. At-Least-Once Processing](#7-at-least-once-processing)
- [8. Commit Before Processing](#8-commit-before-processing)
- [9. Replication](#9-replication)
- [10. ISR](#10-isr)
- [11. Consumer Rebalancing](#11-consumer-rebalancing)
- [12. `session.timeout.ms` vs `max.poll.interval.ms`](#12-sessiontimeoutms-vs-maxpollintervalms)
  - [`session.timeout.ms`](#sessiontimeoutms)
  - [`max.poll.interval.ms`](#maxpollintervalms)
- [13. Kafka Streams](#13-kafka-streams)
- [14. State Stores](#14-state-stores)
- [15. Transactions](#15-transactions)
- [16. `read_committed`](#16-readcommitted)
- [17. Exactly-Once and External Systems](#17-exactly-once-and-external-systems)
- [Trap 1 — "Kafka guarantees ordering"](#trap-1-kafka-guarantees-ordering)
- [Trap 2 — "More consumers always means more throughput"](#trap-2-more-consumers-always-means-more-throughput)
- [Trap 3 — "Committed offset means record was processed"](#trap-3-committed-offset-means-record-was-processed)
- [Trap 4 — "Exactly once means no duplicate external side effects"](#trap-4-exactly-once-means-no-duplicate-external-side-effects)
- [Trap 5 — "Replication factor 3 means three brokers must always be available"](#trap-5-replication-factor-3-means-three-brokers-must-always-be-available)
- [Trap 6 — "JSON is a schema"](#trap-6-json-is-a-schema)
- [Trap 7 — "`session.timeout.ms` controls processing time"](#trap-7-sessiontimeoutms-controls-processing-time)
- [Scenario A — Consumer Crash](#scenario-a-consumer-crash)
  - [Question](#question)
  - [Explanation](#explanation)
  - [Question](#question)
  - [Question](#question)
  - [Question](#question)
  - [Question](#question)
- [Question A1](#question-a1)
  - [Expected answer](#expected-answer)
- [Question A2](#question-a2)
  - [Expected answer](#expected-answer)
- [Question A3](#question-a3)
  - [Expected answer](#expected-answer)
- [Question A4](#question-a4)
  - [Expected answer](#expected-answer)
- [Question A5](#question-a5)
  - [Expected answer](#expected-answer)
- [Drill 1 — Explain This Architecture](#drill-1-explain-this-architecture)
- [Kafka Core](#kafka-core)
- [Producer](#producer)
- [Consumer](#consumer)
- [Delivery Semantics](#delivery-semantics)
- [Kafka Streams](#kafka-streams)
- [Data Contracts](#data-contracts)
- [Operations](#operations)
- [Security](#security)
- [54–60: Certification Ready](#5460-certification-ready)
- [48–53: Strong Candidate](#4853-strong-candidate)
- [42–47: Intermediate](#4247-intermediate)
- [36–41: Borderline](#3641-borderline)
- [Below 36: Rebuild the Fundamentals](#below-36-rebuild-the-fundamentals)

---
**Version:** 1.0  
**Date:** August 2026  
**Format:** Full-length practice examination  
**Recommended time:** 120 minutes  
**Questions:** 60  
**Difficulty:** Intermediate → Advanced

---

## How to Use This Mock Exam

This mock exam is designed for a Kafka developer preparing for a certification-level assessment.

It emphasizes:

- Kafka architecture and terminology
- Producers and consumers
- Partitions and ordering
- Consumer groups and rebalancing
- Offsets and delivery semantics
- Replication and fault tolerance
- Kafka Streams
- Transactions and exactly-once processing
- Serialization and schemas
- Performance and reliability
- Security fundamentals
- Troubleshooting and operational reasoning
- Scenario-based questions

### Suggested exam rules

1. Do not consult documentation during the first attempt.
2. Allow approximately **2 minutes per question**.
3. For multiple-choice questions, select the **single best answer** unless explicitly stated otherwise.
4. Record your answers separately before checking the answer key.
5. Review every incorrect answer, not only the final score.

### Suggested scoring

| Score | Interpretation |
|---:|---|
| 54–60 | Excellent — certification-ready |
| 48–53 | Strong — minor revision recommended |
| 42–47 | Good foundation — review weak areas |
| 36–41 | Borderline — significant revision needed |
| < 36 | Rebuild fundamentals before attempting certification |

---

# Part I — Mock Examination

## Section 1 — Kafka Fundamentals

### Question 1

What is the primary unit of parallelism in a Kafka topic?

A. Broker  
B. Partition  
C. Consumer  
D. Producer

---

### Question 2

A topic has three partitions. A producer sends records using a key. What determines the partition when the default key-based partitioning behavior is used?

A. The broker with the lowest ID  
B. The consumer group ID  
C. A hash of the record key  
D. The topic creation timestamp

---

### Question 3

Which statement about ordering is correct?

A. Kafka guarantees global ordering across all partitions of a topic.  
B. Kafka guarantees ordering of records within a partition.  
C. Kafka guarantees ordering across all consumer groups.  
D. Kafka guarantees ordering across all topics.

---

### Question 4

What is the purpose of a Kafka broker?

A. It stores and serves Kafka records and participates in cluster operations.  
B. It only creates consumer groups.  
C. It only performs serialization.  
D. It exists exclusively to run Kafka Streams applications.

---

### Question 5

A topic contains six partitions and a consumer group contains two consumers. Assuming all consumers are eligible for assignment, approximately how many partitions can each consumer process?

A. 1  
B. 2  
C. 3  
D. 6

---

## Section 2 — Producers

### Question 6

What is the main purpose of the producer `acks` configuration?

A. It controls consumer polling frequency.  
B. It controls the level of broker acknowledgment required for a produce request.  
C. It controls partition count.  
D. It controls retention.

---

### Question 7

What is the main benefit of enabling idempotent producer behavior?

A. It prevents duplicate records caused by producer retries from being appended multiple times in the relevant sequence.  
B. It guarantees global ordering across topics.  
C. It eliminates the need for replication.  
D. It automatically creates schemas.

---

### Question 8

A producer sends records to a topic with four partitions. It uses the same non-null key for every record. What is the most important consequence?

A. Records are randomly distributed equally across partitions.  
B. Records with that key are normally routed to the same partition, preserving per-key ordering.  
C. Records are sent only to the controller.  
D. Records are rejected.

---

### Question 9

Which producer setting is most directly related to batching records before sending them?

A. `linger.ms`  
B. `group.id`  
C. `fetch.min.bytes`  
D. `auto.offset.reset`

---

### Question 10

Increasing producer `batch.size` can improve throughput primarily because:

A. Kafka creates more partitions automatically.  
B. More records can potentially be accumulated into a single produce batch.  
C. Consumers skip offsets.  
D. Replication is disabled.

---

## Section 3 — Consumers and Consumer Groups

### Question 11

What identifies a consumer as belonging to a particular consumer group?

A. `client.id`  
B. `group.id`  
C. `transactional.id`  
D. `application.id` only

---

### Question 12

A topic has four partitions and a consumer group has six consumers. How many consumers can actively own partitions at the same time?

A. 2  
B. 4  
C. 6  
D. 10

---

### Question 13

A consumer reads a record but crashes before its offset is committed. What can happen after restart?

A. The record may be read again.  
B. The record is permanently deleted.  
C. Kafka automatically moves the record to another topic.  
D. The partition becomes permanently unavailable.

---

### Question 14

What does `auto.offset.reset=earliest` generally mean when there is no valid committed offset for the consumer group?

A. Start at the newest available records.  
B. Start at the earliest available offset.  
C. Start at offset zero even if it has been deleted.  
D. Disable consumption.

---

### Question 15

Two consumers in the same consumer group cannot normally process the same partition simultaneously under a single assignment.

Why?

A. Kafka assigns a partition to only one consumer within a consumer group at a time.  
B. Kafka allows only one consumer per cluster.  
C. Consumers cannot connect to the same broker.  
D. Consumer groups are limited to one partition.

---

## Section 4 — Offsets and Delivery Semantics

### Question 16

Which statement best describes a Kafka consumer offset?

A. It is the physical byte address of a record on disk.  
B. It represents the position of a consumer group in a partition.  
C. It identifies the producer instance.  
D. It identifies the broker leader.

---

### Question 17

A consumer processes a record successfully and then commits its offset. The application crashes afterward. What is the likely result when it restarts?

A. The already-processed record normally will not be processed again by that group from the committed position.  
B. The entire topic is replayed.  
C. Kafka deletes the partition.  
D. The consumer must start from offset zero.

---

### Question 18

Which processing pattern most naturally risks duplicate processing after a crash?

A. Process → commit offset  
B. Commit offset → process  
C. Stop the application before consuming  
D. Delete the consumer group

---

### Question 19

What does at-least-once processing mean?

A. Every record is guaranteed to be processed exactly once globally.  
B. A record is processed zero or more times.  
C. Records are processed one or more times, so duplicates can occur.  
D. Records are never retried.

---

### Question 20

What is required to reason correctly about "exactly once" in a Kafka application?

A. It is enough to set `acks=0`.  
B. You must consider the complete processing pipeline, including Kafka reads, writes, transactions, and external side effects.  
C. It is guaranteed for every external database automatically.  
D. It only depends on partition count.

---

# Section 5 — Replication and Fault Tolerance

### Question 21

A partition has replication factor 3. What does that mean?

A. Three different topics contain the same data.  
B. Three replicas of that partition exist across brokers.  
C. Three consumer groups consume the partition.  
D. Three producers must write every record.

---

### Question 22

Which replica normally handles client reads and writes for a partition?

A. Any follower chosen randomly  
B. The partition leader  
C. The controller only  
D. The oldest broker

---

### Question 23

What is an ISR?

A. Internal Storage Record  
B. In-Sync Replica set  
C. Internal Security Registry  
D. Indexed Segment Repository

---

### Question 24

A partition has replication factor 3. One follower becomes unavailable but the remaining replicas are healthy and in sync. What can happen?

A. The partition can continue operating, depending on the cluster's configuration and remaining ISR/leader state.  
B. Kafka must delete the partition.  
C. All topics are automatically deleted.  
D. Consumers must permanently stop.

---

### Question 25

Why is replication factor important?

A. It controls JSON serialization.  
B. It provides redundancy and helps Kafka tolerate broker failures.  
C. It determines consumer application IDs.  
D. It disables rebalancing.

---

# Section 6 — Serialization and Schemas

### Question 26

What is the purpose of a Kafka serializer?

A. Convert an application object/value into bytes suitable for transmission.  
B. Assign partitions to brokers.  
C. Commit consumer offsets.  
D. Elect a controller.

---

### Question 27

A producer uses JSON serialization. Which statement is most accurate?

A. JSON automatically provides strong schema evolution guarantees.  
B. JSON is a data representation; schema compatibility must be managed separately if required.  
C. JSON eliminates the need for consumers to deserialize data.  
D. JSON automatically enables Kafka transactions.

---

### Question 28

Why are schema registries commonly used with Avro, Protobuf, or JSON Schema?

A. To store Kafka broker logs.  
B. To centrally manage schemas and compatibility rules.  
C. To replace all Kafka brokers.  
D. To perform consumer-group assignment.

---

### Question 29

A consumer receives bytes encoded using a schema it cannot deserialize. What is the most likely result?

A. Deserialization failure.  
B. Kafka automatically converts the data to Java objects.  
C. The broker rewrites the record.  
D. The partition leader changes.

---

### Question 30

Why is schema compatibility important?

A. It helps producers and consumers evolve data contracts without unexpectedly breaking one another.  
B. It increases the number of brokers automatically.  
C. It eliminates partitions.  
D. It guarantees network availability.

---

# Section 7 — Rebalancing

### Question 31

What is a consumer group rebalance?

A. Redistributing partition assignments among consumers in a group.  
B. Moving all Kafka data to a new cluster.  
C. Rebuilding every topic.  
D. Changing replication factor automatically.

---

### Question 32

Which event can trigger a consumer-group rebalance?

A. A consumer joins or leaves the group.  
B. A producer changes its key.  
C. A topic receives one record.  
D. A schema is read by a consumer.

---

### Question 33

Why can excessive rebalancing be harmful?

A. It can interrupt processing and create overhead.  
B. It increases partition ordering guarantees.  
C. It permanently deletes offsets.  
D. It increases replication factor.

---

### Question 34

Which configuration is directly associated with the time a consumer is allowed between heartbeats before being considered failed by the group coordinator?

A. `session.timeout.ms`  
B. `linger.ms`  
C. `retention.ms`  
D. `segment.bytes`

---

### Question 35

A consumer spends a very long time processing records and stops polling frequently enough. What problem can occur?

A. The consumer can exceed the allowed poll interval and be considered unresponsive, potentially causing a rebalance.  
B. Kafka automatically increases the topic partition count.  
C. The producer becomes the group coordinator.  
D. Replication factor becomes zero.

---

# Section 8 — Kafka Streams

### Question 36

What is Kafka Streams?

A. A client library for building stream-processing applications using Kafka.  
B. A Kafka broker replacement.  
C. A database engine unrelated to Kafka.  
D. A network protocol.

---

### Question 37

What is a Kafka Streams state store used for?

A. Maintaining local state needed by stream-processing operations.  
B. Storing broker configuration only.  
C. Replacing Kafka topics globally.  
D. Storing TLS certificates only.

---

### Question 38

What is a common use case for a KTable?

A. Representing a changing table-like view where updates to a key replace or update previous values.  
B. Creating broker replicas.  
C. Managing consumer credentials.  
D. Creating Docker containers.

---

### Question 39

Why does Kafka Streams use changelog topics for many state stores?

A. To make state recoverable by reconstructing local state from Kafka.  
B. To disable partitioning.  
C. To avoid all serialization.  
D. To prevent application restarts.

---

### Question 40

What is the role of `application.id` in a Kafka Streams application?

A. It identifies the Streams application and is used in important aspects such as consumer grouping and internal topic/state management.  
B. It identifies a Kafka broker's IP address.  
C. It identifies the Kafka cluster controller only.  
D. It is equivalent to a topic name.

---

# Section 9 — Transactions and Exactly-Once Processing

### Question 41

What is a Kafka transaction primarily useful for?

A. Atomically publishing multiple records and coordinating offset commits with Kafka writes.  
B. Increasing topic retention.  
C. Creating partitions automatically.  
D. Encrypting all Kafka traffic.

---

### Question 42

A Kafka Streams application reads input records, updates state, and produces output. Why can transactions be useful?

A. They can provide atomicity across relevant Kafka processing operations.  
B. They eliminate all network failures.  
C. They remove the need for serialization.  
D. They make every external database transactionally consistent automatically.

---

### Question 43

What does `isolation.level=read_committed` control for a Kafka consumer?

A. Whether it reads only committed transactional records or also sees aborted transactional records.  
B. Whether the consumer can commit offsets.  
C. Whether partitions can be created.  
D. Whether replication is enabled.

---

### Question 44

A transactional producer aborts a transaction containing records. What should a `read_committed` consumer observe?

A. The aborted records as normal application data.  
B. The aborted transactional records should not be returned as committed application records.  
C. Only the producer ID.  
D. The records twice.

---

### Question 45

Which statement about exactly-once semantics is the best answer?

A. Exactly-once means no component in any system can ever execute an operation twice.  
B. Kafka can provide strong exactly-once processing semantics for Kafka-to-Kafka workflows when correctly configured, but external side effects require additional transactional/idempotent design.  
C. Exactly-once is achieved by setting `acks=0`.  
D. Exactly-once requires one partition only.

---

# Section 10 — Performance

### Question 46

You need to increase producer throughput. Which approach can help?

A. Increase batching and allow appropriate compression.  
B. Set `batch.size=0`.  
C. Disable all broker acknowledgments in every production scenario.  
D. Use one producer thread regardless of workload.

---

### Question 47

Why can compression improve Kafka performance?

A. It can reduce network and storage bytes at the cost of CPU.  
B. It eliminates serialization.  
C. It disables replication.  
D. It removes the need for partitions.

---

### Question 48

What is the primary scaling mechanism for Kafka topic consumption within a consumer group?

A. Add partitions and distribute them across consumers, subject to assignment and workload constraints.  
B. Add more producers to the same partition only.  
C. Increase retention.  
D. Increase schema versions.

---

### Question 49

A consumer is consistently behind the latest offsets. What does this generally indicate?

A. Consumer lag exists and the consumer is not keeping up with production.  
B. The topic contains no records.  
C. The producer has stopped permanently.  
D. Replication factor is automatically increasing.

---

### Question 50

Which change is most likely to reduce consumer throughput if applied without considering workload?

A. Excessively small fetch/batch behavior causing many small network requests.  
B. Increasing partition count where parallelism is needed.  
C. Using efficient serialization.  
D. Appropriate batching.

---

# Section 11 — Security

### Question 51

What does TLS primarily provide for Kafka connections?

A. Encryption in transit and server/client authentication depending on configuration.  
B. Partition assignment.  
C. Consumer offset storage.  
D. Schema evolution.

---

### Question 52

What is SASL commonly used for?

A. Authentication.  
B. Partition replication.  
C. Log compaction.  
D. Record ordering.

---

### Question 53

What does Kafka authorization control?

A. Which authenticated principals are allowed to perform operations on Kafka resources.  
B. How records are serialized.  
C. How many bytes are in a segment.  
D. Which partition receives a key.

---

### Question 54

A producer can authenticate successfully but receives an authorization error when writing to a topic. What is the most likely explanation?

A. Authentication succeeded, but the principal lacks the required permission.  
B. Serialization is always broken.  
C. The consumer group is too large.  
D. The topic must have exactly one partition.

---

# Section 12 — Troubleshooting Scenarios

### Question 55

A consumer application repeatedly processes the same record after restarting.

Which is the most likely area to investigate first?

A. Offset commit behavior and when commits occur relative to processing.  
B. Topic name capitalization only.  
C. Broker hostname formatting only.  
D. Producer compression.

---

### Question 56

A producer sends records with a key, but records for the same key appear out of order.

Which statement is the best troubleshooting direction?

A. Verify that records for the key are consistently routed to the same partition and that the application is not creating concurrent/reordered writes.  
B. Kafka never preserves ordering.  
C. Increase retention.  
D. Remove all partitions.

---

### Question 57

A consumer group has 12 consumers but only 6 topic partitions. CPU usage is low on half the consumers.

What is the most likely explanation?

A. At most six consumers can actively own those six partitions at one time.  
B. Kafka automatically duplicates partitions for idle consumers.  
C. Every consumer must own every partition.  
D. Consumer groups cannot contain more than six consumers.

---

### Question 58

A consumer application performs expensive processing inside its polling loop. It occasionally gets removed from the consumer group.

What should you investigate?

A. `max.poll.interval.ms`, processing duration, polling strategy, and workload distribution.  
B. Topic retention only.  
C. Producer serializer only.  
D. Broker disk format only.

---

### Question 59

A topic has a single partition. You increase the number of consumers in its group from one to ten. Throughput does not scale as expected.

Why?

A. A single partition cannot be actively assigned to multiple consumers in the same group simultaneously.  
B. Kafka always limits groups to one consumer.  
C. Producers can only send one record per second.  
D. Consumer groups require ten partitions exactly.

---

### Question 60

A Kafka application consumes records, calls an external payment service, then commits its Kafka offset. The application crashes after the payment succeeds but before the offset commit.

What is the key risk?

A. The payment may be performed again when the record is reprocessed.  
B. Kafka automatically rolls back the external payment.  
C. Kafka transactions automatically cover the external payment service.  
D. The partition becomes read-only permanently.

---

# Part II — Answer Sheet

Record your answers before checking the key.

| # | Answer | # | Answer | # | Answer |
|---:|:---:|---:|:---:|---:|:---:|
| 1 | | 21 | | 41 | |
| 2 | | 22 | | 42 | |
| 3 | | 23 | | 43 | |
| 4 | | 24 | | 44 | |
| 5 | | 25 | | 45 | |
| 6 | | 26 | | 46 | |
| 7 | | 27 | | 47 | |
| 8 | | 28 | | 48 | |
| 9 | | 29 | | 49 | |
| 10 | | 30 | | 50 | |
| 11 | | 31 | | 51 | |
| 12 | | 32 | | 52 | |
| 13 | | 33 | | 53 | |
| 14 | | 34 | | 54 | |
| 15 | | 35 | | 55 | |
| 16 | | 36 | | 56 | |
| 17 | | 37 | | 57 | |
| 18 | | 38 | | 58 | |
| 19 | | 39 | | 59 | |
| 20 | | 40 | | 60 | |

---

# Part III — Answer Key

| # | Answer | Core concept |
|---:|:---:|---|
| 1 | B | Partition is Kafka's primary unit of parallelism |
| 2 | C | Key-based partitioning |
| 3 | B | Ordering is guaranteed within a partition |
| 4 | A | Broker stores/serves data and participates in cluster operations |
| 5 | C | Six partitions / two consumers ≈ three each |
| 6 | B | Producer acknowledgment level |
| 7 | A | Idempotent producer prevents duplicate appends from producer retries |
| 8 | B | Same key normally maps to same partition |
| 9 | A | `linger.ms` affects batching delay |
| 10 | B | Larger batches can improve batching efficiency |
| 11 | B | `group.id` identifies consumer group membership |
| 12 | B | One active consumer per partition within a group |
| 13 | A | Uncommitted work can be replayed |
| 14 | B | Start at earliest available offset |
| 15 | A | One partition assignment per group member at a time |
| 16 | B | Consumer-group position in a partition |
| 17 | A | Restart begins from committed position |
| 18 | A | Process-before-commit gives at-least-once behavior |
| 19 | C | At-least-once permits duplicates |
| 20 | B | End-to-end semantics matter |
| 21 | B | Three replicas of each partition |
| 22 | B | Partition leader handles normal client traffic |
| 23 | B | In-Sync Replica |
| 24 | A | Remaining healthy ISR/leader can continue operation |
| 25 | B | Redundancy and fault tolerance |
| 26 | A | Object/value → bytes |
| 27 | B | JSON itself does not provide schema governance |
| 28 | B | Central schema and compatibility management |
| 29 | A | Deserialization failure |
| 30 | A | Protects data-contract evolution |
| 31 | A | Redistributes partition assignments |
| 32 | A | Membership changes can trigger rebalances |
| 33 | A | Rebalances interrupt processing and add overhead |
| 34 | A | Session timeout |
| 35 | A | Excessive processing between polls can trigger removal |
| 36 | A | Kafka client library for stream processing |
| 37 | A | Local state required by processing topology |
| 38 | A | Table-like changing keyed state |
| 39 | A | State recovery |
| 40 | A | Streams application identity |
| 41 | A | Atomic Kafka writes/offset coordination |
| 42 | A | Kafka-side atomic processing |
| 43 | A | Visibility of transactional records |
| 44 | B | Aborted records are hidden from read-committed consumers |
| 45 | B | Strong Kafka EOS; external effects require extra design |
| 46 | A | Batching + compression can increase throughput |
| 47 | A | Less network/storage traffic at CPU cost |
| 48 | A | Partition-based parallelism |
| 49 | A | Consumer lag |
| 50 | A | Inefficient fetch/batching can reduce throughput |
| 51 | A | Encryption/authentication depending on TLS setup |
| 52 | A | Authentication |
| 53 | A | Authorization |
| 54 | A | Authentication ≠ authorization |
| 55 | A | Commit timing |
| 56 | A | Partition routing and concurrent writes |
| 57 | A | Only six partitions can be assigned simultaneously |
| 58 | A | Poll interval and processing duration |
| 59 | A | One partition → one active consumer in the group |
| 60 | A | External side effect can be repeated |

---

# Part IV — Detailed Explanations

## 1. Partitions Are the Unit of Parallelism

Kafka topics are divided into partitions. Each partition is an ordered append-only log.

For a consumer group, a partition can normally be actively assigned to only one consumer at a time.

Therefore:

```text
Topic
 ├── Partition 0 → Consumer A
 ├── Partition 1 → Consumer B
 ├── Partition 2 → Consumer C
 └── Partition 3 → Consumer D
```

Adding consumers beyond the number of partitions does not create additional partition-level parallelism.

**Certification rule:**  
> Consumer parallelism within a group is fundamentally bounded by the number of partitions.

---

## 2. Ordering

Kafka guarantees ordering **within a partition**.

It does not provide a global ordering guarantee across partitions.

Example:

```text
Partition 0:
offset 0 → A
offset 1 → B
offset 2 → C

Partition 1:
offset 0 → X
offset 1 → Y
offset 2 → Z
```

Kafka can guarantee:

```text
A < B < C
X < Y < Z
```

It cannot guarantee a global relationship such as:

```text
A < X < B < Y < C < Z
```

**Certification rule:**  
> If strict ordering is required for a key, route all records for that key to the same partition.

---

## 3. Producer Acknowledgments

The producer's acknowledgment configuration controls how much broker-side acknowledgment is required before the producer considers a request successful.

Conceptually:

```text
acks=0
Producer ─────> Broker

acks=1
Producer ─────> Leader
                 ACK

acks=all
Producer ─────> Leader
                  │
                  ├── Replica
                  └── Replica
                       ACK
```

The exact durability/availability trade-off depends on the cluster configuration, ISR state, and other producer/broker settings.

---

## 4. Idempotent Producer

Network failures can create an ambiguous situation:

```text
Producer
   │
   │ Produce request
   ▼
Broker
   │
   │ Record appended
   ▼
Network failure
   X
Producer does not receive response
```

The producer may retry.

Without appropriate idempotence, a retry can potentially append the same logical record again.

Idempotent producer behavior uses producer identity and sequencing mechanisms to prevent duplicate appends from producer retries.

---

## 5. Consumer Groups

A consumer group provides coordinated consumption.

Example:

```text
Topic
P0 ─────── Consumer A
P1 ─────── Consumer B
P2 ─────── Consumer C
P3 ─────── Consumer D

Consumer Group = payments
```

Another group can independently consume the same partitions:

```text
payments
  ├── P0
  ├── P1
  ├── P2
  └── P3

analytics
  ├── P0
  ├── P1
  ├── P2
  └── P3
```

The groups maintain independent consumption positions.

---

## 6. Offsets

Think of a partition as:

```text
offset:
0   1   2   3   4   5
│   │   │   │   │   │
A   B   C   D   E   F
```

A consumer group has a position indicating where it should continue.

If the application has successfully processed records through offset 3 and commits the appropriate next position, a restart can resume from that committed position.

The key concept is:

> Kafka stores the consumer group's position independently from the actual record data.

---

## 7. At-Least-Once Processing

A common pattern is:

```text
read
  ↓
process
  ↓
commit offset
```

If the application crashes here:

```text
read
 ↓
process successfully
 ↓
CRASH
 ↓
commit never happens
```

The record can be read again.

Therefore:

```text
possible processing:
1st execution
2nd execution
```

This is **at-least-once** processing.

The application must therefore tolerate duplicates when necessary.

---

## 8. Commit Before Processing

Consider:

```text
read
 ↓
commit
 ↓
process
 ↓
CRASH
```

If processing fails after the offset was committed, the consumer may restart after the record and never process it again.

That creates a potential **at-most-once** behavior.

This illustrates the fundamental trade-off:

```text
process → commit
     ↓
at-least-once
possible duplicates

commit → process
     ↓
at-most-once
possible loss
```

---

## 9. Replication

Suppose:

```text
Topic: orders
Partition: P0

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower
```

Replication factor:

```text
RF = 3
```

The replicas provide redundancy.

If Broker 1 fails, Kafka can potentially elect another eligible replica as leader.

---

## 10. ISR

The In-Sync Replica set represents replicas considered sufficiently caught up with the leader according to Kafka's replication rules.

Conceptually:

```text
P0
 ├── Broker 1 — Leader
 ├── Broker 2 — ISR
 └── Broker 3 — ISR
```

If a follower falls too far behind or becomes unavailable, it can leave the ISR.

This is important for durability and leader election behavior.

---

## 11. Consumer Rebalancing

A rebalance changes partition ownership.

Before:

```text
Consumer A → P0 P1
Consumer B → P2 P3
```

After Consumer B leaves:

```text
Consumer A → P0 P1 P2 P3
```

Rebalancing is necessary for correctness, but frequent rebalances can reduce processing efficiency.

Typical causes include:

- Consumer joins
- Consumer leaves
- Consumer crashes
- Membership changes
- Polling/heartbeat problems
- Group-management events

---

## 12. `session.timeout.ms` vs `max.poll.interval.ms`

These settings are commonly confused.

### `session.timeout.ms`

Related to the consumer's heartbeat/session relationship with the group coordinator.

Conceptually:

```text
Consumer
   │ heartbeat
   │ heartbeat
   │ heartbeat
   X
```

If the session expires, Kafka can consider the consumer failed.

### `max.poll.interval.ms`

Controls how long can pass between successful calls to `poll()` before the consumer is considered to have failed from the group's perspective.

A common failure pattern:

```text
poll()
  ↓
process for 20 minutes
  ↓
poll()
```

If the allowed poll interval is shorter than the processing time, the consumer can be removed from the group.

---

## 13. Kafka Streams

Kafka Streams allows Java applications to build processing topologies.

Example:

```text
Input Topic
    │
    ▼
  filter
    │
    ▼
  map
    │
    ▼
  aggregate
    │
    ▼
State Store
    │
    ▼
Output Topic
```

It uses Kafka itself as the underlying distributed infrastructure for input, output, and recovery-related mechanisms.

---

## 14. State Stores

Some stream operations require state.

For example:

```text
orders
  │
  ▼
groupBy(customerId)
  │
  ▼
count()
  │
  ▼
State Store
```

If the application crashes, state must be recoverable.

Kafka Streams can use changelog topics to reconstruct local state.

Conceptually:

```text
Application State
      │
      │ changelog
      ▼
Kafka Topic
      │
      │ recovery
      ▼
New Application Instance
```

---

## 15. Transactions

Kafka transactions can group multiple Kafka operations atomically.

For example:

```text
Consume input
      │
      ├── Produce output A
      ├── Produce output B
      └── Commit consumed offsets
```

The goal is to avoid exposing partially completed Kafka processing.

This is especially important for Kafka-to-Kafka processing.

---

## 16. `read_committed`

Transactional records can be:

```text
COMMITTED
ABORTED
```

A consumer using:

```text
isolation.level=read_committed
```

is intended to read committed transactional data while hiding aborted transactional records from application consumption.

This is an important component of Kafka's transactional processing model.

---

## 17. Exactly-Once and External Systems

A classic interview/certification trap:

```text
Kafka
  ↓
Application
  ↓
Payment API
  ↓
Commit Kafka offset
```

Suppose:

```text
Payment succeeds
       ↓
Application crashes
       ↓
Offset was NOT committed
       ↓
Record is processed again
       ↓
Payment API called again
```

Kafka cannot automatically roll back an arbitrary external HTTP request.

Therefore, robust designs may require:

- Idempotency keys
- External transactions
- Outbox patterns
- Deduplication
- Transaction-aware integrations
- Application-level state

**Important:** Kafka exactly-once semantics should not be interpreted as a universal distributed transaction over every external system.

---

# Part V — Certification Traps

## Trap 1 — "Kafka guarantees ordering"

Incorrect.

Correct:

> Kafka guarantees ordering within a partition.

---

## Trap 2 — "More consumers always means more throughput"

Incorrect.

For a consumer group:

```text
partitions = 4
consumers = 20
```

Only four consumers can actively process partitions at once.

---

## Trap 3 — "Committed offset means record was processed"

Not necessarily.

You must know **when** the commit occurred relative to processing.

---

## Trap 4 — "Exactly once means no duplicate external side effects"

Incorrect.

Kafka's transactional guarantees do not automatically include arbitrary external systems.

---

## Trap 5 — "Replication factor 3 means three brokers must always be available"

Not necessarily.

Replication factor describes the number of replicas. Availability and write behavior depend on leader/ISR state and configuration.

---

## Trap 6 — "JSON is a schema"

JSON is a serialization/data representation. It does not by itself provide the schema governance and compatibility mechanisms typically associated with a schema registry.

---

## Trap 7 — "`session.timeout.ms` controls processing time"

Not directly.

Long processing between polls is more directly related to:

```text
max.poll.interval.ms
```

---

# Part VI — Scenario-Based Mini Exam

## Scenario A — Consumer Crash

A topic contains:

```text
Partition 0

Offset 0 → A
Offset 1 → B
Offset 2 → C
Offset 3 → D
```

A consumer reads A, B, and C. It processes C successfully but crashes before committing the offset.

### Question

What can happen after restart?

A. It must start at offset 0.  
B. It can replay C depending on the last committed position.  
C. Kafka deletes C.  
D. C becomes part of another topic.

**Answer:** B

### Explanation

Kafka's recovery point for the consumer group is based on the committed offset, not merely on the fact that the application executed processing code.

---

# Scenario B — Consumer Scaling

Topic:

```text
P0
P1
P2
P3
P4
P5
P6
P7
```

Consumer group:

```text
C1
C2
C3
C4
```

### Question

What is the maximum number of partitions that can be actively processed concurrently by this group?

**Answer:** 8

There are eight partitions and four consumers. The four consumers can collectively own all eight partitions.

---

# Scenario C — Too Many Consumers

Topic:

```text
P0
P1
P2
```

Consumer group:

```text
C1
C2
C3
C4
C5
```

### Question

How many consumers can actively own partitions?

**Answer:** 3

The other consumers remain members of the group but have no partition assigned while the topology remains unchanged.

---

# Scenario D — Ordering

A producer emits:

```text
customer=A
customer=B
customer=A
customer=C
customer=A
```

All records with the same key are routed consistently to the same partition.

### Question

What ordering guarantee can you rely on for customer A?

**Answer:** The records for customer A are ordered within their partition.

You should not infer a global ordering relationship between customer A and customer B.

---

# Scenario E — External Side Effect

Application:

```text
Kafka record
    ↓
Charge credit card
    ↓
Commit offset
```

The credit-card charge succeeds, but the application crashes before committing.

### Question

What is the most important risk?

**Answer:** The Kafka record can be processed again, potentially charging the card twice.

A production solution should introduce an idempotency mechanism or an appropriate transactional architecture.

---

# Part VII — Advanced Interview Questions

## Question A1

Why does increasing partitions after a topic is already in production require careful consideration?

### Expected answer

Partitioning affects:

- Ordering
- Consumer parallelism
- Key distribution
- Load distribution
- Operational complexity

Changing partition count can alter key-to-partition mapping and therefore affect ordering/distribution assumptions.

---

## Question A2

Why can a single hot key create a bottleneck even when a topic has many partitions?

### Expected answer

If all records for the hot key are routed to the same partition, that partition becomes the bottleneck.

Example:

```text
100 partitions

Customer X
   │
   ▼
Partition 37
   │
   ▼
Huge traffic
```

Adding more consumers does not solve the bottleneck because the key's records must preserve per-partition ordering.

---

## Question A3

What is consumer lag?

### Expected answer

Consumer lag describes how far behind a consumer group's position is relative to the available end of a partition.

Conceptually:

```text
Log end offset = 10,000
Consumer position = 9,700

Lag ≈ 300
```

The precise monitoring metric depends on the tool and terminology being used, but the central idea is the distance between produced data and consumed position.

---

## Question A4

What factors influence Kafka consumer throughput?

### Expected answer

Potential factors include:

- Number of partitions
- Number of consumers
- Fetch sizes
- Poll behavior
- Record size
- Processing cost
- Network bandwidth
- Broker performance
- Serialization/deserialization
- Compression
- Disk and storage characteristics
- Downstream dependencies

---

## Question A5

Why can increasing partition count make an application more scalable?

### Expected answer

Partitions provide independent units of work.

For example:

```text
1 partition
    ↓
1 active consumer

16 partitions
    ↓
up to 16 active consumers
```

This provides more opportunities for parallel processing.

However, partition count should be designed carefully because partitions also create operational and resource costs.

---

# Part VIII — Practical Certification Drills

## Drill 1 — Explain This Architecture

Explain the following without consulting documentation:

```text
             ┌───────────────┐
             │   Producer    │
             └───────┬───────┘
                     │
                     ▼
              ┌─────────────┐
              │    Topic    │
              └──────┬──────┘
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
       P0           P1           P2
        │            │            │
        └────────────┼────────────┘
                     ▼
              Consumer Group
               ┌─────┴─────┐
               ▼           ▼
              C1           C2
```

Be able to explain:

1. Where ordering exists.
2. How partitions provide parallelism.
3. How offsets work.
4. What happens if C1 crashes.
5. What happens if a partition leader fails.
6. How another consumer group would consume the same data.

---

# Part IX — Final Revision Checklist

Before attempting a certification exam, make sure you can explain all of the following without notes.

## Kafka Core

- [ ] Topic
- [ ] Partition
- [ ] Offset
- [ ] Broker
- [ ] Leader
- [ ] Follower
- [ ] ISR
- [ ] Replication factor
- [ ] Consumer group
- [ ] Group coordinator
- [ ] Rebalance

## Producer

- [ ] `acks`
- [ ] Idempotence
- [ ] Retries
- [ ] Batching
- [ ] `linger.ms`
- [ ] `batch.size`
- [ ] Compression
- [ ] Key-based partitioning
- [ ] Producer ordering considerations

## Consumer

- [ ] `group.id`
- [ ] Offset commits
- [ ] `auto.offset.reset`
- [ ] Consumer lag
- [ ] Poll loop
- [ ] `max.poll.interval.ms`
- [ ] `session.timeout.ms`
- [ ] Heartbeats
- [ ] Rebalancing
- [ ] Assignment strategies

## Delivery Semantics

- [ ] At-most-once
- [ ] At-least-once
- [ ] Exactly-once
- [ ] Duplicate processing
- [ ] Offset commit timing
- [ ] Transactions
- [ ] `read_committed`

## Kafka Streams

- [ ] KStream
- [ ] KTable
- [ ] GlobalKTable
- [ ] State stores
- [ ] Changelog topics
- [ ] Repartition topics
- [ ] Windowing
- [ ] Joins
- [ ] `application.id`

## Data Contracts

- [ ] Serializer
- [ ] Deserializer
- [ ] Avro
- [ ] Protobuf
- [ ] JSON Schema
- [ ] Schema Registry
- [ ] Compatibility
- [ ] Schema evolution

## Operations

- [ ] Consumer lag
- [ ] Under-replicated partitions
- [ ] Broker failure
- [ ] Leader election
- [ ] Rebalancing
- [ ] Disk usage
- [ ] Network bottlenecks
- [ ] Throughput
- [ ] Latency

## Security

- [ ] TLS
- [ ] SASL
- [ ] Authentication
- [ ] Authorization
- [ ] ACLs
- [ ] Principal

---

# Part X — Final Score Interpretation

## 54–60: Certification Ready

You understand the core Kafka developer concepts and can reason through common scenarios.

Focus next on:

- Kafka Streams
- Transactions
- Schema evolution
- Performance tuning
- Troubleshooting
- Certification-specific edge cases

## 48–53: Strong Candidate

You have a solid foundation.

Review every incorrect question and make sure you understand **why the other answers were wrong**.

## 42–47: Intermediate

You understand the fundamentals but have gaps.

Prioritize:

1. Consumer groups
2. Offsets
3. Partitions
4. Replication
5. Delivery semantics
6. Kafka Streams

## 36–41: Borderline

Do not rely on memorization.

Build a local Kafka lab and reproduce:

- Consumer crashes
- Rebalances
- Offset commits
- Producer retries
- Partition assignment
- Broker failures
- Kafka Streams state recovery

## Below 36: Rebuild the Fundamentals

Start with the Kafka mental model:

```text
Producer
   │
   ▼
Topic
   │
   ├── Partition 0
   ├── Partition 1
   └── Partition 2
          │
          ▼
    Consumer Group
      ├── C1
      └── C2
```

Then learn:

```text
Partition
   ↓
Offset
   ↓
Consumer Group
   ↓
Replication
   ↓
Delivery Semantics
   ↓
Streams
   ↓
Transactions
```

---

# Final Exam Strategy

When you see a certification question, first identify the concept being tested.

Ask yourself:

```text
Is this about...
        │
        ├── Ordering?
        │       → Partition
        │
        ├── Parallelism?
        │       → Partitions + consumers
        │
        ├── Replay?
        │       → Offset
        │
        ├── Duplicate processing?
        │       → Commit timing / at-least-once
        │
        ├── Broker failure?
        │       → Replication / leader / ISR
        │
        ├── Rebalance?
        │       → Consumer-group membership / polling
        │
        ├── Kafka-to-Kafka atomic processing?
        │       → Transactions / EOS
        │
        ├── State?
        │       → Kafka Streams state store
        │
        └── Data contract?
                → Serialization / schema
```

The most important certification principle is:

> **Do not memorize isolated configuration properties. Understand the relationship between producers, partitions, brokers, offsets, consumer groups, replication, and processing semantics.**

That mental model lets you solve unfamiliar Kafka scenarios rather than simply recalling definitions.

---

# End of Mock Exam

**Recommended next step:** Take the 60-question exam under timed conditions, record your score, then review every incorrect answer and explain the underlying Kafka mechanism in your own words.
