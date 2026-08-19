# Apache Kafka Certification Mastery

## Table of Contents

- [Chapter 2 — Kafka Architecture Deep Dive](#chapter-2-kafka-architecture-deep-dive)
- [1. The Kafka Architecture at a Glance](#1-the-kafka-architecture-at-a-glance)
  - [Certification Rule](#certification-rule)
  - [Application data](#application-data)
  - [Cluster metadata](#cluster-metadata)
  - [Certification Trap](#certification-trap)
  - [Clean/safe behavior](#cleansafe-behavior)
  - [Unclean election](#unclean-election)
  - [Certification Rule](#certification-rule)
  - [Controller leader](#controller-leader)
  - [Partition leader](#partition-leader)
  - [Answer](#answer)
  - [Answer](#answer)
  - [Answer](#answer)
  - [Answer](#answer)
  - [Answer](#answer)
  - [Answer](#answer)
  - [Answer](#answer)
  - [Answer](#answer)
  - [Step 1](#step-1)
  - [Step 2](#step-2)
  - [Step 3](#step-3)
  - [Step 4](#step-4)
  - [Step 5](#step-5)
  - [Step 6](#step-6)
  - [Step 7](#step-7)
- [Chapter 3 — Topics, Partitions, Offsets and Log Internals](#kafka-certification-mastery-chapter-3.md)

---
## Chapter 2 — Kafka Architecture Deep Dive

> Certification track: CCDAK + CCAAK  
> Reference foundation: *Kafka: The Definitive Guide*, 2nd Edition

---

## 1. The Kafka Architecture at a Glance

At the highest level:

```text
                         KAFKA CLUSTER

        ┌─────────────────────────────────────────┐
        │                                         │
        │          KRaft Controller Quorum        │
        │                                         │
        │      Controller 1                       │
        │      Controller 2                       │
        │      Controller 3                       │
        │                                         │
        └───────────────────┬─────────────────────┘
                            │
                     Cluster Metadata
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
   ┌─────────┐         ┌─────────┐         ┌─────────┐
   │ Broker 1│         │ Broker 2│         │ Broker 3│
   └────┬────┘         └────┬────┘         └────┬────┘
        │                   │                   │
        ▼                   ▼                   ▼
   Partitions          Partitions          Partitions
        │                   │                   │
        └───────────────────┼───────────────────┘
                            │
                       Replication
                            │
                            ▼
                           ISR
```

Kafka has two major conceptual planes:

```text
┌─────────────────────────────────────────────┐
│                 CONTROL PLANE               │
│                                             │
│       KRaft / Controllers / Metadata       │
│                                             │
├─────────────────────────────────────────────┤
│                  DATA PLANE                 │
│                                             │
│ Producers / Consumers / Brokers / Logs      │
│                                             │
└─────────────────────────────────────────────┘
```

The essential distinction is:

> **The data plane moves records. The control plane manages cluster metadata and state.**

---

# 2. The Data Plane

The data plane handles Kafka records.

```text
Producer
   │
   ▼
Broker
   │
   ▼
Partition
   │
   ▼
Log
   │
   ▼
Consumer
```

It includes:

- producer requests
- consumer fetch requests
- partition data
- offsets
- replication
- log storage
- consumer groups

---

# 3. The Control Plane

The control plane manages cluster state.

```text
             KRaft Controllers
                    │
                    ▼
             Metadata Quorum
                    │
                    ▼
             Cluster Metadata
```

It is responsible for concepts such as:

- broker membership
- topic metadata
- partition assignments
- partition leadership
- replica assignments
- cluster metadata
- controller elections

A useful certification mental model:

> **Data plane = records. Control plane = metadata.**

---

# 4. Kafka Broker

A broker is a Kafka server that participates in the cluster.

Conceptually:

```text
                       BROKER

┌───────────────────────────────────────────────┐
│                                               │
│                    Broker                     │
│                                               │
│  ┌─────────────┐       ┌──────────────┐      │
│  │ Network     │       │ Request      │      │
│  │ Layer       │──────►│ Processing   │      │
│  └─────────────┘       └──────┬───────┘      │
│                               │              │
│              ┌────────────────┼───────────┐  │
│              │                │           │  │
│              ▼                ▼           ▼  │
│          Producer         Consumer     Admin │
│          Requests         Requests    Requests│
│                                               │
│  ┌────────────────────────────────────────┐   │
│  │              Log Manager               │   │
│  └──────────────────┬─────────────────────┘   │
│                     │                         │
│              ┌──────▼──────┐                  │
│              │    Logs     │                  │
│              └──────┬──────┘                  │
│                     │                         │
│              ┌──────▼──────┐                  │
│              │    Disk     │                  │
│              └─────────────┘                  │
│                                               │
└───────────────────────────────────────────────┘
```

A broker can:

- accept client requests
- store partition data
- act as a partition leader
- act as a partition follower
- replicate partition data
- serve consumer fetch requests
- manage log segments
- expose metrics
- participate in KRaft

---

# 5. Broker and Node Identity

Kafka cluster members need stable identities.

Conceptually:

```text
Node 1
Node 2
Node 3
```

In modern Kafka/KRaft deployments, node roles can be separated.

Possible roles include:

```text
broker
controller
broker,controller
```

This distinction becomes important when designing production clusters.

---

# 6. KRaft

KRaft is Kafka's metadata management architecture based on the Raft consensus algorithm.

The central idea is:

> Kafka manages its own cluster metadata without requiring ZooKeeper.

Conceptually:

```text
Kafka
  │
  ▼
KRaft Controllers
  │
  ▼
Metadata Log
  │
  ▼
Cluster State
```

Modern Kafka certification preparation should therefore focus heavily on KRaft.

---

# 7. KRaft Controllers

Suppose we have three controllers:

```text
Controller 1
Controller 2
Controller 3
```

One controller is the active leader of the metadata quorum.

```text
             Controller Leader
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
    Controller 2        Controller 3
       follower             follower
```

The controllers maintain replicated metadata state.

---

# 8. Controller Quorum

A quorum requires a majority.

For:

```text
3 controllers
```

the majority is:

```text
2
```

Therefore:

```text
3 controllers
1 failure
────────────
2 remaining

Quorum survives
```

But:

```text
3 controllers
2 failures
────────────
1 remaining

No majority
```

The control plane cannot continue making normal quorum-based progress without a majority.

### Certification Rule

For an odd-sized controller quorum:

```text
3 → tolerate 1 controller failure
5 → tolerate 2 controller failures
7 → tolerate 3 controller failures
```

General rule:

```text
failure tolerance = floor((N - 1) / 2)
```

---

# 9. KRaft Metadata Log

Kafka maintains a metadata log for cluster state.

Conceptually:

```text
Metadata Change
      │
      ▼
KRaft Controller
      │
      ▼
Metadata Log
      │
      ▼
Replicated Controller State
```

The metadata log is different from application-data partition logs.

---

# 10. Data Logs vs Metadata Log

Kafka contains two conceptually different kinds of logs.

### Application data

```text
orders-0
payments-2
customers-1
```

These contain application records.

### Cluster metadata

```text
KRaft metadata log
```

This represents cluster metadata/state.

Therefore:

```text
Application Events
        │
        ▼
Partition Logs

Cluster State
        │
        ▼
KRaft Metadata Log
```

Do not confuse them.

---

# 11. Kafka Metadata

Kafka needs to know:

```text
Topic: orders

Partition 0
Leader = Broker 1
Replicas = Broker 1, Broker 2, Broker 3

Partition 1
Leader = Broker 2
Replicas = Broker 2, Broker 3, Broker 1
```

This information is cluster metadata.

The control plane manages it.

---

# 12. Client Metadata

A producer does not simply send records to an arbitrary broker.

Initially it knows one or more bootstrap addresses.

```text
Producer
   │
   │ bootstrap
   ▼
Broker
   │
   │ metadata response
   ▼
Producer
   │
   │ partition leader information
   ▼
Actual Broker
```

For example:

```text
orders-3
leader = broker2
```

The producer then knows where to send the request.

---

# 13. Bootstrap Servers

Example:

```properties
bootstrap.servers=broker1:9092,broker2:9092,broker3:9092
```

The bootstrap servers provide an initial connection point.

The producer uses them to discover cluster metadata.

Conceptually:

```text
bootstrap.servers
        │
        ▼
initial connection
        │
        ▼
metadata
        │
        ▼
partition leaders
```

### Certification Trap

`bootstrap.servers` does not mean that every record is permanently sent to the first broker in the list.

---

# 14. Advertised Listeners

This is one of the most important operational topics.

Suppose:

```text
Producer
   │
   ▼
broker1:9092
```

The producer connects successfully.

Kafka then returns metadata saying:

```text
broker2:9092
```

If the producer cannot resolve or reach that address, communication fails.

Therefore:

> A working bootstrap connection does not guarantee that the entire cluster is reachable.

---

# 15. `listeners` vs `advertised.listeners`

The distinction:

```text
listeners
    │
    ▼
Where Kafka binds/listens

advertised.listeners
    │
    ▼
What Kafka tells clients to connect to
```

Example:

```properties
listeners=INTERNAL://0.0.0.0:9092

advertised.listeners=INTERNAL://kafka1.example.com:9092
```

Kafka may bind to:

```text
0.0.0.0:9092
```

while advertising:

```text
kafka1.example.com:9092
```

This distinction is critical in:

- Docker
- Kubernetes
- cloud deployments
- NAT
- multi-network environments
- local development clusters

---

# 16. Produce Request Flow

Suppose the application creates:

```text
OrderCreated
```

The producer performs conceptually:

```text
Application
     │
     ▼
Producer
     │
     ▼
Serializer
     │
     ▼
Partitioner
     │
     ▼
Partition 2
```

Suppose:

```text
Partition 2 leader = Broker 3
```

Then:

```text
Producer
   │
   ▼
Broker 3
   │
   ▼
orders-2
```

The leader appends the record.

Followers replicate the partition.

```text
                 Broker 3
                  Leader
                    │
             ┌──────┴──────┐
             ▼             ▼
          Broker 1      Broker 2
          Follower      Follower
```

---

# 17. Append-Only Log

Suppose a partition contains:

```text
0 → A
1 → B
2 → C
```

Appending D gives:

```text
0 → A
1 → B
2 → C
3 → D
```

Kafka normally appends to the end of the partition log.

This sequential-write pattern contributes to Kafka's high throughput.

---

# 18. Log Segments

A partition is divided into log segments.

Conceptually:

```text
orders-0/

00000000000000000000.log
00000000000000000000.index
00000000000000000000.timeindex

00000000000000100000.log
00000000000000100000.index
00000000000000100000.timeindex

00000000000000200000.log
00000000000000200000.index
00000000000000200000.timeindex
```

Segments allow Kafka to manage:

- retention
- deletion
- compaction
- rolling
- indexes
- storage efficiently

---

# 19. Indexes

Kafka maintains indexes associated with log segments.

They allow Kafka to locate records efficiently without scanning the entire log.

Conceptually:

```text
Offset
  │
  ▼
Index
  │
  ▼
Log Segment
  │
  ▼
Record
```

Important index concepts include:

- offset index
- time index

These become especially useful when discussing retention, timestamp-based lookup, and log internals.

---

# 20. Page Cache

Kafka relies heavily on the operating system page cache.

Conceptually:

```text
Kafka
  │
  ▼
Filesystem
  │
  ▼
OS Page Cache
  │
  ▼
Disk
```

Kafka does not need to maintain a completely independent application-level cache for every record.

The operating system can cache frequently accessed data.

---

# 21. Sequential I/O

Kafka's append-only model allows efficient sequential I/O.

Conceptually:

```text
Disk

────────────────────────────────────►

A B C D E F G H I J K L M N
```

rather than constantly performing:

```text
random write
random write
random write
random write
```

Sequential workloads are generally more efficient.

---

# 22. Broker Network Architecture

A broker handles traffic from:

- producers
- consumers
- administrators
- other brokers
- controllers

Conceptually:

```text
                 Broker

        ┌─────────────────────┐
        │ Network Threads      │
        └──────────┬──────────┘
                   │
                   ▼
             Request Queue
                   │
                   ▼
          Request Processing
                   │
        ┌──────────┼──────────┐
        ▼          ▼          ▼
     Produce      Fetch      Admin
```

This is a conceptual model rather than a complete implementation diagram.

---

# 23. Producer Request

A producer sends a produce request:

```text
Producer
   │
   │ ProduceRequest
   ▼
Broker
   │
   ▼
Partition Leader
   │
   ▼
Append
   │
   ▼
Replication
```

The response depends on the producer's configured acknowledgment behavior.

This leads directly to:

```text
acks
```

and:

```text
min.insync.replicas
```

---

# 24. Consumer Fetch

Consumers generally fetch records from Kafka.

```text
Consumer
   │
   │ FetchRequest
   ▼
Broker
   │
   ▼
Partition
   │
   ▼
Records
   │
   ▼
Consumer
```

The consumer controls its progress using offsets.

---

# 25. Partition Leader

For every partition there is normally one leader replica.

Example:

```text
orders-0

Broker 1 = Leader
Broker 2 = Follower
Broker 3 = Follower
```

The leader serves client requests for that partition.

---

# 26. Followers

Followers replicate the leader's log.

```text
              Leader
                 │
        ┌────────┴────────┐
        ▼                 ▼
    Follower            Follower
```

If followers fall behind significantly, their ISR status can change.

---

# 27. Replication

Suppose:

```text
Topic: orders
Partition: 0
Replication Factor: 3
```

Kafka can have:

```text
Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower
```

Therefore:

```text
RF = 3
```

Replication provides fault tolerance.

---

# 28. ISR — In-Sync Replicas

ISR means:

> **In-Sync Replicas**

Suppose:

```text
ISR = {B1, B2, B3}
```

If B3 falls behind sufficiently:

```text
ISR = {B1, B2}
```

The ISR is therefore a critical indicator of replica health.

---

# 29. Replication Lag

Suppose:

```text
Leader:
0 A
1 B
2 C
3 D
4 E

Follower 2:
0 A
1 B
2 C
3 D

Follower 3:
0 A
1 B
```

Follower 3 is significantly behind.

Potential causes include:

- disk saturation
- network problems
- CPU pressure
- broker overload
- I/O contention

Replication lag can cause ISR shrinkage.

---

# 30. ISR Shrink

Initial:

```text
ISR = {B1, B2, B3}
```

Follower B3 becomes too far behind:

```text
ISR = {B1, B2}
```

This is an ISR shrink.

Operationally, sustained ISR shrinkage should trigger investigation.

---

# 31. ISR Expansion

When B3 catches up:

```text
Before:

ISR = {B1, B2}

After:

ISR = {B1, B2, B3}
```

This is ISR expansion.

Repeated shrink/expand cycles can indicate an unstable broker or overloaded infrastructure.

---

# 32. Leader Election

Initial state:

```text
orders-0

B1 = Leader
B2 = ISR
B3 = ISR
```

If B1 fails:

```text
B1 ❌
```

An eligible replica can become leader:

```text
B2 = New Leader
B3 = Follower
```

Clients then refresh metadata.

---

# 33. Broker Failure Sequence

A common certification scenario:

```text
Initial:

B1 = Leader
B2 = ISR
B3 = ISR
```

B1 crashes.

Conceptually:

```text
1. Failure detected
2. Controller updates cluster state
3. Eligible replica selected
4. New leader established
5. Metadata changes
6. Clients refresh metadata
7. Traffic resumes
```

The exact timing depends on cluster and client configuration.

---

# 34. Why ISR Matters

Suppose:

```text
B1 = Leader
B2 = ISR
B3 = ISR
```

B1 fails.

B2 and B3 are synchronized replicas and can be considered for leadership.

Now suppose:

```text
B1 = Leader
B2 = ISR
B3 = NOT ISR
```

B2 is the safer candidate because it is synchronized.

This is why ISR is central to Kafka's durability model.

---

# 35. Unclean Leader Election

Consider:

```text
B1 = Leader
B2 = ISR
B3 = out of sync
```

Now B1 and B2 fail.

B3 is the only remaining replica.

Kafka faces a tradeoff.

### Clean/safe behavior

Do not elect an out-of-sync replica.

Advantage:

```text
Data safety
```

Disadvantage:

```text
Reduced availability
```

### Unclean election

Allow B3 to become leader.

Advantage:

```text
Availability
```

Risk:

```text
Potential data loss
```

### Certification Rule

> Unclean leader election can improve availability at the cost of potential data loss.

---

# 36. `min.insync.replicas`

Suppose:

```text
Replication Factor = 3

min.insync.replicas = 2
acks = all
```

Initial:

```text
ISR = {B1, B2, B3}
```

One replica fails:

```text
ISR = {B1, B2}
```

The producer can still satisfy the configured requirement.

Another replica falls out:

```text
ISR = {B1}
```

Now:

```text
ISR count = 1
min.insync.replicas = 2
```

The producer cannot satisfy the durability requirement for an `acks=all` request.

This is intentional.

---

# 37. Controller vs Partition Leader

Do not confuse these concepts.

### Controller leader

Responsible for control-plane metadata coordination.

### Partition leader

Responsible for requests concerning one specific partition.

For example:

```text
Controller Leader
      │
      ├── cluster metadata
      │
      ▼

Partition Leader: orders-0
      │
      ├── produce/fetch for orders-0
      │
      ▼

Partition Leader: orders-1
```

These are different responsibilities.

---

# 38. Partition Assignment

Suppose:

```text
3 brokers
6 partitions
RF = 3
```

A possible assignment:

```text
Partition   Leader   Replicas

P0          B1       B1 B2 B3
P1          B2       B2 B3 B1
P2          B3       B3 B1 B2
P3          B1       B1 B3 B2
P4          B2       B2 B1 B3
P5          B3       B3 B2 B1
```

Good placement aims to distribute:

- replicas
- leadership
- storage
- network traffic

---

# 39. Rack / Availability-Zone Awareness

Consider:

```text
AZ-1
  B1

AZ-2
  B2

AZ-3
  B3
```

With replication factor 3, replicas can be distributed across zones:

```text
orders-0

Leader  → AZ-1
Replica → AZ-2
Replica → AZ-3
```

This provides stronger resilience than placing all replicas in the same failure domain.

---

# 40. End-to-End Producer Flow

```text
Application
     │
     ▼
Kafka Producer
     │
     ▼
Serializer
     │
     ▼
Partitioner
     │
     ▼
Partition
     │
     ▼
Partition Leader
     │
     ▼
Append to Log
     │
     ▼
Replication
     │
     ▼
Acknowledgment
```

The exact acknowledgment behavior depends on producer configuration.

---

# 41. End-to-End Consumer Flow

```text
Kafka Consumer
      │
      ▼
Metadata
      │
      ▼
Partition Assignment
      │
      ▼
Fetch Request
      │
      ▼
Partition Leader
      │
      ▼
Records
      │
      ▼
Deserializer
      │
      ▼
Application
```

---

# 42. Failure Scenario — Consumer and Leader Failure

Initial:

```text
P0 → B1 Leader

Consumer C1
    │
    ▼
B1
```

B1 fails.

New leader:

```text
P0 → B2 Leader
```

The consumer refreshes metadata and continues consuming from its offset.

The consumer does not need to know the physical identity of the old leader to continue processing.

---

# 43. Failure Scenario — Follower Falls Behind

Initial:

```text
ISR = {B1, B2, B3}
```

B3 becomes slow.

Potential causes:

```text
disk saturation
network latency
CPU pressure
broker overload
```

Replication lag grows.

Eventually:

```text
ISR = {B1, B2}
```

The administrator should investigate the underlying cause rather than treating ISR shrinkage as merely a Kafka configuration issue.

---

# 44. Failure Scenario — Insufficient ISR

Configuration:

```text
RF = 3
min.insync.replicas = 2
acks = all
```

Initial:

```text
ISR = {B1, B2, B3}
```

After one failure:

```text
ISR = {B1, B3}
```

Producer can continue.

After another failure:

```text
ISR = {B1}
```

Now:

```text
1 < 2
```

The producer cannot meet the requested durability guarantee.

This is preferable to silently weakening the configured guarantee.

---

# 45. Certification Question #1

A producer connects successfully to a bootstrap broker but cannot connect to the partition leader returned in metadata.

What should you investigate first?

### Answer

Listener and network configuration, especially:

```text
advertised.listeners
DNS
routing
firewalls
container networking
```

---

# 46. Certification Question #2

A topic has:

```text
6 partitions
10 consumers
```

in one consumer group.

How many consumers can actively consume simultaneously?

### Answer

**6**

Four consumers remain idle.

---

# 47. Certification Question #3

A partition has:

```text
RF = 3
ISR = 2
min.insync.replicas = 2
acks = all
```

Can the producer satisfy the ISR requirement?

### Answer

Yes.

There are exactly two in-sync replicas.

---

# 48. Certification Question #4

A partition has:

```text
RF = 3
ISR = 1
min.insync.replicas = 2
acks = all
```

What happens?

### Answer

The producer cannot satisfy the required ISR condition and may receive an insufficient-replicas error.

---

# 49. Certification Question #5

What happens when a follower becomes too far behind?

### Answer

It may be removed from the ISR.

---

# 50. Certification Question #6

What is KRaft responsible for?

### Answer

KRaft provides Kafka's controller-based metadata management and quorum mechanism for cluster state.

---

# 51. Certification Question #7

What is the difference between a controller leader and a partition leader?

### Answer

The controller leader coordinates cluster metadata/control-plane state.

A partition leader handles client requests for a specific partition.

---

# 52. Certification Question #8

What is the main risk of unclean leader election?

### Answer

Potential data loss because an out-of-sync replica may not contain the latest records.

---

# 53. Senior Architecture Exercise

Consider:

```text
3 brokers
3 partitions
RF = 3

P0 → B1 leader
P1 → B2 leader
P2 → B3 leader
```

B1 fails.

Reason through the sequence.

### Step 1

Identify affected partitions:

```text
P0
```

### Step 2

Identify remaining replicas:

```text
B2
B3
```

### Step 3

Check ISR:

```text
ISR = {B1, B2, B3}
```

### Step 4

Elect a new leader:

```text
P0 → B2
```

### Step 5

Update metadata.

### Step 6

Clients refresh metadata.

### Step 7

Traffic resumes.

This reasoning pattern is more valuable than memorizing isolated configuration parameters.

---

# 54. Architecture Cheat Sheet

```text
┌──────────────────────────────────────────────┐
│                  KAFKA                       │
├──────────────────────────────────────────────┤
│                                              │
│ Topic                                         │
│   └── Partitions                              │
│         └── Replicas                          │
│               ├── Leader                      │
│               └── Followers                   │
│                                              │
│ Brokers host replicas                         │
│                                              │
│ Controllers manage metadata                   │
│                                              │
│ KRaft stores metadata state                   │
│                                              │
│ Producers write to partition leaders         │
│                                              │
│ Consumers fetch records                       │
│                                              │
│ Consumer groups provide parallelism           │
│                                              │
│ ISR represents synchronized replicas          │
│                                              │
└──────────────────────────────────────────────┘
```

---

# 55. Certification Knowledge Checklist

You should now be able to explain:

- [ ] Kafka control plane
- [ ] Kafka data plane
- [ ] broker responsibilities
- [ ] KRaft
- [ ] controller quorum
- [ ] metadata
- [ ] bootstrap servers
- [ ] advertised listeners
- [ ] partition leaders
- [ ] followers
- [ ] ISR
- [ ] replication
- [ ] replication lag
- [ ] leader election
- [ ] unclean leader election
- [ ] partition assignment
- [ ] rack awareness
- [ ] produce request flow
- [ ] consumer fetch flow
- [ ] log segments
- [ ] indexes
- [ ] page cache
- [ ] sequential I/O
- [ ] `acks`
- [ ] `min.insync.replicas`
- [ ] broker failure
- [ ] follower failure
- [ ] metadata propagation

---

# 56. Hands-On Lab

Build a small KRaft cluster:

```text
3 Kafka brokers
3 controllers
```

Create:

```text
Topic:
certification-orders

Partitions:
3

Replication Factor:
3
```

Then:

1. Create the topic.
2. Describe the topic.
3. Identify each partition leader.
4. Identify each replica.
5. Identify the ISR.
6. Produce records.
7. Consume records.
8. Stop one broker.
9. Observe leader changes.
10. Restart the broker.
11. Observe ISR recovery.
12. Inspect broker logs.
13. Inspect client metadata behavior.
14. Deliberately break `advertised.listeners`.
15. Observe client failure.
16. Restore the configuration.

The objective is:

```text
Theory
   ↓
Architecture
   ↓
Observation
   ↓
Failure
   ↓
Diagnosis
```

---

# 57. Final Mental Model

```text
                         KAFKA

                   ┌───────────────┐
                   │ KRaft Quorum  │
                   │               │
                   │ Controllers   │
                   │ Metadata Log  │
                   └───────┬───────┘
                           │
                    Cluster Metadata
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ▼                  ▼                  ▼
   ┌─────────┐        ┌─────────┐        ┌─────────┐
   │ Broker1 │        │ Broker2 │        │ Broker3 │
   └────┬────┘        └────┬────┘        └────┬────┘
        │                  │                  │
        ▼                  ▼                  ▼
   ┌─────────┐        ┌─────────┐        ┌─────────┐
   │ P0 L    │        │ P1 L    │        │ P2 L    │
   │ P1 F    │        │ P2 F    │        │ P0 F    │
   │ P2 F    │        │ P0 F    │        │ P1 F    │
   └─────────┘        └─────────┘        └─────────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
                      Replication
                           │
                           ▼
                          ISR

Producer ──► Metadata ──► Partition Leader
                                │
                                ▼
                              Log
                                │
                                ▼
                           Followers

Consumer ◄────────────── Fetch ──┘
```

The essential architecture is:

> **KRaft manages cluster metadata. Brokers manage partition data. Partition leaders handle client traffic for their partitions. Followers replicate leaders. ISR identifies replicas that are sufficiently synchronized. Producers and consumers use metadata to locate the correct brokers.**

---

# 58. Next Chapter

## Chapter 3 — Topics, Partitions, Offsets and Log Internals

The next chapter will cover:

- partition internals
- offset assignment
- log segments
- offset indexes
- time indexes
- timestamps
- retention
- deletion
- log compaction
- tombstones
- ordering
- partition scaling
- partition reassignment
- hot partitions
- key distribution
- storage sizing
- administrator commands
- developer experiments
- certification questions
- certification traps
- practical failure scenarios

This chapter will connect Kafka's architecture directly to its **storage engine and partition behavior**.
