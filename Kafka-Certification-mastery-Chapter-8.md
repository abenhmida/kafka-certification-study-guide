# Chapter 8 — Kafka Administration & Operations Deep Dive

> Certification track: Kafka Developer + Kafka Administrator  
> Reference foundation: *Kafka: The Definitive Guide*, 2nd Edition

---

## 1. Chapter Objectives

This chapter focuses on practical Kafka administration and operations.

You should be able to:

- inspect cluster health
- create and manage topics
- inspect partitions and replicas
- understand topic configuration
- administer consumer groups
- diagnose consumer lag
- manage broker configuration
- perform partition reassignment
- understand preferred leader election
- administer KRaft
- monitor Kafka
- troubleshoot common production failures
- reason about capacity and recovery
- answer administrator certification scenarios

Core mental model:

```text
Kafka Cluster
     |
     +-- Brokers
     |
     +-- Topics
     |     |
     |     +-- Partitions
     |           |
     |           +-- Leaders
     |           +-- Replicas
     |           +-- ISR
     |
     +-- Consumer Groups
     |
     +-- KRaft Controllers
     |
     +-- Monitoring
```

## 2. Kafka Administration Philosophy

Kafka administration is not just starting brokers and creating topics.

A production administrator must continuously manage:

- Availability
- Durability
- Performance
- Capacity
- Security
- Observability
- Recovery

The goal is predictable behavior under both normal traffic and failure.

## 3. Core Administration Areas

The major operational areas are:

- Cluster
- Topics
- Partitions
- Replication
- Consumers
- Configuration
- Storage
- Security
- Monitoring
- Recovery

These areas are interconnected.

## 4. Kafka CLI Toolkit

Common Kafka command-line tools include:

- `kafka-topics.sh`
- `kafka-configs.sh`
- `kafka-consumer-groups.sh`
- `kafka-console-producer.sh`
- `kafka-console-consumer.sh`
- `kafka-reassign-partitions.sh`
- `kafka-metadata-quorum.sh`
- `kafka-broker-api-versions.sh`
- `kafka-storage.sh`

Exact commands and options depend on the Kafka version.

Always check the installed version:

```bash
bin/kafka-topics.sh --help
```

## 5. Bootstrap Server

Administrative clients commonly use:

```bash
--bootstrap-server broker1:9092
```

The bootstrap server provides the initial connection point from which the client discovers Kafka cluster metadata.

It does not mean every request permanently goes to that broker.

## 6. Listing Topics

Typical command:

```bash
bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--list
```

Use it to inspect which topics are visible in the cluster.

## 7. Describing a Topic

Typical command:

```bash
bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--describe \
--topic orders
```

Important fields include:

- Topic
- Partition
- Leader
- Replicas
- ISR

Example:

`P0 -> Leader B1 -> Replicas B1,B2,B3 -> ISR B1,B2,B3`
## 8. Topic Metadata

When troubleshooting a topic, inspect:

- partition count
- leader distribution
- replica placement
- ISR membership

Example:

```text
P0 -> B1 leader -> B1,B2,B3 ISR
P1 -> B2 leader -> B2,B3,B1 ISR
P2 -> B3 leader -> B3,B1,B2 ISR
```

This gives an initial picture of replication and leadership balance.

## 9. Creating a Topic

Example:

```bash
bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--create \
--topic orders \
--partitions 6 \
--replication-factor 3
```

Important parameters:

- `topic`
- `partitions`
- `replication factor`

## 10. Partition Count

Six partitions means:

- P0
- P1
- P2
- P3
- P4
- P5

Partitions provide:

- parallelism
- distribution
- ordering boundaries
- consumer-group concurrency

Partition count is an architectural decision.

## 11. Changing Partition Count

Kafka can increase the number of partitions.

However, increasing partitions can affect keyed workloads.

With the default key-based partitioning behavior, changing the number of partitions can cause future records for a key to map to a different partition.

Therefore:

> Do not treat partition count as a harmless tuning knob.

## 12. Replication Factor

Example:

```bash
--replication-factor 3
```

means each partition has three assigned replicas.

Example:

```text
P0 -> B1
      B2
      B3
```

Replication provides fault tolerance.

## 13. Topic Configuration

Common topic-level properties include:

- `retention.ms`
- `retention.bytes`
- `cleanup.policy`
- `compression.type`
- `min.insync.replicas`
- `max.message.bytes`
- `segment.bytes`

The exact available settings depend on Kafka version.

## 14. Configuration Layers

Think about configuration at several levels:

```text
Kafka defaults
|
v
Broker configuration
|
v
Topic configuration
|
v
Client configuration
```

Do not confuse broker configuration with topic or client configuration.

## 15. Viewing Topic Configuration

Typical command:

```bash
bin/kafka-configs.sh \
--bootstrap-server localhost:9092 \
--entity-type topics \
--entity-name orders \
--describe
```

This is useful for identifying explicit topic overrides.

## 16. Altering Topic Configuration

Example:

```bash
bin/kafka-configs.sh \
--bootstrap-server localhost:9092 \
--entity-type topics \
--entity-name orders \
--alter \
--add-config retention.ms=604800000
```

This sets an explicit topic-level retention value.

## 17. Removing a Topic Override

Conceptually:

```bash
--delete-config retention.ms
```

This removes the explicit topic override so the relevant default can apply again.

Always verify the resulting configuration.

## 18. Retention

Kafka can retain data according to:

- `retention.ms`
- `retention.bytes`

Retention is independent of whether a consumer has already processed the record.

A consumer group does not cause Kafka to delete records simply by consuming them.

## 19. Retention Is Segment-Based

Kafka stores data in log segments.

Therefore retention is not an exact per-record stopwatch.

Conceptually:

```text
records
|
v
segments
|
v
retention eligibility
|
v
cleanup
```

This is important when interpreting retention behavior.

## 20. Cleanup Policies

Important policies include:

- `delete`
- `compact`
- `compact,delete`

## 21. Delete Policy

Delete retention removes old log segments when they become eligible.

Typical use:

- events
- logs
- telemetry

where historical retention is bounded.

## 22. Compaction

Log compaction keeps the latest value for a key, subject to Kafka's compaction semantics.

Example:

```text
A -> 1
B -> 5
A -> 2
A -> 3
```

After compaction, the retained state can represent:

```text
A -> 3
B -> 5
```

Compaction is not instantaneous and does not mean every older record disappears immediately.

## 23. Tombstones

A null-valued record can act as a deletion marker in a compacted topic.

Example:

```text
A -> 1
A -> 2
A -> null
```

The tombstone tells Kafka that key A should eventually disappear from the compacted state.

Tombstones themselves are eventually cleaned according to compaction rules.

## 24. Consumer Group Administration

List consumer groups:

```bash
bin/kafka-consumer-groups.sh \
--bootstrap-server localhost:9092 \
--list
```

Describe a group:

```bash
bin/kafka-consumer-groups.sh \
--bootstrap-server localhost:9092 \
--describe \
--group orders-service
```

## 25. Consumer Lag

A simplified model is:

`lag = log end offset - committed/current consumer offset`

Example:

```text
Log End Offset = 10,000
Consumer Offset = 9,700

Lag = 300
```

Lag is one of the most important Kafka operational signals.

## 26. Lag Does Not Automatically Mean Failure

A consumer may intentionally have lag.

Example:

```text
Producer = 10,000 msg/s
Consumer = 9,500 msg/s
```

Lag grows.

The important question is whether lag is:

- stable
- growing
- shrinking

## 27. Diagnosing Growing Lag

Possible causes:

- slow application
- slow database
- CPU saturation
- GC pauses
- network latency
- insufficient partition parallelism
- frequent rebalances
- broker performance problems

> Do not automatically add consumers before identifying the bottleneck.

## 28. Consumer Parallelism

Suppose:

- Topic = 6 partitions
- Consumer group = 3 consumers

A possible assignment:

```text
C1 -> P0 P3
C2 -> P1 P4
C3 -> P2 P5
```

At most six consumers can actively own partitions of that topic simultaneously.

## 29. Too Many Consumers

If:

- 6 partitions
- 12 consumers

some consumers will have no partitions.

```text
C1 -> P0
C2 -> P1
C3 -> P2
C4 -> P3
C5 -> P4
C6 -> P5

C7-C12 -> idle
```

Adding consumers beyond partition count does not create more partition-level parallelism.

## 30. Consumer Rebalancing

A rebalance can occur when:

- consumer joins
- consumer leaves
- consumer crashes
- subscription changes
- group membership changes

Partitions are redistributed.

Frequent rebalances can cause:

- processing pauses
- latency
- duplicate processing
- unstable assignments
## 31. Rebalance Troubleshooting

Investigate:

- consumer crashes
- poll interval
- processing duration
- network stability
- deployment churn
- GC pauses
- group configuration

> A consumer that takes too long to poll/process can repeatedly leave and rejoin the group.

## 32. Consumer Offset Reset

Kafka provides administrative mechanisms for resetting group offsets.

Safe operational process:

1. Stop consumers.
2. Identify the group.
3. Determine desired offset.
4. Preview the operation.
5. Apply reset.
6. Verify.
7. Restart consumers.

Offset reset is potentially destructive and should be controlled.

## 33. Offset Reset Strategies

Common strategies include:

- `earliest`
- `latest`
- specific offset
- timestamp
- datetime
- duration

The exact CLI syntax varies by Kafka release.

## 34. Topic Deletion

Example:

```bash
bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--delete \
--topic orders
```

Topic deletion is destructive.

Always verify:

- cluster
- topic name
- business ownership
- backup/recovery expectations

before performing it.

## 35. Dynamic Broker Configuration

Modern Kafka supports dynamic configuration for many settings.

Typical administrative mechanism:

- `kafka-configs.sh`

Dynamic configuration can reduce unnecessary broker restarts.

However:

> Not every Kafka configuration is dynamically changeable.

Always verify the installed Kafka version's configuration reference.

## 36. Quotas

Kafka supports quotas to prevent a client from monopolizing broker resources.

Conceptually:

```text
Producer A -> 20 MB/s
Producer B -> 100 MB/s
Consumer C -> 30 MB/s
```

Quotas can help protect cluster stability.

## 37. Partition Reassignment

Partition reassignment moves replicas between brokers.

Example:

```text
Before:
P0 -> B1 B2 B3

After:
P0 -> B2 B3 B4
```

Typical workflow:

```text
generate assignment
|
review
|
execute
|
monitor
|
verify
```
## 38. Why Reassignment Is Expensive

Moving replicas consumes:

- network
- disk I/O
- CPU
- broker resources

Aggressive reassignment can degrade production traffic.

## 39. Reassignment Throttling

Use throttling when appropriate so that recovery/rebalancing does not consume all available bandwidth.

Goal:

> restore balance without creating a production incident

## 40. Preferred Leader Election

Partition replicas have an ordering.

Example:

```text
P0 -> [B1, B2, B3]
       ^
       preferred replica
```

After failures or operational changes, leadership can become uneven.

Preferred leader election can restore leadership distribution toward the preferred replicas.

## 41. Leader Imbalance

Example:

```text
B1 -> 70% of partition leaders
B2 -> 15%
B3 -> 15%
```

Even if storage is balanced, B1 may experience disproportionate:

- CPU
- network
- request load

Monitor leader distribution.

## 42. Adding a Broker

Adding `B4` does not automatically redistribute all existing partition replicas.

Existing assignments may remain:

```text
B1
B2
B3
```

Reassignment may be required to use the new capacity.

## 43. Storage Administration

Monitor:

- disk capacity
- disk latency
- I/O throughput
- log directories
- filesystem errors

A full or failing disk can cause serious Kafka incidents.

## 44. Multiple Log Directories

A broker can be configured with multiple log directories.

Conceptually:

```text
Broker
|
+-- /data1
|
+-- /data2
```

Partition placement and directory health must be monitored.

A failed directory can affect the partitions stored there.

## 45. Cluster Health

A useful health checklist is:

- brokers available
- controller quorum healthy
- offline partitions = 0
- under-replicated partitions = 0
- ISR stable
- consumer lag within SLA
- disk capacity healthy
- network capacity healthy
- request latency acceptable

No single metric proves cluster health.

## 46. Important Metrics

Know the purpose of metrics such as:

- `UnderReplicatedPartitions`
- `OfflinePartitionsCount`
- `IsrShrinksPerSec`
- `IsrExpandsPerSec`
- `UncleanLeaderElectionsPerSec`
- `LeaderElectionRateAndTimeMs`
- request latency
- bytes in/out
- disk utilization
- consumer lag

Exact metric names can vary by Kafka version.

## 47. Under-Replicated Partitions

If `UnderReplicatedPartitions > 0`, investigate:

- broker failures
- slow disks
- network problems
- CPU saturation
- replica recovery
- reassignment
- throttling

Persistent under-replication means durability is degraded.

## 48. Offline Partitions

If `OfflinePartitionsCount > 0`, at least one partition has no leader.

This is significantly more severe than ordinary under-replication.

Potential effects:

- producer failures
- consumer failures
- unavailable partition data

## 49. KRaft Administration

Modern Kafka deployments use KRaft for cluster metadata management.

Conceptually:

```text
Controller quorum
|
v
Cluster metadata
|
+-- brokers
+-- topics
+-- partitions
+-- leadership
```

## 50. KRaft Controller Quorum

A production KRaft deployment normally uses multiple controllers.

Conceptually:

```text
C1
C2
C3
```

With three controllers, losing one can still leave a majority:

```text
C1
C3
```

The exact topology depends on whether nodes are combined broker/controller roles or dedicated roles.

## 51. KRaft Administration Tool

A useful command family is `kafka-metadata-quorum.sh`. Use it to inspect metadata quorum state.

The exact syntax depends on the Kafka version.

## 52. Broker API Versions

Useful command: `kafka-broker-api-versions.sh`. This can help inspect broker API compatibility.

Useful during:

- upgrades
- client compatibility troubleshooting
- protocol diagnosis

## 53. Advertised Listeners

A classic Kafka networking problem is:

```text
client
|
v
bootstrap broker
|
v
metadata
|
v
advertised broker address
|
X
client cannot reach it
```

The client may successfully bootstrap but fail afterward.

Check:

- `listeners`
- `advertised.listeners`
- DNS
- routing
- firewalls
- security protocol
## 54. Security Administration Overview

Kafka security involves:

- TLS
- SASL
- Authentication
- Authorization
- ACLs

Authentication asks:

> Who are you?

Authorization asks:

> What are you allowed to do?

Detailed security is covered in the dedicated security chapter.

## 55. Monitoring Architecture

A common model:

```text
Kafka
|
v
JMX / metrics
|
v
metrics collector
|
v
monitoring platform
|
v
dashboards + alerts
```

Monitor both Kafka infrastructure and application behavior.

## 56. Good Alerts

Useful alerts include:

- offline partitions
- persistent under-replication
- controller instability
- rapid ISR shrinkage
- disk near capacity
- consumer lag beyond SLA
- high request latency
- broker unavailable

Avoid alerting on every transient event.

## 57. Capacity Planning

Kafka capacity planning includes:

- throughput
- storage
- partitions
- replication
- network
- CPU
- memory
- recovery bandwidth
- failure headroom

## 58. Storage Estimate

Approximate replicated storage:

`daily data x retention days x replication factor`

Example:

```text
200 GB/day
x 7 days
x RF 3
= 4.2 TB
```

Then add headroom for:

- indexes
- segments
- recovery
- growth
- reassignment
- operational safety

> Do not size disks to 100% utilization.

## 59. Network Capacity

Broker traffic can include:

```text
producer ingress
+
replication traffic
+
consumer egress
+
recovery
+
reassignment
```

Therefore application ingress alone is not enough for network sizing.

## 60. Partition Capacity

Too many partitions can increase:

- metadata
- memory
- file descriptors
- leader management
- recovery time
- rebalancing cost
- controller workload

> More partitions are not automatically better.

## 61. Failure Headroom

Suppose:

```text
B1 = 70%
B2 = 70%
B3 = 70%
```

If `B1` fails, `B2` and `B3` may become overloaded.

Therefore:

> Fault tolerance requires spare capacity, not just replicas.

## 62. Operational Troubleshooting Model

Use a layered approach:

1. Network
2. Broker process
3. Cluster metadata
4. Topic
5. Partition
6. Replication
7. Consumer group
8. Application

This prevents confusing symptoms with root causes.

## 63. Incident — Consumer Lag

**Symptom:** consumer lag increasing

Investigate:

- consumer assignment
- consumer processing time
- CPU
- GC
- downstream systems
- rebalance frequency
- partition count
- broker health
- network

> Do not automatically add consumers.

## 64. Incident — ISR Shrinks

**Symptom:** ISR repeatedly shrinks

Possible causes:

- slow disk
- network latency
- CPU saturation
- GC pauses
- reassignment
- recovery
- throttling

Persistent ISR shrinkage requires root-cause investigation.

## 65. Incident — Disk Nearly Full

If disk usage reaches a dangerous threshold:

- increase storage
- review retention
- rebalance replicas
- identify abnormal producers
- remove unnecessary data according to policy

> Do not wait until the filesystem is completely full.

## 66. Incident — Broker Failure

**Runbook:**

1. Confirm broker failure.
2. Identify affected partitions.
3. Inspect leaders.
4. Inspect ISR.
5. Check offline partitions.
6. Check remaining broker capacity.
7. Restore broker.
8. Monitor replica recovery.
9. Verify ISR expansion.
10. Verify cluster health.

## 67. Incident — Network Connectivity

Check:

- DNS
- IP routing
- firewall
- listeners
- `advertised.listeners`
- TLS
- SASL
- broker availability

> Kafka connection errors are often caused by metadata or networking rather than a dead broker.

## 68. Incident — Topic Configuration

Before changing configuration:

1. Identify current value.
2. Determine effective value.
3. Understand scope.
4. Check business impact.
5. Change minimally.
6. Monitor.
7. Verify.

> Avoid random Kafka tuning.

## 69. Certification Drill

### Question 1

A topic has 6 partitions and a consumer group has 12 consumers. How many consumers can actively own partitions simultaneously?

**Answer**

> At most **6**. The remaining consumers are idle for that topic assignment.

## 70. Certification Drill

### Question 2

What does `UnderReplicatedPartitions > 0` mean?

**Answer**

> At least one partition has fewer in-sync replicas than its assigned replica set. The partition may still have a leader and serve traffic.

## 71. Certification Drill

### Question 3

What does `OfflinePartitionsCount > 0` mean?

**Answer**

> At least one partition currently has no leader available. This is a high-severity availability condition.

## 72. Certification Drill

### Question 4

Does adding a broker automatically redistribute all existing data?

**Answer**

> **No.** Partition reassignment or another explicit placement mechanism may be required.

## 73. Certification Drill

### Question 5

Why throttle reassignment?

**Answer**

> Replica movement consumes network and disk resources and can interfere with production workloads.

## 74. Certification Drill

### Question 6

What should you inspect if a Kafka client bootstraps successfully but cannot connect to the broker returned in metadata?

**Answer**

> Strong candidates include:
> - `advertised.listeners`
> - DNS
> - routing
> - firewalls
> - security protocol

## 75. Certification Drill

### Question 7

What command is commonly used for consumer group administration?

**Answer**

> `kafka-consumer-groups.sh`

## 76. Certification Drill

### Question 8

What command is commonly used for topic administration?

**Answer**

> `kafka-topics.sh`

## 77. Certification Drill

### Question 9

What command is commonly used for configuration administration?

**Answer**

> `kafka-configs.sh`

## 78. Certification Drill

### Question 10

What command is associated with partition reassignment?

**Answer**

> `kafka-reassign-partitions.sh`

## 79. Administrator Certification Checklist

You should be able to:

- create topics
- describe topics
- inspect partitions
- inspect leaders
- inspect replicas
- inspect ISR
- alter topic configuration
- understand retention
- understand compaction
- inspect consumer groups
- diagnose lag
- reset offsets safely
- perform reassignment
- understand reassignment throttling
- identify offline partitions
- identify under-replication
- monitor disk
- monitor network
- understand KRaft quorum
- troubleshoot advertised listeners
- understand quotas
- understand security administration
- reason about capacity
- build incident runbooks

## 80. Final Administrator Mental Model

When something breaks:

```text
Client problem
|
v
Network?
|
v
Broker available?
|
v
Metadata healthy?
|
v
Partition has leader?
|
v
ISR healthy?
|
v
Consumer group healthy?
|
v
Application healthy?
```

For every configuration change:

```text
Observe
|
v
Understand
|
v
Change
|
v
Monitor
|
v
Verify
```

## 81. Chapter Takeaway

Kafka administration is the discipline of maintaining the relationship between brokers, partitions, replicas, consumers, configuration, and infrastructure while preserving availability, durability, performance, and operational safety.

> For certification, do not memorize commands in isolation.

For each command, understand:

- What does it inspect?
- What does it change?
- What risk does it create?
- How do I verify the result?

That mental model is much more valuable than memorizing shell syntax.
