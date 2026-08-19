# Kafka Certification — Chapter 7: Consumer Groups


## Table of Contents

- [Certification Objectives](#certification-objectives)
- [7.1 What Is a Consumer Group?](#71-what-is-a-consumer-group)
  - [Fundamental rule](#fundamental-rule)
- [7.2 Consumer Groups Provide Parallelism](#72-consumer-groups-provide-parallelism)
- [7.3 More Consumers Do Not Always Mean More Throughput](#73-more-consumers-do-not-always-mean-more-throughput)
  - [Certification rule](#certification-rule)
- [7.4 Same Group vs Different Groups](#74-same-group-vs-different-groups)
  - [Same group](#same-group)
  - [Different groups](#different-groups)
  - [Important certification rule](#important-certification-rule)
- [7.5 Group Coordinator](#75-group-coordinator)
- [7.6 Consumer Group Lifecycle](#76-consumer-group-lifecycle)
- [7.7 What Is a Rebalance?](#77-what-is-a-rebalance)
- [7.8 Common Causes of Rebalances](#78-common-causes-of-rebalances)
- [7.9 Heartbeats](#79-heartbeats)
- [7.10 `session.timeout.ms`](#710-sessiontimeoutms)
- [7.11 `heartbeat.interval.ms`](#711-heartbeatintervalms)
- [7.12 Critical Distinction: Heartbeat vs Poll](#712-critical-distinction-heartbeat-vs-poll)
  - [Heartbeat-related settings](#heartbeat-related-settings)
  - [Poll-related setting](#poll-related-setting)
- [7.13 `max.poll.interval.ms`](#713-maxpollintervalms)
  - [Certification rule](#certification-rule)
- [7.14 `max.poll.records`](#714-maxpollrecords)
- [7.15 Production Example](#715-production-example)
- [7.16 Partition Assignment Strategies](#716-partition-assignment-strategies)
- [7.17 RangeAssignor](#717-rangeassignor)
- [7.18 RoundRobinAssignor](#718-roundrobinassignor)
- [7.19 StickyAssignor](#719-stickyassignor)
- [7.20 CooperativeStickyAssignor](#720-cooperativestickyassignor)
  - [Conceptual eager approach](#conceptual-eager-approach)
  - [Cooperative approach](#cooperative-approach)
- [7.21 Static Membership](#721-static-membership)
- [7.22 Group Generation](#722-group-generation)
- [7.23 Group States](#723-group-states)
- [7.24 Rebalance Storm](#724-rebalance-storm)
- [7.25 Consumer Groups and Ordering](#725-consumer-groups-and-ordering)
- [7.26 Consumer Groups and Keys](#726-consumer-groups-and-keys)
- [Scenario 1 — Consumers vs Partitions](#scenario-1-consumers-vs-partitions)
  - [Answer](#answer)
- [Scenario 2 — Long Processing](#scenario-2-long-processing)
  - [Answer](#answer)
- [Scenario 3 — Independent Applications](#scenario-3-independent-applications)
  - [Answer](#answer)
- [Scenario 4 — Crash Before Commit](#scenario-4-crash-before-commit)
  - [Answer](#answer)
- [Scenario 5 — Slow Consumer](#scenario-5-slow-consumer)
  - [Answer](#answer)
- [Scenario 6 — Certification Multiple Choice](#scenario-6-certification-multiple-choice)
  - [Answer](#answer)
- [Scenario 7 — Static Membership](#scenario-7-static-membership)
  - [Answer](#answer)
  - [Consumer processing](#consumer-processing)
  - [Partition count](#partition-count)
  - [Consumer count](#consumer-count)
  - [Broker/network health](#brokernetwork-health)
  - [Downstream dependencies](#downstream-dependencies)
  - [Consumer Group](#consumer-group)
  - [Partition Ownership](#partition-ownership)
  - [Parallelism](#parallelism)
  - [Group Coordinator](#group-coordinator)
  - [Rebalance](#rebalance)
  - [Heartbeat](#heartbeat)
  - [Polling](#polling)
  - [Assignment](#assignment)
  - [Static Membership](#static-membership)
  - [Critical distinction](#critical-distinction)
  - [Most important rule](#most-important-rule)
- [Five Answers You Absolutely Must Know](#five-answers-you-absolutely-must-know)

---
## Certification Objectives

By the end of this chapter, you should be able to:

- Explain consumer groups and partition ownership.
- Calculate consumer-group parallelism.
- Explain group coordinators and rebalances.
- Distinguish heartbeat/session timeouts from `max.poll.interval.ms`.
- Understand partition assignment strategies.
- Explain sticky and cooperative rebalancing.
- Understand static membership.
- Diagnose consumer lag and rebalance storms.
- Answer certification-style consumer-group scenarios.

---

## 7.1 What Is a Consumer Group?

A consumer group is a set of consumers cooperating to consume records from one or more Kafka topics.

Example:

```text
Topic: orders

P0  P1  P2  P3  P4  P5

Consumer Group: orders-service

C1 -> P0 P1
C2 -> P2 P3
C3 -> P4 P5
```

### Fundamental rule

> Within a consumer group, a partition can be assigned to only one consumer at a time.

This is the foundation of Kafka consumer parallelism.

---

## 7.2 Consumer Groups Provide Parallelism

Suppose a topic has six partitions:

```text
P0 P1 P2 P3 P4 P5
```

With one consumer:

```text
C1 -> P0 P1 P2 P3 P4 P5
```

With three consumers:

```text
C1 -> P0 P1
C2 -> P2 P3
C3 -> P4 P5
```

Therefore:

```text
maximum active consumers <= number of partitions
```

Adding consumers beyond the partition count does not increase partition-level parallelism.

---

## 7.3 More Consumers Do Not Always Mean More Throughput

Example:

```text
Partitions = 4
Consumers  = 10
```

Possible assignment:

```text
C1 -> P0
C2 -> P1
C3 -> P2
C4 -> P3

C5 -> idle
C6 -> idle
C7 -> idle
C8 -> idle
C9 -> idle
C10 -> idle
```

Only four consumers can actively own partitions.

### Certification rule

```text
active consumers <= partitions
```

---

## 7.4 Same Group vs Different Groups

Consider topic `orders`.

### Same group

```text
orders
   |
   v
orders-group
   |
   +--> C1
   +--> C2
   +--> C3
```

Partitions are distributed among C1, C2 and C3.

### Different groups

```text
                 orders
                   |
          +--------+--------+
          |                 |
          v                 v
 orders-group         fraud-group
          |                 |
          v                 v
 orders-service       fraud-service
```

Each group independently consumes the topic.

### Important certification rule

If two independent applications must each receive every record, they normally need different `group.id` values.

---

## 7.5 Group Coordinator

Kafka assigns a broker to coordinate each consumer group.

Conceptually:

```text
Consumer C1
Consumer C2
Consumer C3
      |
      v
+-------------------+
| Group Coordinator |
+-------------------+
      |
      +-- Membership
      +-- Heartbeats
      +-- Group generations
      +-- Rebalance coordination
      +-- Offset commits
```

The group coordinator is responsible for group coordination. It does not have to be the broker hosting the topic partition being consumed.

---

## 7.6 Consumer Group Lifecycle

A simplified lifecycle is:

```text
Consumer starts
      |
      v
Find group coordinator
      |
      v
Join group
      |
      v
Group membership established
      |
      v
Partition assignment
      |
      v
Consume
```

When membership changes:

```text
Consumer joins/leaves/crashes
          |
          v
       Rebalance
          |
          v
   New partition assignment
```

---

## 7.7 What Is a Rebalance?

A rebalance redistributes partitions among consumers in a group.

Before:

```text
C1 -> P0 P1 P2
C2 -> P3 P4 P5
```

C2 crashes:

```text
C1 -> P0 P1 P2 P3 P4 P5
```

If C3 joins:

```text
C1 -> P0 P1
C2 -> P2 P3
C3 -> P4 P5
```

The exact assignment depends on the assignment strategy and group membership.

---

## 7.8 Common Causes of Rebalances

Rebalances can occur when:

- A consumer joins.
- A consumer leaves.
- A consumer crashes.
- A consumer exceeds `max.poll.interval.ms`.
- A subscription changes.
- Topic partition changes require reassignment.
- Consumer instances repeatedly restart.

---

## 7.9 Heartbeats

Consumers send heartbeats to the group coordinator.

```text
Consumer                    Coordinator
    |                            |
    |------ heartbeat ---------->|
    |                            |
    |------ heartbeat ---------->|
    |                            |
    |------ heartbeat ---------->|
```

Important settings:

```properties
heartbeat.interval.ms
session.timeout.ms
```

If heartbeats stop for too long, the coordinator can consider the consumer failed.

---

## 7.10 `session.timeout.ms`

`session.timeout.ms` determines how long the coordinator waits for heartbeats before considering a consumer dead.

Example:

```properties
session.timeout.ms=45000
```

Conceptually:

```text
Last heartbeat
      |
      |-----------------------|
                              |
                        session timeout
                              |
                              v
                       Consumer removed
                              |
                              v
                          Rebalance
```

The broker's configured minimum and maximum values also constrain valid client settings.

---

## 7.11 `heartbeat.interval.ms`

This controls the heartbeat frequency.

Example:

```properties
heartbeat.interval.ms=3000
```

Conceptually:

```text
heartbeat
   |
 3 sec
   |
heartbeat
   |
 3 sec
   |
heartbeat
```

The heartbeat interval should be substantially smaller than the session timeout so there are multiple heartbeat opportunities before the session expires.

---

## 7.12 Critical Distinction: Heartbeat vs Poll

This is a major certification topic.

### Heartbeat-related settings

```text
heartbeat.interval.ms
session.timeout.ms
```

### Poll-related setting

```text
max.poll.interval.ms
```

A consumer can still send heartbeats while processing records.

However, if the application fails to call `poll()` within `max.poll.interval.ms`, it can lose its group membership.

Therefore:

```text
heartbeat timeout != poll timeout
```

---

## 7.13 `max.poll.interval.ms`

Example:

```properties
max.poll.interval.ms=300000
```

This is five minutes.

Suppose:

```text
poll()
   |
   +---- database operations
   +---- HTTP calls
   +---- business processing
   +---- complex computation
   |
   v
poll()
```

If the processing between polls takes seven minutes:

```text
7 minutes > 5 minutes
```

the consumer can leave the group and a rebalance can occur.

### Certification rule

`max.poll.interval.ms` is primarily about the maximum time between calls to `poll()`.

---

## 7.14 `max.poll.records`

This controls the maximum number of records returned by one `poll()`.

Example:

```properties
max.poll.records=100
```

Compared with:

```properties
max.poll.records=5000
```

Conceptually:

```text
max.poll.records
       |
       v
batch size
       |
       v
processing duration
       |
       v
max.poll.interval.ms
```

Reducing the batch size can help prevent long processing intervals.

---

## 7.15 Production Example

Configuration:

```properties
max.poll.interval.ms=300000
max.poll.records=500
```

Suppose every record requires approximately one second of processing.

Worst-case simplified processing time:

```text
500 records × 1 second
=
500 seconds
```

But:

```text
max.poll.interval.ms
=
300 seconds
```

Therefore the design is risky.

Potential solutions:

- Reduce `max.poll.records`.
- Parallelize processing carefully.
- Batch downstream operations.
- Improve database/API performance.
- Use appropriate worker pools.
- Reconsider the processing architecture.

Simply increasing `max.poll.interval.ms` can hide a throughput problem rather than solve it.

---

## 7.16 Partition Assignment Strategies

Important strategies for certification:

```text
RangeAssignor
RoundRobinAssignor
StickyAssignor
CooperativeStickyAssignor
```

---

## 7.17 RangeAssignor

Range assignment is performed on a per-topic basis.

Example:

```text
Topic A:
P0 P1 P2 P3

Consumers:
C1 C2
```

Possible assignment:

```text
C1 -> P0 P1
C2 -> P2 P3
```

With multiple topics, RangeAssignor can produce uneven assignments in some subscription patterns.

---

## 7.18 RoundRobinAssignor

Round-robin assigns partitions in rotation.

Example:

```text
P0 -> C1
P1 -> C2
P2 -> C3
P3 -> C1
P4 -> C2
P5 -> C3
```

It can provide more even distribution across multiple topics when subscriptions are compatible.

---

## 7.19 StickyAssignor

Sticky assignment attempts to:

1. Keep partitions balanced.
2. Minimize unnecessary partition movement.

Partition movement can be expensive when consumers maintain local state.

Potential costs include:

```text
partition movement
       |
       +--> state rebuilding
       +--> cache rebuilding
       +--> network traffic
       +--> CPU
       +--> disk I/O
```

Therefore minimizing movement is valuable.

---

## 7.20 CooperativeStickyAssignor

Cooperative rebalancing reduces disruption.

### Conceptual eager approach

```text
Before:

C1 -> P0 P1
C2 -> P2 P3

       REBALANCE

Assignments revoked

       NEW ASSIGNMENT

C1 -> P0 P2
C2 -> P1 P3
```

### Cooperative approach

```text
Before:

C1 -> P0 P1
C2 -> P2 P3

       REBALANCE

Only necessary partitions move

       NEW ASSIGNMENT

C1 -> P0 P2
C2 -> P1 P3
```

The important idea is incremental partition movement and reduced disruption.

---

## 7.21 Static Membership

Kafka supports static membership through:

```properties
group.instance.id
```

Example:

```properties
group.id=payments
group.instance.id=payments-consumer-01
```

The instance ID should be stable and unique.

Static membership can reduce unnecessary rebalances caused by temporary restarts.

---

## 7.22 Group Generation

Kafka tracks consumer-group generations.

```text
Generation 10
      |
      v
   Rebalance
      |
      v
Generation 11
```

This helps Kafka distinguish current group state from stale group membership information.

---

## 7.23 Group States

Important group states include:

```text
Empty
PreparingRebalance
CompletingRebalance
Stable
Dead
```

A healthy operating group is normally:

```text
Stable
```

Repeated transitions between stable and rebalance states can indicate instability.

---

## 7.24 Rebalance Storm

A rebalance storm can look like:

```text
JOIN
  |
SYNC
  |
ASSIGN
  |
LEAVE
  |
JOIN
  |
SYNC
  |
ASSIGN
  |
LEAVE
```

Possible causes:

- Consumer crashes.
- Container restarts.
- Network instability.
- Long JVM GC pauses.
- Excessive record-processing time.
- Incorrect timeout configuration.
- Unstable deployments.

Consequences:

```text
Rebalance
   |
   v
Processing disruption
   |
   v
Consumer lag
   |
   v
Throughput degradation
```

---

## 7.25 Consumer Groups and Ordering

Kafka guarantees ordering within a partition.

Example:

```text
P0:

0 Order-A
1 Order-B
2 Order-C
```

A consumer owning P0 can process:

```text
A -> B -> C
```

in partition order.

Kafka does not provide a global ordering guarantee across partitions.

```text
P0: A B C
P1: X Y Z
```

There is no guaranteed global sequence such as:

```text
A X B Y C Z
```

---

## 7.26 Consumer Groups and Keys

Suppose:

```text
customerId = 123
```

is the record key.

Records with the same key are normally routed to the same partition under a consistent partitioning configuration.

```text
Customer 123

Event 1
Event 2
Event 3
Event 4

       |
       v
      P4
```

A consumer group member owns P4.

This is important for workloads such as:

- Payments
- Account updates
- Inventory
- Customer state
- Order lifecycle events

---

# Certification Scenarios

## Scenario 1 — Consumers vs Partitions

Topic:

```text
12 partitions
20 consumers
```

How many consumers can actively process partitions?

### Answer

```text
12
```

Eight consumers can be idle.

---

## Scenario 2 — Long Processing

A consumer calls `poll()`, receives 1,000 records, and processes them for eight minutes.

Configuration:

```properties
max.poll.interval.ms=5 minutes
```

### Answer

The consumer can lose group membership and a rebalance can occur.

Key comparison:

```text
time between poll() calls
>
max.poll.interval.ms
```

---

## Scenario 3 — Independent Applications

Two applications must each process every order.

### Answer

Use different group IDs:

```text
orders
   |
   +--> orders-service-group
   |
   +--> fraud-service-group
```

---

## Scenario 4 — Crash Before Commit

A consumer processes records and crashes before committing offsets.

### Answer

Those records can be processed again after restart.

This is typical at-least-once behavior:

```text
poll
 |
process
 |
X crash
 |
no commit
 |
restart
 |
process again
```

---

## Scenario 5 — Slow Consumer

A consumer spends several minutes processing records.

Could this cause a rebalance?

### Answer

Yes.

If the consumer exceeds the relevant polling/liveness constraints, Kafka can remove it from the group and redistribute its partitions.

---

## Scenario 6 — Certification Multiple Choice

Which setting is primarily associated with the maximum time between consumer `poll()` calls?

A. `heartbeat.interval.ms`

B. `session.timeout.ms`

C. `max.poll.interval.ms`

D. `fetch.max.wait.ms`

### Answer

**C — `max.poll.interval.ms`**

---

## Scenario 7 — Static Membership

Which configuration identifies a static consumer instance?

A. `consumer.id`

B. `group.instance.id`

C. `group.member.id`

D. `consumer.instance`

### Answer

**B — `group.instance.id`**

---

# Administrator Perspective

Inspect a consumer group with:

```bash
kafka-consumer-groups.sh   --bootstrap-server localhost:9092   --describe   --group orders
```

Important fields include:

```text
GROUP
TOPIC
PARTITION
CURRENT-OFFSET
LOG-END-OFFSET
LAG
CONSUMER-ID
HOST
CLIENT-ID
```

Example:

```text
GROUP   TOPIC   PARTITION   CURRENT   LOG-END   LAG
orders  orders      0          900       1000    100
orders  orders      1          950       1000     50
orders  orders      2          700       1000    300
```

Partition 2 has the largest lag and should be investigated.

---

# Diagnosing Consumer Lag

If lag continuously increases, investigate:

### Consumer processing

- CPU
- Database latency
- HTTP calls
- Serialization
- Business logic

### Partition count

Too few partitions limit parallelism.

### Consumer count

Too few consumers can leave partitions overloaded.

### Broker/network health

Fetch latency and network issues can slow consumers.

### Downstream dependencies

A consumer can be blocked by:

```text
Database
External API
Cache
Other services
```

---

# Consumer Group Diagnostic Model

```text
                 Consumer Lag
                      |
          +-----------+-----------+
          |                       |
       Growing?                Stable?
          |
          v
   Check consumer
          |
    +-----+-----+
    |           |
 Processing   Fetching
    |           |
    v           v
CPU/DB/API   Broker/network
```

For frequent rebalances:

```text
Rebalance rate
      |
      v
High?
      |
      +--> crashes
      +--> max.poll.interval
      +--> GC pauses
      +--> network
      +--> deployments
```

---

# High-Value Configuration Table

| Configuration | Meaning |
|---|---|
| `group.id` | Consumer group identity |
| `group.instance.id` | Static consumer identity |
| `enable.auto.commit` | Automatic offset commits |
| `auto.offset.reset` | Behavior when no valid committed offset exists |
| `max.poll.records` | Maximum records returned by one poll |
| `max.poll.interval.ms` | Maximum interval between polls |
| `session.timeout.ms` | Consumer session timeout |
| `heartbeat.interval.ms` | Heartbeat frequency |
| `partition.assignment.strategy` | Partition assignment strategy |
| `fetch.min.bytes` | Minimum data requested by a fetch |
| `fetch.max.wait.ms` | Maximum wait for fetch data |

---

# Certification Mental Model

When you see a consumer-group question, ask:

```text
1. How many partitions?
        |
2. How many consumers?
        |
3. Same group or different groups?
        |
4. Who owns each partition?
        |
5. What caused the membership change?
        |
6. What are the heartbeat settings?
        |
7. What is max.poll.interval.ms?
        |
8. What is the assignment strategy?
        |
9. What offsets have been committed?
        |
10. What happens after failure?
```

---

# Chapter Cheat Sheet

### Consumer Group

```text
group.id
```

identifies the group.

### Partition Ownership

```text
1 partition
     |
     v
1 consumer
     |
     v
within one group
```

### Parallelism

```text
active consumers <= partitions
```

### Group Coordinator

Coordinates:

```text
membership
heartbeats
generations
rebalances
offset commits
```

### Rebalance

Common triggers:

```text
join
leave
crash
subscription change
poll timeout
```

### Heartbeat

```text
heartbeat.interval.ms
session.timeout.ms
```

### Polling

```text
max.poll.interval.ms
max.poll.records
```

### Assignment

Know:

```text
Range
RoundRobin
Sticky
CooperativeSticky
```

### Static Membership

```text
group.instance.id
```

### Critical distinction

```text
heartbeat timeout
       !=
poll timeout
```

### Most important rule

```text
Partitions determine maximum
consumer-group parallelism.
```

---

# Final Exam Drill

Answer these without looking at the chapter:

1. A topic has 5 partitions and 10 consumers. How many consumers can actively own partitions?
2. Can two consumers in the same group consume the same partition simultaneously?
3. Can two different groups consume the same partition?
4. What is the group coordinator?
5. What causes a rebalance?
6. What is the purpose of a heartbeat?
7. What does `session.timeout.ms` control?
8. What does `max.poll.interval.ms` control?
9. What does `max.poll.records` control?
10. What is static membership?
11. What does `group.instance.id` do?
12. What is the difference between eager and cooperative rebalancing?
13. Why is sticky assignment useful?
14. Why can a rebalance storm hurt performance?
15. Why can increasing consumers fail to increase throughput?
16. Why are different `group.id` values required for independent applications?
17. How can slow downstream services cause consumer-group problems?
18. What is the relationship between partitions and ordering?
19. What happens to partitions when a consumer crashes?
20. How would you investigate a consumer group whose lag is continuously increasing?

## Five Answers You Absolutely Must Know

```text
Partitions > consumers
    -> some consumers can be idle

Consumers > partitions
    -> cannot increase partition-level parallelism

Same group
    -> partitions are shared

Different groups
    -> each group consumes independently

Slow poll loop
    -> possible group membership loss + rebalance
```

---

# Chapter 7 Summary

Consumer groups are central to Kafka scalability and fault tolerance.

The essential concepts are:

- A partition is assigned to at most one consumer within a group.
- Partitions determine maximum consumer-group parallelism.
- Different consumer groups consume independently.
- The group coordinator manages membership and coordination.
- Rebalances redistribute partitions.
- Heartbeats and polling timeouts are different concepts.
- `max.poll.interval.ms` protects group membership from consumers that stop polling for too long.
- `max.poll.records` controls the maximum records returned by one poll.
- Sticky strategies minimize unnecessary partition movement.
- Cooperative rebalancing reduces disruption.
- `group.instance.id` enables static membership.
- Consumer lag and rebalance frequency are important operational signals.

**Certification priority: VERY HIGH**
