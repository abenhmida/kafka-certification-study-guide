# Chapter 5 — Consumers and Consumer Groups Deep Dive

> Certification track: Kafka Developer + Kafka Administrator  
> Reference foundation: *Kafka: The Definitive Guide*, 2nd Edition

# Apache Kafka Certification Mastery

## Table of Contents

- [Chapter 5 — Consumers and Consumer Groups Deep Dive](#chapter-5--consumers-and-consumer-groups-deep-dive)
  - [Pattern A](#pattern-a)
  - [Pattern B](#pattern-b)
  - [Pattern C](#pattern-c)
  - [Answer](#answer)
  - [Consumer groups](#consumer-groups)
  - [Offset management](#offset-management)
  - [Rebalancing](#rebalancing)
  - [Liveness](#liveness)
  - [Processing guarantees](#processing-guarantees)
  - [Performance](#performance)
  - [Failure handling](#failure-handling)

---

## Chapter 5 — Consumers and Consumer Groups Deep Dive

> Certification track: CCDAK + CCAAK  
> Reference foundation: *Kafka: The Definitive Guide*, 2nd Edition

---

## 1. Why Consumers Matter

Kafka consumers are responsible for reading records from partitions and delivering them to application code.

The basic flow is:

```text
Kafka Partition
      │
      ▼
Consumer Fetch
      │
      ▼
Consumer Buffer
      │
      ▼
Application Poll
      │
      ▼
Processing
      │
      ▼
Offset Commit
```

For certification, the most important concepts are:

- consumer groups
- partition assignment
- offsets
- polling
- heartbeats
- rebalancing
- commits
- lag
- failure recovery
- delivery semantics

---

## 2. Consumer Architecture

A consumer does not normally receive records through a broker "push".

Instead, it sends fetch requests.

```text
Consumer
   │
   │ FetchRequest
   ▼
Broker
   │
   │ records
   ▼
Consumer
```

This pull-based architecture gives consumers control over their consumption rate.

---

## 3. Consumer Group

A consumer group is a set of consumers cooperating to consume a topic.

Example:

```text
Topic: orders

P0
P1
P2
P3
```

Consumer group:

```text
orders-service

C1 → P0
C2 → P1
C3 → P2
C4 → P3
```

Each partition is assigned to one consumer within the group at a time.

---

## 4. Consumer Group Parallelism

Suppose:

```text
Partitions = 4
Consumers = 4
```

Possible assignment:

```text
C1 → P0
C2 → P1
C3 → P2
C4 → P3
```

Now add:

```text
C5
```

There are still only four partitions.

Therefore:

```text
C5 → idle
```

Maximum active parallelism is bounded by the partition count.

---

## 5. One Consumer Can Process Multiple Partitions

Suppose:

```text
Partitions = 6
Consumers = 2
```

Possible assignment:

```text
C1 → P0 P2 P4
C2 → P1 P3 P5
```

Therefore:

> A consumer can own multiple partitions.

But:

> A partition is assigned to at most one consumer in a consumer group at a time.

---

## 6. Same Topic, Different Groups

Suppose:

```text
orders
 ├── P0
 ├── P1
 └── P2
```

Group A:

```text
A1 → P0
A2 → P1
A3 → P2
```

Group B:

```text
B1 → P0
B2 → P1
B3 → P2
```

Both groups independently consume the topic.

This is one of Kafka's key architectural properties.

---

## 7. Consumer Group Isolation

Offsets are maintained per consumer group.

Conceptually:

```text
orders / P0

Group A → offset 100
Group B → offset 450
Group C → offset 72
```

Therefore one group's progress does not automatically advance another group's position.

---

## 8. Group ID

The consumer group is identified by:

```properties
group.id
```

Example:

```properties
group.id=orders-service
```

Changing the group ID effectively creates a different consumer group with its own committed offsets.

This is a common way to create an independent consumer application.

---

## 9. Consumer Position

Suppose:

```text
P0

100 → A
101 → B
102 → C
103 → D
```

A consumer has fetched:

```text
A
B
C
```

Its current position can be:

```text
103
```

meaning:

```text
next record to fetch = 103
```

This is different from the committed offset.

---

## 10. Current Position vs Committed Offset

This distinction is fundamental.

```text
Current position
    ↓
where the consumer currently expects to read

Committed offset
    ↓
where the consumer group has durably recorded its progress
```

Example:

```text
Current position = 103
Committed offset = 101
```

If the consumer crashes, recovery can begin from the committed position rather than the in-memory current position.

---

## 11. Offset Commit

Suppose:

```text
100 → A
101 → B
102 → C
103 → D
```

The consumer successfully processes:

```text
A
B
C
```

It can commit:

```text
103
```

The conceptual meaning is:

```text
processed through offset 102
next position = 103
```

This is why committed offsets represent the next position to consume.

---

## 12. Automatic Offset Commit

Kafka consumers can automatically commit offsets.

Configuration:

```properties
enable.auto.commit=true
```

The consumer periodically commits offsets based on the configured auto-commit interval.

This is convenient, but it can be dangerous if application processing takes longer than expected.

---

## 13. Why Auto Commit Can Cause Duplicates

Suppose:

```text
Consumer fetches:
A
B
C
```

The application is still processing C.

If offsets are committed before processing fully completes:

```text
commit → next offset
```

Then the consumer crashes.

On restart:

```text
Kafka believes records were already processed
```

The application may skip work.

This can lead to message loss from the application's processing perspective.

---

## 14. Manual Commit

For precise processing semantics, applications can commit explicitly.

Conceptually:

```text
poll()
  │
  ▼
process records
  │
  ▼
processing successful
  │
  ▼
commit offset
```

This provides greater control over the relationship between:

```text
business processing
```

and:

```text
offset advancement
```

---

## 15. Commit After Processing

A common at-least-once pattern is:

```text
poll
  ↓
process
  ↓
commit
```

Failure before commit:

```text
process succeeds
    │
    X
commit fails
    │
    ▼
record processed again
```

Result:

```text
duplicate processing
```

This is why the model is generally called:

> at-least-once

rather than exactly-once.

---

## 16. At-Least-Once Semantics

At-least-once means:

```text
A record should not be lost due solely to advancing the consumer offset too early,
but a record may be processed more than once.
```

Example:

```text
Record
  │
  ▼
process
  │
  X
commit fails
  │
  ▼
restart
  │
  ▼
process again
```

Therefore application operations should ideally be idempotent when duplicate processing is possible.

---

## 17. At-Most-Once Semantics

At-most-once processing prioritizes avoiding duplicates at the cost of potential loss.

Conceptually:

```text
commit
  ↓
process
```

If the consumer crashes after commit but before processing:

```text
offset advanced
record not processed
```

The application can lose the record from its processing perspective.

---

## 18. Exactly-Once

Exactly-once processing requires more than simply choosing when to commit.

For Kafka-to-Kafka processing, transactions can coordinate:

```text
consumed offsets
+
produced records
```

atomically.

Conceptually:

```text
Input
  │
  ▼
Consumer
  │
  ▼
Process
  │
  ├──► Output records
  │
  └──► Consumed offsets
          │
          ▼
      Transaction
```

---

## 19. Consumer Poll Loop

A typical consumer loop looks conceptually like:

```java
while (running) {
    records = consumer.poll(...);

    for (record : records) {
        process(record);
    }

    consumer.commitSync();
}
```

The exact implementation depends on the application's failure and delivery requirements.

The important lifecycle is:

```text
poll
  ↓
process
  ↓
commit
  ↓
poll again
```

---

## 20. Why Poll Matters

The consumer must continue polling.

The poll loop is not merely:

```text
fetch messages
```

It also participates in consumer group liveness and coordination.

If application processing blocks for too long, the consumer can be considered unhealthy depending on the configured timing constraints.

---

## 21. `max.poll.interval.ms`

This configuration limits the maximum delay between successful calls to `poll()` before the consumer is considered to have failed its group-level processing interval.

Example:

```properties
max.poll.interval.ms=300000
```

If application processing takes too long between polls, the consumer can leave the group and partitions can be reassigned.

---

## 22. Long Processing Problem

Suppose:

```text
poll()
   │
   ▼
10-minute processing
   │
   ▼
poll()
```

but:

```properties
max.poll.interval.ms=5 minutes
```

The consumer can exceed the allowed interval.

Result:

```text
consumer considered failed
        │
        ▼
rebalance
        │
        ▼
partitions reassigned
```

This is a classic source of unexpected rebalances.

---

## 23. `max.poll.records`

This configuration limits the number of records returned by a poll.

Example:

```properties
max.poll.records=100
```

If processing one record takes:

```text
200 ms
```

then processing 100 records sequentially could take approximately:

```text
100 × 200 ms
= 20 seconds
```

This helps estimate whether the application can stay within:

```text
max.poll.interval.ms
```

---

## 24. Poll Interval Sizing

A useful reasoning model is:

```text
maximum records per poll
×
worst-case processing time per record
<
max.poll.interval.ms
```

Leave substantial headroom.

Do not design exactly at the limit.

---

## 25. Heartbeats

Consumer group membership also involves heartbeats.

Conceptually:

```text
Consumer
   │
   │ heartbeat
   ▼
Group Coordinator
```

Heartbeats tell the group coordinator that the consumer is alive.

---

## 26. `session.timeout.ms`

If the coordinator does not receive heartbeats within the configured session timeout, the consumer can be considered dead.

Conceptually:

```text
Heartbeat
   │
   ▼
Coordinator
   │
   X
No heartbeat
   │
   ▼
Session timeout
   │
   ▼
Consumer removed
```

This can trigger a rebalance.

---

## 27. `heartbeat.interval.ms`

This controls the approximate heartbeat frequency.

A common design relationship is:

```text
heartbeat.interval.ms
<
session.timeout.ms
```

The consumer sends heartbeats regularly enough to maintain membership.

The exact recommended values depend on the Kafka version and deployment.

---

## 28. Group Coordinator

Each consumer group has a broker acting as its group coordinator.

Conceptually:

```text
Consumer C1 ─┐
Consumer C2 ─┼──► Group Coordinator
Consumer C3 ─┘
```

The coordinator manages group membership and coordination.

It is not necessarily the leader of the partitions being consumed.

---

## 29. Group Membership

When a consumer joins:

```text
Consumer starts
     │
     ▼
Join group
     │
     ▼
Group coordination
     │
     ▼
Partition assignment
```

When a consumer leaves or fails:

```text
Member disappears
      │
      ▼
Group detects change
      │
      ▼
Rebalance
      │
      ▼
New assignment
```

---

## 30. Rebalancing

A rebalance redistributes partitions among group members.

Before:

```text
C1 → P0 P1
C2 → P2 P3
```

C2 leaves.

After:

```text
C1 → P0 P1 P2 P3
```

If C3 joins:

```text
C1 → P0 P1
C3 → P2 P3
```

depending on the assignment strategy.

---

## 31. Why Rebalances Matter

Rebalances can temporarily disrupt processing.

During a rebalance:

```text
old assignment
      │
      ▼
revoke / transition
      │
      ▼
new assignment
      │
      ▼
resume processing
```

Frequent rebalances can reduce throughput and increase latency.

---

## 32. Common Rebalance Causes

- consumer starts
- consumer stops
- consumer crashes
- consumer misses group timing constraints
- topic partition count changes
- subscription changes
- group membership changes

---

## 33. Eager Rebalancing

Traditional eager rebalancing can revoke partitions from consumers and redistribute the entire assignment.

Conceptually:

```text
Before:

C1 → P0 P1
C2 → P2 P3

Rebalance

C1 → nothing
C2 → nothing

Reassign

C1 → P0 P2
C2 → P1 P3
```

This can cause a stop-the-world effect for the group.

---

## 34. Cooperative Rebalancing

Cooperative rebalancing aims to reduce disruption.

Instead of revoking everything, partitions can transition incrementally.

Conceptually:

```text
Current assignment
       │
       ▼
minimal revocation
       │
       ▼
incremental reassignment
```

This can reduce unnecessary pauses.

---

## 35. Static Membership

Kafka supports static group membership using:

```properties
group.instance.id
```

A stable identity can reduce unnecessary rebalances when a consumer restarts temporarily.

Conceptually:

```text
Consumer instance A
      │
      ▼
stable identity
      │
      ▼
temporary restart
      │
      ▼
return
```

This is especially useful for long-lived consumer instances.

---

## 36. Partition Assignment Strategies

Kafka supports different assignment approaches.

Conceptually:

```text
Partitions
    │
    ▼
Assignment Strategy
    │
    ├── Range
    ├── RoundRobin
    ├── Sticky
    └── CooperativeSticky
```

The objective is to distribute partitions among consumers.

Different strategies optimize different properties.

---

## 37. Range Assignment

Range assignment considers partitions by topic and assigns contiguous ranges.

For example:

```text
Partitions:
P0 P1 P2 P3 P4 P5

Consumers:
C1 C2

Possible:
C1 → P0 P1 P2
C2 → P3 P4 P5
```

With multiple topics, range assignment can sometimes produce uneven distributions.

---

## 38. Round Robin

Round-robin assignment distributes partitions cyclically.

Example:

```text
Partitions:
P0 P1 P2 P3 P4 P5

C1 → P0 P2 P4
C2 → P1 P3 P5
```

This can provide more even distribution across topics depending on subscriptions.

---

## 39. Sticky Assignment

Sticky assignment attempts to:

```text
balance partitions
+
minimize unnecessary movement
```

This is useful because moving partitions during every rebalance can be expensive.

---

## 40. Cooperative Sticky

Cooperative sticky combines:

```text
sticky assignment
+
cooperative rebalance behavior
```

The goal is:

```text
balanced assignment
+
minimal disruption
```

This is an important modern Kafka consumer concept.

---

## 41. Consumer Lag

Consumer lag represents how far a consumer/group is behind the partition's current end.

Conceptually:

```text
Log End Offset = 1000
Consumer position/committed progress = 850

Lag ≈ 150
```

The exact metric interpretation depends on whether you are looking at committed offset, current position, or monitoring-system semantics.

---

## 42. Lag Is Not Always Bad

A small or temporary lag can be normal.

Example:

```text
Traffic spike
     │
     ▼
Producer rate > consumer rate
     │
     ▼
lag increases
     │
     ▼
consumer catches up
     │
     ▼
lag decreases
```

The important signal is often:

```text
lag trend
```

rather than a single number.

---

## 43. Persistent Lag

Persistent increasing lag can indicate:

```text
consumer too slow
too few partitions
too few consumers
downstream dependency slow
CPU bottleneck
I/O bottleneck
GC pressure
serialization cost
external API latency
```

Kafka itself may not be the root cause.

---

## 44. Consumer Scaling

Suppose:

```text
12 partitions
```

Start:

```text
3 consumers
```

Average:

```text
4 partitions/consumer
```

Scale to:

```text
6 consumers
```

Average:

```text
2 partitions/consumer
```

Scale to:

```text
12 consumers
```

Potentially:

```text
1 partition/consumer
```

Scale to:

```text
20 consumers
```

Eight consumers may be idle.

---

## 45. Consumer Scaling Is Partition-Bounded

The rule:

```text
active consumers ≤ partitions
```

within a consumer group.

Therefore if you need 20-way consumer parallelism:

```text
topic must have enough partitions
```

This is why partition count is also a consumer architecture decision.

---

## 46. Poison Message

A poison message is a record that repeatedly fails processing.

Example:

```text
P0

100 → good
101 → poison
102 → good
```

If the consumer always retries 101 synchronously:

```text
101
 ↓
failure
 ↓
101
 ↓
failure
 ↓
101
```

The partition can stop making progress.

---

## 47. Poison Message Strategies

Possible patterns include:

```text
retry topic
dead-letter topic
bounded retry
error topic
parking lot topic
manual intervention
```

A common architecture:

```text
Main Topic
    │
    ▼
Consumer
    │
    ├── success ──► business processing
    │
    └── failure ──► Retry/DLT
```

---

## 48. Why DLT Is Not a Kafka Native Guarantee

A dead-letter topic is an application architecture pattern.

Kafka does not automatically provide a universal DLT behavior for every consumer.

The application/framework must implement the policy.

This is a common certification/interview distinction.

---

## 49. Consumer Failure Scenario

Suppose:

```text
Consumer C1 → P0
```

C1 crashes.

The group coordinator detects the failure.

Then:

```text
C1 leaves group
     │
     ▼
rebalance
     │
     ▼
P0 assigned to C2
```

C2 resumes from the group's committed offset.

---

## 50. Failure Scenario — Processing Before Commit

Suppose:

```text
offset 100 → record A
```

Consumer processes A.

Before commit:

```text
consumer crashes
```

After restart:

```text
offset 100
```

A is processed again.

This is expected under at-least-once processing.

---

## 51. Failure Scenario — Commit Before Processing

Suppose:

```text
offset 100 → A
```

Consumer commits:

```text
101
```

Then crashes before processing A.

After restart:

```text
starts at 101
```

A may never be processed.

This demonstrates the tradeoff between:

```text
at-least-once
```

and:

```text
at-most-once
```

---

## 52. Manual Commit Patterns

### Pattern A

```text
poll
process
commit
```

Usually favors at-least-once.

### Pattern B

```text
poll
commit
process
```

Can favor at-most-once but risks loss.

### Pattern C

```text
poll
process transactionally
commit offsets as part of transaction
```

Can support Kafka-to-Kafka exactly-once processing.

---

## 53. `commitSync`

A synchronous commit waits for the commit result.

Conceptually:

```text
process
   │
   ▼
commitSync()
   │
   ▼
wait for response
```

This gives stronger control but can add latency.

---

## 54. `commitAsync`

An asynchronous commit does not block in the same way.

Conceptually:

```text
process
   │
   ▼
commitAsync()
   │
   └── continue
```

It can improve throughput but requires careful handling of commit ordering and failures.

---

## 55. Async Commit Hazard

Suppose:

```text
commit offset 100
commit offset 200
```

If asynchronous operations complete in an unexpected order, careless handling can create incorrect progress.

A common pattern is:

```text
commitAsync()
...
commitSync()
```

before shutdown or critical ownership transitions.

The exact strategy should match the application's processing model.

---

## 56. Offset Commit Per Record vs Batch

Committing every record:

```text
process A → commit
process B → commit
process C → commit
```

can increase overhead.

Batch committing:

```text
process A
process B
process C
commit
```

reduces commit frequency.

But more records may need to be replayed after a crash.

This is a classic throughput vs duplicate-processing tradeoff.

---

## 57. Consumer Fetching

Consumers use fetch-related configurations to control how much data is retrieved.

Important concepts include:

```text
fetch.min.bytes
fetch.max.wait.ms
max.partition.fetch.bytes
fetch.max.bytes
```

These affect:

- throughput
- latency
- memory
- network efficiency

---

## 58. `fetch.min.bytes`

The broker can wait until at least the configured amount of data is available before responding, subject to the relevant timing behavior.

Higher values can improve throughput by returning larger fetches.

But they can also increase latency when traffic is low.

---

## 59. `fetch.max.wait.ms`

This limits how long the broker may wait to satisfy the fetch conditions.

Think:

```text
fetch request
      │
      ▼
wait for enough data
      │
      ├── enough data
      └── timeout threshold
```

It interacts with `fetch.min.bytes`.

---

## 60. `max.partition.fetch.bytes`

This limits the amount of data returned for a partition in a fetch response.

This is important when records are large.

If a single record is larger than the configured fetch size, Kafka has specific behavior to ensure progress rather than simply making the record permanently unreadable.

The important operational lesson is:

> Large record sizes require coordinated producer, broker and consumer configuration.

---

## 61. `fetch.max.bytes`

This controls the maximum amount of data returned for a fetch request across partitions, subject to Kafka's handling of individual records and partition fetch limits.

Think:

```text
FetchRequest
   │
   ├── P0
   ├── P1
   ├── P2
   └── P3
        │
        ▼
  response bounded by fetch limits
```

---

## 62. Consumer Memory

Consumer configuration affects memory usage.

Potential contributors include:

```text
fetch buffers
application queues
deserialized objects
processing batches
downstream buffers
```

If the application polls huge amounts of data and processes slowly, memory pressure can become severe.

---

## 63. Backpressure in Consumers

Suppose:

```text
Kafka → 1000 records/sec
Application → 500 records/sec
```

Then:

```text
lag increases
```

Possible solutions:

```text
increase consumers
increase partitions
optimize processing
batch downstream calls
remove slow dependencies
increase processing concurrency
```

But simply increasing consumer threads does not help if the topic has too few partitions.

---

## 64. Multi-Threaded Consumer Design

KafkaConsumer is not generally designed to be shared arbitrarily across application threads.

A common model is:

```text
Consumer thread
      │
      ▼
poll()
      │
      ▼
worker pool
      │
      ▼
processing
```

But if using asynchronous processing, you must carefully manage:

- ordering
- offset commits
- partition ownership
- rebalance events
- processing completion

This is an advanced design area.

---

## 65. Partition-Level Ordering with Parallel Processing

Suppose:

```text
P0:
A
B
C
```

If the application processes:

```text
A → worker 1
B → worker 2
C → worker 3
```

completion could become:

```text
B
C
A
```

Kafka delivered records in order, but the application destroyed processing order.

Therefore:

> Kafka partition ordering does not automatically guarantee application-level completion ordering.

---

## 66. Preserving Ordering

If strict ordering is required:

```text
partition
    │
    ▼
ordered processing
```

or use partition-aware worker serialization:

```text
P0 → Worker A
P1 → Worker B
P2 → Worker C
```

The application architecture must preserve the required ordering semantics.

---

## 67. Consumer Group Coordinator vs Partition Leader

These are different concepts.

```text
Group Coordinator
    ↓
manages consumer group membership/offset coordination

Partition Leader
    ↓
serves reads/writes for a partition
```

The same broker can perform both roles, but they are conceptually distinct.

---

## 68. Certification Trap — One Group

Question:

> If two applications use the same `group.id`, will both receive every record?

No.

They cooperate as one group.

Partitions are divided among them.

If both applications must independently receive every event, they should use different group IDs.

---

## 69. Certification Trap — Different Groups

Question:

> If two consumer groups read the same topic, does consuming by group A advance group B's offsets?

No.

Offsets are maintained independently per group.

---

## 70. Certification Trap — More Consumers

Question:

> A topic has 3 partitions. You add 10 consumers. Will throughput increase tenfold?

No.

At most 3 consumers can actively consume those 3 partitions within that group.

---

## 71. Certification Trap — Commit

Question:

> If a consumer commits offset 500, what record does it resume from?

Conceptually:

```text
offset 500
```

The committed offset represents the next position.

Records before 500 are considered processed from the group's perspective.

---

## 72. Certification Trap — Crash

Question:

> Consumer processes a record successfully but crashes before committing.

What happens?

The record can be processed again after restart.

This is normal at-least-once behavior.

---

## 73. Certification Trap — Poll

Question:

> Why can a consumer that is still running be removed from its group?

Because application processing can prevent timely polling and violate group timing constraints such as `max.poll.interval.ms`.

Running as a process is not sufficient.

The consumer must maintain group membership correctly.

---

## 74. Certification Trap — Heartbeat

Question:

> Are heartbeats the same thing as calls to `poll()`?

No.

They are related to consumer group liveness but serve different purposes.

The application must also respect the poll interval and processing constraints.

---

## 75. Certification Scenario

Configuration:

```properties
max.poll.records=500
max.poll.interval.ms=300000
```

Worst-case processing time:

```text
1 second per record
```

Potential processing time:

```text
500 × 1 second
= 500 seconds
```

But:

```text
max.poll.interval.ms = 300 seconds
```

Therefore the consumer can exceed the poll interval.

Possible result:

```text
rebalance
```

A better approach may be:

```text
reduce max.poll.records
```

or:

```text
increase max.poll.interval.ms
```

or:

```text
optimize processing
```

depending on requirements.

---

## 76. Certification Scenario — Consumer Lag

Metrics:

```text
Producer rate = 20,000 records/sec
Consumer rate = 15,000 records/sec
```

Lag will tend to increase.

Possible responses:

```text
1. Add consumers if partitions permit.
2. Increase partition count if necessary for future scaling.
3. Optimize consumer processing.
4. Investigate downstream dependencies.
5. Check broker/fetch performance.
```

---

## 77. Administrator Scenario — Rebalance Storm

Symptoms:

```text
frequent rebalances
consumer lag spikes
processing pauses
group members constantly changing
```

Investigate:

```text
consumer crashes
GC pauses
slow processing
max.poll.interval.ms
session.timeout.ms
heartbeat configuration
network instability
deployment churn
```

Do not immediately blame Kafka brokers.

---

## 78. Administrator Scenario — One Consumer Slow

Suppose:

```text
P0 → Consumer C1
P1 → Consumer C2
P2 → Consumer C3
```

Lag:

```text
P0 = 2,000,000
P1 = 10
P2 = 5
```

This suggests a partition-specific or consumer-specific problem.

Investigate:

```text
C1 CPU
C1 GC
downstream dependency
hot key
large records
partition workload
```

Adding a fourth consumer may not help because P0 is still a single partition.

---

## 79. Administrator Scenario — Rebalance After Slow Processing

Suppose:

```text
poll()
processing starts
processing takes 8 minutes
```

Configuration:

```text
max.poll.interval.ms = 5 minutes
```

The consumer may be removed from the group.

Then:

```text
rebalance
```

When the consumer returns:

```text
another rebalance
```

This can create a cycle:

```text
slow processing
   ↓
rebalance
   ↓
reassignment
   ↓
duplicate/replayed work
   ↓
more processing delay
   ↓
rebalance
```

This is a serious production failure mode.

---

## 80. Consumer Design Checklist

Before deploying a consumer, answer:

```text
1. What delivery guarantee is required?
2. When is processing considered successful?
3. When should offsets be committed?
4. Can processing be repeated safely?
5. How long can one poll batch take?
6. What is max.poll.interval.ms?
7. How many partitions exist?
8. How many consumers are required?
9. What happens when a consumer crashes?
10. What happens when a record repeatedly fails?
11. Is ordering required?
12. How is lag monitored?
13. What happens during rebalance?
14. Are downstream calls idempotent?
15. Is Kafka-to-Kafka exactly-once required?
```

---

## 81. Consumer Troubleshooting Matrix

| Symptom | Likely Areas |
| --------- | -------------- |
| Increasing lag | Processing too slow, insufficient parallelism |
| Frequent rebalances | Poll interval, crashes, network, deployments |
| Duplicate processing | Commit timing, crashes, retries |
| Missing records | Commit before processing, retention |
| One partition lagging | Hot partition, slow consumer |
| Consumer kicked from group | `max.poll.interval.ms`, session timeout |
| High fetch latency | Broker/network/fetch configuration |
| High memory | Large fetches, application queues, object retention |
| Processing pauses | Rebalance or downstream dependency |
| Uneven assignment | Assignment strategy/subscription topology |

---

## 82. Hands-On Lab — Consumer Group Basics

Create:

```text
certification-consumer
```

with:

```text
6 partitions
```

Start:

```text
3 consumers
```

Use the same:

```text
group.id=certification-group
```

Observe assignments.

Expected conceptual result:

```text
C1 → P0 P3
C2 → P1 P4
C3 → P2 P5
```

The exact assignment can differ by strategy.

---

## 83. Hands-On Lab — Add Consumers

Start:

```text
3 consumers
```

Then add:

```text
C4
C5
C6
```

Observe:

```text
rebalance
```

Then add:

```text
C7
```

Observe that there are no more partitions available for another active consumer.

---

## 84. Hands-On Lab — Different Group IDs

Start:

```text
group-A
group-B
```

against the same topic.

Produce:

```text
100 records
```

Observe that both groups independently consume the records.

This demonstrates:

```text
topic
 │
 ├── group-A
 │
 └── group-B
```

with independent offsets.

---

## 85. Hands-On Lab — Crash Before Commit

Design:

```text
poll
 ↓
process
 ↓
pause before commit
 ↓
kill consumer
```

Restart it.

Observe that the record can be delivered again.

This demonstrates at-least-once processing.

---

## 86. Hands-On Lab — Commit Before Processing

Reverse the sequence:

```text
poll
 ↓
commit
 ↓
crash
 ↓
process never happens
```

Observe the consequence.

This demonstrates the danger of advancing offsets before business processing completes.

---

## 87. Hands-On Lab — Rebalance

Run:

```text
C1
C2
C3
```

Then terminate C2.

Observe:

```text
C2 leaves
   ↓
rebalance
   ↓
P partitions redistributed
```

Then restart C2 and observe another rebalance.

---

## 88. Hands-On Lab — Poll Interval

Configure a deliberately small:

```properties
max.poll.interval.ms
```

Make processing sleep longer than the configured interval.

Observe:

```text
consumer leaves group
rebalance
partition reassignment
```

This is one of the most valuable consumer labs for certification preparation.

---

## 89. Hands-On Lab — Lag

Generate records faster than the consumer can process.

Observe:

```text
producer rate
consumer rate
lag
```

Then increase consumer parallelism.

Determine whether lag decreases.

Repeat with too few partitions and demonstrate that adding consumers eventually stops helping.

---

## 90. Hands-On Lab — Poison Record

Create:

```text
P0:

A
B
POISON
C
D
```

Make processing of `POISON` fail.

Observe how repeatedly retrying the record can prevent progress.

Then implement:

```text
retry topic
```

or:

```text
dead-letter topic
```

and compare the behavior.

---

## 91. Senior Design Scenario

You operate:

```text
Topic: orders
Partitions: 48
RF: 3

Consumer group:
orders-service

Consumers: 12
```

Each consumer owns approximately:

```text
4 partitions
```

Processing time:

```text
average = 20 ms
p99 = 2 seconds
```

Occasionally, downstream payment processing takes:

```text
8 minutes
```

Current:

```text
max.poll.interval.ms = 5 minutes
```

Question:

What can happen?

### Answer

A consumer can exceed its allowed poll interval and be removed from the group, causing a rebalance.

Potential consequences:

```text
partition movement
duplicate processing
lag spikes
processing interruptions
additional downstream load
```

Possible design options:

```text
increase max.poll.interval.ms
reduce records processed per poll
decouple long-running work
use controlled worker concurrency
make processing idempotent
```

The correct solution depends on the application's ordering and delivery requirements.

---

## 92. Senior Design Scenario — 100 Consumers

Topic:

```text
12 partitions
```

Consumer group:

```text
100 consumers
```

Question:

Why might this be a poor architecture?

Because:

```text
12 partitions
```

can provide at most:

```text
12 active consumers
```

within the group.

The remaining consumers are idle.

The correct solution may be:

```text
increase partition count
```

but only after evaluating:

```text
ordering
storage
broker capacity
recovery
metadata
future growth
```

---

## 93. Senior Design Scenario — Ordering

Requirement:

> All events for the same account must be processed in order, but different accounts should be processed concurrently.

Design:

```text
key = accountId
```

Then:

```text
Account A → P0
Account B → P3
Account C → P1
Account D → P0
```

Account A events remain ordered within P0, while different accounts can execute concurrently.

This is one of the most important Kafka design patterns.

---

## 94. Consumer Mental Model

Memorize:

```text
Consumer
   │
   ▼
Join Group
   │
   ▼
Assignment
   │
   ▼
Poll
   │
   ▼
Process
   │
   ▼
Commit
   │
   ▼
Poll again
```

Failure:

```text
Consumer failure
      │
      ▼
Coordinator detects failure
      │
      ▼
Rebalance
      │
      ▼
New assignment
      │
      ▼
Resume from committed offsets
```

---

## 95. Certification Summary

You should now be able to explain:

### Consumer groups

```text
group.id
partition assignment
parallelism
independent offsets
```

### Offset management

```text
position
committed offset
auto commit
manual commit
sync commit
async commit
```

### Rebalancing

```text
group membership
eager
cooperative
sticky
static membership
```

### Liveness

```text
heartbeats
session timeout
poll interval
```

### Processing guarantees

```text
at-most-once
at-least-once
exactly-once
```

### Performance

```text
fetch sizes
poll batch size
consumer concurrency
lag
```

### Failure handling

```text
crash
duplicate
poison record
rebalance
slow processing
```

---

## 96. Final Exam Mental Model

If you remember only one diagram from this chapter, remember this:

```text
                         TOPIC
                           │
                ┌──────────┼──────────┐
                ▼          ▼          ▼
               P0         P1         P2
                │          │          │
                └──────┬───┴───┬──────┘
                       │
                       ▼
                 CONSUMER GROUP
                       │
             ┌─────────┼─────────┐
             ▼         ▼         ▼
            C1        C2        C3
             │         │         │
             ▼         ▼         ▼
          process   process   process
             │         │         │
             └─────────┼─────────┘
                       ▼
                   COMMIT
                       │
                       ▼
                 GROUP OFFSETS
```

The critical principle is:

> **Kafka consumers do not delete records after processing. They track progress using offsets. Consumer groups divide partitions among members, and failures or membership changes can trigger rebalances.**
