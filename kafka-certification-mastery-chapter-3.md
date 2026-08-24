# Apache Kafka Certification Mastery

## Table of Contents

- [Chapter 3 — Topics, Partitions, Offsets and Log Internals](#chapter-3-topics-partitions-offsets-and-log-internals)
- [Scalability](#scalability)
- [Parallelism](#parallelism)
  - [Delete retention](#delete-retention)
  - [Compaction](#compaction)
  - [Why not simply create thousands of partitions?](#why-not-simply-create-thousands-of-partitions)

---
## Chapter 3 — Topics, Partitions, Offsets and Log Internals

> Certification track: CCDAK + CCAAK  
> Reference foundation: *Kafka: The Definitive Guide*, 2nd Edition

---

# 1. Why This Chapter Matters

Topics and partitions are the foundation of Kafka.

Almost every important Kafka decision eventually comes back to:

```text
How many partitions?
Which key?
Where is the record stored?
What is its offset?
How long is it retained?
How is it replicated?
How is the partition distributed?
```

For developers, partitions determine:

- ordering
- parallelism
- consumer-group scalability
- partitioning strategy
- throughput

For administrators, partitions determine:

- storage
- replication
- broker load
- reassignment
- recovery time
- cluster scalability

A strong Kafka engineer therefore thinks about a topic as:

```text
Topic
  │
  ├── Partition 0
  ├── Partition 1
  ├── Partition 2
  └── ...
```

not as a single queue.

---

# 2. Topic vs Partition

A topic is a logical name.

Example:

```text
orders
```

A partition is the physical/logical unit in which records are ordered and stored.

```text
orders
  │
  ├── orders-0
  ├── orders-1
  └── orders-2
```

Therefore:

> **A topic is a logical abstraction. A partition is the fundamental unit of ordering, storage, replication and parallelism.**

This sentence is worth memorizing.

---

# 3. Partition Ordering

Suppose:

```text
orders-0

0 → A
1 → B
2 → C
3 → D
```

Kafka guarantees:

```text
A < B < C < D
```

within this partition.

Now:

```text
orders-1

0 → X
1 → Y
2 → Z
```

There is no guaranteed relationship between:

```text
orders-0 offset 3
```

and:

```text
orders-1 offset 0
```

Therefore:

> Kafka guarantees ordering within a partition, not across partitions.

---

# 4. Why Partitions Exist

Partitions solve two major problems:

## Scalability

```text
Topic
 ├── P0 → Broker 1
 ├── P1 → Broker 2
 ├── P2 → Broker 3
 └── P3 → Broker 1
```

Work can be distributed across brokers.

## Parallelism

```text
P0 ──► Consumer 1

P1 ──► Consumer 2

P2 ──► Consumer 3
```

Multiple consumers can process partitions concurrently.

---

# 5. Partition Count

Suppose:

```text
orders = 12 partitions
```

One consumer group can have at most 12 actively assigned consumers.

```text
12 partitions
20 consumers

12 active
8 idle
```

Therefore:

> Maximum active consumer parallelism in a consumer group is bounded by the partition count.

This is one of the most frequently tested Kafka concepts.

---

# 6. Can Partition Count Be Increased?

Yes.

A topic can generally have its partition count increased.

For example:

```text
Before:

orders
 ├── P0
 ├── P1
 └── P2
```

After:

```text
orders
 ├── P0
 ├── P1
 ├── P2
 ├── P3
 ├── P4
 └── P5
```

But there is a major consequence:

> Increasing the number of partitions can change key-to-partition mapping.

This can affect ordering for keyed records.

---

# 7. Why Increasing Partitions Can Break Ordering

Suppose a producer uses a key:

```text
customer-42
```

Initially:

```text
3 partitions

customer-42 → P1
```

After increasing to:

```text
6 partitions
```

the partitioning calculation may result in:

```text
customer-42 → P4
```

Now records for the same key may be distributed differently over time.

Therefore:

> Do not casually increase partition count if your application depends on strict per-key ordering across the lifetime of the data.

---

# 8. Record Structure

A Kafka record can contain:

```text
Record
 ├── key
 ├── value
 ├── headers
 ├── timestamp
 └── offset
```

Example:

```text
key = customer-42

value = {
  "orderId": "ORD-1001",
  "amount": 149.99
}
```

Headers can carry metadata such as:

```text
event-type=OrderCreated
trace-id=abc123
```

---

# 9. Offset

Every record in a partition has an offset.

Example:

```text
orders-0

Offset 0 → A
Offset 1 → B
Offset 2 → C
Offset 3 → D
```

The offset is the record's position in that partition log.

---

# 10. Offset Scope

This is a major certification trap.

An offset is not globally unique.

For example:

```text
orders-0 offset 100
orders-1 offset 100
orders-2 offset 100
```

These are different records.

The identity is effectively:

```text
(topic, partition, offset)
```

not simply:

```text
offset
```

---

# 11. Offset Is Not a Database ID

Do not think of the offset as:

```text
global record ID
```

Think:

```text
Partition position
```

For example:

```text
P0:
0
1
2
3
4

P1:
0
1
2
3
4
```

Both partitions have offset 4.

---

# 12. Log End Offset

Suppose:

```text
orders-0

0 → A
1 → B
2 → C
3 → D
```

The next offset would be:

```text
4
```

This represents the log end position.

A useful mental model is:

```text
records:
0 1 2 3

next append position:
4
```

Consumer lag calculations rely on the relationship between the consumer's position/committed progress and the log end.

---

# 13. Consumer Position vs Committed Offset

Suppose:

```text
0 → A
1 → B
2 → C
3 → D
```

The consumer has fetched:

```text
A
B
C
```

Its current position may now point to:

```text
3
```

meaning:

```text
next record = offset 3
```

But the committed offset could still be:

```text
2
```

depending on when commits occurred.

Therefore distinguish:

```text
Current consumer position
        ≠
Committed offset
```

This distinction becomes critical in consumer failure scenarios.

---

# 14. The "Next Record" Semantics

If a consumer commits:

```text
offset = 3
```

the conceptual meaning is:

> The consumer has successfully processed records before offset 3 and will resume at offset 3.

This is why committed offsets are generally understood as the **next position to consume**, rather than "the last record processed."

Certification questions often exploit this distinction.

---

# 15. Partition Log

A partition is an ordered append-only log.

Conceptually:

```text
orders-0

┌────┬────┬────┬────┬────┬────┐
│ 0  │ 1  │ 2  │ 3  │ 4  │ 5  │
├────┼────┼────┼────┼────┼────┤
│ A  │ B  │ C  │ D  │ E  │ F  │
└────┴────┴────┴────┴────┴────┘
                              ▲
                         append here
```

Records are appended rather than randomly inserted.

---

# 16. Log Segments

A partition is divided into segments.

Conceptually:

```text
Partition P0

Segment 1
 ├── records
 ├── offset index
 └── time index

Segment 2
 ├── records
 ├── offset index
 └── time index

Segment 3
 ├── records
 ├── offset index
 └── time index
```

Kafka can roll segments based on configured policies.

This allows Kafka to manage old data efficiently.

---

# 17. Active Segment

A partition normally has one active segment receiving new records.

Older segments become inactive.

Conceptually:

```text
P0

Segment 1   CLOSED
Segment 2   CLOSED
Segment 3   ACTIVE
                         ▲
                         │
                       writes
```

This matters for:

- retention
- deletion
- compaction
- recovery
- disk management

---

# 18. Segment Rolling

Suppose:

```text
Segment 1
```

reaches the configured segment size or age threshold.

Kafka can roll to:

```text
Segment 2
```

Now:

```text
Segment 1 → inactive
Segment 2 → active
```

Segment rolling is not the same as retention deletion.

This distinction matters.

---

# 19. Retention

Kafka retention determines how long data remains available.

Retention can be controlled using:

```text
retention.ms
retention.bytes
```

Conceptually:

```text
New records
    │
    ▼
Partition
    │
    ▼
Older segments
    │
    ▼
Retention policy
    │
    ▼
Delete eligible segments
```

---

# 20. Time-Based Retention

Suppose:

```text
retention.ms = 604800000
```

This corresponds to approximately:

```text
7 days
```

Kafka can remove old log segments once they become eligible under the retention policy.

Important:

> Retention is about stored data age/size, not whether consumers have processed the records.

---

# 21. Size-Based Retention

Suppose:

```text
retention.bytes = 100GB
```

Kafka limits retained log data according to the configured size policy.

This is useful when storage capacity is the primary constraint.

---

# 22. Retention Is Not Consumer Acknowledgment

Traditional queue thinking:

```text
Consumed → deleted
```

Kafka thinking:

```text
Consumed
    │
    ▼
Offset advances

Record remains
    │
    ▼
Retention policy decides when it disappears
```

This is a crucial architectural difference.

---

# 23. Log Deletion

Kafka generally deletes eligible **log segments**, rather than individually removing arbitrary records from the middle of an active log.

Conceptually:

```text
P0

Segment 1 → DELETE
Segment 2 → DELETE
Segment 3 → KEEP
Segment 4 → ACTIVE
```

Segment-based deletion is efficient.

---

# 24. Log Compaction

Retention and compaction are different.

### Delete retention

Removes old data based on age/size.

### Compaction

Retains the latest value for each key, subject to compaction semantics.

Example:

```text
customer-42 → name=Alice
customer-17 → name=Bob
customer-42 → name=Alicia
customer-42 → name=Alice Smith
```

Compaction can eventually reduce this to the latest state for the key:

```text
customer-17 → name=Bob
customer-42 → name=Alice Smith
```

The exact physical cleanup is asynchronous.

---

# 25. Why Compaction Exists

Compaction is useful for topics representing state.

Examples:

```text
customer-state
account-state
product-state
configuration
```

A compacted topic can conceptually act like:

```text
Key → Latest Value
```

rather than an ever-growing history.

---

# 26. Tombstones

A tombstone is a record with:

```text
key = some-key
value = null
```

Example:

```text
customer-42 → null
```

In a compacted topic, this indicates deletion of the key's state.

Conceptually:

```text
customer-42 → Alice
customer-42 → null
```

Eventually compaction can remove the obsolete records and the key's state.

Tombstones are therefore essential when using Kafka as a compacted state log.

---

# 27. Compaction Does Not Mean Immediate Deletion

This is important.

Compaction is asynchronous.

Suppose:

```text
K1 → A
K1 → B
K1 → C
```

You should not assume that Kafka immediately transforms the disk into:

```text
K1 → C
```

Instead, compaction runs in the background.

Therefore:

> Compaction is an eventual storage optimization/state-retention mechanism, not an instantaneous update operation.

---

# 28. Compacted Topic Semantics

A compacted topic can contain multiple records for the same key while those records have not yet been compacted.

Therefore:

```text
K1 → A
K1 → B
K1 → C
```

can temporarily exist together.

A consumer reading the full log can observe historical values.

Compaction eventually reduces obsolete records.

---

# 29. Delete Retention vs Compaction

Compare:

| Feature | Delete Retention | Compaction |
|---|---|---|
| Main goal | Remove old data | Keep latest value per key |
| Based on | Age/size | Key |
| Historical records | Eventually removed | Obsolete keyed records removed |
| Useful for | Event history | State/change-log topics |
| Tombstones | Not fundamental | Important |

---

# 30. Key-Based Partitioning

Suppose:

```text
key = customer-42
```

The producer's partitioning strategy determines the destination partition.

Conceptually:

```text
customer-42
      │
      ▼
  Partitioner
      │
      ▼
orders-2
```

If the same key consistently maps to the same partition, related records can preserve per-key ordering.

---

# 31. Per-Key Ordering

Suppose:

```text
customer-42

OrderCreated
PaymentReceived
OrderShipped
OrderDelivered
```

If all four records go to:

```text
orders-2
```

Kafka preserves their order within that partition.

This enables a powerful pattern:

> Use the entity identifier as the key when ordering for that entity matters.

Examples:

```text
customerId
accountId
orderId
deviceId
```

---

# 32. Hot Partitions

Key-based partitioning introduces a risk.

Suppose:

```text
customer-42
```

generates 80% of all events.

If all events map to one partition:

```text
P0 → 80% traffic
P1 → 5%
P2 → 5%
P3 → 5%
P4 → 5%
```

P0 becomes a hot partition.

Adding more consumers will not necessarily solve the problem because one partition is still the bottleneck.

---

# 33. Hot Partition Symptoms

Typical symptoms:

```text
One partition has much higher throughput
One broker has much higher load
Consumer lag concentrated on one partition
Uneven network traffic
Uneven disk usage
```

This is often a partitioning/key-design problem rather than simply a "need more consumers" problem.

---

# 34. Partition Count and Throughput

A useful conceptual model:

```text
More partitions
      │
      ├── more parallelism
      ├── more consumer concurrency
      ├── more potential throughput
      └── more metadata / operational overhead
```

Therefore:

> More partitions are not free.

Too many partitions can increase:

- memory usage
- file handles
- metadata overhead
- recovery work
- rebalance complexity
- controller metadata size
- operational complexity

Partition planning is an architectural decision.

---

# 35. Partition Count Is Difficult to Reduce

Increasing partition count is supported.

Reducing partition count for an existing topic is not a normal supported operation.

Therefore:

> Choose the initial partition count carefully.

If the topic later needs more parallelism, increasing partitions is possible, but it can affect key distribution and ordering.

---

# 36. Partition Reassignment

Partition replicas can be reassigned across brokers.

Example:

```text
Before:

P0 → B1 B2 B3

After:

P0 → B2 B3 B4
```

Reassignment can be used for:

- balancing storage
- balancing network traffic
- adding brokers
- removing brokers
- rack/AZ optimization
- correcting poor distribution

---

# 37. Reassignment Is Not Free

Moving replicas requires network and disk I/O.

Conceptually:

```text
Broker 1
   │
   │ replica movement
   ▼
Broker 4
```

Large reassignment operations can consume:

- network bandwidth
- disk bandwidth
- CPU
- broker resources

Therefore reassignment must be planned carefully in production.

---

# 38. Partition Leaders and Load

Suppose all partition leaders are concentrated on one broker:

```text
Broker 1
 ├── P0 Leader
 ├── P1 Leader
 ├── P2 Leader
 ├── P3 Leader
 └── P4 Leader

Broker 2
 └── mostly followers

Broker 3
 └── mostly followers
```

Broker 1 may become overloaded.

Good cluster design attempts to distribute leadership.

---

# 39. Leader Balance

A healthy cluster generally aims for reasonable distribution of:

```text
leaders
replicas
storage
network traffic
```

A cluster can have enough replicas but still have poor performance if leadership is badly concentrated.

---

# 40. Partition Placement

For:

```text
3 brokers
3 partitions
RF = 3
```

a reasonable layout might be:

```text
P0 → B1 leader, B2, B3
P1 → B2 leader, B3, B1
P2 → B3 leader, B1, B2
```

This spreads leadership.

---

# 41. Storage Calculation

Suppose:

```text
incoming data = 100 GB/day
retention = 7 days
replication factor = 3
```

Ignoring compression and overhead:

```text
logical storage
= 100 GB × 7
= 700 GB
```

With RF 3:

```text
physical storage
≈ 700 × 3
= 2.1 TB
```

Then add headroom for:

- segment overhead
- indexes
- filesystem usage
- reassignments
- broker imbalance
- operational safety

Do not design disks at 100% capacity.

---

# 42. Partition Storage Distribution

Suppose:

```text
Total logical data = 900 GB
Partitions = 9
```

A rough average:

```text
900 / 9 = 100 GB per partition
```

With:

```text
RF = 3
```

the cluster stores approximately:

```text
2.7 TB
```

of replica data.

Actual usage depends on:

- compression
- indexes
- record sizes
- segment overhead
- distribution
- retention behavior

---

# 43. Partition Count and Recovery

More partitions can also increase recovery and operational work.

Suppose:

```text
10,000 partitions
```

A broker failure can cause many leadership and replica-management operations.

Therefore partition count affects:

```text
normal operation
+
failure handling
```

This is an administrator-level design consideration.

---

# 44. Timestamp Concepts

Kafka records have timestamps.

Depending on configuration and producer behavior, timestamps can represent different notions of event/append time.

Important timestamp-related concepts include:

```text
CreateTime
LogAppendTime
```

The configured topic/broker behavior determines how timestamps are interpreted.

These timestamps are useful for:

- retention
- time-based lookup
- stream processing
- event-time reasoning
- operational analysis

---

# 45. Log Append Time

With log append time semantics, the broker determines the timestamp associated with the record when it is appended.

Conceptually:

```text
Producer
   │
   │ record
   ▼
Broker
   │
   ▼
append timestamp
```

This differs from relying purely on the producer's event timestamp.

---

# 46. Create Time

With create-time semantics, the timestamp originates from the producer side.

Conceptually:

```text
Producer
   │
   │ timestamp = T
   ▼
Kafka
```

This can be useful for event-time applications.

---

# 47. Certification Trap — Retention and Consumption

**Question:**

A record is consumed immediately. Does Kafka delete it immediately?

**Answer:**

No.

Retention and compaction policies determine when data is removed.

---

# 48. Certification Trap — Ordering

**Question:**

A topic has five partitions. Does Kafka guarantee that records produced to different partitions are globally ordered?

**Answer:**

No.

Only per-partition ordering is guaranteed.

---

# 49. Certification Trap — Offset

**Question:**

Can two records have the same offset?

**Answer:**

Yes, if they belong to different partitions.

Example:

```text
P0 offset 100
P1 offset 100
```

---

# 50. Certification Trap — Consumers

**Question:**

A topic has 4 partitions and a consumer group has 8 consumers. Can all 8 process records simultaneously?

**Answer:**

No.

At most 4 consumers can have active partition assignments.

---

# 51. Certification Trap — Partition Increase

**Question:**

Does increasing the number of partitions preserve all existing key-to-partition mappings?

**Answer:**

Not necessarily.

The partitioning calculation can map keys differently after the partition count changes.

---

# 52. Certification Trap — Compaction

**Question:**

Does compaction immediately remove all older records for a key?

**Answer:**

No.

Compaction is asynchronous and runs in the background.

---

# 53. Certification Trap — Tombstone

**Question:**

What does a tombstone represent in a compacted topic?

**Answer:**

A record with a key and a null value, representing deletion of that key's current state.

---

# 54. Certification Trap — Hot Partition

**Question:**

A topic has 20 partitions, but one partition has 90% of the traffic. Will adding more consumers necessarily fix the bottleneck?

**Answer:**

No.

One partition can still only be actively processed by one consumer within a consumer group.

The underlying problem may be an unbalanced partitioning/key strategy.

---

# 55. Developer Lab — Observe Offsets

Create:

```text
certification-offsets
```

with:

```text
3 partitions
```

Produce several records.

Inspect:

```text
partition
offset
key
value
timestamp
```

Your goal is to observe:

```text
Partition 0:
0
1
2
3

Partition 1:
0
1
2

Partition 2:
0
1
```

Notice that offsets restart from zero for each partition.

---

# 56. Developer Lab — Key Distribution

Produce records using:

```text
customer-1
customer-1
customer-2
customer-2
customer-3
customer-3
```

Observe where records land.

Verify whether records with the same key are routed consistently to the same partition under the producer's partitioning behavior.

Then explain why this matters for ordering.

---

# 57. Developer Lab — Increase Partitions

Create:

```text
orders
3 partitions
```

Produce keyed records.

Then increase to:

```text
6 partitions
```

Produce more records using the same keys.

Observe partition distribution.

The goal is to understand why increasing partition count can affect future key mapping.

---

# 58. Administrator Lab — Inspect Partition Distribution

Create a topic:

```text
certification-orders
```

with:

```text
12 partitions
RF = 3
```

Inspect:

```text
leaders
replicas
ISR
```

Build a table:

```text
Partition | Leader | Replicas | ISR
----------|--------|----------|----
0         | B1     | B1,B2,B3 | B1,B2,B3
1         | B2     | B2,B3,B1 | B2,B3,B1
...
```

Determine whether leadership is balanced.

---

# 59. Administrator Lab — Simulate a Hot Partition

Create a topic with:

```text
6 partitions
```

Generate most records with the same key.

Observe:

```text
partition throughput
consumer lag
broker load
```

The objective is to demonstrate:

```text
More partitions ≠ automatically more effective throughput
```

if the partitioning strategy is unbalanced.

---

# 60. Administrator Lab — Retention

Configure a test topic with a very short retention period.

Produce records.

Observe:

```text
records
   │
   ▼
segments
   │
   ▼
retention threshold
   │
   ▼
segment deletion
```

Important:

Do not expect immediate deletion at the exact millisecond the retention threshold is reached. Segment rolling and cleanup behavior matter.

---

# 61. Administrator Lab — Compaction

Create a compacted topic.

Produce:

```text
customer-1 → Alice
customer-2 → Bob
customer-1 → Alicia
customer-1 → null
```

Observe the topic before and after compaction.

The goal is to understand:

```text
historical updates
        ↓
compaction
        ↓
latest keyed state
        ↓
tombstone-based deletion
```

---

# 62. Senior-Level Scenario

You operate:

```text
12 brokers
600 partitions
RF = 3
```

A broker contains a large number of replicas.

The broker is failing due to disk pressure.

Your task is to reason about:

```text
1. Which partitions are affected?
2. Which are leaders?
3. Which replicas are in ISR?
4. How much data must move?
5. Where will replicas be reassigned?
6. Will reassignment increase network pressure?
7. Will leadership become unbalanced?
8. Is there enough disk headroom?
9. What happens if another broker fails?
10. How will recovery affect the cluster?
```

This is the type of reasoning expected from a senior Kafka administrator.

---

# 63. Design Exercise — Choose Partition Count

Suppose an application expects:

```text
Peak throughput = 200 MB/s
```

and one partition can safely sustain approximately:

```text
20 MB/s
```

A rough starting point is:

```text
200 / 20 = 10 partitions
```

Then consider:

```text
consumer parallelism
future growth
replication
broker count
hot keys
recovery
storage
operational overhead
```

Therefore partition count should not be selected using throughput alone.

---

# 64. Design Exercise — Choose the Key

Requirement:

> All events belonging to the same customer must be processed in order.

Possible key:

```text
customerId
```

Architecture:

```text
customerId
    │
    ▼
Partitioner
    │
    ▼
One partition
    │
    ▼
Ordered processing
```

If instead you use:

```text
random UUID
```

events for the same customer can be distributed across partitions.

Ordering is then lost at the partition level.

---

# 65. Senior Interview Question

### Why not simply create thousands of partitions?

Because partitions have costs.

More partitions can increase:

- broker memory requirements
- metadata size
- file handles
- network connections
- recovery work
- controller work
- rebalance complexity
- replication overhead
- operational complexity

The correct answer is:

> Choose enough partitions to satisfy throughput and parallelism requirements while considering storage, recovery, metadata and operational costs.

---

# 66. Chapter 3 Knowledge Checklist

You should be able to explain:

- [ ] Topic vs partition
- [ ] Partition ordering
- [ ] Partition parallelism
- [ ] Partition count
- [ ] Increasing partition count
- [ ] Key-based partitioning
- [ ] Hot partitions
- [ ] Offset semantics
- [ ] Offset scope
- [ ] Consumer position
- [ ] Committed offset
- [ ] Log end position
- [ ] Log segments
- [ ] Active segment
- [ ] Segment rolling
- [ ] Retention
- [ ] Delete retention
- [ ] Compaction
- [ ] Tombstones
- [ ] CreateTime
- [ ] LogAppendTime
- [ ] Partition reassignment
- [ ] Leader distribution
- [ ] Rack/AZ awareness
- [ ] Storage sizing
- [ ] Recovery implications
- [ ] Certification traps

---

# 67. Final Mental Model

```text
                         TOPIC

              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
             P0           P1           P2
              │            │            │
              ▼            ▼            ▼
          ┌────────┐   ┌────────┐   ┌────────┐
          │ Segment│   │ Segment│   │ Segment│
          ├────────┤   ├────────┤   ├────────┤
          │ Offset │   │ Offset │   │ Offset │
          │ Index  │   │ Index  │   │ Index  │
          │ Time   │   │ Time   │   │ Time   │
          │ Index  │   │ Index  │   │ Index  │
          └────────┘   └────────┘   └────────┘
              │            │            │
              ▼            ▼            ▼
          Retention     Retention    Retention
              │            │            │
              ▼            ▼            ▼
          Deletion / Compaction / Tombstones
```

The essential model:

> **A Kafka topic is divided into partitions. Each partition is an ordered append-only log identified by offsets. Partitions are the unit of ordering, replication and consumer-group parallelism. Logs are divided into segments, which enables retention and compaction. Keys influence partition placement and therefore per-key ordering.**