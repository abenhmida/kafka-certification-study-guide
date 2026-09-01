# Chapter 6 — Kafka Storage, Replication, ISR and Fault Tolerance

> Certification track: Kafka Developer + Kafka Administrator  
> Reference foundation: *Kafka: The Definitive Guide*, 2nd Edition

## Table of Contents

- [Chapter 6 — Kafka Storage, Replication, ISR and Fault Tolerance](#chapter-6-kafka-storage-replication-isr-and-fault-tolerance)
  - [Answer](#answer)
  - [Answer](#answer)
  - [Failure 1](#failure-1)
  - [Failure 2](#failure-2)
  - [Question 1](#question-1)
  - [Answer](#answer)
  - [Question 2](#question-2)
  - [Answer](#answer)
  - [Question 3](#question-3)
  - [Answer](#answer)
  - [Question 4](#question-4)
  - [Answer](#answer)
  - [Question 5](#question-5)
  - [Answer](#answer)
  - [Question 6](#question-6)
  - [Answer](#answer)
  - [Question 7](#question-7)
  - [Answer](#answer)
  - [Question 8](#question-8)
  - [Answer](#answer)
- [Chapter 7 — Kafka Administration and Operations Deep Dive](#chapter-7-kafka-administration-and-operations-deep-dive)

---
## Chapter 6 — Kafka Storage, Replication, ISR and Fault Tolerance

> Certification track: CCDAK + CCAAK  
> Reference foundation: *Kafka: The Definitive Guide*, 2nd Edition

---

# 1. Chapter Objectives

This chapter covers one of the most important areas for both Kafka developer and administrator certification:

- Kafka log storage
- partitions
- replicas
- leaders and followers
- replication factor
- ISR
- high watermark
- leader epochs
- replica fetchers
- replication lag
- broker failure
- leader election
- unclean leader election
- `min.insync.replicas`
- rack awareness
- partition reassignment
- preferred leader election
- recovery
- durability
- KRaft controller architecture
- failure scenarios
- administrator troubleshooting
- certification questions

The key mental model is:

```text
Topic
  │
  ▼
Partitions
  │
  ▼
Replicas
  │
  ├── Leader
  └── Followers
          │
          ▼
         ISR
```

---

# 2. Kafka Storage Model

Kafka stores records in partitions.

A topic is therefore not a single file.

Conceptually:

```text
Topic: orders

P0
P1
P2
P3
```

Each partition is an ordered append-only log.

---

# 3. Partition Log

Consider:

```text
Partition 0

Offset   Record
------   ------
100      A
101      B
102      C
103      D
104      E
```

Kafka appends new records to the end.

```text
A → B → C → D → E → F
```

Offsets identify positions in the partition.

---

# 4. Append-Only Design

Kafka generally does not modify records in place.

Instead:

```text
append
append
append
append
```

This design provides:

- sequential I/O characteristics
- efficient writes
- simple ordering
- predictable log structure

Kafka consumers track their position using offsets.

---

# 5. Log Segments

A partition is stored as a sequence of log segment files.

Conceptually:

```text
Partition P0

segment-00000000000000000000
segment-00000000000000100000
segment-00000000000000200000
...
```

The active segment receives new writes.

Older segments become eligible for retention or deletion according to configured policies.

---

# 6. Why Segments Exist

Kafka needs to manage large logs without treating an entire partition as one enormous file.

Segments allow Kafka to:

- roll files
- delete old data efficiently
- apply retention
- build indexes
- recover logs
- manage storage incrementally

---

# 7. Offset Index

Kafka maintains indexes that help locate records efficiently.

Conceptually:

```text
Offset Index

100 → file position X
200 → file position Y
300 → file position Z
```

Kafka can locate the approximate physical position and scan forward.

It does not need to scan the entire partition from offset zero.

---

# 8. Time Index

Kafka also maintains time-based indexing information.

Conceptually:

```text
Timestamp
    │
    ▼
segment / approximate offset
```

This helps Kafka locate records by timestamp.

For example:

```text
seek to records around 10:00
```

without scanning the entire log.

---

# 9. Retention

Kafka retains records based on policies.

Common concepts:

```text
retention.ms
retention.bytes
```

Retention is independent of whether a consumer has processed a record.

This is a critical difference from traditional message queues.

---

# 10. Consumer Progress Does Not Delete Records

Suppose:

```text
Consumer group A
offset = 500
```

Kafka does not delete records merely because group A consumed them.

Another group can still consume them if the records remain within retention.

Conceptually:

```text
Kafka Log
│
├── Group A → offset 500
├── Group B → offset 100
└── Group C → offset 900
```

The log is retained according to topic/broker retention policies.

---

# 11. Replication

Kafka provides fault tolerance through replication.

Suppose:

```text
replication.factor = 3
```

A partition has:

```text
Replica 1
Replica 2
Replica 3
```

These replicas are normally placed on different brokers.

---

# 12. Leader and Followers

For a replicated partition:

```text
Partition P0

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower
```

The leader handles normal client requests for the partition.

Followers replicate the leader's log.

---

# 13. Producer Request Path

For a partition:

```text
Producer
   │
   ▼
Leader
   │
   ├──► Follower
   └──► Follower
```

The producer normally sends to the partition leader.

The leader coordinates replication.

---

# 14. Consumer Request Path

Consumers normally fetch from partition leaders.

Conceptually:

```text
Consumer
   │
   ▼
Partition Leader
   │
   ▼
Records
```

Kafka has supported follower fetching in specific configurations/architectures, but the standard certification mental model remains:

> Clients normally interact with the partition leader.

---

# 15. Replication Factor

Replication factor means:

> The number of replicas for each partition.

Example:

```text
RF = 3
```

means:

```text
P0:
  B1
  B2
  B3
```

It does not mean three copies of the entire topic in one file.

Each partition is replicated independently.

---

# 16. Replication Factor and Fault Tolerance

Suppose:

```text
RF = 3
```

and brokers:

```text
B1
B2
B3
```

If B1 fails:

```text
B2
B3
```

still contain replicas.

A surviving replica can become leader.

This is the foundation of Kafka availability during broker failures.

---

# 17. ISR — In-Sync Replicas

ISR means:

> In-Sync Replicas.

For a partition:

```text
Leader B1
Follower B2
Follower B3
```

if all are sufficiently caught up:

```text
ISR = {B1, B2, B3}
```

ISR membership is dynamic.

---

# 18. ISR Is Not Simply "All Replicas"

Suppose:

```text
Assigned replicas:
B1 B2 B3
```

but B3 falls behind significantly.

Kafka may remove B3 from ISR.

Then:

```text
Assigned replicas:
B1 B2 B3

ISR:
B1 B2
```

The replica still exists, but it is not currently considered in sync.

---

# 19. Why ISR Matters

ISR is critical for:

```text
leader election
acks=all
min.insync.replicas
durability
fault tolerance
```

The certification mental model:

```text
Replica set
    │
    ├── in ISR
    └── out of ISR
```

These are operationally different states.

---

# 20. Follower Replication

Followers fetch records from the leader.

Conceptually:

```text
Leader
  │
  │ replicated data
  ▼
Follower
```

A follower continuously attempts to stay caught up.

---

# 21. Replica Fetching

Kafka brokers use replication mechanisms to transfer partition data from leaders to followers.

Conceptually:

```text
Leader Log
    │
    ▼
Follower Fetch
    │
    ▼
Follower Log
```

If a follower cannot keep up, replication lag grows.

---

# 22. Replication Lag

Suppose:

```text
Leader end offset = 10,000
Follower end offset = 9,500
```

The follower is behind.

Conceptually:

```text
lag = 500 records
```

Replication lag can be caused by:

- slow disk
- network problems
- overloaded broker
- throttling
- CPU pressure
- recovery
- large workloads

---

# 23. Removing a Replica from ISR

If a follower falls sufficiently behind according to Kafka's replica synchronization rules, it can leave the ISR.

Example:

```text
Before:

ISR = B1 B2 B3

B3 becomes slow

After:

ISR = B1 B2
```

This reduces the number of replicas eligible for normal ISR-based guarantees.

---

# 24. ISR Recovery

Suppose B3 catches up.

```text
B3:
9,500
```

Leader:

```text
10,000
```

B3 catches up to the leader.

Once Kafka considers it sufficiently synchronized:

```text
ISR:
B1 B2 B3
```

Again.

---

# 25. High Watermark

The high watermark is an important Kafka replication concept.

Conceptually:

```text
Partition:

0 1 2 3 4 5 6 7 8 9
                ↑
          High Watermark
```

The high watermark represents the point up to which records are considered replicated sufficiently for consumers under Kafka's replication model.

Consumers generally cannot read beyond the high watermark in normal non-isolation semantics.

---

# 26. Why High Watermark Matters

Suppose:

```text
Leader:
0 1 2 3 4 5 6 7

Follower:
0 1 2 3 4
```

The leader may have records that have not yet reached the required replication state.

Kafka uses replication state and the high watermark to control visibility and recovery semantics.

---

# 27. High Watermark vs Log End Offset

These are not the same.

```text
Log End Offset (LEO)
    ↓
end of records currently in the replica log

High Watermark
    ↓
replication visibility boundary
```

Example:

```text
LEO = 1000
HW  = 950
```

There can be records after the high watermark that are not yet considered fully replicated for normal consumer visibility.

---

# 28. Leader Epoch

A leader epoch identifies a period during which a particular broker was leader for a partition.

Conceptually:

```text
Epoch 10
B1 = leader

B1 fails

Epoch 11
B2 = leader
```

Leader epochs help Kafka reason about leadership changes and log consistency.

---

# 29. Why Leader Epochs Matter

Imagine:

```text
Old leader B1
New leader B2
```

B1 later comes back.

Kafka must avoid allowing stale leader data to incorrectly overwrite the new history.

Leader epoch information helps Kafka determine which records belong to the valid current history.

---

# 30. Broker Failure

Suppose:

```text
P0

B1 → Leader
B2 → Follower
B3 → Follower
```

B1 fails.

Kafka can elect another eligible replica:

```text
B2 → New Leader
B3 → Follower
```

The producer refreshes metadata and sends future requests to B2.

---

# 31. Leader Election

Leader election generally selects an eligible replica according to Kafka's replica state.

The goal is:

```text
availability
+
data consistency
```

Kafka prefers replicas that are in the appropriate synchronized state.

---

# 32. Clean Leader Election

A clean election chooses a replica that is considered sufficiently synchronized.

For example:

```text
ISR = B1 B2 B3

B1 fails

B2 becomes leader
```

This preserves the committed history represented by the synchronized replica set.

---

# 33. Unclean Leader Election

Unclean leader election allows a replica outside the ISR to become leader when no suitable in-sync replica is available.

This can restore availability.

But it can cause data loss.

Conceptually:

```text
ISR:
B1

B1 fails

Out-of-sync:
B2
B3

Unclean election:
B2 → leader
```

B2 may not contain all records that existed on B1.

---

# 34. Availability vs Durability

Unclean leader election illustrates a classic distributed-systems tradeoff:

```text
clean election
    ↓
better durability
    ↓
possibly unavailable

unclean election
    ↓
better availability
    ↓
possible data loss
```

For critical data, unclean leader election is generally treated very cautiously.

---

# 35. `unclean.leader.election.enable`

This broker/topic-level behavior controls whether out-of-sync replicas can become leaders when no in-sync replica is available.

Conceptually:

```text
false
    ↓
prefer consistency
    ↓
partition may remain unavailable

true
    ↓
restore availability
    ↓
possible data loss
```

Exact configuration and defaults depend on Kafka version, so certification preparation should always consider the version being examined.

---

# 36. `min.insync.replicas`

This setting establishes a minimum ISR requirement for writes that require the ISR condition.

Example:

```text
RF = 3
min.insync.replicas = 2
acks = all
```

Normal:

```text
ISR = 3
```

One broker fails:

```text
ISR = 2
```

Writes can continue.

Two brokers fail:

```text
ISR = 1
```

Writes requiring two in-sync replicas fail.

---

# 37. Why RF=3 and min ISR=2 Is Common

This configuration creates a useful balance:

```text
Replication Factor = 3
Minimum ISR = 2
```

It allows one replica to be unavailable while maintaining the two-replica write requirement.

But it also means:

```text
two simultaneous replica losses
```

can make the partition unavailable for writes.

---

# 38. Rack Awareness

Kafka can distribute replicas across racks or failure domains.

Conceptually:

```text
Rack A
  B1

Rack B
  B2

Rack C
  B3
```

If Rack A fails:

```text
B2
B3
```

can remain available.

Rack awareness protects against correlated infrastructure failures.

---

# 39. Why Rack Awareness Matters

Without rack awareness:

```text
B1 ─┐
B2 ─┼── same rack
B3 ─┘
```

A rack failure could remove all replicas.

With proper distribution:

```text
Rack A → B1
Rack B → B2
Rack C → B3
```

one rack failure does not necessarily destroy all replicas.

---

# 40. Preferred Replica

Kafka maintains an ordered replica list for each partition.

The first replica in the assignment is commonly referred to as the preferred replica.

Example:

```text
P0 replicas:

[ B1, B2, B3 ]
  ↑
preferred replica
```

The preferred replica is normally the desired leader.

---

# 41. Preferred Leader Election

Over time, leadership can become unevenly distributed.

Example:

```text
B1 → 20 leaders
B2 → 50 leaders
B3 → 30 leaders
```

Kafka administrators can rebalance leadership toward preferred replicas.

The goal is to distribute leader workload more evenly.

---

# 42. Leader Imbalance

Suppose one broker owns many partition leaders.

Then:

```text
B1:
high network
high CPU
high request rate

B2:
low utilization

B3:
low utilization
```

Even if storage is balanced, leader imbalance can create performance problems.

Monitoring leader distribution is therefore important.

---

# 43. Partition Reassignment

Administrators can move partition replicas between brokers.

Example:

```text
Before:

P0 → B1 B2 B3

After:

P0 → B2 B3 B4
```

This is useful for:

- balancing disks
- replacing brokers
- expanding clusters
- changing rack placement
- correcting hot spots

---

# 44. Reassignment Is Not Free

Moving replicas consumes resources.

Potential costs:

```text
network bandwidth
disk I/O
CPU
broker capacity
```

Aggressive reassignment can impact production traffic.

Therefore Kafka administrators often throttle reassignment.

---

# 45. Replica Reassignment Scenario

Suppose:

```text
Broker B4 has just been added.
```

But existing partitions remain on:

```text
B1
B2
B3
```

Adding a broker does not automatically redistribute all existing partition data in the way a naive load balancer might.

Administrators may need to perform partition reassignment.

---

# 46. Adding Brokers Does Not Automatically Solve Existing Imbalance

This is a common operational misunderstanding.

You add:

```text
B4
```

but existing partitions still have replicas on:

```text
B1 B2 B3
```

Therefore:

```text
B4 = empty/new capacity
```

unless data is explicitly moved or new partitions are assigned to it.

Capacity planning must include data redistribution.

---

# 47. Broker Recovery

Suppose B1 fails and B2 becomes leader.

Later B1 returns.

Kafka must recover the replicas hosted on B1.

Conceptually:

```text
B1 fails
  ↓
B2 becomes leader
  ↓
B1 returns
  ↓
B1 catches up
  ↓
B1 becomes synchronized
```

During recovery, disk and network resources are consumed.

---

# 48. Recovery Can Cause Load

A recovering broker may need to catch up many partitions.

This can create:

```text
network traffic
disk writes
disk reads
CPU usage
```

A cluster that is already near saturation can become unstable during recovery.

This is why operational headroom matters.

---

# 49. Disk Failure

If a broker loses a disk containing partition replicas:

```text
replicas become unavailable
```

If other replicas exist:

```text
other brokers
   │
   ▼
continue serving
```

The failed broker can recover data through Kafka's replication mechanisms or administrative recovery procedures.

---

# 50. Broker Failure vs Partition Failure

A broker failure can affect many partitions simultaneously.

Example:

```text
B1 hosts:

P0 leader
P2 follower
P4 leader
P7 follower
P9 leader
...
```

B1 failure impacts every replica hosted there.

Therefore broker-level failure analysis must consider:

```text
number of partitions
leader count
ISR
replication factor
rack placement
```

---

# 51. Controller Role

Kafka needs a control-plane component to manage cluster metadata and partition leadership state.

Modern Kafka deployments use KRaft rather than ZooKeeper for metadata management.

Conceptually:

```text
Kafka Cluster
   │
   ├── Brokers
   │
   └── KRaft Controllers
```

The controller quorum manages cluster metadata and leadership-related control operations.

---

# 52. KRaft Mental Model

In KRaft mode:

```text
Controller quorum
       │
       ▼
cluster metadata
       │
       ├── brokers
       ├── topics
       ├── partitions
       └── leadership state
```

The controllers are not simply "another broker" from the application data path perspective.

They perform control-plane responsibilities.

---

# 53. Controller Quorum

A production KRaft deployment typically uses multiple controllers to maintain quorum.

Conceptually:

```text
C1
C2
C3
```

If one controller fails:

```text
C1
C3
```

can maintain quorum in a three-controller deployment.

The exact deployment architecture depends on the Kafka version and whether nodes are combined broker/controller nodes or dedicated roles.

---

# 54. Controller Failure

A controller failure is different from a partition leader failure.

```text
Partition leader failure
    ↓
leadership for affected partitions changes

Controller failure
    ↓
controller quorum elects another controller
```

This distinction is important for administrator certification.

---

# 55. Replication Factor vs ISR

Do not confuse:

```text
replication factor
```

with:

```text
ISR size
```

Example:

```text
RF = 3
ISR = 2
```

There are still three assigned replicas.

Only two are currently in sync.

---

# 56. Replication Factor vs Available Brokers

Suppose:

```text
RF = 3
```

but only:

```text
2 brokers
```

are available for a new partition assignment.

A three-replica placement cannot be achieved across only two brokers.

The cluster's broker count and placement constraints therefore matter during topic creation and reassignment.

---

# 57. Failure Scenario — One Broker Down

Configuration:

```text
RF = 3
min.insync.replicas = 2
acks = all
```

Initial:

```text
ISR = B1 B2 B3
```

B1 fails:

```text
ISR = B2 B3
```

Producer writes:

```text
SUCCESS
```

assuming no other limiting condition.

---

# 58. Failure Scenario — Two Brokers Down

Same configuration.

After two failures:

```text
ISR = B3
```

Now:

```text
ISR size = 1
min.insync.replicas = 2
```

Producer writes requiring the ISR condition:

```text
FAIL
```

The system sacrifices write availability rather than violating the configured durability requirement.

---

# 59. Failure Scenario — Unclean Election

Suppose:

```text
ISR = B1
```

B1 fails.

Remaining replicas:

```text
B2
B3
```

but both are out of ISR.

If unclean election is disabled:

```text
partition remains unavailable
```

If unclean election is enabled:

```text
B2 may become leader
```

but records that existed only on B1 can be lost.

---

# 60. Failure Scenario — Follower Lag

Suppose:

```text
B1 leader → offset 1,000,000
B2 follower → offset 999,999
B3 follower → offset 800,000
```

B3 is far behind.

Potentially:

```text
ISR = B1 B2
```

B3 is out of sync.

If B1 fails:

```text
B2
```

is a safer election candidate than B3.

---

# 61. Failure Scenario — Leader Returns

Suppose:

```text
B1 leader
B2 follower
B3 follower
```

B1 fails.

B2 becomes leader.

B1 returns with stale data.

B1 must not simply declare itself leader and overwrite the new history.

Kafka's metadata and log recovery mechanisms protect the current cluster history.

---

# 62. Certification Question

What is the difference between:

```text
LEO
```

and:

```text
High Watermark
```

### Answer

LEO is the end position of a replica's local log.

The high watermark represents the replication visibility boundary used by Kafka's replication/consumer semantics.

They can differ.

---

# 63. Certification Question

A topic has:

```text
RF=3
```

Does that mean:

```text
ISR=3
```

always?

### Answer

No.

RF is the configured/assigned number of replicas.

ISR is the current set of replicas considered in sync.

Example:

```text
RF=3
ISR=2
```

is completely possible.

---

# 64. Certification Question

What happens when a follower falls behind?

Possible outcome:

```text
follower removed from ISR
```

This can reduce the number of replicas available for ISR-based durability guarantees.

---

# 65. Certification Question

Why is:

```text
acks=all
```

alone not enough to define a desired durability policy?

Because:

```text
acks=all
```

works together with:

```text
ISR
min.insync.replicas
replication factor
```

A complete durability design considers all of them.

---

# 66. Certification Question

Why can unclean leader election cause data loss?

Because an out-of-sync replica may not contain all records from the failed leader.

If it becomes leader:

```text
missing records
```

may no longer be recoverable through the normal replicated history.

---

# 67. Certification Question

What does rack awareness protect against?

Correlated infrastructure failures such as:

```text
rack failure
power domain failure
network domain failure
```

by distributing replicas across failure domains where configured and supported.

---

# 68. Certification Question

Does adding a broker automatically rebalance every existing partition?

No.

Existing replicas may remain where they are until reassignment or other placement mechanisms move them.

---

# 69. Administrator Metrics

Important replication and broker signals include:

```text
UnderReplicatedPartitions
OfflinePartitionsCount
IsrShrinksPerSec
IsrExpandsPerSec
LeaderElectionRateAndTimeMs
UncleanLeaderElectionsPerSec
BytesInPerSec
BytesOutPerSec
Request latency
Disk utilization
Network utilization
```

The exact metric names can vary by Kafka version.

---

# 70. `UnderReplicatedPartitions`

This is one of the most important operational indicators.

Conceptually:

```text
assigned replicas > ISR
```

Example:

```text
RF = 3
ISR = 2
```

The partition is under-replicated.

Persistent under-replication should be investigated.

---

# 71. Offline Partitions

An offline partition is one for which no leader is currently available.

Example:

```text
P0
 ├── B1 failed
 ├── B2 unavailable
 └── B3 unavailable

No leader
```

This is much more severe than merely having a replica outside the ISR.

---

# 72. Under-Replicated vs Offline

Important distinction:

```text
Under-replicated
    ↓
leader exists
but not all assigned replicas are in ISR
```

versus:

```text
Offline
    ↓
no leader available
```

A cluster can have under-replicated partitions while still serving traffic.

Offline partitions indicate a much more serious availability problem.

---

# 73. Capacity Planning

For a production Kafka cluster, plan for:

```text
storage
network
CPU
memory
partition count
replication
recovery bandwidth
future growth
failure scenarios
```

Do not size only for today's traffic.

---

# 74. Storage Calculation

Suppose:

```text
incoming data = 200 GB/day
retention = 7 days
RF = 3
```

Raw replicated storage is approximately:

```text
200 × 7 × 3
= 4.2 TB
```

Then add headroom for:

```text
segment overhead
indexes
recovery
operational headroom
growth
reassignment
```

Do not deploy at 100% disk utilization.

---

# 75. Network Planning

Replication generates network traffic.

Conceptually:

```text
Producer traffic
      +
replication traffic
      +
consumer traffic
      +
reassignment/recovery
      =
network load
```

A broker may therefore need significantly more network capacity than the application's producer throughput alone suggests.

---

# 76. Failure Headroom

Suppose a cluster is running:

```text
B1 = 70%
B2 = 70%
B3 = 70%
```

If B1 fails, its workload must be absorbed by the remaining brokers.

If they are already close to saturation:

```text
B2 → 100%+
B3 → 100%+
```

The cluster can enter a cascading failure.

Therefore:

> Fault tolerance requires capacity headroom, not just replica copies.

---

# 77. Replication and Durability Mental Model

Memorize:

```text
Replication Factor
        ↓
How many replicas exist?

ISR
        ↓
Which replicas are currently in sync?

min.insync.replicas
        ↓
Minimum ISR requirement for writes that require it

acks
        ↓
How much producer acknowledgment is required?

Unclean election
        ↓
Can an out-of-sync replica become leader?
```

---

# 78. Complete Failure Diagram

```text
                    PRODUCER
                        │
                        ▼
                  Partition Leader
                        │
              ┌─────────┴─────────┐
              ▼                   ▼
         Follower B2         Follower B3
              │                   │
              └─────────┬─────────┘
                        ▼
                       ISR
                        │
                        ▼
                 High Watermark
                        │
                        ▼
                     Consumer
```

If the leader fails:

```text
Leader B1
    │
    X
    │
    ▼
Eligible ISR replica
    │
    ▼
New Leader
```

---

# 79. Advanced Failure Model

Consider:

```text
RF = 3
min.insync.replicas = 2
acks = all
```

State:

```text
B1 = leader
B2 = ISR
B3 = ISR
```

### Failure 1

```text
B3 fails
```

State:

```text
B1 = leader
B2 = ISR
```

Write:

```text
allowed
```

### Failure 2

```text
B1 fails
```

B2 becomes leader.

State:

```text
B2 = leader
ISR = B2
```

Write:

```text
rejected
```

because:

```text
ISR < min.insync.replicas
```

This is the durability protection working as designed.

---

# 80. Senior Interview Question

> Why not always set `min.insync.replicas` equal to the replication factor?

Because then losing even one replica can stop writes.

Example:

```text
RF = 3
min ISR = 3
```

One broker fails:

```text
ISR = 2
```

Writes requiring all three synchronized replicas fail.

That may be appropriate for some workloads, but it reduces write availability.

The correct setting depends on the durability and availability requirements.

---

# 81. Senior Interview Question

> Why not always enable unclean leader election?

Because availability is not free.

Unclean election can restore a leader when ISR replicas are unavailable, but the new leader may be missing records.

Therefore:

```text
availability ↑
potential durability ↓
```

The correct decision depends on business tolerance for data loss.

---

# 82. Senior Interview Question

> Why can a three-replica Kafka cluster still lose data?

Possible reasons include:

```text
unclean leader election
application acknowledgment policy
data loss before required replication
simultaneous failures
incorrect durability configuration
operator error
retention
```

Replication improves durability; it does not make data indestructible.

---

# 83. Senior Interview Question

> Why can a Kafka cluster become unavailable even though replicas exist?

Because:

```text
replicas ≠ currently eligible leaders
```

For example:

```text
RF = 3

ISR = B1

B1 fails

B2/B3 are out of ISR
```

With clean election only:

```text
no eligible leader
```

The partition becomes unavailable.

---

# 84. Administrator Runbook — Under-Replication

When:

```text
UnderReplicatedPartitions > 0
```

investigate:

```text
1. Which partitions?
2. Which brokers host them?
3. Which replicas left ISR?
4. Why are followers lagging?
5. Disk latency?
6. Network saturation?
7. CPU saturation?
8. Broker GC?
9. Recovery/reassignment active?
10. Is the condition transient or persistent?
```

Do not immediately increase replication or restart brokers.

Find the root cause first.

---

# 85. Administrator Runbook — Offline Partition

When:

```text
OfflinePartitionsCount > 0
```

priority is high.

Investigate:

```text
1. Which partition?
2. Which replicas exist?
3. Which brokers are down?
4. Is there an ISR replica?
5. Controller health?
6. Broker connectivity?
7. Disk failure?
8. Election state?
9. Cluster metadata health?
```

An offline partition means clients cannot normally access that partition through a leader.

---

# 86. Administrator Runbook — Broker Failure

When a broker fails:

```text
1. Confirm broker failure.
2. Check affected partitions.
3. Check leader elections.
4. Check ISR.
5. Check offline partitions.
6. Check under-replication.
7. Verify remaining brokers have capacity.
8. Restore failed broker.
9. Monitor replica recovery.
10. Verify ISR expansion.
```

Recovery is not finished merely because the broker process is back online.

---

# 87. Administrator Runbook — Recovery

After broker recovery:

```text
broker returns
      ↓
replicas recover
      ↓
replication catches up
      ↓
ISR expands
      ↓
cluster returns to healthy state
```

Monitor:

```text
ISR
under-replicated partitions
network
disk
recovery rate
```

---

# 88. Exam Drill

### Question 1

A topic has:

```text
RF=3
ISR=2
```

What is true?

A. There are only two replicas.

B. One replica is currently out of ISR.

C. The topic has no leader.

D. The topic has lost data.

### Answer

**B**

There are still three assigned replicas, but only two are currently in sync.

---

# 89. Exam Drill

### Question 2

A partition has:

```text
RF=3
min.insync.replicas=2
acks=all
```

One replica leaves ISR.

Can writes continue?

### Answer

Yes, assuming the remaining two ISR replicas can satisfy all other conditions.

---

# 90. Exam Drill

### Question 3

What is the major risk of unclean leader election?

### Answer

Potential data loss because the elected replica may not contain all records from the failed leader.

---

# 91. Exam Drill

### Question 4

A follower is far behind the leader. What can happen?

### Answer

It can be removed from ISR, reducing the number of synchronized replicas.

---

# 92. Exam Drill

### Question 5

What is the difference between an offline partition and an under-replicated partition?

### Answer

Under-replicated means the partition has a leader but not all assigned replicas are in ISR.

Offline means no leader is available.

---

# 93. Exam Drill

### Question 6

Why use rack awareness?

### Answer

To distribute replicas across failure domains and reduce the risk that a single infrastructure failure removes all replicas.

---

# 94. Exam Drill

### Question 7

What happens when a broker is added to a cluster?

### Answer

The broker becomes available for placement, but existing partition data is not automatically balanced merely because the broker was added. Reassignment or appropriate partition-placement operations may be required.

---

# 95. Exam Drill

### Question 8

Why is replication not sufficient for capacity planning?

### Answer

Replication itself consumes storage, network, disk I/O and recovery resources. The cluster must also have enough headroom to continue operating after failures.

---

# 96. Certification Knowledge Checklist

You should be able to explain:

- [ ] partition logs
- [ ] log segments
- [ ] offset indexes
- [ ] time indexes
- [ ] retention
- [ ] replication factor
- [ ] partition leaders
- [ ] followers
- [ ] ISR
- [ ] replica lag
- [ ] high watermark
- [ ] log end offset
- [ ] leader epoch
- [ ] clean election
- [ ] unclean election
- [ ] `min.insync.replicas`
- [ ] rack awareness
- [ ] preferred replica
- [ ] preferred leader election
- [ ] partition reassignment
- [ ] broker recovery
- [ ] offline partitions
- [ ] under-replicated partitions
- [ ] KRaft controllers
- [ ] controller failure
- [ ] storage capacity
- [ ] network capacity
- [ ] failure headroom

---

# 97. Final Mental Model

The entire chapter can be reduced to:

```text
                    TOPIC
                      │
                      ▼
                  PARTITION
                      │
            ┌─────────┼─────────┐
            ▼         ▼         ▼
           B1        B2        B3
         Leader    Follower  Follower
            │         │         │
            └─────────┼─────────┘
                      ▼
                     ISR
                      │
                      ▼
                High Watermark
                      │
                      ▼
                   Consumer
```

And during failure:

```text
Leader failure
      │
      ▼
ISR replica
      │
      ▼
new leader
      │
      ▼
producer/consumer metadata refresh
      │
      ▼
continued operation
```

If no suitable ISR replica exists:

```text
clean election
      │
      ▼
partition may become unavailable
```

or:

```text
unclean election
      │
      ▼
availability restored
      │
      ▼
possible data loss
```

---

# 98. Chapter 6 Takeaway

The most important certification principle is:

> **Kafka durability is not provided by replication factor alone. You must reason about replica placement, ISR membership, leader election, `acks`, `min.insync.replicas`, high watermark, failure domains, and recovery capacity as one system.**

For administrators, the most important operational distinction is:

```text
Healthy
    ↓
ISR = assigned replicas

Under-replicated
    ↓
leader exists
but ISR is smaller

Offline
    ↓
no leader
```

For senior engineers, the key tradeoff is:

```text
Durability
      ↕
Availability
      ↕
Performance
```

Kafka architecture is about choosing where that balance belongs for the business workload.

---

# 99. Next Chapter

## Chapter 7 — Kafka Administration and Operations Deep Dive

The next chapter will cover:

- broker configuration
- topic administration
- partition management
- replication management
- Kafka CLI tools
- topic creation
- topic alteration
- topic deletion
- configuration overrides
- dynamic broker configuration
- consumer group administration
- offsets
- lag inspection
- partition reassignment
- preferred leader election
- cluster health
- KRaft administration
- monitoring
- JMX
- critical metrics
- capacity planning
- disk management
- security administration overview
- operational runbooks
- failure drills
- certification questions
- administrator exam scenarios
