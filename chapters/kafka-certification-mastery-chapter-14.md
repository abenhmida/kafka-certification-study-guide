# Chapter 14 — Kafka Reliability, Fault Tolerance & Disaster Recovery Deep Dive

### Kafka Developer & Administrator Certification Preparation

> Certification track: Kafka Developer + Kafka Administrator  
> Reference foundation: *Kafka: The Definitive Guide*, 2nd Edition

---

## 1. Learning Objectives

By the end of this chapter, you should be able to explain and troubleshoot:

1. [x] Kafka replication
2. [x] Leader and follower replicas
3. [x] ISR mechanics
4. [x] Broker failures
5. [x] Leader elections
6. [x] replication.factor
7. [x] min.insync.replicas
8. [x] acks
9. [x] unclean leader election
10. [x] data loss scenarios
11. [x] availability vs durability
12. [x] rack/AZ awareness
13. [x] controlled broker shutdown
14. [x] rolling upgrades
15. [x] replica reassignment
16. [x] broker recovery
17. [x] disaster recovery
18. [x] MirrorMaker 2
19. [x] cross-cluster replication
20. [x] RPO and RTO
21. [x] backup/restore considerations
22. [x] multi-AZ Kafka architectures
23. [x] failure scenarios
24. [x] certification-style reliability questions

## 2. What Reliability Means in Kafka

Kafka reliability is not one single property you must distinguish:

```text
Reliability
    |
    +-- Availability
    |
    +-- Durability
    |
    +-- Fault tolerance
    |
    +-- Consistency
    |
    +-- Recoverability
    |
    +-- Disaster recovery
```

These concepts are related but different.

### 2.1. Availability

Availability asks **Can clients currently access the partition?**

For a partition:

```text
Partition P0
    |
    v
  Leader
    |
    +--> available
```

If no broker can provide a valid leader then the partition is unavailable.

### 2.2. Durability

Durability asks **Once data has been acknowledged, how resistant is it to loss?**

For example:

```text
 Producer
    |
    | acks=all
    v
  Leader
    |
    +--> Replica 1
    |
    +--> Replica 2
```

More independent copies generally provide stronger failure tolerance but replication alone is not enough.
The producer acknowledgment policy matters too.

### 2.3. Fault Tolerance

Fault tolerance means the system can continue operating despite failures.
Kafka achieves this through `Replication + Leader election + ISR + Distributed metadata`

Example:

```text
              Partition 0

                 Leader
                Broker 1
                   |
           +-------+-------+
           |               |
        Broker 2         Broker 3
        follower         follower
```

If Broker 1 fails another eligible replica can become leader.

### 2.4. Kafka's Fundamental Reliability Model

The central model is:

```text
Partition
    |
    +-- Leader
    |
    +-- Followers
```

All replicas contain copies of the partition's log. Writes go to the leader.
Followers replicate from the leader.

### 2.5. Example: Replication Factor 3

Suppose:

```text
Topic: orders
Partition: 0
Replication factor: 3
```

The replica set might be:

```text
Broker 1 -> Leader
Broker 2 -> Follower
Broker 3 -> Follower
```

Conceptually:

```text
                orders-0
                   |
        +----------+----------+
        |          |          |
     Broker 1   Broker 2   Broker 3
     Leader     Follower   Follower
```

## 3. What Happens During a Write?

A producer sends ``Produce(order-123)`` to the partition leader. Followers replicate the data.

```text
Producer
   |
   v
Broker 1
 Leader
   |
   +----> Broker 2
   |
   +----> Broker 3
```

The acknowledgment depends on the producer configuration.

### 3.1. acks

The producer's acks configuration controls when the producer considers a write acknowledged.

Important values `acks=0`, `acks=1`, `acks=all`

#### 3.1.1. acks=0

With `acks=0` the producer does not wait for a broker acknowledgment.

Conceptually:

```text
 Producer
    |
    | send
    v
  Broker
```

The producer proceeds without waiting for confirmation.

1. **Advantages**:

* potentially lower latency
* potentially higher throughput

2. **Disadvantage**:

* weakest delivery assurance

For reliability-sensitive workloads, this is generally unsuitable. 

#### 3.1.2. acks=1

With `acks=1` the leader acknowledges the record after accepting it.

Conceptually:

```text
     Producer
        |
        v
      Leader
        |
        | acknowledge
        v
     Producer

```

Followers may still be catching up. If the leader fails before followers have replicated the record, 
the record may be vulnerable depending on the election/recovery situation.

#### 3.1.3. acks=all

With `acks=all` the leader waits for the required in-sync replicas according to the cluster's replication rules.

This is generally the strongest producer acknowledgment mode but remember `acks=all` **does not magically guarantee durability under every configuration**.

You must consider `acks + replication.factor + min.insync.replicas + leader election policy`

## 4. The Reliability Equation

A useful certification mental model is `Durability = replication + ISR + acks + min.insync.replicas + safe leader election`

No single configuration parameter provides the entire guarantee.

## 5. replication.factor

The replication factor determines how many replicas exist for a partition.

For example `replication.factor = 3` means `3 replicas`

Example:

```text
Broker 1
Broker 2
Broker 3
```

## 6. What Replication Factor Does NOT Mean

A replication factor of 3 does not necessarily mean **Three brokers are always healthy**.

You can have:

```text
RF = 3

Broker 1 -> leader
Broker 2 -> follower
Broker 3 -> offline
```

The partition is still configured with three replicas, but only two may currently be in the ISR.

## 7. ISR — In-Sync Replicas

ISR stands for **In-Sync Replicas**. The ISR contains replicas considered sufficiently caught up with the leader.

Example:

```text
Replica set:

Broker 1
Broker 2
Broker 3

ISR:

Broker 1
Broker 2
Broker 3
```

All replicas are healthy.

## 8. ISR Shrink

Suppose Broker 3 falls behind.

```text
Before:

ISR = {B1, B2, B3}

After:

ISR = {B1, B2}

```
This is called an ISR shrink. The cluster has reduced redundancy.

## 9. ISR Expansion

When Broker 3 catches up `ISR = {B1, B2, B3}`. This is an ISR expansion.

A healthy cluster should generally return to its expected ISR state after temporary failures.

## 10. Why ISR Matters

Consider:

```text
RF = 3
ISR = {B1, B2}
```

If **B1** is leader and fails, **B2** can potentially become leader. But if `ISR = {B1}` 
and **B1** fails, there may be no in-sync follower available.
This can create an availability problem.

## 11. min.insync.replicas

`min.insync.replicas` specifies the minimum number of ISR replicas required for certain writes to succeed when using appropriate acknowledgment semantics.

Example:

```text
replication.factor = 3
min.insync.replicas = 2
acks = all
```

Normal state `ISR = 3` writes succeed. If one replica fails `ISR = 2` writes can still satisfy the minimum.

If another replica is lost `ISR = 1` writes requiring the minimum ISR can fail.

## 12. Why min.insync.replicas Matters

It creates a deliberate trade-off `Availability vs Durability`

For example:

```text
RF = 3
min.insync.replicas = 2
acks = all
```

requires at least two in-sync replicas for successful writes.
This prevents the leader from continuing to acknowledge writes when redundancy has degraded below the desired level.

## 13. The Classic Reliability Configuration

A common reliability-oriented design is:

```text
replication.factor = 3
min.insync.replicas = 2
acks = all
```
```text
              Producer
                  |
               acks=all
                  |
                  v
               Leader
              /      \
             /        \
           ISR        ISR
          Broker 2   Broker 3
```

This gives strong protection against a single broker failure, assuming the replicas are placed independently.

## 14. Important Certification Trap

### Question:

**Does replication.factor=3 guarantee that three copies always exist?**

**No**.

A replica can be offline or out of sync. The actual operational state is reflected by **ISR** and related replication health metrics.

## 15. Failure Scenario — One Broker Dies

Initial:

```text
RF = 3

B1 = Leader
B2 = Follower
B3 = Follower

ISR = {B1,B2,B3}
```

B1 fails. Kafka elects an eligible replica:

```text
B2 = Leader
B3 = Follower

Now:

ISR = {B2,B3}
```

The partition remains available.

## 16. What Happens to Producers?

After leader election, producers must discover the new leader. Clients may initially receive errors such as:

`NOT_LEADER_OR_FOLLOWER` or related metadata errors.
The producer refreshes metadata and sends records to the new leader.

## 17. What Happens to Consumers?

Consumers also need to discover the new leader.
Fetch requests may temporarily fail while metadata changes propagate.
The consumer normally refreshes metadata and continues.
A well-designed Kafka client handles these transitions automatically.

## 18. Failure Scenario — Follower Dies

Suppose:

```text
B1 = Leader
B2 = Follower
B3 = Follower
```

B3 fails.

The leader remains **B1** and **ISR** becomes `{B1,B2}`

The partition can continue operating. However, Redundancy has decreased.

The administrator should restore the missing replica.

## 19. Failure Scenario — Two Brokers Die

Initial:

```text
RF = 3
ISR = {B1,B2,B3}

```

**B2** and **B3** fail. Only **B1** remains `ISR = {B1}` now `min.insync.replicas = 2` cannot be satisfied.

With `acks=all` new writes may fail. This is an important distinction **Existing leader availability** 
is not equivalent to **write availability**

## 20. Availability vs Write Availability

You can have **Partition leader exists** while **new writes fail** because `ISR < min.insync.replicas`

This is intentional protection against reducing durability too far.

## 21. Unclean Leader Election

Kafka can be configured regarding whether an **out-of-sync** replica may become leader when no **ISR** replica is available.

This is associated with `unclean.leader.election.enable`, the fundamental trade-off is `Availability vs Data consistency/durability`

## 22. Clean Leader Election

In a clean election, Kafka selects a suitable in-sync replica.

Example:

```text
ISR = {B1,B2}

B1 fails

B2 becomes leader
```

No stale replica is required.

## 23. Unclean Election

Suppose:

```text
B1 = leader
B2 = ISR follower
B3 = stale follower
```

Then:

```text
B1 fails
B2 also unavailable
```

Only **B3** remains, but **B3** is not in the **ISR**. If unclean election is allowed:

```text
B3
|
+--> becomes leader
```

This can restore availability but **_B3 may not contain the latest records_**.

## 24. Data Loss Scenario

Suppose:

```text
Leader B1:
m1
m2
m3
m4
```

Follower B3 has only:

```text
m1
m2
m3
```

If **B1** fails and **B3** becomes **leader** through an **unclean election**:

```text
B3:
m1
m2
m3
```

m4 may be lost from the active log. Thus `Availability ↑` and `Durability ↓`

## 25. Certification Principle

Remember:

>Unclean leader election favors availability over preserving the latest acknowledged data 
> when no in-sync replica is available.

This is a classic administrator exam concept.

## 26. Why Rack Awareness Matters

Replication is not useful enough if all replicas fail together.

Bad architecture:

```text
              Rack A
        +-------+-------+
        |       |       |
       B1      B2      B3
```

A rack failure can remove all replicas.

Better:

```text
       Rack A        Rack B        Rack C
        
        B1            B2            B3
         \             |            /
          \            |           /
              Partition replicas
```

## 27. Availability Zones

In cloud deployments, use independent availability zones.

Example:

```text
AZ-a       AZ-b       AZ-c

 B1         B2         B3
```

A single AZ failure should not remove all replicas.
This is the basic idea behind rack/AZ-aware replica placement.

## 28. Failure Domains

A good reliability architecture considers:

1. [x] Process failure
2. [x] Broker failure
3. [x] Host failure
4. [x] Rack failure
5. [x] AZ failure
6. [x] Region failure

Replication addresses some of these. Cross-region replication addresses larger failures.

## 29. Multi-AZ Architecture

A typical architecture:

```text
                 Kafka Cluster
                      |
       +--------------+--------------+
       |              |              |
      AZ-A           AZ-B           AZ-C
       |              |              |
      B1             B2             B3
       \              |             /
        +-------------+------------+
```

A partition's replicas should be distributed across failure domains where possible.

## 30. Controlled Shutdown

A planned broker shutdown is different from an unexpected failure.
During a controlled shutdown, Kafka can move leadership away from the broker before stopping it.

Conceptually:

```text
    Broker B1
        |
  planned shutdown
        |
        v
  Leadership moved
        |
        v
   Broker stops
```

This reduces unnecessary disruption.

## 31. Why Controlled Shutdown Matters

Without controlled shutdown:

```text
   Broker stops
        |
        v
  Leader elections
        |
        v
Client metadata changes
```

With controlled shutdown:

```text
Leadership transferred
        |
        v
    Broker stops
```

The second approach is generally smoother.

## 32. A rolling restart means:

```text
Restart broker 1
wait for recovery

Restart broker 2
wait for recovery

Restart broker 3
wait for recovery
```

rather than:

```text
Stop all brokers
Restart all brokers
```

The rolling approach helps preserve availability.

## 33. Rolling Upgrade

A rolling upgrade typically follows:

1. [x] Verify cluster health
2. [x] Upgrade one broker
3. [x] Wait for recovery
4. [x] Verify health
5. [x] Upgrade next broker
6. [x] Repeat

Never begin a rolling upgrade with `UnderReplicatedPartitions` already high
or another unresolved cluster-health problem.

## 34. Pre-Upgrade Checklist

Before a rolling upgrade:

1. [x] All brokers healthy
2. [x] No offline partitions
3. [x] ISR healthy
4. [x] Disk capacity sufficient
5. [x] Monitoring operational
6. [x] Rollback plan
7. [x] Client compatibility checked
8. [x] Configuration changes reviewed

## 35. Partition Reassignment

Kafka partitions can be reassigned between brokers.

Example:

```text
Before:

P0 -> B1,B2
P1 -> B1,B3

After:

P0 -> B2,B3
P1 -> B1,B3
```

This can be used for:

* rebalancing
* broker replacement
* capacity changes
* failure recovery

## 36. Why Reassignment Is Risky

Partition reassignment creates additional traffic. You can have
`normal workload + replication traffic + reassignment traffic`

This can saturate:

* network
* disk
* CPU

Therefore, reassignment should be monitored carefully.

## 37. Reassignment During Peak Traffic

Suppose `normal traffic = 700 MB/s` and reassignment adds `400 MB/s` Total `1.1 GB/s`

If the infrastructure supports only `1 GB/s` you have created a new bottleneck.

## 38. Broker Recovery

When a failed broker returns:

```text
      Broker returns
            |
            v
 Replicas begin catching up
            |
            v
       ISR expands
            |
            v
Replication health restored
```

Recovery should be monitored.

## 39. Recovery Metrics

Watch:

1. [x] UnderReplicatedPartitions
2. [x] ISR
3. [x] replication throughput
4. [x] disk I/O
5. [x] network traffic
6. [x] consumer latency
7. [x] producer latency

Recovery itself can affect production workloads.

## 40. Recovery Throttling

Kafka supports mechanisms to limit replication/reassignment traffic.
The objective is **Recovery speed vs Production workload impact**. Too little throttling:

```text
Recovery fast
Production performance suffers
```

Too much throttling:

```text
Production healthy
Recovery takes too long
```

## 41. Disaster Recovery

Disaster recovery addresses failures larger than an individual broker.

Examples:

* Region failure
* Datacenter failure
* Major network outage
* Corrupted cluster
* Operational mistake
* Security incident

A DR architecture needs 

1. backup/recovery strategy
2. replication strategy
3. failover procedure
4. testing

## 42. RPO

RPO means **Recovery Point Objective**, it answers **How much data can we afford to lose?**

Example ``RPO = 5 minutes`` means the organization may tolerate losing up to approximately five minutes of data, depending on the design.

## 43. RTO

RTO means **Recovery Time Objective**, it answers **How quickly must service be restored?**

Example `RTO = 30 minutes` means the target is to restore service within approximately 30 minutes.

## 44. RPO vs RTO

Memorize:

```text
RPO = data loss tolerance
RTO = downtime tolerance
```

Example:

```text
RPO = 1 minute
RTO = 10 minutes
```

means:

```text
Data loss target <= 1 minute
Recovery time target <= 10 minutes
```

## 45. Disaster Recovery Levels

Think in levels:

1. **Level 1**: Broker failure
2. **Level 2**: Rack/AZ failure
3. **Level 3**: Cluster failure
4. **Level 4**: Region failure

Each level requires progressively stronger recovery mechanisms.

## 46. Backup Is Not Replication

This is a critical distinction. Replication:

```text
Cluster A
B1 <--> B2 <--> B3
```

protects against some failures but if an operator accidentally deletes data: ``delete topic/data`` 
replication may reproduce the operational state across replicas.

Replication is not a replacement for appropriate backup/recovery mechanisms.

## 47. Disaster Recovery vs High Availability

### High availability

Keep the service running during failures.

###  Disaster recovery

Restore service/data after a major disaster.

Example **Multi-AZ Kafka** improves **HA**, **Cross-region replication** can improve **DR**.

## 48. MirrorMaker 2

Kafka provides MirrorMaker 2 for replicating data between Kafka clusters.

Conceptually:

```text
Cluster A
|
| MirrorMaker 2
v
Cluster B
```

It can be used for:

1. disaster recovery
2. migration
3. data distribution
4. geo-replication

### 48.1. MirrorMaker 2 Architecture

```text
+-------------------+
| Source Cluster    |
|                   |
| Topic A           |
| Topic B           |
+---------+---------+
          |
          v
   MirrorMaker 2
          |
          v
+-------------------+
| Target Cluster    |
|                   |
| A replicated      |
| B replicated      |
+-------------------+
```

## 49. Active-Passive DR

A simple architecture:

```text
                Primary
               Cluster A
                   |
                   |
              MirrorMaker 2
                   |
                   v
              Cluster B
              DR standby
```

Normal operation ``Clients -> Cluster A``, After disaster ```Clients -> Cluster B```

## 50. Active-Active Architecture

Another model:

```text
 Cluster A <----> Cluster B
    ^                ^
    |                |
 Clients A        Clients B
```

Both clusters serve traffic. This can reduce recovery time but is considerably more complex.

You must handle:

* duplicate data
* conflict resolution
* consumer offsets
* application semantics
* failback

## 51. Failover

A DR (Disaster Recovery) failover procedure should define:

1. Detect failure
2. Declare disaster
3. Stop/redirect producers
4. Select target cluster
5. Verify replicated data
6. Redirect consumers
7. Validate processing
8. Monitor

Do not improvise failover during a real disaster.

## 52. Failback

After the primary cluster is restored:

```text
 Primary restored
        |
        v
  Synchronize data
        |
        v
Validate consistency
        |
        v
Redirect workloads
```

Failback can be more difficult than failover. A DR plan must explicitly include it.

## 53. Consumer Offsets in DR

A critical question is **Where should consumers resume after switching clusters?**

You need a strategy for **consumer group offsets**

Cross-cluster replication can involve offset synchronization mechanisms, but the exact architecture and semantics depend on the MirrorMaker 2 setup.

This is an important operational design issue.

## 54. Duplicate Processing During DR

Suppose the consumer processed:

```text
m1
m2
m3
```

before failure. The DR system may not have exactly the same application-side state.

After failover, the consumer may process:

```text
m3
m4
m5
```

again.

Therefore, DR applications should often be designed with **idempotency** in mind.

## 55. Kafka DR and Exactly Once

Exactly-once semantics do not automatically solve every cross-cluster DR problem.

You must consider:

* Kafka transactions
* consumer offsets
* external systems
* cross-cluster replication
* failover

The end-to-end guarantee depends on the complete architecture.

## 56. Disaster Scenario — Region Failure

Architecture:

```text
    Region A
    Kafka Cluster A
        |
        | MirrorMaker 2
        v
    Region B
    Kafka Cluster B
```

Region A disappears. Applications fail over to **Region B**

But before doing so, verify:

* replication freshness
* consumer offsets
* application state
* network routing
* credentials

## 57. DR Data Lag

Suppose:

```text
Primary:
offset = 10,000,000

DR:
offset = 9,950,000
```

DR lag ``50,000 records``

Your actual achievable RPO depends on how quickly data is replicated and what the business considers recoverable.

This is why DR replication must itself be monitored.

## 58. Monitor MirrorMaker 2

1. [x] replication throughput
2. [x] replication lag
3. [x] connector/task health
4. [x] errors
5. [x] source/target connectivity
6. [x] offset synchronization

A DR architecture without DR monitoring is incomplete.

## 59. Disaster Recovery Testing

A DR plan that has never been tested is only a theory. Regularly test:

1. [x] failover
2. [x] consumer recovery
3. [x] producer recovery
4. [x] DNS/routing changes
5. [x] credentials
6. [x] offset behavior
7. [x] data integrity
8. [x] failback

## 60. Game Day

A useful reliability practice is a controlled failure exercise.

Example:

```text
Game Day

09:00
Stop Broker 2

09:05
Observe leader election

09:10
Check ISR

09:15
Check producer latency

09:20
Check consumer lag

09:30
Restore Broker 2

09:40
Verify ISR recovery
```

This validates assumptions.

## 61. Failure Injection

You can simulate:

* broker crash
* network partition
* disk pressure
* high CPU
* GC pauses
* AZ failure
* consumer crash
* producer failure

The goal is to understand how the system actually behaves.

## 62. Reliability Testing Pyramid

```text
                 DR test
                   /\
                  /  \
               AZ failure
                   /\
                  /  \
              Broker failure
                   /\
                  /  \
             Component tests
```

Test progressively larger failures.

## 63. Data Loss Scenarios

Data can become vulnerable through:

* acks=0
* acks=1
* RF=1
* low ISR
* unclean leader election
* operator mistakes
* disk failures
* region loss
* application bugs

No single parameter eliminates every risk.

## 64. Reliability Anti-Pattern

Bad:

```text
RF = 1
acks = 1
unclean election enabled
single AZ
no DR
```

This provides limited failure protection.

## 65. Better Production Pattern

A more resilient design might use:

```text
RF = 3
min.insync.replicas = 2
acks = all
```

combined with:

1. [x] multi-AZ placement
2. [x] monitoring
3. [x] controlled operations
4. [x] DR replication
5. [x] tested recovery

Exact values depend on workload and operational requirements.

## 66. Reliability Is a System Property

Kafka configuration alone is not enough.

```text
Kafka
|
+-- producer configuration
+-- broker configuration
+-- replica placement
+-- storage
+-- network
+-- monitoring
+-- application behavior
+-- DR
```

Reliability emerges from the entire system.

## 67. Developer Responsibilities

Developers should:

1. [x] use appropriate acks
2. [x] enable idempotence where appropriate
3. [x] handle retries correctly
4. [x] design idempotent consumers
5. [x] handle serialization errors
6. [x] monitor consumer lag
7. [x] handle rebalances
8. [x] avoid blocking poll loops

## 68. Administrator Responsibilities

Administrators should:

1. [x] maintain replication
2. [x] monitor ISR
3. [x] maintain broker health
4. [x] balance partitions
5. [x] manage disk capacity
6. [x] maintain AZ/rack distribution
7. [x] perform safe upgrades
8. [x] test recovery
9. [x] maintain DR

## 69. Reliability Checklist
### Producer

1. [x] Appropriate acks
2. [x] Idempotence where appropriate
3. [x] Retry policy
4. [x] Error handling
5. [x] Timeout configuration

### Topic

1. [x] Appropriate replication factor
2. [x] Appropriate min.insync.replicas
3. [x] Correct partition placement
4. [x] Appropriate retention

### Broker

1. [x] Disk capacity
2. [x] Disk performance
3. [x] Network capacity
4. [x] JVM health
5. [x] Monitoring

### Cluster

1. [x] ISR healthy
2. [x] No offline partitions
3. [x] Leader distribution
4. [x] AZ/rack distribution
5. [x] Recovery procedures

### DR

1. [x] RPO defined
2. [x] RTO defined
3. [x] Cross-cluster replication if required
4. [x] Offset strategy
5. [x] Failover procedure
6. [x] Failback procedure
7. [x] Regular DR tests

## 70. Certification Scenario — acks and ISR

Configuration:

```text
RF = 3
min.insync.replicas = 2
acks = all

```
Current ISR:

```text
B1
B2
B3
```

B3 fails. **What happens?**

<details>
<summary>Answer</summary>
ISR becomes:

B1
B2

Writes using acks=all can still succeed because:

```text
ISR = 2
min.insync.replicas = 2
```

But redundancy is degraded.
</details>

## 71. Certification Scenario — Second Failure

Starting from:

```text
ISR = {B1,B2}
min.insync.replicas = 2
```

B2 fails.

Now:

```text
ISR = {B1}
```

**What happens to writes requiring the minimum ISR?**

<details>
<summary>Answer</summary>
They fail because ``1 < 2``

This protects durability at the cost of write availability.
</details>

## 72. Certification Scenario — Unclean Election

Question: **Why might an administrator enable unclean leader election?**

<details>
<summary>Answer</summary>
To improve availability when no in-sync replica is available. But the trade-off is:

``higher availability + potential data loss``

This should be a deliberate business decision.
</details>

## 73. Certification Scenario — Rack Awareness

Question: **Why distribute replicas across racks/AZs?**
<details>
<summary>Answer</summary>
To prevent a single failure domain from taking all replicas offline.

Example:

Bad:

```text
AZ-A:
B1
B2
B3
```

Good:

```text
AZ-A:
B1

AZ-B:
B2

AZ-C:
B3
```
</details>

## 74. Certification Scenario — Rolling Upgrade

Question: **What should you check before restarting a broker during a rolling upgrade?**

<details>
<summary>Answer</summary>
At minimum:

1. [x] cluster health
2. [x] ISR health
3. [x] offline partitions
4. [x] capacity

Do not intentionally introduce another failure into an already degraded cluster.
</details>

## 75. Certification Scenario — Recovery

A broker returns after several hours. Metrics show ``UnderReplicatedPartitions = 500`` 

Question: **What should you do?**

<details>
<summary>Answer</summary>
Monitor:

1. [x] recovery progress
2. [x] network
3. [x] disk
4. [x] ISR expansion
5. [x] production latency

Do not assume that broker availability immediately means full cluster recovery.
</details>

## 76. Certification Scenario — DR

Your business requirement is:

````text
RPO = 5 minutes
RTO = 15 minutes

````

Your cross-region replication regularly falls ``20 minutes behind``. **Is the DR design meeting the requirement?**

<details>
<summary>Answer</summary>
**No**. The replication system itself violates the desired RPO.
</details>

## 77. Certification Scenario — Backup

Question: **Is replication sufficient protection against accidental deletion?**

<details>
<summary>Answer</summary>

**No**. Replication can propagate the same logical state to multiple replicas.

You need an appropriate recovery mechanism for operational mistakes.
</details>

## 78. Certification Scenario — Consumer Recovery

A consumer processed:

```text
m1
m2
m3
```

and crashed before committing the relevant offset. After restart, it may process `m3` again.

Therefore, `at-least-once` processing can produce duplicates. 
Applications should be designed accordingly.

## 79. Reliability and Idempotency

A reliable Kafka application often combines `Kafka reliability + consumer idempotency`

Example `eventId = 12345` consumer stores **processed event IDs**. If event **12345** arrives again:

```text
 already processed
        |
        v
 ignore duplicate
```

## 80. End-to-End Reliability

Consider:

```text
 Producer
    |
    v
  Kafka
    |
    v
 Consumer
    |
    v
 Database
```

Kafka may guarantee **durable Kafka record** but your application must still ensure **database update** is handled correctly.

Otherwise:

```text
Kafka success
Database failure
```

can still produce inconsistent business state.

## 81. Transactional Outbox Connection

A transactional outbox can help bridge `Database transaction + Kafka publishing`

Conceptually:

```text
  Application
      |
      +--> DB transaction
      |       |
      |       +--> business data
      |       +--> outbox event
      |
      v
Outbox Publisher
      |
      v
    Kafka
```

This is an application-level reliability pattern.

## 82. Exactly-Once Is Not Disaster Recovery

`Exactly-once` semantics help with processing guarantees within supported Kafka transactional boundaries.

They do not automatically solve:

1. [x] region failure
2. [x] operator deletion
3. [x] network disaster
4. [x] DR failover
5. [x] external database consistency

This distinction is important in senior-level interviews and certification preparation.

## 83. Reliability Architecture

A mature Kafka architecture might look like:

```text
                    REGION A
             +-------------------+
             |                   |
             |   Kafka Cluster   |
             |                   |
             |   B1   B2   B3    |
             |   |    |    |     |
             +-------------------+
                       |
                  MirrorMaker 2
                       |
                       v
                    REGION B
             +-------------------+
             |                   |
             |   Kafka Cluster   |
             |                   |
             |   B1   B2   B3+    |
             |                   |
             +-------------------+
```

Within each region:

```text
RF=3
multi-AZ
min.insync.replicas=2
acks=all
```
when appropriate for the workload.

## 84. The Reliability Hierarchy

Think about reliability in this order:

```text

                    Reliability
                         |
          +--------------+--------------+
          |              |              |
      Replication      Failure       Recovery
                       handling
          |              |              |
       RF / ISR       Elections       DR
       minISR         Shutdown        Backup
       AZ/rack        Upgrade         Failover
          |
          v
      Durability
```

## 85. The Most Important Relationships

Memorize these:

1. [x] **RF** = number of replicas
2. [x] **ISR** = replicas currently considered in sync
3. [x] `min.insync.replicas` = minimum ISR required for appropriate acknowledged writes
4. [x] `acks=all` = producer waits for the required in-sync acknowledgments
5. [x] **unclean election** = availability at possible durability/data-loss cost
6. [x] **RPO** = acceptable data-loss window
7. [x] **RTO** = acceptable recovery-time window

## 86. The Golden Reliability Configuration

For certification questions, remember this common pattern:

```text
replication.factor = 3
min.insync.replicas = 2
acks = all
```

with:

1. [ ] replicas distributed across failure domains
2. [ ] monitoring
3. [ ] controlled maintenance
4. [ ] tested DR

But remember: **This is a common reliability pattern, not a universal configuration that is automatically correct for every workload.**

## 87. Cheat Sheet

>**RF** = number of replicas

>**ISR** = replicas currently in sync

>acks=0 → no acknowledgment

>acks=1 → leader acknowledgment

>acks=all → strongest producer acknowledgment mode

>minISR=2 → require at least 2 ISR replicas for appropriate writes

>RF=3 + minISR=2 + acks=all → strong common durability configuration

```text
unclean election
→ availability ↑
→ potential data loss ↑
```

>rack/AZ awareness → protects against shared failure domains

>controlled shutdown → graceful leadership movement

>rolling upgrade → one broker at a time

>RPO → acceptable data loss

>RTO → acceptable recovery time

>MirrorMaker 2→ cross-cluster replication / DR / migration

>Replication != Backup

## 88. Mental Model

If you remember only one diagram from this chapter, remember this:

```text
                         KAFKA RELIABILITY
                                |
          +---------------------+---------------------+
          |                     |                     |
     REPLICATION            AVAILABILITY           RECOVERY
          |                     |                     |
       RF / ISR              Elections              Broker
       minISR                Leaders                Reassignment
       acks                  AZ/Rack                Rolling upgrade
          |                     |                     |
          +---------------------+---------------------+
                                |
                         DISASTER RECOVERY
                                |
                  +-------------+-------------+
                  |                           |
              RPO / RTO                 Cross-cluster
                                            |
                                      MirrorMaker 2
                                            |
                                      Failover/Failback
```

The key principle is:

> Kafka reliability is the combination of replication, correct acknowledgment semantics, 
 failure-domain-aware placement, safe operations, monitoring, and tested recovery procedures.

