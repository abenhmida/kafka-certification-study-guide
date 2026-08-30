# Chapter 13 — Kafka Monitoring, Metrics & Troubleshooting Deep Dive

```text
Certification focus: This chapter covers Kafka observability, JMX metrics, broker/producer/consumer metrics, 
consumer lag, replication health, JVM monitoring, request latency, 
disk and network monitoring, alerting, troubleshooting methodology, Prometheus/Grafana concepts, a
nd certification-style incident scenarios.
```

## 1. Learning Objectives

By the end of this chapter, you should be able to:

1. Explain Kafka's monitoring model.
2. Understand JMX and Kafka metrics.
3. Identify the most important broker metrics.
4. Monitor producer health.
5. Monitor consumer health.
6. Explain consumer lag.
7. Diagnose under-replicated partitions.
8. Diagnose offline partitions.
9. Monitor ISR changes.
10. Monitor controller health.
11. Understand request latency.
12. Monitor network and request-handler utilization.
13. Monitor disk and filesystem performance.
14. Monitor JVM heap and garbage collection.
15. Design Kafka alerts.
16. Distinguish symptoms from root causes.
17. Troubleshoot Kafka systematically.
18. Interpret common Kafka incidents.
19. Prepare for Kafka administrator certification questions.

## 2. Why Kafka Monitoring Matters

Kafka is a distributed system. A cluster can appear healthy from one perspective while being unhealthy from another.

For example:

```text
Producer
   |
   | healthy
   v
Broker
   |
   | healthy
   v
Consumer
   |
   | slow
   v
Application
```

Kafka itself may be operating correctly while consumer lag continuously increases. Therefore, Kafka monitoring must
cover the entire pipeline:

```text
Producer
   |
   v
Kafka Broker
   |
   +--> Replicas
   |
   v
Consumer
   |
   v
Application
```

## 3. The Four Observability Signals

Kafka monitoring should consider:

* Metrics
* Logs
* Traces
* Events/Alerts

1. **Metrics**: Tells what is happening?

* CPU
* throughput
* latency
* consumer lag
* ISR count

1. **Logs**: Tells what did Kafka report?

* leader election
* authentication failure
* replication error
* disk failure

1. **Traces**: Can tell how did an operation propagate through the system? this is especially useful when Kafka is part
   of a larger distributed architecture.
2. **Alerts**: Tell operators that something requires attention.

## 4. Kafka Monitoring Architecture

A typical monitoring architecture looks like:

```text
                         Kafka Cluster
                              |
                 +------------+------------+
                 |            |            |
              Broker 1     Broker 2     Broker 3
                 |            |            |
                 +------------+------------+
                              |
                             JMX
                              |
                              v
                    Metrics Collection
                              |
                    +---------+---------+
                    |                   |
               Prometheus          Other system
                    |
                    v
                 Grafana
                    |
                    v
               Dashboards
                    |
                    v
                 Alerts
```

The exact monitoring stack can vary.

## 5. JMX

Kafka exposes many metrics through: Java Management Extensions — JMX Kafka runs on the JVM, and JMX exposes runtime
metrics.

```text
Kafka JVM
   |
   +-- Broker metrics
   +-- Producer metrics
   +-- Consumer metrics
   +-- JVM metrics
   +-- Request metrics
```

Monitoring systems can collect these metrics.

## 6. Metric Names

Kafka metrics commonly follow patterns involving:

1. [ ] type
2. [ ] name
3. [ ] topic
4. [ ] partition
5. [ ] client-id
6. [ ] broker-id

For example, metrics can describe:

1. [ ] BytesIn
2. [ ] BytesOut
3. [ ] MessagesIn
4. [ ] RequestLatency
5. [ ] UnderReplicatedPartitions

The exact metric names and tags can differ by Kafka version and monitoring integration.

For certification preparation, understand the meaning of the metrics rather than relying exclusively on memorized JMX
object names.

## 7. Monitoring Layers

A useful monitoring hierarchy is:

```text
                    Kafka Monitoring
                          |
       +------------------+------------------+
       |                  |                  |
     Broker            Producer           Consumer
       |                  |                  |
       v                  v                  v
 replication          send rate          fetch rate
 requests             errors             lag
 disk                  latency            poll
 network               batching          commits
 JVM                   buffer            rebalances
```

Then monitor the infrastructure underneath:

* CPU
* Memory
* Disk
* Network
* Filesystem
* JVM

## 8. Broker Monitoring

Important broker categories include:

* Traffic
* Requests
* Replication
* Partitions
* Network
* Disk
* JVM
* Controller

A healthy dashboard should make these visible.

## 9. Bytes In

`BytesIn` represents incoming data traffic to brokers.

```text
Producers
   |
   | 500 MB/s
   v
Broker
```

If ``BytesIn`` suddenly increases

```text
normal = 200 MB/s
current = 800 MB/s
```

investigate:

* producer traffic increase
* deployment changes
* unexpected producer
* retry storm
* traffic spike

## 10. Bytes Out

`BytesOut` represents data leaving brokers. Traffic can come from:

* Consumers
* Inter-broker communication
* Other Kafka operations

A sudden increase may indicate:

* consumer traffic increase
* replication activity
* data movement

## 11. Messages In

Monitoring record rate helps distinguish ``bytes/sec`` from `records/sec`, for example `100,000 records/sec` could
represent
``100 MB/sec`` if records average `1 KB` but `1 GB/sec` if records average `10 KB`. Always monitor both records and
bytes when relevant.

## 12. Request Rate

Kafka brokers process different request types:

* Produce
* Fetch
* Metadata
* Offset
* JoinGroup
* SyncGroup
* Heartbeat
* ApiVersions

Monitoring request rates helps identify workload changes. For example
``Produce requests ↑`` may indicate producer traffic growth while ```Metadata requests ↑↑↑``` may indicate client churn
or inefficient client behavior.

## 13. Request Latency

Request latency is a key health indicator.

```text
Client
  |
  | request
  v
Broker
  |
  | processing
  v
response
```

Monitor

```text
average
p95
p99
```

rather than only averages.

## 14. Why Percentiles Matter

Suppose ``Average = 10 ms`` but `p99 = 1,000 ms`, the average hides a serious tail-latency problem. A production
dashboard should often expose ``p50``, `p95`, `p99` for important request classes.

## 15. Request Handler Utilization

Kafka brokers use request-handling threads to process requests. A useful metric concept is
``RequestHandlerAvgIdlePercent``. High idle percentage generally means **request handler capacity available**
and low idle percentage suggests **request handlers are heavily utilized**

## 16. Network Processor Utilization

Kafka also uses network processors. A metric conceptually representing network processor idle time can help identify
network-thread saturation. if ``network processor idle ≈ very low`` then the broker may be struggling with network
request processing.

## 17. CPU vs Request Handler Saturation

Do not assume ``CPU = 90%`` s always the cause. You should correlate
`CPU + request latency + request-handler utilization + network processor utilization`. A broker can have high CPU
because of:

* compression
* TLS
* GC
* other processes

rather than request handling.

## 18. Replication is one of the most important administrator concerns

Monitor

* ISR
* UnderReplicatedPartitions
* OfflinePartitionsCount
* leader elections
* replication latency

## 18.1. Under-Replicated Partitions

An under-replicated partition is a major warning sign.

```text
RF = 3

Expected:
Leader + Follower 1 + Follower 2

Actual ISR:
Leader + Follower 1
```

One replica has fallen behind. ``UnderReplicatedPartitions > 0`` requires investigation.

## 18.2. Is Under-Replicated Always an Incident?

Not necessarily. During

* controlled maintenance
* broker restart
* partition reassignment

temporary under-replication can occur but **Persistent under-replication is a serious problem.**
The key is to monitor:

* duration
* trend
* affected partitions
* affected brokers

## 18.3. Offline Partitions

An offline partition is much more serious. It means there is no available leader for the partition.

```text
Partition 5

Leader = unavailable
Followers = unavailable or unable to become leader
```

This can make the partition unavailable to clients. An alert such as ``OfflinePartitionsCount > 0`` should normally be
treated as critical.

## 18.4. ISR Shrink Rate

Monitoring only the current ISR size is insufficient. You should also monitor

* ISR shrink events
* ISR expansion events

Frequent shrinking and expansion may indicate:

* disk problems
* network instability
* broker overload
* GC pauses

## 18.5. Example ISR Incident

Suppose

```text
RF = 3

ISR:
Broker 1
Broker 2
Broker 3
```

Then

```text
Broker 3
   |
   v
falls behind
```

ISR becomes

```text
Broker 1
Broker 2
```

If this happens frequently:

```text
ISR shrink
ISR expand
ISR shrink
ISR expand
```

you may have an unstable broker.

## 19. Controller Monitoring

Kafka has controller responsibilities for cluster metadata and coordination. Depending on Kafka architecture/version,
the implementation details differ, but operators still need to monitor:

* controller health
* controller changes
* metadata propagation
* leader elections

Frequent controller changes are not normal steady-state behavior.

## 20. Controller Changes

A controller transition may occur because of:

* broker failure
* controller failure
* network problems
* cluster instability

Frequent changes indicate instability. Monitor:

* controller count/state
* controller changes
* leader elections

## 21. Leader Elections

Kafka partitions have leaders. If leaders continuously change ``leader election rate ↑``. investigate:

* broker failures
* network instability
* controller instability
* resource exhaustion

Frequent leader elections can affect latency and availability.

## 22. Preferred Leader Balance

Kafka can distribute leadership across brokers. A poorly balanced cluster may have:

```text
Broker 1:
80% of leaders

Broker 2:
10%

Broker 3:
10%
```

This can create:

```text
Broker 1
   |
   +-- high network
   +-- high CPU
   +-- high disk
```

while other brokers are underutilized.

## 23. Broker Balance

Monitor per-broker:

* BytesIn
* BytesOut
* CPU
* disk usage
* partition count
* leader count
* request rate

A healthy cluster should generally avoid extreme imbalance.

## 24. Consumer Monitoring

Consumer monitoring should cover:

* lag
* records consumed
* fetch latency
* poll behavior
* commit rate
* rebalance rate
* errors
* processing latency

### 24.1. Consumer Lag

Consumer lag can be thought of as ``latest available offset - consumer's committed/processed position``

```text
Latest offset = 1,000,000
Consumer position = 999,500

Lag = 500
```

### 24.2. Lag Per Partition

Do not monitor only total lag.

```text
Partition 0 = 10
Partition 1 = 15
Partition 2 = 20
Partition 3 = 500,000
```

Total lag ``500,045``. The important information is **Partition 3 is the problem**. This could indicate

* hot partition
* slow consumer assignment
* processing imbalance

### 24.3. Maximum Lag

records-lag-max or equivalent monitoring is often particularly useful because ``total lag`` can hide
`one severely delayed partition`
while maximum lag exposes the worst case.

### 24.4. Consumer Lag Trend

The trend matters more than a single measurement. Healthy:

```text
100
120
110
105
100
```

Concerning:

```text
100
500
2,000
10,000
50,000
```

The second indicates that consumption is falling behind.

### 24.5. Consumer Lag Recovery

Suppose

```text
Incoming = 100 MB/s
Consumer = 80 MB/s
```

Lag grows. You increase consumers and reach ``Consumer = 150 MB/s``. Now lag should eventually decline. This is a good
example of measuring
``lag growth rate`` rather than just lag value.

### 24.6. Consumer Rebalances

Frequent consumer-group rebalances can hurt throughput. Monitor:

* rebalance rate
* rebalance duration
* consumer membership changes

Possible causes:

* consumer crashes
* slow processing
* max.poll.interval.ms exceeded
* network instability
* deployment churn

### 24.7. Rebalance Storm

A rebalance storm looks like:

```text
consumer joins
   |
rebalance
   |
consumer leaves
   |
rebalance
   |
consumer joins
   |
rebalance
   |
...
```

Consequences:

* processing interruptions
* lag growth
* duplicate processing
* throughput reduction

### 24.8. Consumer Poll Monitoring

The consumer should call ``poll()`` regularly. If application processing takes too long:

```text
poll
 |
 +---- long processing
 |
 v
poll delayed
```

and ``max.poll.interval.ms`` may be exceeded.

## 25. Producer Monitoring

Monitor:

* record-send-rate
* record-error-rate
* request-rate
* request-latency
* batch-size
* compression-rate
* buffer availability
* retry rate

### 25.1. Producer Error Rate

A sudden increase in producer errors may indicate:

* broker unavailable
* authorization failure
* record too large
* timeout
* metadata issue
* insufficient ISR
* network problem

The metric tells you **that** something is wrong. Logs and error codes often tell you **what**.

### 25.2. Producer Retries

Retries can be healthy in small amounts.

```text
temporary network issue
      |
      v
    retry
      |
      v
    success
```

But sustained high retries indicate a problem. Potential causes:

* broker overload
* network instability
* timeouts
* leader movement

### 25.3. Producer Request Latency

Suppose

```text
producer latency
10 ms -> 20 ms -> 100 ms -> 500 ms
```

while ``producer throughput ↓``. Investigate the broker and network. Potential causes:

* disk
* CPU
* network
* replication
* request queues

### 25.4. Producer Buffer Exhaustion

If ``buffer.available.bytes`` falls toward zero repeatedly, producers may block. Possible root cause **producer
generating records faster than Kafka can accept them**

Potential solutions include:

* increase throughput
* improve batching
* increase broker capacity
* add partitions
* reduce compression CPU pressure

But increasing buffer memory alone may only delay the problem.

## 26. VM Monitoring

Kafka is a JVM application. Monitor:

* heap used
* heap committed
* GC frequency
* GC pause time
* non-heap memory
* threads
* CPU

### 26.1. Heap Utilization

Suppose

```text
Heap = 16 GB
Used = 15.5 GB
```

Persistent high utilization deserves investigation. Possible causes:

* heap too small
* high metadata load
* too many partitions
* application/plugin behavior
* memory leak

### 26.2. GC Monitoring

Watch:

* GC count
* GC pause duration
* allocation rate
* old-generation occupancy

Long GC pauses can cause:

* request latency
* heartbeat delays
* replication delays
* consumer lag

### 26.3. GC and ISR

Suppose Broker 2 experiences ``20-second GC pause``, during the pause:

```text
Broker 2
   |
   X
cannot process replication normally
```

Followers may fall behind, result ``ISR shrinks`` thus `GC problem -> replication problem -> ISR problem`

### 26.4. JVM Heap vs Page Cache

Remember the relationship from Chapter 11:

```text
Physical RAM
 |
 +-- JVM heap
 |
 +-- OS page cache
 |
 +-- native memory
```

Over-allocating JVM heap can reduce page cache. This can increase disk reads and potentially hurt Kafka performance.

## 27. Disk Monitoring

Monitor:

* disk utilization
* IOPS
* throughput
* latency
* queue depth
* filesystem usage

### 27.1. Disk Space Alert

A Kafka broker should not be allowed to run until ``disk = 100%``. At that point the broker may already have severe
problems. Use warning and critical thresholds. Example conceptual policy:

```text
70% -> warning
80% -> investigate
90% -> critical planning
```

Exact thresholds depend on environment.

### 27.2. Disk Latency

High disk latency can produce:

* write latency
* replication lag
* consumer fetch latency
* request latency

A broker can have ``disk capacity = 70%`` but still be unhealthy because `disk latency = extremely high`. Capacity and
performance are different dimensions.

### 27.3. Disk Throughput

Monitor ``MB/s`` and compare against the hardware's expected capability, if the workload requires `500 MB/s`
but the disk can sustain only ``200 MB/s`` the disk is a likely bottleneck.

## 28. Filesystem Monitoring

Kafka stores `partition logs`, `segments`, `indexes` on disk. So monitor:

* filesystem capacity
* inode usage where relevant
* mount availability
* disk errors

A failed mount can be catastrophic.

## 29. Network Monitoring

Monitor:

* network ingress
* network egress
* packet errors
* connection counts
* network saturation

Also distinguish ``client traffic`` from `inter-broker traffic`.

### 29.1. Network Saturation Example

Suppose ``Network interface = 10 Gbps`` and traffic reaches `9.8 Gbps`. You have very little headroom for:

* replication bursts
* recovery
* consumer spikes
* administrative operations

This can create instability even before the theoretical maximum is reached.

## 30. Monitoring Replication During Recovery

Suppose a broker fails. A replacement broker must catch up:

```text
Leader
   |
   +--> Recovery replica
```

During recovery:

```text
network ↑
disk ↑
```

Potentially affecting normal workloads. Therefore, recovery capacity must be included in monitoring and capacity
planning.

## 31. Alerting Philosophy

Avoid alerting on every metric.

**Good alerts should be**:

* actionable
* meaningful
* stable

**Bad alert**:
`CPU > 70%` by itself.

**Better**:

```text
CPU > 85%
AND
request latency elevated
AND
condition persists for 10 minutes
```

## 32. Critical Kafka Alerts

A strong baseline includes alerts for:

* OfflinePartitionsCount > 0
* UnderReplicatedPartitions > 0 for sustained period
* consumer lag above SLA
* disk space critically low
* broker unavailable
* controller instability
* authentication failure spikes
* producer error spikes
* consumer rebalance storms

## 33. Warning vs Critical

Example:

```text
Consumer lag
   |
   +-- 10,000 -> warning
   |
   +-- 100,000 -> critical
```

But the threshold should be based on business requirements. For some workloads ``lag = 10,000`` may be insignificant.
For a low-latency trading-like system, even ``lag = 100`` could be unacceptable.

## 34. Alert on Rate, Not Just Value

Consider `consumer lag = 10,000` is that bad?. If the system normally has `lag = 10,000` and is stable, it may be fine
but `lag growth = +5,000/sec` is clearly concerning. Therefore, monitor `absolute lag +lag growth rate`.

## 35. Prometheus

A common monitoring architecture uses Prometheus to collect metrics.

```text
      Kafka
       |
       v
  JMX / Exporter
       |
       v
   Prometheus
       |
       v
Time-series database
```

Prometheus can scrape exposed metrics.

## 36. Grafana can visualize metrics

```text
Prometheus
|
v
Grafana
|
+-- Broker dashboard
+-- Producer dashboard
+-- Consumer dashboard
+-- JVM dashboard
+-- Replication dashboard 
```

## 37. Recommended Dashboards

### 37.1. **Cluster overview**

* broker count
* controller state
* offline partitions
* under-replicated partitions
* bytes in/out

### 37.2. **Broker dashboard**

* CPU
* heap
* GC
* disk
* network
* request latency
* request handler utilization

### 37.3. **Consumer dashboard**

* lag
* records consumed
* fetch latency
* rebalances
* processing time

### 37.4. **Producer dashboard**

* send rate
* errors
* retries
* latency
* batch size
* compression
* buffer utilization

## 38. Cluster Health Dashboard

A simple conceptual dashboard:

```text
+------------------------------------------------+
|           Kafka Cluster Health                 |
+------------------------------------------------+
| Brokers:              3 / 3                    |
| Offline Partitions:   0                        |
| Under Replicated:     0                        |
| Controller Changes:   0                        |
+------------------------------------------------+
| Bytes In       450 MB/s                        |
| Bytes Out      1.2 GB/s                        |
+------------------------------------------------+
| CPU       B1 45%   B2 52%   B3 48%             |
| Disk      B1 60%   B2 55%   B3 63%             |
+------------------------------------------------+
```

## 39. Troubleshooting Methodology

When Kafka has a problem:

1. Define the symptom
2. Determine scope
3. Determine start time
4. Check recent changes
5. Check cluster health
6. Check broker health
7. Check replication
8. Check producer
9. Check consumer
10. Correlate metrics and logs
11. Identify root cause
12. Apply controlled remediation
13. Verify recovery

### 39.1. Step 1 — Define the Symptom

Bad:

```text
Kafka is slow.
```

Good:

```text
Producer p99 latency increased from 20 ms to 500 ms at 09:15.
```

This immediately narrows the investigation.

### 39.2. Step 2 — Determine Scope

Ask:

```text
One producer?
All producers?

One topic?
All topics?

One broker?
All brokers?

One consumer group?
All consumer groups?
```

Scope is extremely valuable.

### 39.3. One Topic vs All Topics

If:

```text
Topic A = healthy
Topic B = lagging
Topic C = healthy
```

the issue may be:

```text
topic-specific
partition-specific
consumer-specific
```

If all topics are affected, investigate:

```text
cluster
network
storage
infrastructure
```

first.

### 39.4. One Broker vs All Brokers

Suppose:

```text
Broker 1 = high disk latency
Broker 2 = normal
Broker 3 = normal
```

This strongly suggests `broker-specific problem` rather than a cluster-wide Kafka problem.

### 39.5. Check Recent Changes

Many incidents are caused by:

* deployment
* configuration change
* topic change
* partition reassignment
* network change
* certificate rotation
* firewall change
* broker restart
* client upgrade

Always ask **What changed immediately before the incident?**

### 39.6. Logs + Metrics

Metrics tell you `WHAT` and logs often help explain `WHY`

Example:

```text
Metric:
ISR shrinking

Log:
Follower fetch timeout

Infrastructure:
disk latency high
```

Together these point toward a disk/storage problem.

### 39.7. Troubleshooting Scenario — Producer Timeout

Symptoms:

```text
producer latency ↑
timeouts ↑
```

Investigation:

* Broker CPU normal
* Disk normal
* Network 99%

Likely problem `network saturation`, do not immediately increase `request.timeout.ms`

That might hide the symptom rather than solve the cause.

### 39.8. Troubleshooting Scenario — Consumer Lag

Symptoms `lag ↑`, check:

* producer rate
* consumer rate
* partition distribution
* consumer CPU
* processing latency
* rebalance rate
* broker health

If `consumer processing time ↑` the application is likely the bottleneck.

### 39.9. Troubleshooting Scenario — ISR Shrinking

Symptoms:

```text
UnderReplicatedPartitions > 0
ISR shrinking
```

Check:

* disk latency
* network
* CPU
* GC
* broker logs
* follower fetch performance

### 39.10. Troubleshooting Scenario — Offline Partition

Symptoms `OfflinePartitionsCount > 0` Priority `CRITICAL`

Investigate:

* broker availability
* leader election
* controller
* replica availability
* network

This is an availability incident rather than merely a performance warning.

### 39.11. Troubleshooting Scenario — Frequent Rebalances

Symptoms:

```text
consumer group
rebalance
rebalance
rebalance
```

Check:

* consumer crashes
* max.poll.interval.ms
* processing duration
* heartbeat/session behavior
* network connectivity
* deployment churn

### 39.12. Troubleshooting Scenario — High CPU

High CPU alone is insufficient. Break it down:

```text
CPU ↑
|
+--> compression?
+--> TLS?
+--> GC?
+--> request handling?
+--> replication?
+--> partition overhead?
+--> other processes?
```

Then correlate with Kafka metrics.

### 39.13. Troubleshooting Scenario — High Memory

Check:

* JVM heap
* GC
* partition count
* metadata
* page cache
* off-heap/native memory

Remember `high RAM usage` does not automatically mean `memory leak`. The operating system may intentionally use RAM for
page cache.

### 39.14. Troubleshooting Scenario — High Disk Usage

Distinguish `disk capacity` from `disk performance`

Example `Disk = 80% full` doesn't necessarily mean disk I/O is the bottleneck. Conversely `Disk = 40% full` can still
have `very high I/O latency`

### 39.15. Troubleshooting Scenario — High Network

Determine whether the traffic is:

* producer ingress
* consumer egress
* replication
* recovery

Then identify which workload changed.

### 39.16. Troubleshooting Scenario — One Hot Partition

Symptoms:

```text
Partition 7:
900 MB/s

Partition 0-6:
50 MB/s
```

Possible cause `partition key skew` investigate `producer key distribution` before changing broker configurations.

### 39.17. Troubleshooting Decision Tree

```text
                    Kafka Problem
                         |
               +---------+---------+
               |                   |
          Availability?        Performance?
               |                   |
          +----+----+         +----+----+
          |         |         |         |
       Offline    ISR       Latency    Lag
      partitions  shrink      |         |
          |         |         |         |
       Broker?    Disk?     Broker?   Consumer?
                  Network?  Network?  Processing?
                  CPU?      Disk?     Partitions?
                  GC?       CPU?
```

This is a useful mental model for certification questions.

## 40. Root Cause vs Symptom

Example `Consumer lag ↑` is a symptom. Potential root cause `consumer processing latency ↑`. Another
`UnderReplicatedPartitions ↑` is a symptom. Potential root cause `follower disk latency ↑`.

**Do not stop at the first metric that changed.**

## 41. Five Whys Example

1. [x] **Problem**: Consumer lag increased.
2. [x] **Why?**: Consumer processing slowed.
3. [x] **Why?**: Database calls became slower.
4. [x] **Why?**: Database connection pool became saturated.
5. [x] **Why?**: A deployment increased the number of records processed per event.

**Root cause**: Application deployment changed downstream workload. Kafka was not necessarily the root cause.

## 42. Correlation Example

Suppose you see:

```text
09:00
CPU normal
ISR healthy
lag stable

09:15
deployment

09:17
consumer processing latency ↑

09:20
consumer lag ↑
```

The temporal relationship strongly suggests investigating the deployment.

## 43. Monitoring During Broker Failure

Suppose Broker 2 fails.

Watch:

* OfflinePartitions
* UnderReplicatedPartitions
* Leader elections
* ISR
* Network
* Disk
* Consumer lag
* Producer errors

Expected behavior:

```text
 Broker failure
      |
      v
Leader elections
      |
      v
  ISR changes
      |
      v
   Recovery
      |
      v
 ISR restored
```

## 44. Healthy Recovery

After a broker failure `UnderReplicatedPartitions` may temporarily increase.

The important question is **Does the cluster recover to the expected state?**

Healthy:

```text
0
100
500
200
50
0
```

Unhealthy:

```text
0
100
500
1000
2000
3000

```

The second indicates worsening replication health.

## 45. Alert Fatigue

If operators receive `1000 Kafka alerts/day` they will eventually ignore them.

Prefer:

* few
* high-value
* actionable
* alerts

Examples:

* offline partitions
* persistent under-replication
* critical disk space
* consumer SLA breach
* broker unavailable

## 46. Alert Design

A good alert includes:

1. [x] What happened?
2. [x] Where?
3. [x] When?
4. [x] How severe?
5. [x] What should the operator check?

Example:

```text
CRITICAL:
Topic orders has 3 offline partitions
for more than 2 minutes.
```

Check

1. [x] broker availability.
2. [x] leader election state.
3. [x] and replica health.

## 47. Monitoring and SLOs

Kafka monitoring should connect to business requirements.

Example:

```text
Producer availability >= 99.99%
Consumer lag < 5 seconds
```

Then dashboards and alerts should measure those objectives.

## 48. SLO Example

SLO `99% of records processed within 10 seconds`

Monitoring:

1. [x] consumer lag
2. [x] processing latency
3. [x] end-to-end latency

is more useful than simply monitoring `CPU`

## 49. End-to-End Latency

Kafka's broker latency is not necessarily application end-to-end latency. A complete path might be:

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
   |
   v
  API
```

Measure:

```text
 event creation
      |
      v
 Kafka publish
      |
      v
 Kafka consume
      |
      v
processing complete
```

when the business SLA requires it.

## 50. Developer Perspective

Developers should monitor:

1. [X] producer errors
2. [X] producer latency
3. [X] consumer lag
4. [X] consumer processing time
5. [X] consumer rebalances
6. [X] commit failures
7. [X] serialization failures

They should also instrument application logic.

## 51. Administrator Perspective

Administrators should monitor:

1. [X] broker health
2. [X] replication
3. [X] disk
4. [X] network
5. [X] CPU
6. [X] JVM
7. [X] partitions
8. [X] controller
9. [X] request latency
10. [X] cluster capacity

## 52. Shared Responsibility

The strongest Kafka operations model combines:

```text
Developer
|
+--> application metrics
+--> producer/consumer metrics
+--> processing latency
```

```text
Administrator
|
+--> broker metrics
+--> infrastructure
+--> replication
+--> capacity
```

## 53. Certification Question

Question 1: **Which metric is most directly associated with partitions whose replicas are not sufficiently caught up?**

* A. BytesOut
* B. UnderReplicatedPartitions
* C. Consumer lag
* D. HeapUsed

Answer: **B**

Question 2: **A consumer group's lag is continuously increasing. Which should you investigate first?**

* A. Only broker disk capacity
* B. Consumer processing rate versus incoming record rate
* C. Topic retention only
* D. Controller count

Answer: **B**

Question 3: **What does `OfflinePartitionsCount` indicate?**

* A. Partitions with no available leader
* B. Partitions with high consumer lag
* C. Partitions with compressed messages
* D. Partitions with no retention policy

Answer: **A**

Question 4: **Why are p99 latency metrics useful?**

* A. They show only the fastest requests
* B. They expose tail latency that averages can hide
* C. They replace all Kafka logs
* D. They measure disk capacity

Answer: **B**

Question 5: **A broker has high CPU and high GC pause times. What should you investigate?**

* A. Only consumer offsets
* B. JVM memory/GC behavior and CPU workload
* C. Topic names
* D. Retention timestamps

Answer: **B**

Question 6: **A consumer group has frequent rebalances. Which is a possible cause?**

* A. Consumer processing exceeds max.poll.interval.ms
* B. Topic retention is enabled
* C. Compression is disabled
* D. Replication factor is 1

Answer: **A**

Question 7: **Which is generally the most critical condition?**

* A. One consumer has 100 records of lag
* B. One broker has 50% CPU
* C. An offline partition exists
* D. A producer batch is 50% full

Answer: **C**

**A cluster has**

```text
CPU = 30%
Disk = 30%
Network = 98%
```

Question 7: **What should you investigate first?**

* A. JVM heap
* B. Network capacity
* C. Partition retention
* D. Consumer commit interval

Answer: **B**

A broker shows:

```text
UnderReplicatedPartitions > 0
ISR shrinking
Disk latency ↑
```

Question 8: **What is a likely cause?**

* A. Schema evolution
* B. Disk/storage bottleneck
* C. Consumer offset reset
* D. Topic naming

Answer: **B**

Question 9: **Which is a better alert?**

* A. CPU > 70%
* B. OfflinePartitionsCount > 0
* C. Batch size < 100 KB
* D. One request was slow

Answer: **B**

Question 10: **Why should consumer lag be monitored per partition?**

* A. Kafka doesn't support topic-level lag
* B. One hot or stuck partition can be hidden by aggregate numbers
* C. Consumer groups only have one partition
* D. Partition offsets are not measurable

Answer: **B**

Question 11: **What is the best general troubleshooting strategy?**

* A. Change all Kafka configurations simultaneously
* B. Restart the entire cluster
* C. Establish the symptom, scope the problem, correlate metrics/logs, identify the bottleneck, then remediate
* D. Increase JVM heap immediately

Answer: **C**

## 54. Administrator Scenario — Replication Failure

You have:

```text
RF = 3
min.insync.replicas = 2
acks = all
```

Broker 3 becomes unavailable.

Monitoring shows:

```text
ISR reduced from 3 to 2
UnderReplicatedPartitions > 0
OfflinePartitions = 0
```

What does this tell you?

**Analysis**: The cluster still has ``2 ISR replicas`` so the `min.insync.replicas=2` condition can still be satisfied.

However `UnderReplicatedPartitions > 0` means the cluster has reduced redundancy.

The correct operational response is to **investigate** and **restore** the missing replica.

## 55. Administrator Scenario — Offline Partition

You observe `OfflinePartitionsCount = 4`. What is the priority? `CRITICAL`

Why?

Because partitions have no available leader and may be unavailable to clients.

Investigate:

1. [x] broker availability
2. [x] controller state
3. [x] replica availability
4. [x] network
5. [x] disk

## 56. Administrator Scenario — Consumer Lag

Metrics:

```text
Producer rate = 100 MB/s
Consumer rate = 70 MB/s
Lag = increasing
```

Root cause `consumer capacity < incoming workload`

Possible remediation `increase consumer parallelism` but only if **enough partitions exist and downstream processing can
handle the additional concurrency.**

## 57. Administrator Scenario — Rebalance Storm

Metrics:

```text
Consumer lag ↑
Rebalance rate ↑
Consumer processing latency ↑
```

Application logs show `processing time > max.poll.interval.ms`

Root cause **consumer processing is too slow between polls**

Potential remediation

```text
reduce max.poll.records
        +
optimize processing
        +
adjust max.poll.interval.ms if justified
```

Do not simply increase the timeout without understanding why processing is slow.

## 58. Administrator Scenario — Hot Partition

Metrics:

```text
Partition 0: 900 MB/s
Partition 1: 40 MB/s
Partition 2: 45 MB/s
Partition 3: 42 MB/s
```

Likely root cause `partition key skew`

Potential remediation `review partition key` not merely `increase consumer count`

## 59. Administrator Scenario — JVM Problem

Metrics:

```text
CPU = 80%
GC pauses = 5 seconds
Request latency = 1 second
ISR shrinking
```

There is a chain:

```text
        GC pauses
            |
            v
broker unable to process normally
            |
            v
    replication delayed
            |
            v
        ISR shrinks
            |
            v
request latency increases
```

The JVM problem may be the underlying cause.

## 60. Administrator Scenario — Network Problem

Metrics:

````text
Network utilization = 99%
Producer latency ↑
Consumer fetch latency ↑
Replication lag ↑
CPU normal
Disk normal
````

Likely root cause `network saturation`
This is a classic correlation question.

## 61. Administrator Scenario — Disk Problem

Metrics:

```text
Disk utilization = 95%
Disk latency = high
CPU = 40%
Network = 50%
ISR shrinking
```

Likely root cause `storage bottleneck`

Potential actions:

1. [x] investigate disk
2. [x] rebalance workloads
3. [x] increase storage performance
4. [x] add capacity

## 62. The Kafka Troubleshooting Matrix

| Symptom              | First things to investigate                            |
|----------------------|--------------------------------------------------------|
| Producer latency ↑	   | Broker, network, disk, replication                     |
| Producer errors ↑	    | Logs, broker availability, authorization, message size |
| Consumer lag ↑	       | Consumer processing, partitions, broker, producer rate |
| ISR shrinking	        | Disk, network, CPU, GC                                 |
| Offline partitions	   | Broker/controller/replica availability                 |
| Frequent rebalances  | 	Poll interval, crashes, network                        |
| High CPU	             | Compression, TLS, GC, requests, replication            |
| High disk latency	    | Storage throughput/IOPS                                |
| High network	         | Producer, consumer, replication traffic                |
| One hot partition	    | Key distribution                                       |
| High GC	              | Heap, allocation, partition/metadata load              |
| High request latency	 | CPU, disk, network, queues                             |

## 63. Safe Troubleshooting Pattern

Use:

```text
 Observe
   |
 Measure
   |
Hypothesize
   |
  Test
   |
Remediate
   |
 Verify
```

For example:

```text
         Lag ↑
          |
          v
Measure consumer processing
          |
          v
     Processing slow
          |
          v
Check downstream database
          |
          v
     Database slow
          |
          v
Fix database bottleneck
          |
          v
    Lag decreases
```

## 64. The Most Important Metrics to Memorize

For the administrator exam, prioritize:

1. [x] UnderReplicatedPartitions
2. [x] OfflinePartitionsCount
3. [x] BytesIn
4. [x] BytesOut
5. [x] Request latency
6. [x] Request handler utilization
7. [x] Network processor utilization
8. [x] ISR changes
9. [x] leader elections
10. [x] consumer lag
11. [x] disk usage
12. [x] disk latency
13. [x] JVM heap
14. [x] GC pauses

## 65. The Most Important Relationships to Memorize

```text
ISR shrinking -> replication problem
```
```text
Offline partition -> availability problem
```
```text
Consumer lag increasing -> consumption cannot keep up with production
```
```text
Frequent rebalances -> consumer group instability
```
```text
High disk latency -> potential replication/request latency problem
```
```text
High network utilization -> potential producer/consumer/replication bottleneck
```
```text
Long GC pauses -> broker responsiveness can degrade
```
```text
One hot partition -> partition-key/distribution problem
```

## 66. Certification Exam Traps

* **Trap 1**: Consumer lag always means Kafka broker failure. **False**. The consumer application may simply be too slow.

* **Trap 2**: More consumers always increase throughput. **False**. Parallelism is limited by partitions.

* **Trap 3**: High CPU means Kafka is CPU-bound. **Not necessarily**. Investigate what is consuming CPU.

* **Trap 4**: Under-replicated partitions mean data is already lost. **False**. They indicate reduced replication health, not necessarily data loss.

* **Trap 5**: Offline partitions are merely a performance warning. **False**. They indicate an availability problem.

* **Trap 6**: High disk usage means high disk latency. **False**. Capacity and performance are different metrics.

* **Trap 7**: Average latency is sufficient. **False**. Tail latency can be hidden by averages.

* **Trap 8**: Increasing JVM heap always improves Kafka performance. **False**. Kafka also relies heavily on OS page cache.

## 67. Practical Exercise 1 — Identify the Bottleneck

You receive:

```text
Producer rate:          500 MB/s
Producer latency p99:   600 ms

Broker CPU:              35%
Broker disk:             40%
Broker network:          98%

Consumer lag:            normal
ISR:                     healthy
```
_Question_: What is the most likely bottleneck?

_Answer_: Network

Reason `network = 98%` while `CPU, disk, replication and consumers` appear healthy.

## 68. Practical Exercise 2 — Consumer Lag

Metrics:

```text
Producer: 200 MB/s
Consumer: 120 MB/s
Broker: healthy
Consumer CPU: 95%
Lag: increasing
```

_Question_: What is the likely bottleneck?

_Answer_: Consumer processing capacity, the consumer cannot process data as quickly as it arrives.

## 69. Practical Exercise 3 — ISR

Metrics:

```text
RF = 3

ISR:
Broker 1
Broker 2

Broker 3:
disk latency = extremely high
```

_Question_: What is the likely root cause?

_Answer_: Broker 3 storage performance, the follower cannot keep up.

## 70. Practical Exercise 4 — Rebalances

Metrics:

```text
Consumer lag ↑
Rebalance rate ↑
Processing time = 400 seconds
max.poll.interval.ms = 300 seconds
```

_Answer_; The consumer is exceeding the allowed processing interval.

Potential consequences:

1. [ ] consumer removed
2. [ ] rebalance
3. [ ] partition reassignment
4. [ ] lag growth

## 71. Practical Exercise 5 — Hot Partition

Metrics:

```text
Topic:
P0 = 1 GB/s
P1 = 20 MB/s
P2 = 18 MB/s
P3 = 22 MB/s
```

_Question_: What should you investigate?

_Answer_: Partition-key distribution, look for a hot key or skewed partitioning.

## 72. Practical Exercise 6 — Cluster Failure

Metrics:

```text
OfflinePartitionsCount = 2
UnderReplicatedPartitions = 20
Broker count = 2/3
```

_Question_: What should be investigated first?

_Answer_: Broker failure + leader/replica availability + controller state,  **This is an availability incident**.

## 73. Practical Exercise 7 — GC

Metrics:

```text
Heap utilization = 90%
GC pauses = 4 seconds
Request latency = 800 ms
ISR shrinking
```

_Question_: Could GC be causing replication problems?

_Answer_: Yes. The chain can be:

```text
GC pause -> broker unavailable for processing -> follower falls behind -> ISR shrinks
```

## 74. Practical Exercise 8 — Disk

Metrics:

```text
Disk capacity = 50%
Disk latency = very high
Network = 40%
CPU = 30%
ISR shrinking
```

_Question_: Is disk still a possible bottleneck?

_Answer_: Absolutely. Disk capacity and disk performance are different.

## 75. Practical Exercise 9 — Alert Design

Which is more useful?

* **Alert A**

```text
CPU > 70%
```

* **Alert B**

```text
UnderReplicatedPartitions > 0
for 5 consecutive minutes
```

_Answer_: Generally **B** is more Kafka-specific and actionable.

## 78. Practical Exercise 10 — Lag Trend

Scenario:

```text
09:00  lag = 1,000
09:05  lag = 1,100
09:10  lag = 1,050
09:15  lag = 1,000
```

_Question_: Is the consumer necessarily unhealthy?

_Answer_: No. Lag is stable/decreasing.

Compare with:

```text
09:00 1,000
09:05 5,000
09:10 20,000
09:15 80,000
```

which indicates a clear problem.

## 79. Administrator Exam Strategy

When a question gives you several metrics, identify:

1. The symptom 
Example: `consumer lag`

2. The correlated metric
Example: `consumer CPU = 100%`

3. The likely bottleneck
Example: `consumer processing`

4. The best action
Prefer the action that addresses the bottleneck rather than merely increasing a random limit.

## 80. Developer Exam Strategy

When a question concerns client behavior, focus on:

1. [x] producer
2. [x] batching
3. [x] compression
4. [x] acks
5. [x] retries
6. [x] idempotence
7. [x] consumer poll
8. [x] offsets
9. [x] rebalances
10. [x] lag
11. [x] serialization

When it concerns infrastructure, think:

1. [x] broker
2. [x] partition
3. [x] replication
4. [x] ISR
5. [x] disk
6. [x] network
7. [x] JVM

## 81. Administrator Exam Strategy

For administrator questions, prioritize:

* availability
* replication
* capacity
* monitoring
* disk
* network
* broker health
* partition distribution
* JVM
* security

A useful priority hierarchy is:

1. Availability
2. Durability
3. Data integrity
4. Performance
5. Capacity
6. Optimization

## 82. Final Certification Cheat Sheet

```text
                 KAFKA TROUBLESHOOTING
                         |
       +-----------------+-----------------+
       |                 |                 |
   Availability       Performance       Consumer
       |                 |                 |
 Offline partitions   Latency            Lag
 ISR                  CPU                Poll
 Leader elections     Disk               Rebalance
 Broker failure       Network            Processing
 Controller           GC                 Commit
       |                 |                 |
       +-----------------+-----------------+
                         |
                         v
                    ROOT CAUSE
```

```text
OfflinePartitionsCount > 0 = availability problem
```
```text
UnderReplicatedPartitions > 0 = replication health problem
```
```text
ISR shrinking = replica falling behind
```
````text
Lag increasing = consumer cannot keep up
````
```text
Frequent rebalances = consumer-group instability
```
```text
High disk latency = possible storage bottleneck
```
```text
High network utilization = possible network bottleneck
```
```text
Long GC pauses = possible broker responsiveness problem
```
````text
One hot partition = possible key-distribution problem
````