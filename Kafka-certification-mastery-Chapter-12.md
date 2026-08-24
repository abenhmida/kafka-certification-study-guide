# Chapter 11 — Kafka Performance, Tuning & Capacity Planning

```text
Certification focus: This chapter covers the performance model of Apache Kafka, 
producer/consumer tuning, broker configuration, batching, compression, replication, partitions, 
JVM and disk considerations, capacity planning, benchmarking, bottleneck diagnosis, 
and certification-style scenarios.
```
## 1. Learning Objectives

By the end of this chapter, you should be able to:

1. Explain Kafka's performance model.
2. Distinguish throughput, latency, and capacity.
3. Understand how partitions affect parallelism.
4. Tune producers for throughput and latency.
5. Understand batching and linger.ms.
6. Understand batch.size.
7. Choose an appropriate compression codec.
8. Understand buffer.memory.
9. Understand max.in.flight.requests.per.connection.
10. Understand acknowledgments and durability trade-offs.
11. Tune consumers.
12. Understand fetch.min.bytes.
13. Understand fetch.max.wait.ms.
14. Understand max.poll.records.
15. Understand max.poll.interval.ms.
16. Tune broker disk and network performance.
17. Understand page cache.
18. Understand replication overhead.
19. Understand ISR behavior.
20. Estimate Kafka storage requirements.
21. Estimate network requirements.
22. Estimate partition requirements.
23. Design performance tests.
24. Diagnose bottlenecks systematically.
25. Answer Kafka developer and administrator certification questions.

## 2. The Kafka Performance Model

Kafka performance is fundamentally driven by:
```text
                    Kafka Performance
                          |
        +-----------------+------------------+
        |                 |                  |
      CPU               Disk              Network
        |                 |                  |
 serialization       sequential I/O      replication
 compression         page cache          producers
 request handling    log segments        consumers
```
But performance also depends on:
* partitions
* batching
* compression
* replication
* consumer fetch size
* producer configuration
* broker configuration
* JVM
* filesystem
* hardware
* message size
* workload pattern

## 3. Throughput vs Latency

These concepts are frequently confused.

### 3.1. Throughput

How much data Kafka processes per unit of time.  Example ``500 MB/sec`` or `100,000 records/sec`

### 3.2. Latency

How long an individual operation takes. Example ``Producer request latency = 5 ms``, a system can have
``high throughput + moderate latency`` or `low latency + lower throughput` depending on workload and configuration.

## 4. The Fundamental Trade-Off

Kafka performance tuning is often a trade-off:
```text
        Larger batches
              |
       higher throughput
              |
   potentially higher latency
```
Whereas
```text
       Smaller batches
              |
       lower waiting time
              |
  potentially lower throughput
```
The goal is not `Make every Kafka configuration as large or as small as possible` but `Optimize for the application's actual workload and SLA`.

## 5. Kafka's Sequential I/O Model

Kafka stores records in append-only logs.

Conceptually:
```text
Partition 0

+----+----+----+----+----+----+
| m1 | m2 | m3 | m4 | m5 | m6 |
+----+----+----+----+----+---->
                    append
```
This sequential append pattern is one of the reasons Kafka can achieve high throughput.
Kafka does not need to update arbitrary database rows for every record.

## 6. Partition as the Fundamental Unit

A Kafka topic is divided into partitions:
```text
Topic
 |
 +-- Partition 0
 +-- Partition 1
 +-- Partition 2
 +-- Partition 3
```
Partitions provide:

* ordering
* parallelism
* storage distribution
* replication units

A critical principle **Kafka parallelism is fundamentally partition-driven**.

## 7. More Partitions ≠ Automatically More Performance

Suppose ``Topic = 3 partitions`` and you increase it to `Topic = 100 partitions`

You have increased potential parallelism. But you also increase:

* metadata
* file descriptors
* replication work
* recovery work
* controller metadata
* consumer assignment complexity
* operational overhead

``more partitions`` is not equivalent to `free performance`

## 8. Partition Planning

Suppose the workload requires ``600 MB/sec`` and benchmarking shows one partition can sustainably process `30 MB/sec`.
A rough lower bound is ``600 / 30 = 20 partitions`` but production design should include headroom. For example `20 × 1.5 = 30 partitions`
could be a more conservative starting point. The actual number must be validated through benchmarking.

## 9. Producer Performance

Producer throughput is strongly affected by:
* batching
* compression
* acks
* message size
* network
* buffer capacity
* number of concurrent requests

A simplified model:
````text
Application
    |
    v
 Producer
    |
    +--> batching
    |
    +--> compression
    |
    v
 Network
    |
    v
 Broker
````

## 10. Producer Batching

Kafka producers batch records before sending them.

Instead of
```text
record 1 -> request
record 2 -> request
record 3 -> request
record 4 -> request
```
the producer can send:
```text
record 1
record 2
record 3
record 4
    |
    v
one request
```
This reduces:

* request overhead
* network overhead
* system calls
* protocol overhead

and generally improves throughput.

## 11. batch.size

batch.size controls the target batch size for a partition. ``batch.size = 64 KB`` the producer attempts to accumulate records into batches up to that size.

Important:

`batch.size` is not a command saying "wait until the batch is full before sending."

A batch can be sent before reaching the configured size.

## 12. linger.ms

linger.ms allows the producer to wait briefly for more records to arrive. For example `linger.ms = 5`

Conceptually
```text
record arrives
      |
      v
wait briefly
      |
      +--> more records arrive
      |
      v
larger batch
      |
      v
    send
```
This can improve throughput at the cost of some latency.

## 13. batch.size vs linger.ms
```text
batch.size = how large the batch can become

linger.ms = how long the producer may wait for more records
```
They work together.

## 14. Low-Latency Producer

For a latency-sensitive workload, you may prefer:
```text
small linger + appropriate batch size + efficient serialization
```

For example ``linger.ms=0`` may reduce intentional batching delay, but this does not mean batching disappears completely.
Modern Kafka producer implementations may still batch records naturally.

## 15. High-Throughput Producer

For throughput-oriented workloads:
```text
larger batches + some linger + compression
```
can often produce significantly better efficiency. Example configuration:
```properties
batch.size=65536
linger.ms=5
compression.type=zstd
```
The correct values depend on:

* record size
* traffic rate
* latency SLA
* CPU
* network
* broker capacity

## 16. Compression

Kafka supports compression codecs such as:
* gzip
* snappy
* lz4
* zstd

Compression can reduce:
```text
network bandwidth
disk usage
```
but increases CPU work.
```text
Without compression

100 MB
  |
  v
Network
  |
  v
100 MB


With compression

100 MB
  |
compress
  |
  v
35 MB
  |
  v
Network
```
Actual compression ratio depends on the data.

### 16.1. Compression Trade-Off

Compression is a three-way trade-off:
```text
CPU
 |
 +------ compression
 |
Network
 |
 +------ bandwidth
 |
Disk
 |
 +------ storage
```
Highly compressible data can benefit enormously. Already compressed data such as:
* JPEG
* PNG
* ZIP
* GZIP

may provide little additional benefit.

### 16.2. LZ4
LZ4 is commonly attractive when balancing ``speed + reasonable compression``.
It is often used in throughput-sensitive systems where CPU efficiency matters.

### 16.3. Zstandard
Zstandard (zstd) provides strong compression efficiency and can be a good choice when ``network bandwidth + storage``
are important.
It may require more CPU than very lightweight compression depending on settings and workload.

### 16.4. GZIP
GZIP generally provides good compression but can be more CPU-intensive than alternatives such as LZ4 or Snappy.
The correct choice should be benchmarked.

Certification principle **There is no universally best compression codec**.

## 17. Producer buffer.memory
The producer maintains memory for records waiting to be sent.
```text
Application
     |
     v
Producer Buffer
     |
     +---- batch
     +---- batch
     +---- batch
     |
     v
  Network
```
If the producer cannot send quickly enough, the buffer can fill.

## 18. max.block.ms
When producer memory is exhausted or metadata is unavailable, producer calls may block up to a configured limit.
```text
Producer buffer full
       |
       v
producer.send(...)
       |
       v
     wait
       |
       +---- capacity becomes available
       |
       +---- timeout
```
This is important when diagnosing producer latency.

## 19. acks

The producer acknowledgment configuration controls how much broker acknowledgment is required.

1. ``acks=0`` means minimal acknowledgment.
2. ``acks=1`` means the leader acknowledges.
3. ``acks=all`` requires acknowledgment from the leader after the required in-sync replica conditions are satisfied.

### 19.1. acks=0
The producer does not wait for broker acknowledgment.

**Advantages**:
* low producer-side latency
* potentially high throughput

**Disadvantages**:
* weaker delivery assurance
  
This is appropriate only when the application can tolerate potential loss.

### 19.2. acks=1
The leader acknowledges the write after accepting it. If the leader fails before replication to followers, 
data may potentially be lost depending on the replication state and broker configuration.

### 19.3. acks=all
The leader waits for the required in-sync replicas according to Kafka's replication semantics.

This provides stronger durability. Often combined with ``min.insync.replicas=2`` for stronger durability requirements.

## 20. acks and Durability

A common production pattern:
```properties
replication.factor=3
min.insync.replicas=2
acks=all
```
```text
Producer
   |
   v
Leader
   |
   +--> Replica 1
   |
   +--> Replica 2
```
The combination protects against certain broker failures while retaining availability 
when at least the configured number of replicas remain in sync.

## 21. min.insync.replicas

Suppose
```properties
replication.factor=3
min.insync.replicas=2
acks=all
```
Kafka requires at least two in-sync replicas for the write to succeed.
If only one replica remains in sync ``ISR = 1`` then the producer can receive an error rather than silently accepting 
a write that violates the intended durability policy.

## 22. acks=all Does Not Mean Every Replica
``acks=all`` does not mean the producer waits for every replica in the cluster. 
It works with the partition's ISR and min.insync.replicas semantics.

## 23. max.in.flight.requests.per.connection

This controls how many unacknowledged requests can be sent on one connection.
Higher values can improve throughput but ordering and retry behavior must be considered.
With idempotence enabled, Kafka producer semantics place constraints on this configuration 
and maintain ordering guarantees within the supported limits.

## 24. Idempotent Producer

Idempotence prevents certain duplicate records caused by producer retries.
```text
Producer
   |
   v
  send
   |
network problem
   |
 retry
   |
   v
 Broker
```
Without appropriate idempotence, retries can potentially create duplicates.
With idempotence ``producer identity + sequence numbers`` allow the broker to detect duplicate retry attempts.

## 25. Idempotence and Performance

Idempotence introduces some protocol constraints but is generally designed to provide strong reliability without requiring applications to sacrifice Kafka producer performance unnecessarily.

For modern Kafka versions, idempotence is enabled by default under the standard compatible producer configuration.

## 26. Producer Concurrency

A single producer can maintain multiple in-flight requests.
```text
Producer
 |
 +--> Request 1
 +--> Request 2
 +--> Request 3
 +--> Request 4
```
This allows the producer to keep the network busy. But excessive concurrency can increase
* memory
* broker request load
* complexity

and must be balanced.

## 27. Message Size

Large messages affect
* producer memory
* network
* broker memory
* disk
* consumer memory
* latency
* replication

Relevant configurations can include:
```properties
message.max.bytes
max.message.bytes
fetch.max.bytes
max.partition.fetch.bytes
```
These settings exist at different layers and must be compatible.

## 28. Large Messages
Suppose ``message = 20 MB`` but ``broker/topic limit = 10 MB`` Then the producer will fail.
Large-message designs should therefore consider the complete path:
```text
Producer
   |
   v
Broker
   |
   v
Replica
   |
   v
Consumer
```
Every relevant limit must accommodate the record.

## 29. Consumer Performance

Consumer throughput depends heavily on:
* fetch size
* number of partitions
* processing speed
* poll frequency
* deserialization
* network
* application logic

### 29.1. fetch.min.bytes

The consumer can request that the broker wait until at least a certain amount of data is available before responding, 
subject to the relevant wait limit.
```properties
fetch.min.bytes=1
```
means minimal waiting for accumulated data. A larger value can improve throughput by allowing larger fetch responses.
But it can increase latency when traffic is sparse.

### 29.2. fetch.max.wait.ms

This limits how long the broker waits for enough data to satisfy the fetch request.
```text
fetch request
     |
     v
wait for data
     |
     +---- enough data
     |
     +---- timeout
     |
     v
  response
```
It works together with ``fetch.min.bytes``

### 29.3. fetch.min.bytes + fetch.max.wait.ms
```text
fetch.min.bytes = desired minimum amount of data

fetch.max.wait.ms = maximum wait time
```
This creates a throughput/latency trade-off.

### 29.4. max.partition.fetch.bytes

Controls the maximum amount of data returned per partition in a fetch.
This matters when ``messages are large`` or `partitions have high throughput`. It must be large enough to accommodate the 
largest message under the relevant Kafka configuration constraints.

### 29.5. fetch.max.bytes

Controls the maximum amount of data returned for a fetch request overall.
```text
Fetch
 |
 +-- Partition 0
 +-- Partition 1
 +-- Partition 2
 +-- Partition 3
```
The total response is constrained by the fetch limit.

### 29.6. max.poll.records

Controls the maximum number of records returned from a single poll() call.
```properties
max.poll.records=500
```
This does **not** directly determine how much data the broker sends over the network.

It controls how many records the consumer returns to the application per poll.

### 29.7. max.poll.interval.ms

This is a critical consumer setting. It limits the maximum delay between successful calls to poll() before the consumer is considered to have failed its processing responsibility.

Suppose ``max.poll.interval.ms=300000`` and processing takes `400 seconds`. The consumer may be removed from the group 
and can cause a rebalance.

### 29.8. Long Consumer Processing

Suppose:
```text
      poll()
        |
        v
process 10,000 records
        |
        v
 takes 8 minutes
```
but:
```text
max.poll.interval.ms = 5 minutes
```
The consumer can be considered unresponsive. Possible consequences:

* consumer removed
* rebalance
* partitions reassigned
* duplicate processing depending on commits

### 29.9. max.poll.records as a Processing Control

If processing each record is expensive, reducing: 
```properties
max.poll.records
```
can reduce the amount of work between polls.
```text
1000 records
   |
   v
very slow processing
```
could become
```text
100 records
   |
   v
shorter processing interval
```
This must be balanced against throughput.

### 29.10. Consumer Commit Frequency

Committing offsets more frequently can reduce duplicate processing after failure.
But frequent commits introduce overhead.
```text
Process
Process
Process
Commit
```
versus
```text
Process
Commit
Process
Commit
Process
Commit
```
The second pattern generally creates more commit overhead.

### 29.11. At-Least-Once Consumer Pattern

A common pattern
```text
poll
 |
process
 |
commit offset
```
If the consumer crashes after processing but before committing:
```text
process
   |
crash
   |
restart
   |
reprocess
```
This can produce duplicates. That is normal under at-least-once semantics.

### 29.12. Throughput Optimization

For throughput-oriented consumers:
```text
larger fetches + efficient deserialization + batch processing + appropriate poll sizes
```
can improve performance.
For latency-sensitive workloads:
```text
smaller fetch delays + prompt processing
```
may be preferred.

## 30. Broker Performance

Broker performance depends heavily on
* disk
* network
* CPU
* page cache
* partition count
* replication
* request rate
* message size

A useful simplified model
```text
    Producer
       |
    Network
       |
    Broker
     /    \
    Disk  Page Cache
     |
 Replication
     |
  Network
```

### 30.1. Disk Performance

Kafka performs well with sequential writes, but workloads still depend on:

* disk throughput
* IOPS
* latency
* filesystem
* disk queue depth

Fast storage can improve:

* log writes
* recovery
* state movement
* replication
* consumer fetches when data is not cached

### 30.2. Page Cache

Kafka relies heavily on the operating system's page cache.
````text
Kafka
 |
 v
OS Page Cache
 |
 v
Disk
````
Recently accessed data may be served from memory instead of disk.
**Kafka does not simply depend on raw disk speed**. Available RAM is important.

#### 30.2.1. Why Kafka Uses Page Cache
```text
Producer
   |
   v
append log
   |
   v
OS cache
   |
   v
  disk
```
The OS can optimize filesystem access. Consumers reading recently written data may obtain it directly from cache.
This is particularly useful for ``tail reads`` where consumers are close to the end of the log.

### 30.3. JVM Heap

Kafka brokers should not simply be given all available machine RAM as JVM heap. Why?
Because Kafka benefits significantly from ``OS page cache`` 
```text
Machine RAM
 |
 +-- JVM heap
 |
 +-- OS page cache
 |
 +-- filesystem / native memory
```
Oversizing the JVM heap can reduce the memory available for page cache.

### 32.4. Garbage Collection
Kafka brokers are JVM applications. Large heaps can result in ``longer GC considerations`` while too-small heaps can produce
```text
allocation pressure
OutOfMemoryError
```
Modern JVMs and Kafka versions should be tuned using measurements rather than old blanket recommendations.

### 32.5. Network Performance
Kafka can be highly network-intensive.
Network traffic includes
```text
producer -> broker
broker -> consumer
leader -> follower replication
broker -> broker
```
Replication can approximately multiply network requirements.

#### 32.5.1. Replication Traffic
Suppose
```text
incoming writes = 100 MB/sec
replication factor = 3
```
A simplified conceptual model is:
```text
100 MB/sec
   |
   +--> follower 1
   |
   +--> follower 2
```
So additional replication traffic can be roughly ``200 MB/sec`` in addition to the original producer-to-leader traffic.
Actual traffic patterns depend on architecture and protocol behavior.

#### 32.5.2. Network Capacity Planning
Suppose ``producer ingress = 500 MB/sec`` and `replication factor = 3`. You should not provision the broker network for only `500 MB/sec`
You need to account for
```text
producer traffic + replication + consumer traffic + inter-broker traffic
```
A real capacity model should include headroom.

#### 32.5.3. Consumer Egress
If ``Kafka writes = 500 MB/sec`` and consumers read the same data `consumer traffic` can add substantial network load.
A broker handling:
```text
producer ingress + replication + consumer egress
```
can require much more network capacity than the raw producer rate suggests.

#### 32.5.4. Replication Factor
Replication factor determines how many copies of each partition exist.
``RF = 3`` means
```text
Leader
Follower
Follower
```

**Benefits**:
* fault tolerance
* availability
* durability

**Costs**:
* disk
* network
* replication CPU
* recovery work

## 33. ISR
ISR means ``In-Sync Replicas``
```text
Replication factor = 3

Leader
Follower 1
Follower 2

ISR = {Leader, Follower 1, Follower 2}
```
If a follower falls too far behind ``ISR = {Leader, Follower 1}``

### 33.1. ISR and Durability
Consider
```text
RF=3
min.insync.replicas=2
acks=all
```
If ISR becomes:
```text
Leader
Follower 1
```
writes can continue. If ISR becomes:
```text
Leader
```
only one replica is in sync. Then `acks=all` writes may fail because `ISR < min.insync.replicas`.
This protects durability at the cost of availability.

## 34. The Durability/Availability Trade-Off
Higher durability requirements can reduce write availability.
```text
Higher durability
       |
       v
more replicas required
       |
       v
fewer failure scenarios tolerated
       |
       v
potential write rejection
```
This is intentional.

## 35. Storage Capacity Planning
Suppose:
```text
incoming data = 200 GB/day
retention = 7 days
RF = 3
```
Raw logical storage:
```text
200 × 7 = 1,400 GB
```
With RF=3:
```text
1,400 × 3 = 4,200 GB
```
So roughly ``4.2 TB`` of replicated storage is needed before additional operational headroom.

### 35.1. Add Headroom
Never size disks exactly to calculated retention. If calculated requirement is ``4.2 TB`` you also need room for:
* segment behavior
* rebalancing
* replication
* temporary growth
* traffic spikes
* operational safety
* filesystem overhead

A production design might therefore provision significantly more.

## 36. Retention
Storage requirements depend heavily on:
```text
retention.bytes
retention.ms
```
If data arrives faster than expected ``storage consumption ↑``; If retention increases `storage consumption ↑`;
If replication factor increases ``storage consumption ↑``

## 37. Partition Count and Storage
Suppose ``100 TB`` of retained data is distributed across `1,000 partitions`, average logical data per partition 
``100 TB / 1000 = 100 GB``.
Partition distribution should also account for:
* key skew
* traffic skew
* broker balance

## 38. Hot Partitions
Suppose ``Topic = 20 partitions`` but one key produces `60% of all traffic` If that key maps to one partition:
```text
Partition 7
   |
   +-- 60% traffic
```
You have a hot partition. Adding more partitions may not solve the problem if the same hot key continues mapping to one partition.

### 38.1. Key Skew
Ideal distribution:
```text
P0 -> 25%
P1 -> 25%
P2 -> 25%
P3 -> 25%
```
Bad distribution:
```text
P0 -> 70%
P1 -> 10%
P2 -> 10%
P3 -> 10%
```
This can create:

* uneven CPU
* uneven disk usage
* uneven network usage
* consumer lag
* reduced throughput

### 38.2. Solving Hot Keys
Potential approaches include:
* better partition key
* key salting
* application-level sharding
* different data model

But changing the key can affect:
* ordering
* joins
* aggregation semantics

Therefore, this is a data-model decision, not merely a tuning knob.

## 39. Compression and Capacity Planning
Suppose
```text
raw data = 1 TB/day
compression ratio = 2:1
```
Approximate compressed data ``500 GB/day`` but compression ratio must be measured using the real workload,
do not assume ``50%`` without benchmarking.

## 40. Producer Throughput Formula
A rough conceptual model:
```text
Throughput ≈ records per batch × batches per second × number of active producers
```
But real throughput is bounded by:
* CPU
* network
* broker
* disk
* partitions
* serialization
* compression

Therefore, formulas provide starting estimates, not guarantees.

## 41. Consumer Throughput
Suppose ``1 consumer = 20,000 records/sec`` and `topic = 8 partitions`, A theoretical starting point could be
``8 consumers/tasks``. But actual throughput depends on:
* record size
* processing cost 
* partition distribution

## 42. Consumer Group Parallelism
A consumer group with ``5 consumers`` and `3 partitions` will have at most `3 consumers` actively consuming those partitions.
The other consumers may remain idle.

## 43. One Partition → One Consumer
Within a consumer group ``one partition`` is assigned to `at most one consumer` at a time.
This guarantees ordered processing within the partition.

## 44. Ordering vs Throughput
Suppose all events for a customer must remain ordered. Using ``customerId`` as the partition key can ensure that records for that key go to the same partition.
But this also means ``one hot customer`` can become `one hot partition` Therefore ordering requirements can limit scalability.

## 45. Batch Size and Compression Interaction
Compression is usually more effective on larger batches.
```text
Small batches
   |
   v
less context
   |
   v
lower compression efficiency
```
Whereas
```text
larger batches
   |
   v
more repeated patterns
   |
   v
better compression
```
This is one reason batching can improve both throughput and network efficiency.

## 46. Producer Tuning Workflow
Do not change ten parameters simultaneously. Use:
1. Establish baseline
2. Measure throughput
3. Measure latency
4. Increase batching
5. Test compression
6. Test concurrency
7. Test acknowledgment strategy
8. Measure broker impact
9. Test failure behavior

This produces meaningful results.

## 47. Consumer Tuning Workflow
Start with:
1. Measure lag
2. Measure processing latency
3. Measure poll frequency
4. Measure fetch sizes
5. Adjust fetch settings
6. Adjust max.poll.records
7. Verify max.poll.interval.ms
8. Measure again

Do not tune based only on CPU utilization.

## 48. Broker Tuning Workflow
Measure:
1. CPU
2. memory
3. disk throughput
4. disk latency
5. network
6. request latency
7. under-replicated partitions
8. ISR changes
9. consumer lag

Then identify the actual bottleneck.

## 49. Important Kafka Metrics
**producers**:
* `record-send-rate`
* `record-error-rate`
* `request-latency`
* `batch-size`
* `compression-rate`
* `buffer-available-bytes`
* `record-queue-time`

**consumers**:
* `records-consumed-rate`
* `fetch-latency`
* `fetch-size`
* `records-lag`
* `records-lag-max`
* `poll latency`

**brokers**:
* `BytesIn`
* `BytesOut`
* `Request latency`
* `UnderReplicatedPartitions`
* `OfflinePartitionsCount`
* `ISR changes`
* `NetworkProcessorAvgIdlePercent`
* `RequestHandlerAvgIdlePercent`
* `Disk utilization`

Exact metric names can vary by Kafka version and monitoring integration.

## 50. Consumer Lag
Consumer lag is one of the most important operational metrics.
``Latest offset - Consumer offset = Lag``
```text
Latest = 10,000
Consumer = 9,500

Lag = 500
```

## 51. Growing Lag
If lag continuously grows ``incoming rate > processing rate``. Possible causes:
* consumer too slow
* insufficient consumers
* hot partition
* slow downstream service
* broker/network issue
* large messages
* GC

## 52. Lag Does Not Always Mean Kafka Is Slow
This is a critical troubleshooting principle. A consumer may lag because ``application processing`` is slow, Kafka itself may be healthy.
``lag ↑`` does not automatically mean `Kafka broker problem`

## 53. Broker CPU Bottleneck
Symptoms may include:
* high CPU
* high request latency
* low idle time

Possible causes:
* compression
* TLS
* request processing
* too many partitions
* metadata operations
* replication

## 54. Broker Disk Bottleneck
Symptoms:
* high disk utilization
* high I/O latency
* increased request latency
* consumer fetch delays
* replication lag

Potential responses:
* faster disks
* more brokers
* better partition distribution
* more memory/page cache
* workload redistribution

## 55. Network Bottleneck

Symptoms:

* high network utilization
* request latency
* replication lag
* consumer lag
* producer throttling

Check:

* ingress
* egress
* replication traffic
* consumer traffic

## 56. Replication Bottleneck

Suppose `follower cannot keep up` then `ISR shrinks`

Potential consequences:

* acks=all failures
* reduced durability margin
* leader election risk
* recovery pressure

Investigate:

* disk
* network
* broker CPU
* follower load

## 57. Too Many Partitions
Too many partitions can create:
* metadata overhead
* file descriptors
* memory usage
* controller workload
* rebalance overhead
* recovery time

This is why partition count should be planned rather than arbitrarily increased.

## 58. Too Few Partitions
Too few partitions can cause:

* insufficient parallelism
* consumer bottlenecks
* hot partitions
* limited producer concurrency

The correct answer lies between ``too few`` and `too many`.

## 59. Capacity Planning Example
Suppose the system requires:
```text
Incoming: 400 MB/sec

Peak: 600 MB/sec

Retention: 7 days

RF: 3
```
Logical daily data at peak ``600 MB/sec × 86,400`` Approximately `51.84 TB/day` Seven days `362.88 TB`
Replication ``362.88 × 3 = 1,088.64 TB`` That's approximately `1.09 PB` before operational headroom.
This demonstrates why capacity planning must consider **peak traffic**, retention, and replication.

## 60. Capacity Planning Formula
A useful starting formula:
```text
Storage = ingress rate × retention duration × replication factor × overhead factor 
```
Where ``ingress rate`` should represent the expected retained data rate. If compression is significant
```text
effective storage = raw storage × compression factor
```
But compression must be measured.

## 61. Network Capacity Formula
Conceptually
```text
Broker network ≈ producer ingress + consumer egress + replication traffic + other inter-broker traffic
```
Then add ``headroom`` for:
* peaks
* rebalancing
* recovery
* operational traffic

## 62. Disk Capacity Formula
Conceptually ``Disk = daily data × retention × RF`` Then adjust for `compression + filesystem overhead + segment behavior + headroom`

## 63. Capacity Planning Is Not Just Storage
You need to plan:
* CPU
* RAM
* Disk
* Network
* Partitions
* Brokers
* Replication
* Consumers

A cluster can have enough disk but still fail because ``network saturated`` or `CPU saturated` or `too many partitions`

## 64. Performance Testing
A realistic Kafka performance test should measure:
* throughput
* latency
* CPU
* memory
* disk
* network
* consumer lag
* replication health

Test:
* normal load
* peak load
* burst load
* failure scenarios
* recovery

## 65. Benchmarking Anti-Pattern
Bad test:
```text
10 seconds
localhost
one producer
tiny messages
no replication
```
Then conclude: "Kafka can process 5 million messages/sec."
This tells you very little about production performance.

## 66. Better Benchmark
A meaningful benchmark should approximate:
```text
production message size
production partitions
production replication factor
production compression
production producer count
production consumer count
production hardware
production network
```
and should run long enough to observe steady-state behavior.

## 67. Failure Testing
Performance testing should include failures. Example:
```text
Normal
  |
  v
Broker failure
  |
  v
Leader election
  |
  v
Recovery
  |
  v
Normal
```
Measure:
* latency
* throughput
* consumer lag
* ISR
* recovery time

## 68. Benchmarking Percentiles
Average latency can hide important problems. Suppose ``average = 10 ms`` but `p99 = 500 ms`. The system may feel slow for 1% of requests.
Therefore, measure:
````text
p50
p95
p99
p99.9
````
when latency matters.

## 69. Throughput Measurement
Measure both ``records/sec`` and `bytes/sec` because `1 million small records` and `1 million large records`
represent radically different workloads.

## 70. Record Size Matters
Suppose ``records/sec = 100,000`` at `1 KB/record` throughput is approximately `100 MB/sec`, At `10 KB/record`
it becomes ``1 GB/sec``. Therefore **records/sec** alone is insufficient.

## 71. Producer Compression Test
Run:
```text
No compression
LZ4
Snappy
Zstd
GZIP
```
Measure:
```text
CPU
network
throughput
latency
disk
```
Choose based on actual results.

## 72. Tuning Example
Initial configuration:
```properties
batch.size=16384
linger.ms=0
compression.type=non
```
Benchmark:
```text
500 MB/sec
p99 = 20 ms
CPU = 35%
Network = 80%
```
Try:
```properties
batch.size=65536
linger.ms=5
compression.type=lz4
```
Potential result:
```text
700 MB/sec
p99 = 25 ms
CPU = 50%
Network = 60%
```
This may be better if the SLA allows the additional latency.

## 73. Don't Optimize One Metric
Suppose tuning gives:
```text
throughput: +50%
CPU: +100%
latency: +300%
```
Is it better? Only if:
```text
latency SLA
CPU capacity
```
still satisfy requirements. Performance optimization is a multi-dimensional problem.

## 74. Certification Questions

A producer needs maximum throughput and can tolerate a small amount of latency.

_Question_ **Which changes are likely helpful?**

* A. Disable batching
* B. Increase batching and use a suitable compression codec
* C. Set linger.ms=0 exclusively
* D. Reduce partition count

Answer: **B**

---------
_Question_ **What does batch.size control?**

* A. Maximum Kafka partition size
* B. Target batch size used by the producer for records destined for a partition
* C. Consumer fetch size
* D. Broker log segment size

Answer: **B**

---------
_Question_ **What does fetch.min.bytes influence?**

* A. Minimum broker disk capacity
* B. Amount of data the broker attempts to accumulate before satisfying a consumer fetch
* C. Maximum message size
* D. Number of partitions

Answer: **B**

---------

A consumer processes records for 10 minutes before calling poll() again, while max.poll.interval.ms is 5 minutes.

_Question_ **What can happen?**

* A. Nothing
* B. Consumer can be considered failed and removed from the group
* C. Broker deletes the topic
* D. Producer stops

Answer: **B**

------------

_Question_ **What happens when a consumer group has 10 consumers but only 4 partitions?**

* A. Each consumer gets one partition
* B. Six consumers can remain without assigned partitions
* C. Kafka creates six partitions
* D. All consumers process every partition

Answer: **B**

--------------

_Question_ **Which configuration combination is commonly used for stronger producer durability?**

* A.
````text
acks=0
min.insync.replicas=1
````

* B.
```text
acks=all
min.insync.replicas=2
```

* C.
```text
acks=0
min.insync.replicas=3
```

* D.
```text
acks=1
min.insync.replicas=0
```

Answer: **B**

-------

_Question_ **If replication factor is 3 and min.insync.replicas=2, what happens if ISR drops to 1 while using acks=all?**

* A. Writes continue normally
* B. Writes requiring the ISR condition can fail
* C. Kafka automatically creates a replica
* D. The topic is deleted

Answer: **B**

--------

_Question_ **Which component provides a large amount of Kafka's read performance?**

* A. OS page cache
* B. DNS
* C. Schema Registry
* D. ZooKeeper

Answer: **A**

----------

_Question_ **Why shouldn't all server RAM necessarily be allocated to the JVM heap?**

* A. Kafka cannot use RAM
* B. Kafka benefits from OS page cache
* C. JVMs cannot exceed 1 GB
* D. Replication stops

Answer: **B**

--------

_Question_ **A topic has one extremely hot key. Increasing partition count may not solve the problem because:**

A. Kafka ignores partitions
B. The hot key can continue mapping to the same partition
C. Consumers cannot use multiple partitions
D. Producers cannot use keys

Answer: **B**

-------

_Question_ **Which metric is particularly useful for detecting follower replication problems?**

A. Under-replicated partitions
B. Consumer application logs only
C. Schema count
D. Topic name length

Answer: **A**

------

_Question_ **Which statement is correct?**

A. More partitions always improve performance
B. Fewer partitions always improve performance
C. Partition count is a trade-off between parallelism and operational overhead
D. Partition count has no performance impact

Answer: **C**

---

_Question_ **A consumer's lag is continuously increasing. What is the first general interpretation?**

A. Consumer processing capacity is below incoming workload
B. Kafka automatically deleted messages
C. Producer is necessarily broken
D. Schema Registry is down

Answer: **A**

---

## 76. Administrator Troubleshooting Scenario
You observe:
```text
Producer throughput ↓
Broker CPU = 30%
Broker disk = 30%
Network = 95%
```
Most likely bottleneck? ``Network``
Do not immediately tune CPU or disk.

## 77. Administrator Troubleshooting Scenario
You observe:
```text
Consumer lag ↑
Broker CPU normal
Broker network normal
Consumer CPU = 100%
```
Likely bottleneck:
```text
Consumer/application processing
```

## 78. Administrator Troubleshooting Scenario
You observe:
````text
Consumer lag ↑
Consumer CPU = 20%
Broker CPU = 40%
Disk latency very high
````
Potential bottleneck:
```text
Disk I/O
```
Investigate:
```text
storage throughput
latency
IOPS
filesystem
```

## 79. Administrator Troubleshooting Scenario
You observe:
```text
ISR shrinking
followers falling behind
disk latency high
```
Potential root cause:
````text
broker disk bottleneck
````
The follower cannot process and replicate data quickly enough.

## 80. Administrator Troubleshooting Scenario
You observe:
```text
One partition: 900 MB/sec

Other partitions: 50 MB/sec each
```
Likely cause:
```text
key skew / hot partition
```
The solution is likely at the partitioning/data-model level rather than simply adding consumers.

## 81. Administrator Troubleshooting Scenario
You increase:
```text
consumer count: 4 -> 20
```
but throughput does not increase.
```text
Topic: 4 partitions
```
Why? Because the topic has only four partitions available for consumer-group parallelism.

## 82. Administrator Troubleshooting Scenario
You increase `linger.ms` and throughput improves, but p99 latency violates the SLA.
Correct conclusion:
```text
The optimization improved throughput
but violated latency requirements.
```
Possible response:
```text
reduce linger
or
find a better batching/partition/compression balance
```

## 83. Performance Tuning Golden Rules

* Rule 1: Measure first.
* Rule 2: Change one major variable at a time.
* Rule 3: Measure both throughput and latency.
* Rule 4: Use production-like data.
* Rule 5: Test failure scenarios.
* Rule 6: Watch CPU, disk, network, and lag together.
* Rule 7: Don't increase partitions blindly.
* Rule 8: Don't increase JVM heap blindly.
* Rule 9: Don't assume consumer lag means broker failure.
* Rule 10: Always account for replication when capacity planning.

## 84. Developer Certification Cheat Sheet
```text
batch.size
    -> producer batch target

linger.ms
    -> producer batching delay

compression.type
    -> network/storage vs CPU trade-off

acks
    -> producer durability/ack behavior

buffer.memory
    -> producer buffering

fetch.min.bytes
    -> consumer fetch accumulation

fetch.max.wait.ms
    -> consumer fetch wait limit

max.partition.fetch.bytes
    -> fetch size per partition

fetch.max.bytes
    -> overall fetch response limit

max.poll.records
    -> records returned per poll

max.poll.interval.ms
    -> maximum processing gap between polls
```

## 85. Administrator Certification Cheat Sheet
```text
Replication Factor
    -> number of replicas

ISR
    -> replicas currently in sync

min.insync.replicas
    -> minimum ISR required for relevant writes

acks=all
    -> strongest standard producer acknowledgment mode

UnderReplicatedPartitions
    -> replication health indicator

Partitions
    -> parallelism + ordering + distribution

Page Cache
    -> important Kafka read/write performance mechanism

Disk
    -> log and recovery performance

Network
    -> producer + consumer + replication traffic
```

## 86. Mental Model
When analyzing Kafka performance, think:
```text
                    WORKLOAD
                       |
          +------------+-------------+
          |            |             |
       records        size          rate
          |            |             |
          +------------+-------------+
                       |
                       v
                  PARTITIONS
                       |
          +------------+-------------+
          |            |             |
        CPU          Disk         Network
          |            |             |
          +------------+-------------+
                       |
                       v
                  REPLICATION
                       |
                       v
                   CONSUMERS
                       |
                       v
                 APPLICATION
```
Every performance problem should be mapped somewhere in this model.

## 87. What You Should Memorize for the Exam
The most important performance relationships are:
```text
More partitions -> more potential parallelism -> more operational overhead
```
```text
Larger batches -> higher throughput -> potentially higher latency
```
```text
Compression -> less network/storage -> more CPU
```
```text
Higher replication -> better fault tolerance -> more disk + network
```
````text
More consumers -> more parallelism -> only up to available partitions
````
```text
acks=all + min.insync.replicas -> stronger durability
    ->
potentially lower availability during failures
```
```text
max.poll.interval.ms -> maximum allowed processing gap
```
```text
Hot key -> hot partition -> Limited scalability
```
```text
Consumer lag -> incoming rate > processing rate
```