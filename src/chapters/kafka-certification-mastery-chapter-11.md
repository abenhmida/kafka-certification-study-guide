# Chapter 11 — Kafka Streams Deep Dive

```text
Certification focus: This chapter covers Kafka Streams from fundamentals through production architecture, 
stateful processing, repartitioning, joins, windowing, state stores, fault tolerance, exactly-once processing, 
scaling, and troubleshooting.
```

## 1. Learning Objectives

By the end of this chapter, you should be able to:

1. Explain what Kafka Streams is.
2. Understand the Kafka Streams architecture.
3. Distinguish KStream, KTable, and GlobalKTable.
4. Build and reason about a Streams topology.
5. Distinguish stateless and stateful operations.
6. Explain repartitioning.
7. Understand joins.
8. Understand aggregations and reductions.
9. Explain windowing.
10. Understand state stores.
11. Explain changelog topics.
12. Understand caching.
13. Explain processing guarantees.
14. Understand exactly-once semantics.
15. Explain task assignment and scaling.
16. Understand standby replicas.
17. Diagnose common Kafka Streams failures.
18. Understand Interactive Queries.
19. Design production Kafka Streams applications.
20. Answer certification-style questions.

## 2. What Is Kafka Streams?

Kafka Streams is a Java/Scala library for building applications that process data stored in Kafka. It is not a separate
streaming cluster. A Kafka Streams application runs as a normal application:

```text
        +-----------------------------+
        | Kafka Streams Application   |
        |                             |
        |   Topology                  |
        |   Tasks                     |
        |   State Stores              |
        +-----------------------------+
                      |
                      v
                 Kafka Cluster
```

This is fundamentally different from Kafka Connect.

```text
Kafka Connect = Integration

Kafka Streams = Stream Processing
```

## 3. Kafka Streams Is a Library

One of the most important certification concepts **Kafka Streams is embedded into your application**.

You don't deploy a special "Kafka Streams server" like you deploy a Kafka broker.

For example:

```kotlin
fun main() {
    val builder = StreamsBuilder()

    builder.stream<String, Order>("orders")
        .filter { _, order -> order.amount > 100 }

    val topology = builder.build()

    KafkaStreams(topology, properties).start()
}
```

The application itself contains the Streams runtime.

## 4. High-Level Architecture

Consider:

```text
                    orders
                   payments
                  customers
                      |
                      v
        +---------------------------+
        | Kafka Streams Application |
        |                           |
        |      Source Nodes         |
        |           |               |
        |     Processing Nodes      |
        |           |               |
        |      State Stores         |
        |           |               |
        |       Sink Nodes          |
        +---------------------------+
                      |
                      v
                 Kafka topics
```

Kafka Streams uses Kafka itself for:

- input
- output
- state recovery
- coordination
- partitioning
- fault tolerance

## 5. Streams Application

A Streams application consists of one or more application instances.

```text
                    Kafka Cluster
                         |
          +--------------+--------------+
          |              |              |
          v              v              v
      Streams 1      Streams 2      Streams 3
```

Each instance can process a subset of the Kafka partitions. For example:

```text
Topic: orders

Partition 0
Partition 1
Partition 2
Partition 3
```

With two Streams instances:

```text
Instance 1
  P0
  P1

Instance 2
  P2
  P3
```

## 6. application.id

The most important Kafka Streams configuration property is:

```properties
application.id=order-processing
```

It identifies a Streams application. It is used for:

- consumer group identity
- state directory naming
- internal topic naming
- coordination

Conceptually:

```text
application.id
      |
      +---- consumer group
      |
      +---- internal topics
      |
      +---- local state identity
```

Two applications with different `application.id` values are independent applications.

## 7. KStream

A KStream represents an unbounded stream of events.

Example:

```text
orders:

Order 1
Order 2
Order 3
Order 4
...
```

Every record represents an event. Conceptually: `KStream<K, V>` Example:

```kotlin
val orders: KStream<String, Order> =
    builder.stream("orders")
```

## 8. KTable

A KTable represents a changing state.

Suppose:

```text
customer=42 -> balance=100
```

Later:

```text
customer=42 -> balance=150
```

The latest value represents the current state.

```text
KStream
    |
    | updates
    v
KTable
    |
    v
current state
```

## 9. KStream vs KTable

This is a fundamental certification topic.

**KStream** represents ***events***

```text
OrderCreated
OrderCreated
OrderCancelled
OrderCreated
```

**KTable** represents ***latest state per key***

```text
customer-1 -> ACTIVE
customer-2 -> BLOCKED
customer-3 -> ACTIVE
```

Memorize

```text
KStream = event stream

KTable = changing state
```

## 10. KTable as a Changelog

Suppose a topic contains:

```text
key=A value=10
key=B value=20
key=A value=15
```

A KTable eventually represents:

```text
A -> 15
B -> 20
```

The second record for A replaces the previous value. This is why KTables are often described as **changelog streams**
interpreted as tables.

## 11. GlobalKTable

A GlobalKTable is a table replicated to every Streams application instance.

Architecture:

```text
Kafka Topic
     |
     +---------> Instance 1
     |
     +---------> Instance 2
     |
     +---------> Instance 3
```

Every instance receives all partitions of the GlobalKTable's source topic. This is useful for relatively small reference
data.

## 12. KTable vs GlobalKTable

### KTable

Data is partitioned.

```text
Instance 1 -> subset
Instance 2 -> subset
Instance 3 -> subset
```

### GlobalKTable

Every instance has the complete table.

```text
Instance 1 -> complete table
Instance 2 -> complete table
Instance 3 -> complete table
```

Trade-off:

```text
GlobalKTable + simple lookup/join but more network/storage consumption
```

## 13. Topology

A topology describes how records flow through the application.

Example:

```text
orders
   |
   v
filter
   |
   v
  map
   |
   v
groupByKey
   |
   v
aggregate
   |
   v
orders-by-customer
```

You can think of it as a directed graph:

```text
  Source
    |
    v
Processor
    |
    v
Processor
    |
    v
State Store
    |
    v
  Sink
```

## 14. Source Nodes

A source node reads records from Kafka.

Example:

```kotlin
val orders = builder.stream<String, Order>("orders")
```

Topology:

```text
Kafka topic
    |
    v
Source node
```

## 15. Sink Nodes

A sink node writes records to Kafka.

Example:

```kotlin
orders.to("processed-orders")
```

Topology:

```text
Processing
    |
    v
Sink node
    |
    v
Kafka topic
```

## 16. Stateless Processing

Stateless operations do not require remembering previous records.

Examples:

```text
filter
map
mapValues
flatMap
branch
selectKey
```

Example:

```kotlin
orders
    .filter { _, order -> order.amount > 100 }
```

Each record can be processed independently.

## 17. Stateful Processing

Stateful operations depend on previous records.

Examples:

```text
aggregate
reduce
count
join
windowed aggregation
```

Example:

```text
Order 1 -> total = 100
Order 2 -> total = 150
Order 3 -> total = 250
```

The processor needs state.

## 18. filter

Example:

```kotlin
orders.filter { _, order ->
    order.amount > 100
}
```

Input:

```text
A -> 50
B -> 200
C -> 150
```

Output:

```text
B -> 200
C -> 150
```

No persistent state is required.

## 19. mapValues

mapValues changes the value while keeping the key.

```kotlin
orders.mapValues { order ->
    order.amount * 1.2
}
```

Conceptually:

```text
K -> V1
     |
     v
K -> V2
```

The key remains unchanged.

## 20. map

map can change both key and value.

```text
oldKey -> oldValue
       |
       v
newKey -> newValue
```

This matters because changing the key can affect partitioning.

## 21. selectKey

selectKey changes the record key.

Example:

```kotlin
orders.selectKey { _, order ->
    order.customerId
}
```

Before

```text
orderId -> Order
```

After

```text
customerId -> Order
```

This is often necessary before grouping or joining by another business key.

## 22. Why Keys Matter

Kafka partitioning is key-driven.

```text
record
   |
   v
  key
   |
   v
partition
```

Therefore

```text
same key
   |
   v
same partition
```

assuming the same partitioning configuration. This provides the foundation for local stateful processing.

## 23. groupByKey

Suppose:

```text
customer-1 -> order-1
customer-2 -> order-2
customer-1 -> order-3
```

Using

```kotlin
orders.groupByKey()
```

groups records according to their existing keys. Then:

```kotlin
count()
```

can produce:

```text
customer-1 -> 2
customer-2 -> 1
```

## 24. groupBy

groupBy allows you to derive a new grouping key.

Example:

```kotlin
orders.groupBy { _, order ->
    order.customerId
}
```

This can cause repartitioning because records may need to move to partitions corresponding to the new key.

## 25. Repartitioning

Repartitioning is one of the most important Kafka Streams concepts.

Suppose records currently use `orderId` as their key but you want to aggregate by `customerId`. The records may need to
be redistributed.

```text
Current partitioning
       |
       v
    new key
       |
       v
repartition topic
       |
       v
new partitioning
```

## 26. Why Repartitioning Is Necessary

Suppose:

```text
Partition 0
  order-1 customer-A
  order-2 customer-B

Partition 1
  order-3 customer-A
```

If you need `customer-A` to be processed by one task, all records for `customer-A` must reach the same partition.
Therefore Kafka Streams may create an internal repartition topic.

## 27. Repartition Topic

Conceptually:

```text
    orders
      |
      v
selectKey(customerId)
      |
      v
repartition topic
      |
      v
  aggregate
```

It provides:

- redistribution
- partitioning by new key
- fault tolerance
- decoupling between topology stages

## 28. Repartitioning Cost

Repartitioning introduces overhead:

```text
network traffic
    +
Kafka writes
    +
Kafka reads
    +
serialization
    +
storage
```

Therefore, unnecessary repartitioning should be avoided.

Certification question:

**Why can changing a record key cause repartitioning?**

```text
Because downstream stateful operations may require records with the same key to be colocated.
```

## 29. Aggregation

An aggregation combines records into state.

Example:

```text
customer-A -> 100
customer-A -> 50
customer-A -> 75
```

Aggregation:

```text
customer-A -> 225
```

Typical APIs:

```text
aggregate()
reduce()
count()
```

## 30. reduce

reduce combines values of the same type.

Example:

```kotlin
stream
    .groupByKey()
    .reduce { oldValue, newValue ->
        oldValue + newValue
    }
```

If:

```text
A -> 10
A -> 20
A -> 30
```

result

```text
A -> 60
```

## 31. count

Count records per key.

```kotlin
stream
    .groupByKey()
    .count()
```

Input

```text
A -> event1
A -> event2
B -> event3
```

result

```text
A -> 2
B -> 1
```

## 32. aggregate

aggregate is more flexible.

Example:

```text
Orders
   |
   v
Aggregate
   |
   v
CustomerSummary
```

You can create a completely different result object.

```text
      Order
        |
        v
CustomerStatistics
```

## 33. State Stores

Stateful processing requires state. Kafka Streams can maintain local state stores.

Example:

```text
Processor
    |
    v
State Store
```

The state store may contain:

```text
customer-A -> 500
customer-B -> 750
```

## 34. Local State

State stores are local to a Streams task. For example:

```text
Instance 1
  |
  +-- Task 0
       |
       +-- State Store

Instance 2
  |
  +-- Task 1
       |
       +-- State Store
```

The state is partitioned according to Kafka's partitioning model.

## 35. Changelog Topics

Local state must survive failures. Kafka Streams can back up state through changelog topics.

Conceptually:

```text
State Store
     |
     v
Changelog Topic
     |
     v
   Kafka
```

If an instance fails:

```text
Kafka changelog
       |
       v
new instance
       |
       v
restore state
```

This is a critical fault-tolerance mechanism.

## 36. State Recovery

Suppose:

```text
Instance 1
  |
  +-- State Store
```

Instance 1 crashes.

A replacement instance can:

```text
Kafka changelog
       |
       v
restore local state
```

This avoids requiring the application to reconstruct everything from an external database.

## 37. Persistent vs In-Memory State

Kafka Streams supports different state-store implementations. A persistent local store can be backed by disk.

The key architectural concept is:

```text
local state
    +
Kafka-backed recovery
```

This gives Streams applications fault tolerance without requiring all state to live remotely.

## 38. Windowing

Windowing allows aggregation over time.

Example `Count orders every 5 minutes` instead of `all orders ever` we process

```text
10:00-10:05
10:05-10:10
10:10-10:15
```

## 39. Tumbling Windows

Tumbling windows do not overlap.

Example:

```text
10:00 ───── 10:05
10:05 ───── 10:10
10:10 ───── 10:15
```

Each event belongs to one window.

## 40. Hopping Windows

Hopping windows can overlap.

Example:

```text
Window 1: 10:00 - 10:05
Window 2: 10:02 - 10:07
Window 3: 10:04 - 10:09
```

This is useful for rolling calculations.

## 41. Sliding Windows

Sliding windows are based on event relationships rather than fixed non-overlapping intervals.

They are useful for calculations such as `events occurring within the previous N milliseconds`

## 42. Session Windows

Session windows group activity separated by periods of inactivity.

Example:

```text
Event
Event
Event

--- inactivity ---

Event
Event
```

This can represent:

```text
User session 1
User session 2
```

Session windows are useful for user activity analysis.

## 43. Event Time

Event time is the timestamp associated with the event.

Example:

```text
Event generated:
10:00:05

Arrives:
10:00:08
```

The event's logical time is `10:00:05` not necessarily its processing time.

## 44. Processing Time

Processing time is the time at which the application processes the event.

```text
event timestamp = 10:00:05
processing time = 10:00:08
```

The difference is important in distributed systems.

## 45. Out-of-Order Events

Events may arrive out of order.

Example:

```text
Event A timestamp = 10:00:01
Event B timestamp = 10:00:05
Event C timestamp = 10:00:03
```

Processing order:

```text
A
B
C
```

Event-time order:

```text
A
C
B
```

Kafka Streams uses timestamp and windowing semantics to handle such scenarios.

## 46. Grace Periods

A grace period allows late events to arrive after a window would otherwise have ended.

Conceptually:

```text
Window
10:00 - 10:05

Grace period
10:05 - 10:07
```

An event arriving during the grace period may still be considered for the window, depending on the topology and
configuration.

## 47. Stream-Stream Join

Two event streams can be joined.

Example:

```text
orders
   |
   +--------+
            |
            v
           JOIN
            ^
            |
   +--------+
   |
payments
```

A join typically requires records to have compatible keys and appropriate temporal semantics.

```text
orders:

order-100 -> $250
```

```text
payments:

order-100 -> PAID
```

Join:

```text
order-100
    |
    +-- order = $250
    +-- payment = PAID
```

Output:

```text
order-100 -> OrderPaid
```

## 49. KTable-KTable Join

Two tables can be joined based on their current state.

Example:

```text
CustomerTable
     +
AccountTable
     |
     v
CustomerAccountView
```

If either table changes, the resulting table may be updated.

## 50. KStream-KTable Join

A stream can be enriched using table state.

Example:

```text
Order event
    |
    v
Customer KTable
    |
    v
Enriched Order
```

Suppose

```text
Order:
customerId=42

Customer table:
42 -> Alice
```

result

```text
Order -> customerName=Alice
```

This is a very common pattern.

## 51. GlobalKTable Join

A GlobalKTable can be useful for enrichment.

```text
orders
   |
   v
GlobalKTable
   |
   v
enriched orders
```

Because every Streams instance has the full table, certain joins can avoid repartitioning the stream. However, the
reference dataset is replicated to every instance.

## 52. Join Key Problem

Suppose:

```text
Order key = orderId
Customer key = customerId
```

But you want:

```text
Order.customerId == Customer.customerId
```

The keys don't align. You may need `selectKey(customerId)` followed by repartitioning. This is a common certification
scenario.

## 53. Co-Partitioning

For many Kafka Streams joins, the participating data must be co-partitioned.

That means:

```text
same logical key
        |
        v
same partition
```

If streams are not co-partitioned, Kafka Streams may need repartitioning.

## 54. Why Co-Partitioning Matters

Suppose:

```text
Orders
Partition 0 -> customer A

Customers
Partition 1 -> customer A
```

A local join cannot find both records without moving data. Therefore:

```text
  repartition
      |
      v
co-partitioned data
      |
      v
    join
```

## 55. Caching

Kafka Streams can cache intermediate results.

Conceptually:

```text
    Input
      |
      v
   Processor
      |
      v
    Cache
      |
      v
State Store / Kafka
```

Caching can reduce:

- unnecessary downstream writes
- state-store updates
- network traffic

It can improve performance.

## 56. Cache Trade-Off

Caching can make output appear less immediately granular because intermediate updates may be coalesced.

Example:

Without caching:

```text
A -> 1
A -> 2
A -> 3
A -> 4
```

With caching, downstream consumers may see fewer updates such as:

```text
A -> 4
```

depending on topology and cache behavior.

## 57. Exactly-Once Processing

Kafka Streams supports processing guarantees that can provide exactly-once semantics within supported Kafka processing
boundaries.

Conceptually:

```text
Input
  |
  v
Processing
  |
  +--> State Store
  |
  +--> Output Topic
```

The goal is to atomically coordinate:

```text
state updates
+
output records
```

with Kafka transactions.

## 58. Exactly-Once vs At-Least-Once

1. At-least-once:

```text
    process
       |
       +--> output
       |
     crash
       |
     retry
       |
duplicate possible
```

2. Exactly-once:

```text
process
   |
   +--> state + output transaction
             |
           commit
```

If the transaction fails, the effects can be aborted rather than exposed as successful output.

## 59. Exactly-Once Does Not Mean Everything Is Exactly Once

A critical certification point **Kafka Streams' exactly-once guarantees do not automatically make arbitrary external
side effects exactly once**.

For example:

```text
Kafka Streams
     |
     v
External REST API
```

A REST API call is outside Kafka's transaction boundary unless additional mechanisms are used. Therefore:

```text
Kafka EOS != global distributed transaction
```

## 60. Standby Replicas

Kafka Streams can maintain standby replicas for state stores.

Example:

```text
Active:
Task 1
State Store

Standby:
Task 1
State replica
```

If the active task fails

```text
Standby
   |
   v
becomes active
```

This can significantly reduce state restoration time.

## 61. Why Standby Replicas Matter

Without standby:

```text
            Failure
              |
              v
   restore state from changelog
              |
              v
     potentially long recovery
```

With standby:

```text
        Failure
          |
          v
     promote standby
          |
          v
     faster recovery
```

This is particularly important for large state stores.

## 62. Scaling Streams

Kafka Streams scales by using partitions.

Suppose:

```text
Topic = 12 partitions
```

You can distribute work across multiple application instances.

Example:

```text
Instance 1 -> P0 P1 P2 P3
Instance 2 -> P4 P5 P6 P7
Instance 3 -> P8 P9 P10 P11
```

More instances do not help if there are not enough partitions to distribute.

## 63. Maximum Parallelism

A simplified mental model:

```text
maximum useful task parallelism ≈ number of input partitions
```

If:

```text
topic has 6 partitions
```

creating:

```text
20 Streams instances
```

does not create 20-way parallel processing for that source. Some instances may have no active tasks.

## 64. Rebalancing

When a Streams instance joins or leaves:

```text
   Instance joins
         |
         v
     rebalance
         |
         v
  tasks redistributed
```

Similarly:

```text
Instance fails
     |
     v
 rebalance
     |
     v
tasks reassigned
```

Rebalancing can temporarily affect throughput and latency.

## 65. Task

A task is a unit of Streams processing associated with one or more input partitions.

Conceptually:

```text
Task
 |
 +-- Partition 0
 +-- Partition 2
```

The task executes the topology for its assigned partitions.

## 66. Stream Task vs Connect Task

Do not confuse them.

1. **Kafka Connect Task**: Moves data between Kafka and external systems.
2. **Kafka Streams Task**:  Processes Kafka records according to a Streams topology.

Both use the word "task", but they have different meanings.

## 67. Internal Topics in Kafka Streams

Kafka Streams may create internal topics for:

```text
repartition
changelog
```

These topics are important to application correctness and recovery. Example:

```text
        Input
          |
          v
    Repartition Topic
          |
          v
    Stateful processor
          |
          v
     Changelog Topic
```

## 68. Repartition vs Changelog Topic

This distinction is important.

1. **Repartition topic**: redistribute records according to a key
2. **Changelog topic**: backup state-store updates

## 69. Internal Topic Naming

Kafka Streams derives internal topic names from the application identity and topology. The exact names depend on
topology and Kafka Streams version. Therefore, don't rely on memorizing literal topic names.

Understand their purpose instead.

## 70. Topology Optimization

Kafka Streams can optimize topology execution. Potential goals include:

* reducing unnecessary repartitioning
* reducing intermediate topics
* optimizing processing paths

However, optimization should be validated using the actual topology and runtime behavior.

## 71. Topology Description

You can inspect the topology.

Conceptually:

```kotlin
println(topology.describe())
```

This is extremely useful for debugging. You may discover:

```text
source
processor
repartition
aggregate
state store
sink
```

## 72. Debugging Topologies

When debugging a Streams application, ask:

1. What are the input topics?
2. What are the keys?
3. Where does repartitioning occur?
4. Which operations are stateful?
5. Which state stores exist?
6. Which internal topics are created?
7. Where are records written?

This approach is much more effective than looking only at application logs.

## 73. Interactive Queries

Interactive Queries allow applications to expose locally maintained state.

Conceptually:

```text
       Kafka
         |
         v
  Streams application
         |
         v
     State Store
         |
         v
      Query API
         |
         v
       Client
```

Example:

```text
GET /customers/42
```

The application can query its local state.

## 74. Interactive Query Challenge

In a distributed application:

```text
Instance 1 -> customers A-C
Instance 2 -> customers D-F
Instance 3 -> customers G-I
```

A request for customer `G` must reach the instance that owns the corresponding state. Therefore, a complete Interactive
Query architecture often requires:

* metadata discovery
* routing
* application-level API
* state-store access

## 75. State Store Types

Common state-store concepts include:

* KeyValueStore
* WindowStore
* SessionStore

These represent different forms of local state.

### 75.1. KeyValueStore

Stores:

```text
key -> value
```

Example:

```text
customer-A -> 1000
customer-B -> 2500
```

Useful for:

* aggregations
* lookup state
* materialized views

### 75.2. WindowStore

Stores data associated with time windows.

Example:

```text
customer-A
  |
  +-- 10:00 -> 5 events
  +-- 10:05 -> 7 events
  +-- 10:10 -> 4 events
```

### 75.3. SessionStore

Stores session-based data.

Example:

```text
User A
  |
  +-- Session 1
  +-- Session 2
```

The sessions are separated by inactivity periods.

## 76. Record Timestamp

Kafka Streams associates timestamps with records. Timestamp extraction can influence:

* event-time processing
* windows
* joins
* ordering semantics

This is why timestamp behavior must be understood when debugging windowed applications.

## 77. Serialization

Kafka Streams requires serializers/deserializers for keys and values.

Conceptually:

```text
Kafka bytes
   |
Deserializer
   |
Java/Kotlin object
   |
Processing
   |
Serializer
   |
Kafka bytes
```

Example:

```properties
default.key.serde=...
default.value.serde=...
```

### 77.1. Serde

Kafka Streams commonly uses `Serde` which combines `Serializer + Deserializer`. This is different terminology from Kafka
Connect converters.

```text
Kafka Producer/Consumer -> Serializer/Deserializer

Kafka Streams -> Serde

Kafka Connect -> Converter
```

## 78. Streams and Schema Registry

A Streams application can use schema-based serialization.

For example:

```text
Kafka
 |
Avro
 |
Streams
 |
Avro
 |
Kafka
```

Schema Registry can manage schemas. The application must configure appropriate Serdes.

## 79. Error Handling

Streams applications can encounter:

* deserialization errors
* processing exceptions
* production errors
* state-store errors

Different categories require different handling strategies.

### 79.1. Deserialization Errors

Suppose an input record cannot be deserialized.

```text
    Kafka
     |
     v
 Deserializer
     |
     X
    ERROR
```

Depending on configuration, the application may:

* fail
* skip/continue
* route errors through configured mechanisms

The correct strategy depends on business requirements.

### 79.2. Production Errors

Suppose output cannot be written to Kafka.

```text
Processing
   |
   v
Producer
   |
   X
Kafka error
```

This is fundamentally different from a malformed input record. Production error handling must preserve processing
guarantees.

### 79.3. Uncaught Exceptions

If an unexpected exception escapes processing, the Streams application may enter an error state or shut down depending
on configuration and exception handling. The key operational principle **Don't treat all exceptions as equivalent**.

Classify them:

* input error
* processing error
* Kafka error
* state-store error
* external dependency error

## 80. External Calls Inside Streams

Consider:

```text
   Kafka
     |
     v
  Streams
     |
     v
  HTTP API
```

This introduces problems:

* latency
* retries
* timeouts
* duplicate calls
* backpressure
* transaction-boundary issues

Kafka Streams is usually strongest when processing is based on Kafka data rather than blocking on slow external
services.

## 81. Streams Application Shutdown

A graceful shutdown allows the application to:

* stop accepting new work
* commit/complete processing appropriately
* close state stores
* close Kafka clients
* leave the group cleanly

Typical application code should register a shutdown hook.

Conceptually:

```kotlin
Runtime.getRuntime().addShutdownHook(
    Thread {
        streams.close()
    }
)
```

## 82. Production Configuration

Important configuration areas include:

```text
application.id
bootstrap.servers
default key/value Serdes
processing guarantee
state directory
replication settings
cache settings
commit configuration
```

Exact configuration names and available options depend on Kafka Streams version.

## 83. State Directory

Streams maintains local state under a configured state directory.

Conceptually:

```text
/state-dir/
    application-id/
        task-0/
        task-1/
```

The state directory should be on reliable local storage with sufficient capacity.

Do not treat it as the only source of truth because state recovery relies on Kafka-backed changelog data.

## 84. State Disk Capacity

If an application has ``500 GB`` of local state, the state directory needs enough disk space. Monitor:

* disk usage
* IOPS
* latency
* filesystem health

A full state directory can cause serious application failures.

## 85. Kafka Streams on Kubernetes

A common deployment is:

```text
Kubernetes
 |
 +-- Streams pod 1
 +-- Streams pod 2
 +-- Streams pod 3
       |
       v
Kafka cluster
```

Kafka Streams instances automatically participate in the same application group when they share ``application.id``
and compatible topology/configuration.

## 86. Rolling Deployment

When deploying a new version:

```text
v1
 |
 +-- instance 1
 +-- instance 2
 +-- instance 3
```

gradually replace with:

```text
v2
 |
 +-- instance 1
 +-- instance 2
 +-- instance 3
```

During deployment ``rebalances + state movement + possible temporary latency`` must be considered.

## 87. Application Versioning

Changing a Streams topology can affect:

* state stores
* internal topics
* repartitioning
* task compatibility
* state restoration

Therefore, topology changes should be treated carefully in production. Some changes require explicit application
migration/versioning strategies.

## 88. Streams Performance Model

A useful mental model `` Throughput = partitions × parallel tasks × processing capacity``. But real performance also
depends on:

* serialization
* network
* state stores
* disk
* Kafka brokers
* CPU
* GC
* downstream systems

## 89. Common Performance Bottleneck

Suppose:

```text
Kafka input = 100k records/sec
Streams = 20k records/sec
```

Do not immediately assume Kafka is slow. Investigate:

* CPU
* serialization
* state-store I/O
* GC
* repartitioning
* network
* application logic

## 90. Repartitioning Performance

If the topology contains:

```text
    source
      |
  selectKey
      |
  repartition
      |
   aggregate
```

you have additional Kafka traffic. If throughput is unexpectedly low, inspect the topology. A topology with excessive
repartitioning can create:

* CPU overhead
* network overhead
* Kafka I/O
* latency

## 91. State Store Performance

Stateful workloads may be constrained by local storage.

Monitor:

* disk latency
* disk throughput
* cache hit rate
* state-store size
* compaction

A state-heavy topology can behave very differently from a purely stateless topology.

## 92. Compaction

Many state-related Kafka topics use compaction semantics.

Conceptually:

```text
key=A -> 1
key=A -> 2
key=A -> 3
```

Eventually older records for the same key can be cleaned up according to log-compaction rules.

This helps maintain recoverable state without keeping every historical update forever.

## 93. Certification Scenario: KStream or KTable?

Question:

You receive:

```text
customer-1 -> balance=100
customer-1 -> balance=150
customer-1 -> balance=200
```

You want the current balance. Use `KTable` why: Because the latest value for the key represents current state.

## 94. Certification Scenario: Counting Events

Input:

```text
A -> event
A -> event
B -> event
```

You want:

```text
A -> 2
B -> 1
```

Use

```text
KStream
  |
groupByKey
  |
count
```

## 95. Certification Scenario: Enrichment

You have:

```text
orders
customers
```

Every order contains:

```text
customerId
```

You want to enrich orders with customer information.

A common approach is:

```text
KStream<Order>
      |
      v
KTable<Customer>
      |
      v
KStream enriched
```

## 96. Certification Scenario: Wrong Key

Input order key ``orderId`` and you want to aggregate by `customerId`. You should expect:

````text
selectKey/customer grouping
           |
           v
    possible repartition
           |
           v
       aggregation
````

## 97. Certification Scenario: State Recovery

Question:

**A Streams instance crashes. How can its local state be reconstructed?**

Answer:

**Kafka changelog topics**

The state store can be restored from its changelog.

## 98. Certification Scenario: Faster Recovery

Question:

**A state store is several hundred GB and recovery from Kafka is slow. What can reduce recovery time?**

Answer:

**Standby replicas**

They maintain replicas of state on other instances.

## 99. Certification Scenario: More Instances

Input topic
``12 partitions``
You deploy
``20 Streams instances``

**Will all 20 necessarily process data?**

**No**.

There is not enough partition-level parallelism for all instances to have active work for that source.

## 100. Certification Scenario: GlobalKTable

Question:

**You have a relatively small reference dataset that every Streams instance needs locally.**

Potential choice ``GlobalKTable`` Trade-off ``simple local lookup + full replication``
but ``more network + more local storage``

## 101. Certification Scenario: Exactly Once

Question:

**Does enabling exactly-once processing make a REST API call exactly once?**

**No**.

Kafka's **transactional boundary** does **not** automatically include arbitrary external APIs.

## 102. Certification Scenario: Repartition vs Changelog

Question:

**Which topic redistributes records according to a new key?**
**_Repartition topic_**.

Question:

**Which topic backs up state-store changes?**
**_Changelog topic_**.

## 103. Developer Mock Questions

_Question 1_

**What is Kafka Streams?**

A. A Kafka broker mode B. A Java/Scala stream-processing library C. A replacement for Kafka Connect D. A database

Answer: **B**

_Question 2_

**What does KStream primarily represent?**

A. Current state B. An event stream C. A Kafka partition D. A state store

Answer: **B**

_Question 3_

**What does KTable represent?**

A. A changing state keyed by record key B. A Kafka broker C. A Connect worker D. A schema

Answer: **A**

_Question 4_

**What is a GlobalKTable?**

A. A table stored only on the broker B. A table replicated to every Streams instance C. A Kafka Connect topic D. A
Schema Registry table

Answer: **B**

_Question 5_

**What operation can cause repartitioning?**

A. filter B. mapValues C. Changing the key before a key-dependent operation D. peek

Answer: **C**

_Question 6_

**What is a state store?**

A. Local state maintained by a Streams task B. Kafka broker metadata C. A Connect internal topic D. A Schema Registry
database

Answer: **A**

_Question 7_

**What protects state-store data against instance failure?**

A. DNS B. Changelog topics C. Consumer groups alone D. SMTs

Answer: **B**

_Question 8_

**What is a standby replica?**

A. A duplicate Kafka broker B. A replica of state maintained for faster recovery C. A duplicate producer D. A second
Schema Registry

Answer: **B**

_Question 9_

**What is a repartition topic used for?**

A. Schema storage B. State backup C. Redistribution of records according to a key D. Connector configuration

Answer: **C**

_Question 10_

**Which API is appropriate for complex stateful stream processing?**

A. Kafka Streams B. Kafka Connect SMT C. Kafka AdminClient D. Schema Registry

Answer: **A**

## 104. Administrator Mock Questions

_Question 11_

**What controls the identity of a Streams application?**

`application.id`.

_Question 12_

**What determines much of the maximum parallelism available to a Streams application?**

`Kafka partitions`.

_Question 13_

**Why can excessive repartitioning hurt performance?**

Because it introduces additional:

* Kafka writes
* Kafka reads
* network traffic
* serialization
* storage

_Question 14_

**Why are standby replicas useful?**

They reduce recovery time for stateful tasks.

_Question 15_

**What happens when a Streams instance joins or leaves the application?**

Tasks may be rebalanced across instances.

## 105. Advanced Design Exercise

Design a real-time customer analytics platform.

Requirements:

* Orders arrive through Kafka.
* Customer profiles are stored in Kafka.
* Calculate customer order totals.
* Calculate five-minute order counts.
* Expose current customer statistics.
* Survive application-instance failures.
* Scale horizontally.

Architecture:

```text
                         Kafka
                           |
          +----------------+----------------+
          |                                 |
       orders                         customers
          |                                 |
          v                                 v
     +------------------------------------------+
     |          Kafka Streams Application       |
     |                                          |
     |  KStream<Order>                          |
     |       |                                  |
     |       +----> KTable<Customer>            |
     |       |          |                       |
     |       |          v                       |
     |       +------> Enrichment                |
     |                  |                       |
     |                  v                       |
     |            GroupBy Customer              |
     |                  |                       |
     |                  v                       |
     |              Aggregate                   |
     |                  |                       |
     |             +----+----+                  |
     |             |         |                  |
     |          State      Window               |
     |          Store      Store                |
     |             |         |                  |
     +-------------+---------+------------------+
                   |
                   v
             Kafka output
```

Fault tolerance:

```text
State Stores
     |
     v
Changelog Topics
     |
     v
   Kafka
```

Faster recovery ``Active Task + Standby Replica``

## 106. Production Checklist

Before deploying a Kafka Streams application, verify:

**Kafka**

1. [ ] Sufficient partition count
2. [ ] Replication configured correctly
3. [ ] Internal topics monitored
4. [ ] ACLs configured
5. [ ] TLS/SASL configured where required

**Streams**

1. [ ] Correct application.id
2. [ ] Correct Serdes
3. [ ] State directory configured
4. [ ] Processing guarantee selected intentionally
5. [ ] Repartitioning understood
6. [ ] State stores sized appropriately
7. [ ] Standby replicas considered

**Application**

1. [ ] Graceful shutdown
2. [ ] Error handling
3. [ ] Monitoring
4. [ ] Logging
5. [ ] Metrics
6. [ ] Health checks
7. [ ] Capacity testing

## 107. Troubleshooting Decision Tree

When a Streams application is slow:

```text
Slow?
 |
 +--> Kafka lag?
 |       |
 |       +--> Check partitions / consumers
 |
 +--> CPU high?
 |       |
 |       +--> Check processing logic / serialization
 |
 +--> Disk high?
 |       |
 |       +--> Check state stores
 |
 +--> Network high?
 |       |
 |       +--> Check repartitioning
 |
 +--> Rebalances?
 |       |
 |       +--> Check instance stability / configuration
 |
 +--> External calls slow?
         |
         +--> Check downstream dependency
```

## 108. Kafka Streams Mental Model

```text
                  Kafka
                    |
                    v
                KStream
                    |
          +---------+---------+
          |                   |
      Stateless            Stateful
      operations           operations
          |                   |
          |             +-----+-----+
          |             |           |
          |          State Store   Windows
          |             |
          |          Changelog
          |             |
          +------+------+
                 |
                 v
              Kafka
```

```text
KStream = events

KTable  = current state

GlobalKTable = state replicated everywhere

State Store = local processing state

Repartition = move records to correct partition

Changelog = recover state

Standby = faster state recovery
```

## 109. Certification Cheat Sheet

Concept Remember

| Term              | Meaning                             |
|-------------------|-------------------------------------|
| Kafka Streams	     | Stream-processing library           |
| application.id    | 	Application identity / group        |
| KStream	           | Event stream                        |
| KTable	            | Changing state                      |
| GlobalKTable      | 	Full table on each instance         |
| Topology	          | Processing graph                    |
| Stateless	         | No persistent previous-record state |
| Stateful          | 	Requires state                      |
| groupByKey()	      | Group using existing key            |
| groupBy()	         | Group using derived key             |
| Repartition       | 	Redistribute by key                 |
| Aggregation       | 	Combine records                     |
| State Store       | 	Local state                         |
| Changelog         | 	State recovery                      |
| Repartition topic | 	Redistributes records               |
| Window	            | Time-bounded processing             |
| Tumbling          | 	Non-overlapping                     |
| Hopping           | 	Overlapping fixed windows           |
| Session	           | Activity separated by inactivity    |
| KStream-KTable    | 	Stream enrichment                   |
| KTable-KTable     | 	Table state join                    |
| GlobalKTable	      | Replicated reference data           |
| Co-partitioning	   | Same keys → compatible partitions   |
| Standby replica	   | Faster state recovery               |
| Interactive Query | 	Query local state                   |
| Serde             | 	Serializer + Deserializer           |
| Connect Converter | 	Connect serialization layer         |
| EOS               | 	Kafka transactional processing      |
| External API	      | Outside Kafka transaction boundary  |