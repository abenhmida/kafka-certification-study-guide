# Chapter 10 — Kafka Connect Deep Dive

This chapter focuses on Kafka Connect:

- architecture
- connectors
- workers
- tasks
- converters
- Single Message Transforms
- error handling
- offsets
- internal topics,
- distributed mode
- security
- operations
- certification-style questions.

## 1. Chapter Objectives

By the end of this chapter, you should be able to:

* Explain the purpose of Kafka Connect.
* Distinguish source and sink connectors.
* Explain workers and tasks.
* Understand standalone vs distributed mode.
* Understand connector, task, and worker configuration.
* Explain converters and serialization.
* Understand Schema Registry integration.
* Explain internal Kafka Connect topics.
* Understand connector offsets.
* Configure error handling and dead-letter queues.
* Understand Single Message Transforms.
* Explain connector REST APIs.
* Troubleshoot Connect deployments.
* Understand Connect security.
* Distinguish Kafka Connect from Kafka Streams.
* Answer certification-style questions.

## 2. What Is Kafka Connect?

Kafka Connect is a framework for moving data between Kafka and external systems. Instead of writing a custom application
such as:

```text
                Database
                    |
                    v
           Custom Java Application
                    |
                    v
                  Kafka
```

you can use:

```text
                Database
                    |
                    v
               Kafka Connect
                    |
                    v
                 Kafka
```

For data going in the opposite direction:

```text
                Kafka 
                  | 
                  v 
             Kafka Connect 
                  | 
                  v 
               Database
```

Kafka Connect is designed for integration rather than general-purpose application processing.

## 3. Source vs Sink Connectors

There are two fundamental connector types.

### Source Connector

Moves data into Kafka.

```text
        External System 
               | 
               v 
         Source Connector 
               | 
               v 
             Kafka
```

Examples:

```text
PostgreSQL -> Kafka 
MongoDB -> Kafka 
Filesystem -> Kafka 
```

### Sink Connector

Moves data out of Kafka.

```text
        Kafka 
          | 
          v 
     Sink Connector 
          | 
          v 
    External System
```

Examples:

```text
Kafka -> Elasticsearch 
Kafka -> PostgreSQL 
Kafka -> S3
```

Memorize:

```text
SOURCE = external system -> Kafka
SINK = Kafka -> external system
```

This is one of the most important Connect certification concepts.

## 4. Kafka Connect Architecture

A simplified architecture:

```text
                             Kafka Cluster
                                  |
                   +--------------+--------------+
                   |                             |
            Connect Internal Topics       Data Topics
                   |
            +------+------+
            |             |
       Config Topic   Offset Topic 
            | 
       Status Topic 
            | 
            v 
+---------------------------+ 
| Kafka Connect             | 
|                           | 
| Worker 1                  | 
| Worker 2                  | 
| Worker 3                  | 
|                           | 
+---------------------------+ 
    |               | 
    |               | 
Source Connectors Sink Connectors 
    |               | 
    v               v 
External Systems External Systems
```

## 5. Worker

A **worker** is a Kafka Connect process. A worker provides the runtime environment in which connectors and tasks
execute.

Conceptually:

```text
        Connect Worker 
              | 
              +-- Connector A 
              |       +-- Task 1 
              |       +-- Task 2 
              | 
              +-- Connector B 
                      +-- Task 1 
                      +-- Task 2
```

The worker manages:

* connector lifecycle
* task lifecycle
* configuration
* communication
* REST API
* coordination offset handling
* status information

## 6. Connector

A connector defines how Connect interacts with an external system. For example **PostgreSQL Source Connector**

defines:

* how to connect to PostgreSQL
* what tables to read
* how records are represented
* how source offsets are tracked

A connector itself generally coordinates tasks rather than processing every individual record directly.

## 7. Tasks

Tasks perform the actual data movement.

For example:

```text
        Connector 
            | 
            +-- Task 0 
            +-- Task 1 
            +-- Task 2
```

If the source system supports parallelism, multiple tasks can process different portions of the workload.

A useful mental model:

```text
        Connector 
            | 
            | defines integration 
            v 
          Tasks 
            | 
            | perform work 
            v 
    Kafka / External System
```

## 8. Connector vs Task

This distinction is frequently tested.

## Connector

Responsible for:

- configuration
- coordination
- determining task assignments
- lifecycle

## Task

Responsible for:

* actual record movement
* processing assigned work
* reading/writing records

Remember:

```text
Connector = coordination
Task = execution
```

## 9. Example: Source Connector

Imagine a database containing:

```text
orders
-------------------------
id | customer | amount
-------------------------
1  | Alice    | 100 
2  | Bob      | 200 
3  | Carol    | 150
```

A source connector might produce:

```text
Kafka topic: orders

key=1 value={...} 
key=2 value={...} 
key=3 value={...}
```

Architecture:

```text
        PostgreSQL 
            | 
            v 
      Source Connector 
            | 
            +-- Task 0 
            +-- Task 1 
            | 
            v 
        Kafka topic
```

## 10. Example: Sink Connector

Suppose Kafka contains:

```text
orders 
payments 
customers
```

A sink connector can consume records and write them to another system.

```text
        Kafka 
          | 
          v 
     Sink Connector 
          | 
          +-- Task 0 
          +-- Task 1 
          | 
          v 
     Elasticsearch
```

The sink connector tracks how far it has consumed from Kafka.

## 11. Standalone Mode

Standalone mode is intended for simpler deployments.

Architecture:

```text
        +-----------------------+ 
        |    Connect Worker     | 
        |                       | 
        |   Connector + Tasks   | 
        +-----------------------+
```

Configuration is commonly supplied through local configuration files or command-line configuration.

**Advantages**:

* simple easy to start
* useful for development
* useful for small deployments

**Disadvantages**:

* limited fault tolerance
* not designed for large production clusters
* configuration management is less centralized

## 12. Distributed Mode

Distributed mode runs multiple Connect workers.

```text
                     +----------------+
                     | Connect Worker |
                     +----------------+
                             |
                 +-----------+-----------+
                 |                       |
            +---------------+ +---------------+ 
            | Connect Worker| | Connect Worker| 
            |       2       | |       3       | 
            +---------------+ +---------------+
```

Connect automatically distributes connector tasks across workers.

This provides:

* scalability
* fault tolerance
* automatic task rebalancing

For production deployments, distributed mode is generally preferred.

## 13. Distributed Mode Coordination

Connect workers coordinate using Kafka.

Important internal topics include:

* `config.storage.topic`
* `offset.storage.topic`
* `status.storage.topic`

Conceptually:

```text
        Connect Worker 1 
              | 
        Connect Worker 2 
              | 
        Connect Worker 3 
              | 
              v 
   +----------------------+ 
   | Kafka Connect Topics | 
   +----------------------+ 
    |         |           | 
  config   offsets      status
```

## 14. Config Topic

The configuration topic stores connector configuration in distributed mode.

Conceptually:

```text
        config topic 
              | 
              +-- connector A configuration 
              +-- connector B configuration 
              +-- connector C configuration
```

This allows the Connect cluster to share connector configuration.

## 15. Offset Topic

The offset topic stores source or sink progress. For example:

```text
Source:
database position = 1,235,678

Sink:
Kafka partition 2 
offset = 98,421
```

Connect persists this state. If a task restarts, it can resume from the stored position.

## 16. Status Topic

The status topic stores information about:

* workers
* connectors
* tasks

For example:

```text
connector=orders-source 
status=RUNNING

task=0 
status=RUNNING

task=1 
status=FAILED
```

This status information is exposed through the Connect REST API as well.

## 17. Internal Topics Must Be Reliable

The Connect internal topics are critical. If they are lost or corrupted, Connect may lose:

* connector configurations
* offsets
* task status

Production deployments should therefore configure them carefully. Consider `replication.factor >= 3` for important
production internal topics when the cluster topology supports it. Also consider:

* cleanup policies
* replication
* access control
* availability
* monitoring

## 18. Connector Configuration

A connector configuration typically contains:

```properties
name=orders-source
connector.class=...
tasks.max=3
```

Then connector-specific properties. For example:

```properties
database.hostname=db.example.com
database.port=5432
database.user=...
database.password=...
```

The exact properties depend on the connector implementation.

## 19. tasks.max

One of the most important Connect configuration properties is `tasks.max=3`. It specifies the maximum number of tasks
that the connector can use. It does not necessarily mean that three tasks will always run. The connector determines how
much parallelism it can actually use.

Therefore `tasks.max=10` does not guarantee **10 active tasks**

## 20. Tasks and Parallelism

Suppose:

`tasks.max=4` and the connector can split its workload into four independent units.

Then:

```text
        Connector 
            | 
            +-- Task 0 
            +-- Task 1 
            +-- Task 2 
            +-- Task 3
```

But if the source can only support two parallel tasks:

```text
        Connector 
            | 
            +-- Task 0 
            +-- Task 1
```

Increasing `tasks.max` does not magically create more parallelism.

## 21. Connector Rebalancing

In distributed mode, tasks can move between workers.

Example:

```text
Before:

Worker 1 
Task A 
Task B

Worker 2 
Task C

Worker 3 
Task D
```

Worker 2 fails. Connect can rebalance:

```text
Worker 1 
Task A 
Task B

Worker 3 
Task C 
Task D
```

This is one of the key benefits of distributed mode.

## 22. Connector Failure

A connector can fail independently of the Connect cluster.

Example:

```text
          Worker 
            | 
            +-- Connector A -> RUNNING 
            | 
            +-- Connector B -> FAILED
```

The entire Connect worker does not necessarily have to fail because one connector failed. This distinction is important
operationally.

## 23. Connector REST API

Kafka Connect provides a REST API. Typical endpoint:

```text
http://connect-host:8083
```

List connectors:

```bash
curl http://connect-host:8083/connectors
```

Get connector configuration:

```bash
curl http://connect-host:8083/connectors/orders-source/config
```

Get status:

```bash
curl http://connect-host:8083/connectors/orders-source/status
```

## 24. Creating a Connector

A connector can be created using REST.

Example:

```bash
curl -X POST \
    -H "Content-Type: application/json" \
    http://connect-host:8083/connectors \
    -d '{
        "name": "orders-source",
        "config": {
        "connector.class": "...",
        "tasks.max": "3"
        } 
    }'
```

The exact connector-specific configuration depends on the plugin.

## 25. Connector Lifecycle

Typical states include:

```text
UNASSIGNED 
RUNNING 
PAUSED 
FAILED
```

A connector also has task states.

Example:

```text
Connector:
RUNNING

Tasks:
Task 0 -> RUNNING 
Task 1 -> RUNNING 
Task 2 -> FAILED
```

A connector can therefore appear healthy while an individual task has failed. Always inspect both connector and task
status.

## 26. Pause and Resume

Connect supports pausing connectors.

Conceptually:

```text
        RUNNING 
           | 
           v 
         PAUSED 
           | 
           v 
        RUNNING
```

Pause is useful for operational procedures such as:

* maintenance
* downstream outage
* controlled migration
* troubleshooting

## 27. Restarting Tasks

A failed task can often be restarted without restarting the entire Connect cluster.

For example:

```bash
curl -X POST \
  http://connect-host:8083/connectors/orders-source/tasks/1/restart
```

The exact API behavior depends on Kafka Connect version.

The important operational concept is **Connector, task, and worker have different lifecycle scopes.**

## 28. Converters

Converters transform between Kafka's internal record representation and the serialized data exchanged with Kafka. Common
converters include:

* JSON
* Avro
* Protobuf
* String
* ByteArray

Conceptually:

```text
        External data 
             | 
             v 
          Connector 
             | 
             v 
          Converter 
             | 
             v 
         Kafka record
```

## 29. Converter vs Serializer

Do not confuse Kafka Connect converters with Kafka producer serializers.

A regular Kafka producer uses:

```text
Serializer
```

Kafka Connect uses:

```text
Converter
```

Conceptually:

```text
        Kafka Producer 
             | 
         Serializer 
             | 
           Kafka
```

while:

```text
        Kafka Connect 
             | 
         Converter 
             | 
           Kafka
```

This distinction is frequently tested.

## 30. JSON Converter

A JSON converter can represent records as JSON.

Example:

```Json
{
  "id": 100,
  "customer": "Alice",
  "amount": 250
}
```

JSON is easy to inspect but may not provide the same schema governance capabilities as a schema-based format.

## 31. Avro Converter

With Schema Registry, Kafka Connect can use Avro.

Architecture:

```text
        Kafka Connect 
             | 
       Avro Converter 
             | 
             +------> Schema Registry 
             | 
             v 
           Kafka
```

Schema Registry stores schema definitions.

Kafka records reference schemas rather than embedding the entire schema repeatedly in the same way JSON often does.

## 32. Protobuf Converter

Protobuf is another schema-based serialization approach.

Architecture:

```text
        Kafka Connect 
             | 
       Protobuf Converter 
             | 
             +------> Schema Registry 
             | 
             v 
           Kafka
```

Protobuf can provide strongly defined message structures and efficient serialization.

## 33. JSON Schema

JSON Schema is another schema format supported in Kafka ecosystem integrations.

Conceptually:

```text
         JSON
          +
        Schema
```

This provides more structure than arbitrary JSON.

## 34. Schema Registry Integration

A common production architecture is:

```
            Schema Registry
                  |
                  |
         +--------+--------+
         |                 |
     Producer           Connect
         |                 |
         +--------+--------+
                  |
                 Kafka
```

Connect can use Schema Registry-aware converters.

Important:

**Schema Registry is not part of the Kafka broker itself**.

It is a separate service.

## 35. key.converter and value.converter

Connect commonly distinguishes key and value conversion.

Example:

```properties
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=io.confluent.connect.avro.AvroConverter
```

Do not assume key and value must use the same converter.

## 36. Schemas in Kafka Connect

Connect's internal data model supports structured records. A record can contain:

```text
        Schema
          +
        Value
```

Example:

```text
Schema: 
    id -> INT64 
    customer -> STRING 
    amount -> DECIMAL

Value: 
    100 
    Alice 
    250.00
```

This allows converters to serialize structured records appropriately.

## 37. Single Message Transforms

Single Message Transforms are abbreviated **SMT**. SMTs modify individual records as they pass through Connect.

Conceptually:

```text
        Source 
          | 
          v 
        Record 
          | 
          v 
         SMT 
          | 
          v 
        Kafka
```

Or:

```text
        Kafka 
          | 
          v 
         SMT 
          | 
          v 
         Sink
```

## 38. Common SMT Uses

SMTs can be used for:

* renaming fields
* adding fields
* removing fields
* modifying topic names
* modifying record keys
* filtering or routing in supported patterns
* masking fields in supported transformations

Example:

```text
        topic: 
        orders.raw
            |
            v
       RegexRouter SMT
            |
            v
      orders.production
```

## 39. SMT Is Not a Stream Processing Engine

A common mistake is using SMTs for complex business logic. SMTs are intended for lightweight per-record transformations.
For complex operations such as:

* joins
* aggregations
* windowing
* stateful processing
* complex event transformations

Kafka Streams or another processing system may be more appropriate.

## 40. Error Handling

Kafka Connect provides configurable error handling. Possible strategies include:

* `FAIL`
* `IGNORE`
* `DEAD LETTER QUEUE`

The exact configuration depends on connector behavior and Kafka Connect version.

## 41. Fail on Error

The strictest strategy is to fail the task.

Conceptually:

```text
        Record 
          | 
          v 
    Processing error 
          | 
          v 
      TASK FAILED
```

This is appropriate when data loss or silent skipping is unacceptable.

## 42. Ignore Errors

Another approach is to continue processing.

```text
Record 1 -> SUCCESS 
Record 2 -> ERROR -> IGNORE 
Record 3 -> SUCCESS
```

This can prevent a single bad record from stopping a pipeline.

However **Ignoring errors can cause data loss**. Therefore, it must be chosen intentionally.

## 43. Dead Letter Queue

A Dead Letter Queue is commonly abbreviated **DLQ**

Instead of stopping:

```text
        Bad record 
            | 
            v 
           DLQ
```

while good records continue.

Architecture:

```text
        Kafka 
          | 
          v 
     Sink Connector 
          | 
          +---- good record ----> Target 
          | 
          +---- bad record -----> DLQ topic
```

This is a very useful production pattern.

## 44. DLQ Metadata

A DLQ can contain information about the failure. Depending on configuration, useful metadata may include:

* original topic
* partition
* offset
* exception information
* connector/task information

This makes operational investigation easier.

## 45. Error Handling Strategy

A production decision should consider:

```text
        Can we tolerate skipping records? 
            |                    |  
           no                   yes 
            |                    | 
           FAIL             IGNORE / DLQ
```

For business-critical data pipelines, DLQs often provide a useful compromise:

```text
Do not stop the entire pipeline
              +  
Do not silently discard the bad record
```

## 46. Retry Configuration

Connect can retry certain failures.

Conceptually:

```text
        Record 
           | 
           v 
        Failure 
           | 
           v 
         Retry 
           | 
           +---- success 
           | 
           +---- failure 
           | 
           v 
       DLQ / FAIL
```

Retry configuration should be designed carefully. Retries are useful for transient failures but do not solve permanent
data errors.

## 47. Offset Management

Connect must know where it is in a data source or Kafka topic.

For source connectors:

```text
        External system 
               | 
               v 
        Source position 
               | 
               v 
     Connect offset storage
```

For sink connectors:

```text
        Kafka partition 
              | 
              v 
        Consumed offset 
              | 
              v 
     Connect offset storage
```

## 48. Source Offsets

Suppose a database connector processes:

```text
Transaction ID = 5000
```

Connect records that position. After restart:

```text
Resume from approximately: 
Transaction ID = 5000
```

The exact offset representation depends on the connector. Examples can include:

* database log position
* timestamp
* sequence number
* file position

## 49. Sink Offsets

For sink connectors, offsets are related to Kafka partitions.

Example:

```text
Topic: orders

Partition 0 -> offset 1000 
Partition 1 -> offset 900 
Partition 2 -> offset 1200
```

The sink connector uses stored progress to determine where to continue.

## 50. Delivery Semantics

Kafka Connect pipelines must be evaluated for delivery semantics. Possible outcomes can include:

* at-most-once
* at-least-once
* exactly-once

The actual guarantee depends on:

* connector implementation
* source system
* sink system
* configuration
* transaction support

Do not assume **Kafka Connect automatically provides exactly-once semantics for every connector**.

## 51. At-Least-Once and Duplicates

Suppose:

```text
Kafka offset 100
```

is written to a database. The database write succeeds. Before Connect records the corresponding progress, the task
crashes. After restart:

```text
Kafka offset 100
```

may be processed again.

Result:

```text
duplicate write
```

Therefore, sinks often need idempotent behavior or deduplication.

## 52. Idempotent Sinks

An idempotent sink produces the same final result if the same record is processed multiple times.

For example:

```SQL
INSERT ...ON CONFLICT DO
UPDATE
```

can provide a form of idempotent behavior. This is an important architectural consideration for Connect pipelines.

## 53. Exactly-Once Considerations

Exactly-once behavior requires more than simply setting a configuration property. You need compatible semantics across:

```text
        Source
           + 
        Connect
           +       
         Kafka
           +
         Sink
```

If the external system cannot participate in the required transactional semantics, true end-to-end exactly-once
processing may not be possible.

## 54. Connector Plugins

Connectors are implemented as plugins. A worker must be able to discover the connector plugin. Typical configuration
includes:

```properties
plugin.path=/usr/share/java,/usr/share/confluent-hub-components
```

If a connector class cannot be found, you may see errors such as:

```text
ClassNotFoundException
```

or:

```text
Failed to find any class that implements Connector
```

## 55. Plugin Isolation

Connector plugins can have their own dependencies. Poor dependency isolation can result in:

```text
Class conflict 
NoSuchMethodError 
ClassNotFoundException

```

Production Connect deployments should manage plugin versions carefully. Avoid blindly installing incompatible versions
of connector libraries.

## 56. Connector Plugin Discovery

Useful REST endpoint:

```bash
curl http://connect-host:8083/connector-plugins
```

This helps verify that the worker sees installed connector plugins. If your connector is missing from this endpoint,
investigate:

* plugin path
* installation
* permissions
* package structure
* worker restart
* dependency compatibility

## 57. Connect REST API and Distributed Mode

In distributed mode, the REST API can be exposed by each worker. The cluster handles connector ownership and
coordination. When administering a distributed cluster, think:

```text
       REST request 
            | 
            v 
       Connect worker 
            | 
            v 
  Connect cluster coordination 
            | 
            v 
    Connector/task assignment
```

## 58. Connect Security

Kafka Connect itself should be secured. Security considerations include:

### REST API

Protect the Connect REST interface.

### Kafka

Secure:

* worker → Kafka
* internal topics
* connector data topics

### External systems

Secure:

* database connections
* cloud services
* REST APIs
* object storage

### Secrets

Protect:

* passwords
* API tokens
* certificates
* private keys

## 59. Kafka ACLs for Connect

Connect workers may need access to Kafka topics.

For example:

```text
        Connect worker 
              |
              +--> READ source topics 
              |
              +--> WRITE sink/internal topics 
              | 
              +--> READ/WRITE internal topics
```

The exact permissions depend on connector type and configuration. A common production failure is:

```text
Kafka connectivity: OK 
Authentication: OK 
Authorization: DENIED
```

## 60. Connect and Schema Registry Security

If using Schema Registry:

```text
        Connect 
           | 
           | TLS 
           | authentication 
           v 
     Schema Registry
```

The worker may need credentials or certificates to:

* retrieve schemas
* register schemas
* update schemas

Depending on the converter and connector configuration.

## 61. Monitoring Kafka Connect

Monitor:

**Workers**

* CPU
* Memory
* GC
* Threads
* Network

**Connectors**

* RUNNING
* PAUSED
* FAILED

**Tasks**

* RUNNING
* FAILED

**Throughput**

* records processed
* records/sec
* bytes/sec

**Errors**

* failed records
* retries
* DLQ records

## 62. Important Connect Metrics

Useful metrics include:

1. [ ] source-record-poll-rate
2. [ ] source-record-write-rate
3. [ ] sink-record-read-rate
4. [ ] sink-record-send-rate
5. [ ] task-error-metrics

Exact metric names can vary by version and monitoring integration.

The important idea is to monitor:

1. [ ] throughput
2. [ ] latency
3. [ ] errors
4. [ ] retries
5. [ ] task health

## 63. Scaling Kafka Connect

Suppose:

```text
100 million records/day
```

and one worker cannot process the workload.

You can scale horizontally:

1. Worker 1
2. Worker 2
3. Worker 3
4. Worker 4

Connect distributes tasks among workers.

But remember **Workers available != Parallelism automatically available**

The connector must support the desired task parallelism.

## 64. Scaling tasks.max

A common tuning mistake is:

```text
        Performance problem 
                | 
                v 
          tasks.max = 100
```

This is not necessarily correct.

First determine:

* source parallelism
* destination throughput
* partition count
* connector limitations
* CPU
* network
* external-system capacity

Then tune task count.

## 65. Backpressure

Consider:

```text
        Kafka 
          | 
          v 
     Sink Connector 
          | 
          v 
       Database
```

If the database is slow:

```text
Kafka production rate > database consumption rate
```

the sink can fall behind.

Monitor:

* consumer lag
* task throughput
* database latency

Adding tasks may help only if the database can support more concurrent writes.

## 66. Connector Restart vs Worker Restart

If:

```text
Task 2 -> FAILED
```

you may restart the task if:

```text
Worker -> unhealthy
```

you may restart or replace the worker. Do not automatically restart the entire Connect cluster for a single connector
problem. Operational scope matters.

## 67. Common Problem: Connector Not Found

Error:

```text
ClassNotFoundException
```

Check:

1. plugin.path
2. connector installation
3. worker filesystem
4. permissions
5. dependency compatibility

Then verify:

```bash
curl http://connect-host:8083/connector-plugins
```

## 68. Common Problem: Task Keeps Failing

Investigate:

1. connector configuration
2. external system
3. authentication
4. authorization
5. malformed records
6. schema compatibility
7. converter configuration
8. serialization errors
9. network connectivity
10. connector-specific limitations

Do not repeatedly restart the task without identifying the cause.

## 69. Common Problem: Serialization Error

Example:

```text
SerializationException 
DataException 
Unknown magic byte
```

Possible causes:

* wrong converter
* incorrect serialization format
* Schema Registry mismatch
* incompatible producer/consumer expectations
* corrupted or unexpected data

Check:

* key.converter
* value.converter
* schemas
* Schema Registry
* topic data format

## 70. Common Problem: Schema Registry Failure

Example:

```text
Unable to connect to Schema Registry
```

Check:

* URL
* DNS
* network
* TLS
* credentials
* permissions
* schema subject
* compatibility settings

## 71. Common Problem: Consumer Lag in Sink Connector

If a sink connector is falling behind:

```text
        Kafka 
          | 
          | records 
          v 
    Sink Connector 
          | 
          | too slow 
          v 
    External system
```

Check:

* sink throughput
* number of tasks
* destination latency
* connector batching
* network
* Kafka partition count
* worker CPU
* worker memory

## 72. Kafka Connect vs Kafka Streams

This distinction is extremely important.

**Kafka Connect** is designed for:

```text
Integration
```

Example:

```text
Kafka -> Elasticsearch 
PostgreSQL -> Kafka 
```

**Kafka Streams** is designed for:

```text
Stream processing
```

Example:

```text
        orders 
          | 
          v 
         Join 
          | 
          v 
      Aggregate 
          | 
          v 
    customer-orders
```

## 73. Connect vs Streams Comparison

| Feature             | Kafka Connect            | Kafka Streams              | 
|---------------------|--------------------------|----------------------------|
| Main purpose        | Integration              | Stream processing          |
| Source/Sink systems | Yes                      | Can interact through Kafka |
| Stateful processing | Limited                  | Yes                        |
| Joins               | No general-purpose joins | Yes                        |
| Windows             | No                       | Yes                        |
| Aggregations        | Not primary purpose      | Yes                        |
| SMTs                | Yes                      | No                         |
| REST management     | Yes                      | Application-specific       |
| Worker cluster      | Yes                      | No Connect worker concept  |

Memorize:
```text
Connect = MOVE DATA
Streams = PROCESS DATA
```

## 74. Certification Trap: tasks.max

Question:

**Setting tasks.max=20 guarantees 20 tasks.**

Answer:

**False**.

It specifies the maximum number of tasks.
The connector determines how many tasks can actually be created.

## 75. Certification Trap: Worker Count

Question:

**Adding more workers always increases connector throughput.**

Answer:

**False**.

Additional workers provide capacity, but throughput depends on:
number of tasks connector implementation partitioning external-system capacity bottlenecks

## 76. Certification Trap: Source vs Sink

Remember:
```text
SOURCE 
External -> Kafka

SINK 
Kafka -> External
```

Do not reverse these.

## 77. Certification Trap: Internal Topics

Connect's distributed mode depends on internal topics.

Important:

1. [ ] **config.storage.topic** 
2. [ ] **offset.storage.topic** 
3. [ ] **status.storage.topic**

These are not application data topics.
They store Connect cluster state.

## 78. Certification Trap: Converter vs Connector

* **A connector determines**: How to communicate with an external system.
* **A converter determines**: How Kafka Connect records are serialized/deserialized.

They are different layers.

## 79. Certification Trap: SMT vs Streams

SMT:
```text
simple per-record transformation
```

Streams:
```text
complex stream processing
```
Do not use SMTs as a replacement for Kafka Streams.

## 80. Certification Trap: Exactly Once

Never assume:
```text
Kafka Connect = exactly once
```

Instead ask:

1. Does this specific connector support the required semantics? 
2. Does the source support them? Does the sink support them?
3. Does the configuration enable them?

## 81. Developer Certification Questions 

_Question 1_

**What is the primary purpose of Kafka Connect?**

A. Replace Kafka brokers 
B. Integrate Kafka with external systems 
C. Perform all stream processing 
D. Manage Kafka partitions

Answer: **B**

_Question 2_

**A source connector moves data in which direction?**

A. Kafka → database 
B. Database → Kafka 
C. Kafka → Kafka Streams 
D. Broker → controller

Answer: **B**

_Question 3_

**What performs the actual data movement in Kafka Connect?**

A. Worker configuration 
B. Connector metadata 
C. Tasks 
D. Schema Registry

Answer: **C**

_Question 4_

**What does `tasks.max` represent?**

A. Maximum number of workers 
B. Maximum number of tasks for a connector 
C. Number of Kafka partitions 
D. Maximum records per second

Answer: **B**

_Question 5_

**Which internal topic stores connector offsets?**

A. Config topic 
B. Status topic 
C. Offset topic 
D. Data topic

Answer: **C**

_Question 6_

**Which component determines how Kafka Connect records are serialized?**

A. Worker 
B. Converter 
C. Connector 
D. Task scheduler

Answer: **B**

_Question 7_

**Which component is most appropriate for complex joins and stateful aggregations?**

A. SMT 
B. Kafka Connect 
C. Kafka Streams 
D. Converter

Answer: **C**

_Question 8_

**What is a DLQ used for?**

A. Store Kafka broker metadata 
B. Store records that cannot be processed successfully 
C. Store consumer offsets 
D. Store connector configuration

Answer: **B**

_Question 9_

**What happens when a Connect worker fails in distributed mode?**

A. The Kafka cluster necessarily fails
B. Tasks can be reassigned to other workers 
C. All Kafka topics are deleted 
D. Schema Registry stops

Answer: **B**

_Question 10_

**Which endpoint can be used to inspect connector plugins?**

A. /topics 
B. /connector-plugins 
C. /plugins/status 
D. /schema/plugins

Answer: **B**

## 82. Administrator Questions 

_Question 11_

**Which three internal topics are especially important in distributed Connect?**

A. input/output/error 
B. config/offset/status 
C. producer/consumer/admin 
D. source/sink/schema

Answer: **B**

_Question 12_

**A connector configuration exists, but the connector class cannot be loaded. What should be checked first?**

A. Partition leader election 
B. plugin.path and connector installation 
C. Consumer offset reset 
D. Replication factor of application topics

Answer: **B**

_Question 13_

**A sink task repeatedly fails because it cannot authenticate to the destination database. Increasing tasks.max is
unlikely to fix the problem.**

Answer: **True**

Authentication must be fixed first.

_Question 14_

**A sink is falling behind because the destination database is saturated. Adding more tasks could make the situation
worse.**

Answer: **True**

More tasks can increase concurrent load against the destination.

_Question 15_

**What is the main benefit of distributed Connect mode?**

A. Removes the need for Kafka 
B. Provides scalability and fault tolerance 
C. Eliminates serialization 
D. Eliminates connectors

Answer: **B**

## 83. Advanced Scenario

You have:
```text
      Kafka 
        | 
        | 50,000 records/sec 
        v 
  Sink Connector 
        | 
        v 
    PostgreSQL
```

The sink is processing:
```text
10,000 records/sec
```

and lag continuously increases.

Possible investigation:

1. PostgreSQL throughput
2. connector task count
3. Kafka partition count
4. batch configuration
5. worker CPU
6. worker network
7. database connection pool
8. indexes / locks
9. transaction latency

Do not immediately assume **`tasks.max` must be increased**

First identify the bottleneck.

## 84. Advanced Scenario: Worker Failure

Initial state:
```text
Worker 1 
Task A 
Task B

Worker 2 
Task C 
Task D

Worker 3 
Task E
```

Worker 2 fails.

In distributed mode:
```text
Worker 1 
Task A 
Task B 
Task C?

Worker 3 
Task D? 
Task E
```

Tasks can be redistributed. The exact assignment depends on Connect's coordination and current cluster state.

## 85. Advanced Scenario: Bad Record

Suppose:
```text
Record 100 -> SUCCESS 
Record 101 -> INVALID 
Record 102 -> SUCCESS
```

With fail-on-error:
```text
         Task 
          | 
      Record 101 
          | 
        ERROR 
          | 
      TASK FAILED
```

With DLQ:
```text
        Record 101 
        | +----> DLQ

        Record 102 
        | +----> SUCCESS
```

This is why DLQs are useful in data pipelines.

## 86. Production Architecture

A mature Connect deployment might look like:

```text
                    +------------------+
                    | Schema Registry  |
                    +------------------+
                            |
                            |
 +-------------+       +----+-----+       +--------------+
 | PostgreSQL  |------>|          |       | Elasticsearch|
 +-------------+       | Connect  |------>|              |
                       | Cluster  |       +--------------+
 +-------------+       |          |
 | Other Source|------>| Worker 1 |
 +-------------+       | Worker 2 |
                       | Worker 3 |
                       +----+-----+
                            |
                            v
                      +-----------+
                      |  Kafka    |
                      |  Cluster  |
                      +-----------+
```

Security:

* TLS 
* SASL 
* ACLs 
* Secrets 
* Network segmentation 
* Monitoring

Operational requirements:

* High availability 
* Internal topic replication 
* Connector monitoring 
* Task monitoring 
* DLQ monitoring 
* Capacity planning

## 87. Hands-On Lab 
### Lab Objective

Deploy a small Kafka Connect cluster and practice:

1. Plugin discovery 
2. Connector creation 
3. Task inspection 
4. REST API management 
5. Converter configuration 
6. Error handling 
7. DLQ
8. Connector restart 
9. Distributed workers 

* **Step 1** — Verify Connect
```bash
  curl http://localhost:8083/
```
Expected response contains version information.

* **Step 2** — List Connectors 
```bash
  curl http://localhost:8083/connectors
```
* **Step 3** — List Plugins
```bash
  curl http://localhost:8083/connector-plugins
```

Verify that your desired connector appears.

* **Step 4** — Create a Connector 
```bash
  curl -X POST http://localhost:8083/connectors 
    -H "Content-Type: application/json"  
    -d '{ 
          "name": "demo-source", 
          "config": { 
            "connector.class": "YOUR_CONNECTOR_CLASS", 
            "tasks.max": "2" 
            } 
         }' 
```

* **Step 5** — Inspect Status 
```bash
  curl  http://localhost:8083/connectors/demo-source/status 
```

Check:

1. [x] connector state
2. [x] task states
3. [x] worker assignment 

**Step 6** — Inspect Configuration 
curl http://localhost:8083/connectors/demo-source/config 

**Step 7** — Pause 

```bash
curl -X PUT http://localhost:8083/connectors/demo-source/pause
```

**Step 8** — Resume

```bash
curl -X PUT http://localhost:8083/connectors/demo-source/resume 
```

**Step 9** — Delete 

```bash
curl -X DELETE http://localhost:8083/connectors/demo-source
```

## 88. Certification Memory Map

Memorize this diagram:

```
              Kafka Connect
                   |
          +--------+--------+
          |                 |
       SOURCE              SINK
          |                 |
  External -> Kafka     Kafka -> External
          |                 |
       Connector          Connector
          |                 |
        Tasks             Tasks
          |                 |
      Converter          Converter
          |                 |
        Kafka             Kafka
```

Distributed mode:

```
         Connect Cluster
                |
   +------------+------------+
   |            |            |
Worker 1     Worker 2     Worker 3
   |            |            |
   +------------+------------+
                |
         Internal Topics
      /         |         \
  config      offsets     status
```

## 89. Final Cheat Sheet 
```text
Kafka Connect 
    = integration framework

Source Connector 
    = external -> Kafka

Sink Connector 
    = Kafka -> external

Worker 
    = Connect process/runtime

Connector 
    = integration definition/coordination

Task 
    = actual data movement

tasks.max 
    = maximum task count

Standalone 
    = simple/single process

Distributed 
    = scalable/fault-tolerant cluster

Config Topic 
    = connector configuration

Offset Topic 
    = connector progress

Status Topic 
    = worker/connector/task status

Converter 
    = serialization/deserialization

SMT 
    = per-record transformation

DLQ 
    = failed records

REST API 
    = Connect administration

Connect 
    = MOVE DATA

Streams 
    = PROCESS DATA
```

## 91. Final Takeaways

The most important certification concepts are:

1. [ ] Kafka Connect is an integration framework. 
2. [ ] Source connectors move external data into Kafka. 
3. [ ] Sink connectors move Kafka data into external systems. 
4. [ ] Workers provide the runtime environment. 
5. [ ] Tasks perform the actual data movement. 
6. [ ] `tasks.max` is a maximum, not a guarantee. 
7. [ ] Distributed mode provides scalability and fault tolerance. 
8. [ ] Config, offset, and status topics are critical internal topics. 
9. [ ] Converters control serialization/deserialization. 
10. [ ] Schema Registry is separate from Kafka brokers. 
11. [ ] SMTs are lightweight per-record transformations. 
12. [ ] DLQs prevent bad records from necessarily stopping an entire pipeline. 
13. [ ] Connector plugins must be installed and discoverable through plugin.path. 
14. [ ] Connect offsets enable progress tracking and recovery. 
15. [ ] Exactly-once semantics depend on the complete source-to-sink architecture. 
16. [ ] Kafka Connect is primarily for integration; Kafka Streams is for stream processing. 
17. [ ] Production Connect requires security, monitoring, capacity planning, and reliable internal topics.