# Part I — Kafka Fundamentals
## Module 1 — Meet Kafka

Based primarily on Definitive Guide Chapter 1.

You will master:

- Event streaming
- Publish/subscribe
- Topics
- Partitions
- Records
- Keys
- Values
- Headers
- Offsets
- Brokers
- Clusters
- Producers
- Consumers
- Consumer groups
- Replicas
- Leaders and followers
- ISR
- Retention
- Log segments
- Kafka's architecture
- Certification focus

### CCDAK

- Understand Kafka architecture
- Understand message structure
- Understand partitioning
- Understand consumer groups
- Understand offsets

### CCAAK

- Understand cluster architecture
- Understand brokers
- Understand replication
- Understand durability
- Understand HA

# Part II — Kafka Architecture Deep Dive
## Module 2 — Topics, Partitions and Replication

This is where we'll go much deeper than the book's introductory explanation.

You will learn:

Topic: `orders`

```text
Partition 0
├── offset 0
├── offset 1
├── offset 2
└── offset 3
```

```text
Partition 1
├── offset 0
├── offset 1
└── offset 2
```

```text
Partition 2
├── offset 0
├── offset 1
└── offset 2
```

Then:

```text
                    Topic: orders


             ┌───────────────┐
             │ Partition 0   │
             └───────┬───────┘
                     │
          ┌──────────┼──────────┐
          ▼          ▼          ▼
       Broker 1   Broker 2   Broker 3
       Leader     Follower   Follower
```

We'll investigate:

- `replication.factor`
- leader election
- ISR
- `min.insync.replicas`
- `acks`
- unclean leader election
- replica lag
- preferred replica election
- partition reassignment
- partition count
- partition ordering
- partition scalability

And we'll repeatedly ask the certification question:

> What actually happens inside Kafka when X fails?

# Part III — Storage
## Module 3 — Kafka Storage Internals

Based on the storage sections of The Definitive Guide.

Topics:

- append-only log
- log segments
- segment rolling
- indexes
- offset index
- time index
- retention
- deletion
- compaction
- tombstones
- disk usage
- filesystem considerations
- page cache
- sequential I/O

We'll build scenarios such as:

```text
orders-0/

00000000000000000000.log
00000000000000000000.index
00000000000000000000.timeindex

00000000000000123456.log
00000000000000123456.index
00000000000000123456.timeindex
```

And answer:

- Why doesn't Kafka store every message in a database?
- How does Kafka find offset 1,250,000 efficiently?
- What happens when retention deletes a segment?

# Part IV — Producers
## Module 4 — Kafka Producer Internals

This is a major CCDAK area.

You will learn the complete producer pipeline:

```text
Application
│
▼
KafkaProducer
│
▼
Serializer
│
▼
Partitioner
│
▼
RecordAccumulator
│
▼
Batch
│
▼
Network Thread
│
▼
Broker
│
▼
Partition Leader
```

Deep topics:

- `bootstrap.servers`
- serializers
- keys
- partitioners
- batching
- compression
- `linger.ms`
- `batch.size`
- `buffer.memory`
- `max.in.flight.requests.per.connection`
- `acks`
- retries
- delivery timeout
- request timeout
- idempotence
- producer sequence numbers
- duplicate messages
- ordering guarantees
- producer transactions

We'll also create Java/Kotlin producer laboratories.

# Part V — Consumers
## Module 5 — Consumer Internals

This will be one of the largest modules.

```text
                   Kafka Cluster


        P0       P1       P2       P3
        │        │        │        │
        ▼        ▼        ▼        ▼


      Consumer Group A
       │        │        │
       ▼        ▼        ▼
      C1       C2       C3
```

You'll master:

- `poll()`
- fetch requests
- offsets
- committed offsets
- position
- consumer groups
- group coordinator
- partition assignment
- rebalancing
- cooperative rebalancing
- static membership
- heartbeats
- `session.timeout.ms`
- `max.poll.interval.ms`
- `enable.auto.commit`
- manual commit
- synchronous commit
- asynchronous commit
- offset reset
- consumer lag

And the classic certification scenarios:

- Consumer crashes after processing but before committing.
- Consumer commits before processing.
- Consumer dies during rebalance.
- Partition count increases.
- Consumer group has more consumers than partitions.

# Part VI — Reliability & Delivery Semantics
## Module 6 — Kafka Delivery Guarantees

We will make this extremely practical.

```text
                 DELIVERY SEMANTICS


              ┌────────────────────┐
              │      Kafka         │
              └─────────┬──────────┘
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
      At-most-once   At-least-once  Exactly-once
```

You'll understand precisely:

- at-most-once
- at-least-once
- exactly-once
- idempotent producer
- transactions
- transactional IDs
- producer epochs
- consumer isolation
- `read_committed`
- `read_uncommitted`
- offset commits in transactions

And importantly:

> exactly-once processing vs exactly-once delivery vs exactly-once effects.

# Part VII — Schemas
## Module 7 — Schema Management

We'll cover:

- Avro
- JSON Schema
- Protobuf
- Schema Registry
- subjects
- schema versions
- compatibility
- backward compatibility
- forward compatibility
- full compatibility
- schema evolution
- serialization/deserialization

Example:

```text
Producer
│
▼
Avro Serializer
│
▼
Schema Registry
│
▼
Kafka
│
▼
Avro Deserializer
│
▼
Consumer
```

We'll create schema-evolution exercises.

# Part VIII — Kafka Connect
## Module 8 — Kafka Connect

CCDAK and CCAAK both require knowledge of Connect.

You'll learn:

```text
              Kafka Connect


        ┌─────────────────────┐
        │    Connect Cluster  │
        │                     │
        │ Worker 1            │
        │ Worker 2            │
        │ Worker 3            │
        └──────────┬──────────┘
                   │
        ┌──────────┼──────────┐
        ▼          ▼          ▼
      Source     Source     Sink
     Connector  Connector  Connector
        │          │          │
        ▼          ▼          ▼
      DB/API     Files      Elasticsearch
```

Topics:

- workers
- connectors
- tasks
- source connectors
- sink connectors
- distributed mode
- standalone mode
- internal topics
- offsets
- configs
- status
- error handling
- DLQ
- transformations
- converters
- scaling

# Part IX — Kafka Streams
## Module 9 — Kafka Streams

We'll cover:

- Streams DSL
- Processor API
- topology
- `KStream`
- `KTable`
- `GlobalKTable`
- state stores
- repartition topics
- changelog topics
- joins
- windows
- aggregations
- exactly-once processing
- state recovery

Architecture:

```text
Kafka Topic
│
▼
KStream
│
├──────────────┐
▼              ▼
filter()          map()
│              │
└──────┬───────┘
▼
groupByKey()
│
▼
aggregate()
│
▼
KTable
│
▼
Output Topic
```
# Part X — Administration

This is where the curriculum shifts toward CCAAK.

## Module 10 — Kafka Administration

We'll master:

- `kafka-topics`
- `kafka-console-producer`
- `kafka-console-consumer`
- `kafka-consumer-groups`
- `kafka-configs`
- `kafka-reassign-partitions`
- `kafka-storage`
- `kafka-metadata`
- `kafka-acls`
- `kafka-cluster`

And practice:

- create topics
- alter topics
- describe topics
- increase partitions
- configure retention
- configure compaction
- inspect consumer groups
- reset offsets
- alter configurations
- move partitions
- verify replicas
- investigate failures

# Part XI — Kafka Cluster Configuration
## Module 11 — Broker Configuration

We'll build a configuration decision matrix.

For example:

| Configuration | Purpose | Certification importance |
| :--- | :--- | :--- |
| `num.partitions` | Default partition count | ⭐⭐⭐ |
| `default.replication.factor` | Default replication | ⭐⭐⭐ |
| `min.insync.replicas` | Durability | ⭐⭐⭐⭐⭐ |
| `log.retention.ms` | Retention | ⭐⭐⭐⭐ |
| `log.segment.bytes` | Segment rolling | ⭐⭐⭐ |
| `unclean.leader.election.enable` | Availability vs durability | ⭐⭐⭐⭐⭐ |
| `message.max.bytes` | Message limits | ⭐⭐⭐⭐ |
| `replica.fetch.max.bytes` | Replica fetching | ⭐⭐⭐ |
| `auto.create.topics.enable` | Topic creation | ⭐⭐⭐ |
| `num.network.threads` | Network processing | ⭐⭐⭐ |
| `num.io.threads` | Disk processing | ⭐⭐⭐ |

# Part XII — KRaft

Because this is a modern certification path, we will explicitly cover KRaft, rather than relying on older ZooKeeper-based Kafka knowledge.

Topics:

- KRaft architecture
- controllers
- controller quorum
- metadata log
- controller election
- broker/controller roles
- combined mode
- dedicated controllers
- metadata quorum
- cluster ID
- storage formatting
- migrations from ZooKeeper
- failure scenarios

### KRaft Cluster

```text
        Controller 1
              │
        Controller 2 ─── Metadata Quorum
              │
        Controller 3
              │
       ┌──────┼──────┐
       ▼      ▼      ▼
     Broker  Broker  Broker
       1       2       3
```

# Part XIII — Security
## Module 13 — Kafka Security

CCAAK has a dedicated security section worth 15% in the current blueprint.

We'll cover:

```text
             Kafka Security


                  TLS
                   │
        ┌──────────┴──────────┐
        │                     │
Authentication          Encryption
│                     │
┌────┼────┐                │
▼    ▼    ▼                ▼
TLS  SASL OAuth          Data in transit
│
▼
Authorization
│
▼
ACLs
```

Topics:

- SSL/TLS
- SASL
- SASL/SCRAM
- Kerberos
- OAuth
- authentication
- authorization
- ACLs
- encryption in transit
- encryption at rest
- principal
- super users
- listener security
- security protocols

# Part XIV — Networking
## Module 14 — Kafka Networking

This is critical for administrators.

We'll deeply investigate:

```text
Client
│
│ bootstrap.servers
▼
Broker
│
│ metadata response
▼
Client
│
│ advertised.listeners
▼
Correct Broker
```

Topics:

- listeners
- advertised listeners
- listener protocols
- DNS
- TCP
- ports
- NAT
- load balancers
- Docker networking
- Kubernetes networking
- external clients
- internal clients
- multi-network Kafka

This will directly address classic production errors such as:

> Connection to node -1 could not be established

and:

> UnknownHostException

# Part XV — Observability
## Module 15 — Kafka Monitoring

We'll create a complete Kafka observability model:

```text
                    Kafka


                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
      Broker         Producer       Consumer
        │              │              │
        ▼              ▼              ▼
      Metrics        Metrics        Metrics
        │              │              │
        └──────────────┼──────────────┘
                       ▼
                    JMX
                       │
                       ▼
                 Prometheus
                       │
                       ▼
                   Grafana
```

You'll learn:

- throughput
- request latency
- request rate
- network throughput
- disk usage
- CPU
- JVM
- GC
- under-replicated partitions
- offline partitions
- ISR shrink/expand
- consumer lag
- controller metrics
- producer metrics
- consumer metrics

# Part XVI — Troubleshooting
## Module 16 — Production Incident Lab

This will be particularly important for the administrator certification.

We'll create incidents such as:

### Incident 1
Consumer lag increasing

You investigate:

```text
Consumer throughput
↓
poll()
↓
processing time
↓
max.poll.interval.ms
↓
rebalance
↓
lag
```

### Incident 2
`UnderReplicatedPartitions` > 0

You investigate:

```text
Broker
│
├── disk
├── network
├── CPU
├── JVM
└── replica fetcher
```

### Incident 3
`ProducerTimeoutException`

### Incident 4
`NotEnoughReplicasException`

### Incident 5
`LeaderNotAvailableException`

### Incident 6
`RecordTooLargeException`

### Incident 7
Consumer group constantly rebalancing

### Incident 8
Broker disappears

### Incident 9
Disk reaches 95%

### Incident 10
Kafka cluster becomes unavailable

# Part XVII — High Availability & Disaster Recovery
## Module 17 — HA / DR

We'll cover:

- replication
- ISR
- rack awareness
- broker failure
- controller failure
- multi-AZ
- multi-datacenter
- MirrorMaker 2
- active/passive
- active/active
- RPO
- RTO
- cluster recovery
- disaster scenarios

The Definitive Guide contains a dedicated chapter on cross-cluster mirroring and another on security, making it a useful foundation for these subjects.

# Part XVIII — Performance Engineering
## Module 18 — Kafka Performance

We'll investigate performance from first principles:

```text
                 Throughput
                    │
       ┌────────────┼────────────┐
       ▼            ▼            ▼
    Producer      Broker      Consumer
       │            │            │
    batching      disk         fetch
    compression   network      processing
    linger        CPU          poll
```

Topics:

- throughput vs latency
- batching
- compression
- producer tuning
- consumer tuning
- broker tuning
- partition scaling
- disk performance
- network performance
- JVM
- page cache
- GC

# Part XIX — Certification Scenario Engine

This will be one of the most valuable parts of the material.

Instead of only asking:

> What is acks=all?

We'll ask:

> A topic has RF=3 and min.insync.replicas=2. Two brokers fail. What happens when a producer uses acks=all?

Or:

```text
Broker 1 = Leader
Broker 2 = ISR
Broker 3 = ISR

min.insync.replicas = 2
acks = all
```

Then:

```text
Broker 2 fails
│
▼
ISR = {Broker 1, Broker 3}
│
▼
Producer continues
```

But:

```text
Broker 3 fails too
│
▼
ISR = {Broker 1}
│
▼
ISR < min.insync.replicas
│
▼
Producer fails
```

This style of reasoning is what we'll use throughout.

# Part XX — Certification Mock Exams

We'll finish with several examination modes.

### Mock Exam A — Developer Fundamentals
50 questions

### Mock Exam B — Producer / Consumer
50 questions

### Mock Exam C — Schema / Connect / Streams
50 questions

### Mock Exam D — Administrator
50 questions

### Mock Exam E — Security / Networking
50 questions

### Mock Exam F — Operations / Troubleshooting
50 questions

### Mock Exam G — Full CCDAK
90-minute simulation

### Mock Exam H — Full CCAAK
90-minute simulation