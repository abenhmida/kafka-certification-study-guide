# Mock Exam G — Full CCDAK Simulation

## 90-Minute Simulation

**Recommended time:** 90 minutes  
**Questions:** 60  
**Target:** 80%+

> This simulation emphasizes developer-oriented Kafka concepts: producers, consumers, serialization, delivery semantics, transactions, Connect, Streams, and troubleshooting.

## Questions

### 1.
A six-partition topic is consumed by three consumers in one group. What is the maximum partition ownership per consumer in a balanced assignment?

A. 1  
B. 2  
C. 3  
D. 6

### 2.
Why use a record key?

A. Partition selection and logical identity  
B. Encryption  
C. Broker discovery  
D. Consumer authentication

### 3.
What guarantees ordering?

A. Topic  
B. Partition  
C. Consumer group  
D. Broker

### 4.
What happens if the consumer commits before processing completes and then crashes?

A. Possible data loss after restart  
B. Duplicate processing only  
C. Automatic rollback  
D. Kafka transaction

### 5.
What happens if processing completes but commit fails?

A. Record may be processed again  
B. Record is guaranteed lost  
C. Topic is deleted  
D. Partition is recreated

### 6.
Which model does this commonly produce?

A. At-least-once  
B. At-most-once  
C. Exactly-once  
D. Zero-times

### 7.
What is idempotence?

A. Making repeated producer operations safe from duplicate effects within Kafka's supported producer semantics  
B. Increasing partitions  
C. Disabling retries  
D. Resetting offsets

### 8.
What does `acks=all` provide?

A. Strong producer acknowledgment based on ISR/min-ISR configuration  
B. No acknowledgment  
C. Consumer acknowledgment  
D. Schema validation

### 9.
Why combine idempotence and transactions?

A. Support stronger exactly-once processing patterns  
B. Increase partitions  
C. Reduce retention  
D. Disable consumers

### 10.
What is producer fencing?

A. Preventing an obsolete transactional producer instance from continuing  
B. Consumer authentication  
C. Topic deletion  
D. Partition reassignment

### 11.
What does `read_committed` do?

A. Hide aborted transactional records  
B. Ignore committed records  
C. Reset offsets  
D. Disable transactions

### 12.
What is a poison pill?

A. Record that repeatedly causes processing failure  
B. Broker failure  
C. Tombstone only  
D. Consumer group

### 13.
What is a DLQ?

A. Destination for records that cannot be processed normally  
B. Broker metadata store  
C. Partition leader  
D. Schema Registry

### 14.
What is a retry topic?

A. Mechanism for delayed/repeated processing attempts  
B. Replacement for a broker  
C. Security topic  
D. Schema topic

### 15.
Why use backoff?

A. Prevent immediate repeated retries from overwhelming a failing dependency  
B. Increase replication  
C. Reduce partitions  
D. Disable consumer commits

### 16.
What is Kafka Connect?

A. Integration framework  
B. Broker  
C. Schema format  
D. Consumer protocol

### 17.
Source connector direction?

A. External -> Kafka  
B. Kafka -> External  
C. Broker -> Controller  
D. Schema -> Kafka

### 18.
Sink connector direction?

A. Kafka -> External  
B. External -> Kafka  
C. Broker -> Controller  
D. Schema -> Kafka

### 19.
What is a Connect task?

A. Execution unit  
B. Broker  
C. Schema subject  
D. Consumer group

### 20.
Can more tasks always improve throughput?

A. No  
B. Yes  
C. Only with Avro  
D. Only with TLS

### 21.
What is Schema Registry?

A. Schema management service  
B. Broker  
C. Consumer group coordinator  
D. Partition manager

### 22.
Why use compatibility rules?

A. Protect independent producers/consumers from incompatible evolution  
B. Increase partition count  
C. Encrypt data  
D. Reduce lag automatically

### 23.
What is Avro?

A. Serialization format  
B. Consumer group  
C. Broker protocol  
D. Security mechanism

### 24.
What is Kafka Streams?

A. Stream-processing library  
B. Broker cluster  
C. Schema Registry  
D. Connect worker

### 25.
What is a KTable?

A. Table-like changelog/state abstraction  
B. Broker  
C. Topic partition  
D. TLS certificate

### 26.
What is a state store?

A. Local state used by stream processing  
B. Broker metadata  
C. Schema store  
D. Consumer group

### 27.
What is a changelog topic?

A. Durable Kafka-backed representation of state changes  
B. Error topic only  
C. Security topic  
D. Broker log

### 28.
Why repartition?

A. Make records colocated by the required downstream key  
B. Increase replication factor  
C. Reset offsets  
D. Change schema compatibility

### 29.
What is a stream-time window?

A. Window driven by record timestamps observed by the stream task  
B. Broker uptime  
C. Consumer session timeout  
D. Schema timestamp

### 30.
What is a grace period?

A. Allow late records to be included within configured window semantics  
B. Broker restart delay  
C. Consumer session timeout  
D. Producer retry count

### 31.
A keyed workload has severe partition skew. What should be investigated?

A. Key distribution  
B. TLS  
C. Schema Registry  
D. Consumer offsets only

### 32.
A producer sends 1 MB batches but receives high latency. What should be investigated?

A. Batch size, linger, network, broker load, compression, and application latency requirements  
B. Only topic retention  
C. Only ACLs  
D. Only schema compatibility

### 33.
A consumer processes slowly because a downstream database is saturated. What is the best first action?

A. Investigate/relieve the database bottleneck rather than blindly adding consumers  
B. Add unlimited consumers  
C. Increase replication  
D. Reset offsets

### 34.
Why can adding consumers fail to improve throughput?

A. Partition count or downstream bottleneck limits parallelism  
B. Kafka does not support consumers  
C. Consumers always serialize processing  
D. Schema Registry blocks them

### 35.
What is `max.poll.interval.ms` related to?

A. Maximum interval between polls before membership may be considered unhealthy  
B. Topic retention  
C. Producer batching  
D. TLS

### 36.
What is `max.poll.records`?

A. Maximum records returned per poll  
B. Maximum topic partitions  
C. Maximum brokers  
D. Maximum schemas

### 37.
What is cooperative rebalancing?

A. Rebalance protocol that can reduce unnecessary partition revocation/movement  
B. Replication strategy  
C. Security mechanism  
D. Compaction policy

### 38.
What is static membership?

A. Stable group identity intended to reduce rebalances from transient restarts  
B. Static topic configuration  
C. Static schema  
D. Static broker IP

### 39.
What does `auto.offset.reset` do?

A. Determines starting behavior when no valid committed offset is available  
B. Always resets offsets  
C. Changes topic retention  
D. Changes replication

### 40.
A consumer has a valid committed offset. Does `auto.offset.reset=earliest` override it?

A. No  
B. Yes  
C. Only for Avro  
D. Only with transactions

### 41.
What does batching improve?

A. Request/network efficiency  
B. Schema compatibility  
C. Authentication  
D. Partition leadership

### 42.
What is compression's primary trade-off?

A. CPU versus network/storage efficiency  
B. Replication versus ordering  
C. Security versus retention  
D. Schema versus offsets

### 43.
What is a transaction boundary?

A. Set of transactional operations committed/aborted together  
B. Topic boundary  
C. Broker boundary  
D. Consumer group boundary

### 44.
Why use transactional offset commits in read-process-write applications?

A. Coordinate consumed offsets with produced output  
B. Increase consumer count  
C. Change partition count  
D. Register schemas

### 45.
What is the danger of infinite retries?

A. A failing record/dependency can block progress indefinitely  
B. Kafka becomes exactly-once  
C. Lag becomes zero  
D. Broker storage disappears

### 46.
What is a common retry architecture?

A. Retry topics + backoff + DLQ/error destination  
B. Delete topic + restart broker  
C. Disable commits  
D. Increase replication only

### 47.
What should be monitored for consumers?

A. Lag, throughput, processing latency, errors, rebalances  
B. Only partition count  
C. Only broker disk  
D. Only schemas

### 48.
What should be monitored for producers?

A. Throughput, error rate, latency, retries, request size, buffer pressure  
B. Only consumer lag  
C. Only topic count  
D. Only schemas

### 49.
What is a common cause of `TimeoutException`?

A. Network/broker/resource/request timeout conditions  
B. Always schema incompatibility  
C. Always consumer lag  
D. Always topic deletion

### 50.
What should you inspect if bootstrap works but broker connections fail?

A. Advertised listeners and network routing  
B. Consumer offsets  
C. Retention  
D. Compaction

### 51.
What is exactly-once not automatically equivalent to?

A. Exactly-once effects in every external system  
B. Kafka transactional semantics  
C. Atomic Kafka output/offset handling  
D. Transactional producer behavior

### 52.
Why are external side effects difficult for exactly-once?

A. Kafka transactions do not automatically make arbitrary external systems transactional  
B. Kafka cannot produce records  
C. Consumers cannot commit  
D. Schemas are immutable

### 53.
What pattern can help with external side effects?

A. Idempotency keys, transactional outbox, or integration-specific transaction patterns  
B. Disable commits  
C. Increase partitions only  
D. Delete duplicates manually

### 54.
What is the transactional outbox pattern?

A. Store business change and event intent transactionally, then publish asynchronously  
B. Store schemas in the database  
C. Store consumer groups externally  
D. Replicate brokers

### 55.
What is the main benefit of the outbox pattern?

A. Avoid dual-write inconsistency between database state and event publication intent  
B. Eliminate Kafka  
C. Eliminate consumers  
D. Eliminate schemas

### 56.
What is idempotent consumption?

A. Processing duplicates without producing incorrect repeated effects  
B. Ignoring all records  
C. Committing every record before processing  
D. Disabling retries

### 57.
What is a good certification answer when asked how to improve reliability?

A. Identify the failure mode, choose appropriate Kafka semantics, and design idempotent/transactional processing where required  
B. Always use more brokers  
C. Always use more partitions  
D. Always disable acknowledgments

### 58.
What is the most important design factor for ordering?

A. Partition key and partition boundaries  
B. Replication factor only  
C. Consumer count only  
D. Schema format

### 59.
What is the most important design factor for consumer parallelism?

A. Number of partitions and consumer-group assignment  
B. Number of schemas  
C. Replication factor only  
D. TLS

### 60.
What is the strongest final developer mental model?

A. Records -> partitions -> producers/consumers -> offsets -> delivery semantics -> schemas -> processing/state  
B. Brokers only  
C. Schemas only  
D. Consumer groups only

# Answer Keys

## Mock Exam G — Full CCDAK

```text
1 B
2 A
3 B
4 A
5 A
6 A
7 A
8 A
9 A
10 A
11 A
12 A
13 A
14 A
15 A
16 A
17 A
18 A
19 A
20 A
21 A
22 A
23 A
24 A
25 A
26 A
27 A
28 A
29 A
30 A
31 A
32 A
33 A
34 A
35 A
36 A
37 A
38 A
39 A
40 A
41 A
42 A
43 A
44 A
45 A
46 A
47 A
48 A
49 A
50 A
51 A
52 A
53 A
54 A
55 A
56 A
57 A
58 A
59 A
60 A
```