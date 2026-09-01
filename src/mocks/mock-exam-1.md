# Mock Exam A — Developer Fundamentals

**Questions: 50**

## Questions

### 1. What is the fundamental unit of parallelism in a Kafka topic?

A. Broker  
B. Partition  
C. Consumer  
D. Record

### 2. What guarantees ordering in Kafka?

A. Ordering across the entire topic  
B. Ordering across all brokers  
C. Ordering within a partition  
D. Ordering across a consumer group

### 3. A topic has 12 partitions and a consumer group has 20 consumers. What is the maximum number of consumers that can actively consume partitions of that topic?

A. 1  
B. 12  
C. 20  
D. 32

### 4. What identifies the position of a record within a partition?

A. Key  
B. Timestamp  
C. Offset  
D. Partition ID

### 5. What happens to a committed consumer offset when a consumer process restarts?

A. It is automatically reset to zero  
B. It can resume from the committed position  
C. It always resumes from the latest record  
D. The topic is replayed from the beginning

### 6. Which statement about Kafka topics is correct?

A. A topic exists on only one broker  
B. A topic is divided into partitions  
C. A topic can contain only one record  
D. A topic has exactly one leader

### 7. What determines which partition receives a keyed record under the default partitioning strategy?

A. Consumer group  
B. Key hashing and partition count  
C. Broker ID only  
D. Consumer offset

### 8. What is a Kafka record key commonly used for?

A. Encryption  
B. Partition selection and logical identity  
C. Broker authentication  
D. Compression

### 9. What is the main purpose of replication?

A. Increase record size  
B. Provide fault tolerance  
C. Remove partitions  
D. Disable consumers

### 10. What is the leader replica responsible for?

A. Only storing metadata  
B. Handling reads/writes for its partition  
C. Managing all topics  
D. Running every consumer

### 11. What is an ISR?

A. Internal Storage Registry  
B. In-Sync Replicas  
C. Internal Security Rule  
D. Indexed Segment Record

### 12. What does a consumer group provide?

A. Shared partition consumption and parallelism  
B. Topic encryption  
C. Broker replication  
D. Schema validation

### 13. If two consumers belong to the same group and subscribe to the same six-partition topic, can both consume the same partition simultaneously under a normal group assignment?

A. Yes, always  
B. No, a partition is assigned to one group member at a time  
C. Only if replication factor is 3  
D. Only with compacted topics

### 14. What is the purpose of a consumer offset?

A. Identify the broker  
B. Identify a record's position in a partition  
C. Identify a schema  
D. Identify a topic owner

### 15. Which statement is true about Kafka retention?

A. Records are deleted immediately after consumption  
B. Records are retained according to topic retention policies  
C. Consumers control physical deletion  
D. Offsets determine deletion directly

### 16. Which cleanup policy is associated with retaining the latest state for keys?

A. delete  
B. compact  
C. archive  
D. snapshot

### 17. What is a tombstone in a compacted Kafka topic?

A. A broker failure marker  
B. A null-valued record used to represent deletion of a key  
C. A corrupted segment  
D. A consumer offset

### 18. What does idempotent producer behavior help prevent?

A. Partition creation  
B. Duplicate records caused by producer retries  
C. Consumer rebalancing  
D. Topic deletion

### 19. What is the purpose of producer retries?

A. Retry failed produce requests  
B. Increase partitions  
C. Reset offsets  
D. Change schemas

### 20. What is `acks=all` intended to provide?

A. The producer waits for the strongest configured acknowledgment from the in-sync replicas  
B. No acknowledgment  
C. Consumer acknowledgment  
D. Schema acknowledgment

### 21. What does `min.insync.replicas` influence?

A. Minimum number of ISR replicas required for certain successful writes  
B. Number of consumer threads  
C. Number of partitions  
D. Compression level

### 22. What is consumer lag?

A. Number of brokers  
B. Difference between the log end position and the consumer's current/committed position  
C. Number of schemas  
D. Number of producers

### 23. What usually triggers a consumer group rebalance?

A. Consumer membership or subscription changes  
B. Every produced record  
C. Every topic retention event  
D. Every schema registration

### 24. What is the main reason to have multiple partitions?

A. Encryption  
B. Parallelism and distribution  
C. Schema validation  
D. Authentication

### 25. Can Kafka guarantee ordering across all partitions of a topic?

A. Yes  
B. No  
C. Only with replication factor 3  
D. Only with transactions

### 26. What does a partition leader represent?

A. The broker currently responsible for serving partition operations  
B. The oldest consumer  
C. The topic owner  
D. The schema owner

### 27. What does a consumer typically do after processing records successfully?

A. Commit offsets  
B. Delete the partition  
C. Change the leader  
D. Reassign replicas

### 28. What is at-least-once processing?

A. Every record is processed zero or more times  
B. Records are processed one or more times, so duplicates are possible  
C. Records are never retried  
D. Records are processed exactly once in all circumstances

### 29. What is exactly-once semantics intended to provide?

A. No Kafka storage  
B. A transactional processing guarantee that avoids externally visible duplicate effects within the supported Kafka processing model  
C. Infinite retention  
D. No consumer commits

### 30. What is a Kafka transaction used for?

A. Atomically coordinate certain Kafka writes and offset commits  
B. Change broker IPs  
C. Delete all records  
D. Create schemas

### 31. Which component stores Kafka records?

A. Broker  
B. Consumer group  
C. Schema Registry  
D. Connect worker only

### 32. What is a segment?

A. A physical/logical chunk of a partition log stored on disk  
B. A consumer group  
C. A schema version  
D. A broker certificate

### 33. What happens when a consumer is slower than the producer for an extended period?

A. Lag may increase  
B. Kafka automatically deletes the consumer  
C. Partition count decreases  
D. Replication stops

### 34. What does `auto.offset.reset` influence?

A. Behavior when no valid committed offset is available  
B. Broker replication  
C. Producer compression  
D. Schema compatibility

### 35. What does `enable.auto.commit` control?

A. Whether consumer offsets are automatically committed by the consumer  
B. Whether producers use transactions  
C. Whether Kafka creates partitions  
D. Whether brokers replicate

### 36. What is the main advantage of manual offset management?

A. More control over when processed records become committed  
B. Faster broker startup  
C. Automatic schema evolution  
D. Automatic replication

### 37. What is a dead-letter topic commonly used for?

A. Storing records that could not be processed successfully  
B. Storing broker metadata  
C. Storing certificates  
D. Storing partition leaders

### 38. What is a retry topic commonly used for?

A. Delaying/retrying failed records according to application policy  
B. Increasing replication factor  
C. Replacing Kafka controllers  
D. Storing ACLs

### 39. What is backpressure?

A. Mechanism/concept for preventing downstream overload when upstream production exceeds processing capacity  
B. A Kafka security protocol  
C. A partition leader election  
D. A schema format

### 40. What is the main benefit of batching producer records?

A. Lower throughput  
B. Better network and request efficiency  
C. Eliminate partitions  
D. Disable retries

### 41. What does compression usually reduce?

A. Network and storage footprint  
B. Number of partitions  
C. Consumer groups  
D. Offset count

### 42. Which format is a binary serialization format commonly used with Kafka?

A. Avro  
B. HTML  
C. CSS  
D. YAML only

### 43. What does Kafka Connect provide?

A. A framework for moving data between Kafka and external systems  
B. A replacement for brokers  
C. A replacement for partitions  
D. A consumer group protocol only

### 44. What is Kafka Streams?

A. A client library for building stream-processing applications  
B. A Kafka broker replacement  
C. A storage filesystem  
D. A security server

### 45. What does a state store in Kafka Streams provide?

A. Local state used by stream-processing applications  
B. Broker certificates  
C. Topic ACLs  
D. Consumer credentials

### 46. What is a repartition topic used for in Kafka Streams?

A. Redistributing records when the required key changes for downstream processing  
B. Storing TLS certificates  
C. Replacing the changelog  
D. Resetting broker offsets

### 47. What is a changelog topic commonly used for in Kafka Streams?

- A. Backing up state-store state for recovery  
- B. Storing user passwords  
- C. Replacing consumer offsets  
- D. Performing TLS negotiation

### 48. What is a schema registry used for?

A. Centralized schema storage and compatibility management  
B. Broker leader election  
C. Consumer group assignment  
D. Disk management

### 49. What is schema compatibility intended to protect?

A. Consumers and producers from incompatible data evolution  
B. Broker CPU  
C. Partition leadership  
D. Network routing

### 50. Which statement best summarizes Kafka?

A. A traditional relational database  
B. A distributed event streaming platform based around partitioned logs  
C. A schema-only service  
D. A network proxy

# Answer Keys

## Mock Exam A — Developer Fundamentals

```text
1 B
2 C
3 B
4 C
5 B
6 B
7 B
8 B
9 B
10 B
11 B
12 A
13 B
14 B
15 B
16 B
17 B
18 B
19 A
20 A
21 A
22 B
23 A
24 B
25 B
26 A
27 A
28 B
29 B
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
40 B
41 A
42 A
43 A
44 A
45 A
46 A
47 A
48 A
49 A
50 B