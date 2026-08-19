# Mock Exam B — Producer / Consumer

**Questions: 50**

## Questions

### 1.
A producer sends records with the same key. What is normally expected?

A. They are sent randomly across partitions  
B. They are mapped consistently to a partition while the partitioning strategy and partition count remain appropriate  
C. They are sent to every partition  
D. They are discarded

### 2.
Why can increasing partition count affect keyed ordering assumptions?

A. The hashing domain changes  
B. Brokers stop replicating  
C. Consumers stop committing  
D. Compression is disabled

### 3.
What does `linger.ms` influence?

A. How long a producer may wait to accumulate records into a batch  
B. Consumer session timeout  
C. Broker retention  
D. Schema compatibility

### 4.
What does `batch.size` influence?

A. Target batch size for producer records  
B. Number of brokers  
C. Topic retention  
D. Consumer group size

### 5.
What is the trade-off of increasing batching?

A. It can improve throughput but may add latency  
B. It always reduces throughput  
C. It disables compression  
D. It removes ordering

### 6.
What does producer idempotence help with?

A. Duplicate writes resulting from producer retries  
B. Consumer authorization  
C. Topic compaction  
D. Broker discovery

### 7.
What is a producer sequence number associated with?

A. Idempotent producer ordering/deduplication semantics  
B. Topic retention  
C. Consumer group membership  
D. Schema compatibility

### 8.
What can cause producer retries?

A. Transient broker/network failures  
B. Schema registration only  
C. Consumer rebalances only  
D. Partition count only

### 9.
Why can retries create duplicates without idempotence?

A. A request may have reached the broker even though the producer did not receive the response  
B. Kafka stores no offsets  
C. Consumers duplicate every record automatically  
D. Topics are compacted

### 10.
What is the role of `acks`?

A. Producer acknowledgment policy  
B. Consumer assignment  
C. Broker authentication  
D. Topic retention

### 11.
What does `acks=0` mean conceptually?

A. Producer does not wait for broker acknowledgment  
B. Producer waits for all replicas  
C. Consumer acknowledges the record  
D. Transaction is committed

### 12.
What does `acks=1` mean conceptually?

A. Leader acknowledgment  
B. All replicas acknowledgment  
C. Consumer acknowledgment  
D. No acknowledgment

### 13.
What does `acks=all` mean conceptually?

A. Strongest configured acknowledgment requiring the leader to wait according to ISR/min-ISR semantics  
B. No acknowledgment  
C. Consumer acknowledgment  
D. Schema Registry acknowledgment

### 14.
Why is `min.insync.replicas` important with `acks=all`?

A. It helps enforce a minimum ISR requirement for writes  
B. It determines partition count  
C. It determines compression  
D. It determines consumer count

### 15.
A topic has RF=3 and min ISR=2. One replica falls out of ISR. What can happen to writes using `acks=all`?

A. They can continue if at least two ISR replicas remain  
B. They always fail immediately  
C. The topic is deleted  
D. Consumers stop automatically

### 16.
A topic has RF=3 and min ISR=2. Two replicas fall out of ISR. What can happen to writes using `acks=all`?

A. Writes may fail because ISR has fallen below min ISR  
B. Writes are guaranteed to succeed  
C. Kafka creates a fourth replica  
D. Consumers automatically reset

### 17.
What is producer fencing?

A. Preventing an old transactional producer instance from continuing to act as the valid producer identity  
B. Deleting a broker  
C. Blocking a consumer group  
D. Resetting offsets

### 18.
What does a transactional ID enable?

A. Stable producer identity for transactional/idempotent producer semantics  
B. Consumer group membership  
C. Topic retention  
D. Schema versioning

### 19.
Why should transactional IDs be managed carefully?

A. Duplicate/conflicting identities can cause fencing and correctness problems  
B. They control partition count  
C. They control broker storage  
D. They disable replication

### 20.
What is the purpose of `sendOffsetsToTransaction` in a read-process-write pattern?

A. Include consumed offsets in the transaction so output and offset progress can be committed atomically  
B. Reset offsets  
C. Delete offsets  
D. Replicate offsets to all brokers manually

### 21.
What does `read_committed` mean for a consumer?

A. It reads committed transactional records and hides aborted transactional records  
B. It disables offsets  
C. It reads only compacted records  
D. It reads only the latest record

### 22.
What does `read_uncommitted` allow?

A. Reading records without filtering out aborted transactional records  
B. Reading only committed offsets  
C. Reading only compacted topics  
D. Reading only leader replicas

### 23.
What is a consumer group coordinator responsible for?

A. Group membership and coordination  
B. Partition storage  
C. Schema storage  
D. TLS certificate issuance

### 24.
What happens when a consumer exceeds relevant poll timing constraints repeatedly?

A. It may be considered unhealthy and trigger a rebalance  
B. It gains more partitions  
C. It becomes the group coordinator  
D. Kafka increases retention

### 25.
What is `max.poll.interval.ms` related to?

A. Maximum interval between consumer poll calls before the consumer can be considered failed from group-management perspective  
B. Broker retention  
C. Producer batch delay  
D. TLS timeout only

### 26.
What is `session.timeout.ms` related to?

A. Consumer liveness/session failure detection  
B. Topic retention  
C. Producer batching  
D. Schema compatibility

### 27.
What is `heartbeat.interval.ms` related to?

A. Consumer group heartbeats  
B. Producer batch size  
C. Broker disk retention  
D. Topic compaction

### 28.
What is `max.poll.records` used for?

A. Limit records returned from a poll call  
B. Limit topic partitions  
C. Limit brokers  
D. Limit replicas

### 29.
Why can reducing `max.poll.records` help a slow consumer?

A. It can reduce the amount of work processed between polls  
B. It increases broker replication  
C. It changes partition count  
D. It disables rebalancing

### 30.
What is cooperative rebalancing intended to improve?

A. Reduce unnecessary partition movement during rebalances  
B. Increase replication factor  
C. Replace transactions  
D. Encrypt records

### 31.
What is static membership intended to reduce?

A. Rebalances caused by transient consumer restarts/rejoins  
B. Topic retention  
C. Producer retries  
D. Schema registration

### 32.
What happens if two consumers in the same group subscribe to different topics?

A. Group assignment depends on their subscriptions and common group coordination  
B. Kafka always assigns every partition to every consumer  
C. Kafka rejects all subscriptions  
D. They become different groups automatically

### 33.
What is a manual partition assignment?

A. Application explicitly assigns partitions instead of relying on group subscription assignment  
B. Broker chooses schemas  
C. Producer chooses leaders  
D. Controller assigns consumer IDs

### 34.
What is a consequence of manual assignment?

A. Group coordination semantics differ because assignment is controlled by the application  
B. Replication stops  
C. Topics become compacted  
D. Producers become transactional

### 35.
What is `auto.offset.reset=earliest` intended to do when no valid committed offset exists?

A. Start from the earliest available offset  
B. Start from the newest record only  
C. Delete the group  
D. Start at offset zero even if unavailable

### 36.
What is `auto.offset.reset=latest` intended to do?

A. Start at the latest available position when no valid committed offset exists  
B. Always replay the topic  
C. Reset all groups  
D. Start from the oldest segment

### 37.
Does `auto.offset.reset=earliest` override a valid committed offset?

A. No  
B. Yes, always  
C. Only with compacted topics  
D. Only with transactions

### 38.
Why might a consumer receive a record again after a crash?

A. Its processing completed but the offset commit did not, resulting in at-least-once replay  
B. Kafka duplicates all records  
C. Replication creates new offsets  
D. Compression duplicates records

### 39.
How can duplicate processing be made safe?

A. Idempotent application logic or transactional/exactly-once design where appropriate  
B. Increase partitions only  
C. Disable commits  
D. Disable retries

### 40.
What is the difference between processing and committing?

A. Processing changes application state; committing records progress to Kafka's consumer offset state  
B. They are always the same operation  
C. Processing changes partition leaders  
D. Commit changes schemas

### 41.
What is a poison-pill record?

A. A record that repeatedly causes processing failure  
B. A broker replica  
C. A tombstone  
D. A schema registry node

### 42.
What is a common strategy for poison pills?

A. Retry with policy, route to a dead-letter/error topic, and preserve observability  
B. Delete the entire topic  
C. Restart every broker  
D. Increase replication factor

### 43.
Why can unbounded retries be dangerous?

A. They can block progress and overload the system  
B. They always reduce lag  
C. They improve availability without trade-offs  
D. They remove duplicates

### 44.
What is backoff?

A. Delay between retry attempts  
B. Partition reassignment  
C. Consumer group deletion  
D. Leader election

### 45.
What is exponential backoff?

A. Increasing retry delay after repeated failures  
B. Decreasing delay to zero  
C. Increasing partition count  
D. Increasing replication factor

### 46.
What does consumer fetch size affect?

A. Amount of data requested/returned per fetch within configured limits  
B. Number of brokers  
C. Topic retention  
D. Schema compatibility

### 47.
What is the danger of excessively large consumer fetches?

A. Memory pressure and increased processing latency  
B. Automatic topic deletion  
C. Replication failure by definition  
D. Loss of schemas

### 48.
What does producer compression trade off?

A. CPU for lower network/storage usage  
B. Security for retention  
C. Replication for ordering  
D. Transactions for schemas

### 49.
What is the main effect of increasing producer concurrency?

A. Potentially higher throughput, with more complexity/resource use  
B. Guaranteed exactly-once semantics  
C. Guaranteed global ordering  
D. Automatic compaction

### 50.
What is the best general strategy for tuning producers and consumers?

A. Benchmark under realistic workloads and tune based on measured bottlenecks  
B. Always maximize every setting  
C. Always minimize every setting  
D. Copy settings from another cluster without testing

# Answer Keys

## Mock Exam B — Producer / Consumer

```text
1 B
2 A
3 A
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
```