# Mock Exam D — Administrator

**Questions: 50**

### 1.
Which command is primarily used to inspect topics?

A. kafka-topics.sh  
B. kafka-configs.sh  
C. kafka-consumer-groups.sh  
D. kafka-storage.sh

### 2.
Which command is primarily used to inspect consumer groups?

A. kafka-consumer-groups.sh  
B. kafka-topics.sh  
C. kafka-reassign-partitions.sh  
D. kafka-metadata-quorum.sh

### 3.
Which command is used for Kafka configuration administration?

A. kafka-configs.sh  
B. kafka-topics.sh  
C. kafka-console-consumer.sh  
D. kafka-storage.sh

### 4.
Which command is associated with partition reassignment?

A. kafka-reassign-partitions.sh  
B. kafka-configs.sh  
C. kafka-consumer-groups.sh  
D. kafka-storage.sh

### 5.
What does replication factor represent?

A. Number of replicas assigned to each partition  
B. Number of consumers  
C. Number of schemas  
D. Number of brokers globally

### 6.
What does ISR stand for?

A. In-Sync Replicas  
B. Internal Storage Records  
C. Indexed Schema Registry  
D. Inter-Server Routing

### 7.
What does an under-replicated partition indicate?

A. Fewer replicas are currently in sync than the assigned replica set  
B. No partition exists  
C. The topic is compacted  
D. The consumer has no offset

### 8.
What does an offline partition indicate?

A. No leader is currently available  
B. One replica is slow  
C. A consumer is idle  
D. The topic has retention enabled

### 9.
Which is more severe from an availability perspective?

A. Offline partition  
B. One ISR shrink event  
C. One consumer lag spike  
D. One producer retry

### 10.
Why is disk utilization important?

A. Kafka stores partition logs on disk and needs capacity for writes/recovery  
B. Disk is used only for schemas  
C. Disk is irrelevant to Kafka  
D. Disk controls ACLs

### 11.
Why can too many partitions be harmful?

A. Metadata and operational overhead increase  
B. Kafka stops supporting replication  
C. Producers cannot batch  
D. Consumers cannot commit

### 12.
What happens when a broker is added?

A. Capacity becomes available, but existing partitions are not necessarily automatically rebalanced onto it  
B. All data immediately moves to it  
C. All topics are deleted  
D. All consumers restart

### 13.
What is partition reassignment used for?

A. Moving partition replicas between brokers  
B. Changing schema compatibility  
C. Resetting consumer offsets  
D. Creating users

### 14.
Why throttle reassignment?

A. Protect production performance  
B. Increase retention  
C. Disable replication  
D. Increase message size

### 15.
What is leader imbalance?

A. Too many partition leaders concentrated on a broker  
B. Too many consumers in a group  
C. Too many schemas  
D. Too many topics

### 16.
What can leader imbalance cause?

A. Uneven request/network/CPU load  
B. Automatic schema deletion  
C. Lower replication factor  
D. Consumer group deletion

### 17.
What is KRaft?

A. Kafka's metadata quorum architecture replacing ZooKeeper-based metadata management  
B. Kafka Connect protocol  
C. A serialization format  
D. A consumer API

### 18.
What does the KRaft controller quorum manage?

A. Cluster metadata  
B. Application business data  
C. Consumer application state only  
D. External databases

### 19.
Why does a controller quorum need a majority?

A. To maintain consensus and availability of metadata decisions  
B. To increase compression  
C. To store application records  
D. To reset offsets

### 20.
What is a rolling upgrade?

A. Updating brokers incrementally while maintaining service where supported  
B. Deleting all brokers at once  
C. Recreating every topic  
D. Resetting every consumer

### 21.
What should be checked before a rolling upgrade?

A. Cluster health and version compatibility  
B. Only disk names  
C. Only topic names  
D. Only consumer count

### 22.
What does `advertised.listeners` control conceptually?

A. Addresses Kafka tells clients to use for broker connections  
B. Topic retention  
C. Consumer offsets  
D. Schema compatibility

### 23.
Why can an incorrect advertised listener be serious?

A. Clients can bootstrap successfully but fail when connecting to advertised broker addresses  
B. It deletes partitions  
C. It changes schemas  
D. It changes replication factor

### 24.
What is dynamic broker configuration?

A. Runtime-configurable broker settings supported by Kafka  
B. Topic deletion  
C. Consumer assignment  
D. Schema evolution

### 25.
Can every broker configuration be dynamically changed?

A. No  
B. Yes  
C. Only in development  
D. Only with three brokers

### 26.
What is a quota used for?

A. Limiting resource usage by clients  
B. Increasing partitions  
C. Encrypting data  
D. Assigning leaders

### 27.
What is a preferred replica?

A. Replica designated first in the partition's replica assignment  
B. Slowest replica  
C. Consumer replica  
D. Schema replica

### 28.
What can preferred leader election help with?

A. Restoring leadership distribution toward preferred replicas  
B. Resetting offsets  
C. Changing topic names  
D. Increasing retention

### 29.
What should a healthy cluster generally have?

A. No offline partitions and no persistent under-replication  
B. Maximum possible lag  
C. Full disks  
D. Constant ISR shrinkage

### 30.
What does `bytes in` represent conceptually?

A. Incoming traffic to a broker  
B. Consumer lag  
C. Disk usage  
D. Number of partitions

### 31.
What does `bytes out` represent conceptually?

A. Outgoing broker traffic  
B. Number of schemas  
C. Disk usage  
D. Consumer count

### 32.
Why monitor request latency?

A. It can reveal broker or resource saturation affecting clients  
B. It determines schema compatibility  
C. It determines topic names  
D. It replaces consumer lag

### 33.
What is an ISR shrink?

A. A replica leaving the in-sync replica set  
B. A partition being deleted  
C. A consumer leaving a group  
D. A schema being removed

### 34.
What can repeated ISR shrinkage indicate?

A. Replication or broker resource problems  
B. Successful schema evolution  
C. Healthy compaction  
D. Consumer success

### 35.
What should you investigate if a broker repeatedly becomes unavailable?

A. Process, host, network, resource, storage, and orchestration issues  
B. Only consumer offsets  
C. Only schema versions  
D. Only topic names

### 36.
Why is failure headroom important?

A. Remaining brokers need capacity to absorb traffic after failures  
B. It increases schema compatibility  
C. It reduces partition count automatically  
D. It eliminates monitoring

### 37.
What does retention primarily control?

A. How long/how much log data remains eligible for retention cleanup  
B. Consumer membership  
C. Broker authentication  
D. Partition leader election

### 38.
What does compaction primarily preserve?

A. Latest state for keys, subject to compaction semantics  
B. Every historical record forever  
C. Consumer offsets  
D. Broker configuration

### 39.
What is a tombstone?

A. Null-valued record used to represent deletion of a key in a compacted log  
B. Broker certificate  
C. Consumer error  
D. Partition leader

### 40.
What is a log segment?

A. A chunk of partition log data stored on disk  
B. Consumer group  
C. Schema subject  
D. Broker user

### 41.
Why are segment sizes operationally relevant?

A. Retention and log management operate around segments  
B. They control ACLs  
C. They control consumer membership  
D. They determine schema compatibility

### 42.
What is the purpose of `kafka-metadata-quorum.sh`?

A. Inspect KRaft metadata quorum state  
B. Inspect consumer lag only  
C. Configure schemas  
D. Move partitions

### 43.
What is the purpose of `kafka-broker-api-versions.sh`?

A. Inspect broker API support  
B. Reset offsets  
C. Create topics  
D. Compact logs

### 44.
What is a good first step during a Kafka incident?

A. Establish symptoms and cluster health before changing configuration  
B. Restart all brokers immediately  
C. Delete the affected topic  
D. Increase every timeout

### 45.
What is a dangerous troubleshooting practice?

A. Changing many settings simultaneously without measurement  
B. Inspecting metrics  
C. Checking logs  
D. Verifying cluster state

### 46.
Why is monitoring consumer lag alone insufficient?

A. Lag can have different causes and must be correlated with throughput, processing time, and broker health  
B. Lag never matters  
C. Lag is always zero  
D. Lag replaces all broker metrics

### 47.
What should happen after a production configuration change?

A. Monitor and verify its effect  
B. Immediately make another unrelated change  
C. Restart all consumers regardless of change  
D. Delete monitoring

### 48.
What is the purpose of least privilege?

A. Limit administrative/security permissions to what is necessary  
B. Increase partition count  
C. Reduce disk usage  
D. Improve compression

### 49.
What is capacity planning?

A. Estimating resources needed for expected workload, growth, replication, and failure recovery  
B. Choosing a schema format only  
C. Resetting offsets  
D. Creating consumers

### 50.
What is the strongest administrator mindset?

A. Understand state, identify root cause, make controlled changes, and verify results  
B. Restart everything first  
C. Increase every resource setting  
D. Ignore metrics

# Answer Keys

## Mock Exam D — Administrator

```text
1 A
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