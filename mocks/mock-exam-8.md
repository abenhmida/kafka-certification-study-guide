# Mock Exam H — Full CCAAK Simulation

## 90-Minute Simulation

**Recommended time:** 90 minutes  
**Questions:** 60  
**Target:** 80%+

> This simulation emphasizes administration: cluster architecture, KRaft, replication, partitions, configuration, security, monitoring, storage, capacity, and troubleshooting.

## Questions

### 1.
What is the main purpose of replication?

A. Fault tolerance  
B. Schema validation  
C. Consumer assignment  
D. Compression

### 2.
What is ISR?

A. In-Sync Replicas  
B. Internal Storage Registry  
C. Inter-Server Routing  
D. Indexed Schema Record

### 3.
What does an offline partition mean?

A. No leader is available  
B. One consumer is slow  
C. One schema is incompatible  
D. One replica is delayed

### 4.
What does under-replication mean?

A. ISR count is below assigned replica count  
B. No consumers exist  
C. Topic is compacted  
D. Schema is invalid

### 5.
Which condition is most urgent?

A. Offline partitions  
B. One consumer with lag 10  
C. One producer retry  
D. One schema registration

### 6.
What does RF=3 mean?

A. Three replicas per partition  
B. Three consumers  
C. Three brokers total  
D. Three topics

### 7.
Does RF=3 mean a topic must have three partitions?

A. No  
B. Yes  
C. Only in KRaft  
D. Only with compaction

### 8.
What does `min.insync.replicas` control?

A. Minimum ISR needed for writes under applicable acknowledgment semantics  
B. Number of partitions  
C. Number of consumers  
D. Number of controllers

### 9.
Why pair `acks=all` with appropriate min ISR?

A. Strengthen durability guarantees against insufficient ISR  
B. Increase compression  
C. Increase consumers  
D. Change schema

### 10.
What happens when ISR falls below min ISR?

A. Writes requiring that minimum can fail  
B. Kafka always creates replicas  
C. Consumer groups stop automatically  
D. Topic is deleted

### 11.
What is KRaft?

A. Kafka metadata quorum architecture  
B. Kafka Connect  
C. Avro  
D. SASL

### 12.
What is a controller quorum responsible for?

A. Cluster metadata and coordination  
B. Application business logic  
C. External database transactions  
D. Consumer processing

### 13.
Why does KRaft require quorum majority?

A. Consensus  
B. Compression  
C. Retention  
D. Schema validation

### 14.
What tool inspects metadata quorum state?

A. kafka-metadata-quorum.sh  
B. kafka-consumer-groups.sh  
C. kafka-console-producer.sh  
D. kafka-topics.sh

### 15.
What tool inspects broker API support?

A. kafka-broker-api-versions.sh  
B. kafka-configs.sh  
C. kafka-reassign-partitions.sh  
D. kafka-console-consumer.sh

### 16.
What tool manages topics?

A. kafka-topics.sh  
B. kafka-configs.sh  
C. kafka-storage.sh only  
D. kafka-metadata-quorum.sh only

### 17.
What tool manages consumer groups?

A. kafka-consumer-groups.sh  
B. kafka-topics.sh  
C. kafka-storage.sh  
D. kafka-reassign-partitions.sh

### 18.
What tool manages configuration?

A. kafka-configs.sh  
B. kafka-topics.sh  
C. kafka-console-consumer.sh  
D. kafka-metadata-quorum.sh

### 19.
What tool handles partition reassignment?

A. kafka-reassign-partitions.sh  
B. kafka-configs.sh  
C. kafka-topics.sh  
D. kafka-console-producer.sh

### 20.
What does adding a broker do automatically?

A. Provides capacity but does not necessarily redistribute existing partition assignments  
B. Moves all data automatically  
C. Deletes old brokers  
D. Resets consumers

### 21.
What is partition reassignment?

A. Moving replicas between brokers  
B. Changing schemas  
C. Resetting consumer offsets  
D. Changing ACLs

### 22.
Why throttle reassignment?

A. Protect production traffic  
B. Improve schema compatibility  
C. Reduce consumer count  
D. Disable replication

### 23.
What is preferred leader election?

A. Move leadership toward preferred replicas  
B. Change schemas  
C. Reset offsets  
D. Delete partitions

### 24.
What is leader imbalance?

A. Uneven distribution of partition leadership  
B. Uneven schemas  
C. Uneven consumer commits  
D. Uneven certificates

### 25.
Why can leader imbalance hurt performance?

A. Leaders handle partition requests and can become hotspots  
B. Followers cannot store data  
C. Schemas are deleted  
D. Consumers stop

### 26.
What does retention control?

A. Eligibility of old log data for deletion  
B. Consumer membership  
C. ACL permissions  
D. Controller quorum

### 27.
What does compaction preserve?

A. Latest state per key, subject to compaction semantics  
B. All historical data forever  
C. Consumer offsets  
D. Broker configuration

### 28.
What is a tombstone?

A. Null-valued record representing deletion of a key  
B. Broker failure  
C. Controller record  
D. Consumer offset

### 29.
Why are segment sizes important?

A. Retention/cleanup operates around log segments  
B. They determine ACLs  
C. They determine consumers  
D. They determine schema compatibility

### 30.
Why monitor disk usage?

A. Kafka stores partition logs on disk and recovery requires capacity  
B. Disk controls authentication  
C. Disk controls schemas  
D. Disk controls consumer membership

### 31.
What is failure headroom?

A. Spare capacity available to absorb failures/recovery  
B. Extra schema versions  
C. Extra consumers only  
D. Extra ACLs

### 32.
Why is network capacity important?

A. Kafka traffic includes producers, consumers, replication, recovery, and reassignment  
B. Kafka never uses network for replication  
C. Only schemas use network  
D. Network does not affect latency

### 33.
What is a hot partition?

A. Partition receiving disproportionate load  
B. Partition with no leader  
C. Partition with a tombstone  
D. Partition with no records

### 34.
How can hot partitions be mitigated?

A. Better key distribution when business ordering permits  
B. Always increase RF  
C. Disable replication  
D. Delete consumers

### 35.
What does `advertised.listeners` control?

A. Broker addresses returned to clients  
B. Retention  
C. Consumer offsets  
D. ACLs

### 36.
A client bootstraps but cannot connect to the broker returned in metadata. Likely issue?

A. Advertised listener/network configuration  
B. Consumer lag  
C. Schema version  
D. Compaction

### 37.
What does dynamic configuration mean?

A. Certain supported settings can be changed at runtime  
B. Every setting can change without restart  
C. Topics are automatically rebalanced  
D. Consumer offsets are automatic

### 38.
Can every Kafka configuration be dynamically changed?

A. No  
B. Yes  
C. Only with RF=3  
D. Only in KRaft

### 39.
What are quotas for?

A. Limit client resource usage  
B. Increase partitions  
C. Encrypt traffic  
D. Register schemas

### 40.
What should you monitor for broker health?

A. Request latency, traffic, ISR, disk, CPU, controller state, and availability  
B. Only topic count  
C. Only schema count  
D. Only consumer count

### 41.
What should you monitor for consumer health?

A. Lag, throughput, processing latency, errors, and rebalances  
B. Only broker disk  
C. Only schemas  
D. Only replication

### 42.
What should you monitor for replication?

A. Under-replicated partitions, ISR shrink/expand, recovery activity  
B. Only consumer lag  
C. Only topic names  
D. Only schema compatibility

### 43.
What should you do if offline partitions appear?

A. Investigate leader availability immediately  
B. Increase retention  
C. Add schemas  
D. Change consumer count

### 44.
What should you do if persistent under-replication appears?

A. Investigate broker/network/disk/resource/recovery issues  
B. Ignore it  
C. Delete the topic  
D. Disable replication

### 45.
What should you do before changing production configuration?

A. Establish current state and understand impact  
B. Change multiple settings simultaneously  
C. Restart every broker  
D. Disable monitoring

### 46.
What is a good incident response sequence?

A. Detect -> assess -> isolate -> remediate -> verify -> document  
B. Restart -> delete -> investigate  
C. Ignore -> wait -> restart  
D. Change all settings

### 47.
What is authentication?

A. Establishing identity  
B. Granting permissions  
C. Partition assignment  
D. Replication

### 48.
What is authorization?

A. Deciding whether an authenticated principal may perform an operation  
B. Establishing identity  
C. Encrypting disks  
D. Creating topics

### 49.
What is TLS used for?

A. Secure network transport and certificate-based endpoint authentication  
B. Consumer group assignment  
C. Partition reassignment  
D. Retention

### 50.
What is SASL used for?

A. Authentication mechanisms  
B. Replication  
C. Compaction  
D. Partition count

### 51.
What is an ACL?

A. Authorization rule  
B. Schema format  
C. Partition  
D. Consumer offset

### 52.
What is least privilege?

A. Grant only required permissions  
B. Give every application admin rights  
C. Disable authentication  
D. Use one shared identity

### 53.
What should be checked for TLS failures?

A. Certificate chain, truststore, hostname, protocol, key material  
B. Consumer offset only  
C. Partition count only  
D. Retention only

### 54.
What should be checked for authorization failures?

A. Principal and ACL/resource/operation permissions  
B. Disk latency only  
C. Schema compatibility only  
D. Consumer lag only

### 55.
What should be checked for authentication failures?

A. Credentials, mechanism, security protocol, and broker configuration  
B. Partition reassignment  
C. Topic retention  
D. Consumer offset

### 56.
Why is replication not equivalent to backup?

A. Replicas are part of the live cluster and can share failure domains  
B. Replication is always offline  
C. Backup cannot contain Kafka data  
D. Replicas are schemas

### 57.
Why is capacity planning essential?

A. A failure can move workload to fewer brokers  
B. Kafka never fails  
C. Consumers determine disk capacity  
D. Schemas determine CPU

### 58.
What is the best approach to troubleshooting?

A. Layered diagnosis using evidence  
B. Random tuning  
C. Restart everything  
D. Delete data

### 59.
What is the best approach to production changes?

A. Small controlled changes with monitoring and rollback  
B. Large simultaneous changes  
C. No monitoring  
D. Permanent emergency settings

### 60.
What is the strongest administrator mental model?

A. Cluster state -> metadata -> partitions -> replication -> resources -> clients -> application impact  
B. Consumers only  
C. Topics only  
D. Brokers only

# Answer Keys

## Mock Exam H — Full CCAAK

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