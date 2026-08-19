# Mock Exam F — Operations / Troubleshooting

**Questions: 50**

### 1.
Consumer lag is continuously increasing. What should you inspect first?

A. Processing throughput and consumer health  
B. Delete the topic  
C. Increase replication factor  
D. Change schema format

### 2.
Lag is high but decreasing. What does that suggest?

A. Consumer is catching up  
B. Consumer is definitely broken  
C. Broker is definitely down  
D. Topic is corrupt

### 3.
Under-replicated partitions persist for 30 minutes. What does this suggest?

A. A replication/resource problem requiring investigation  
B. Normal operation  
C. Schema incompatibility only  
D. Consumer success

### 4.
Offline partitions are detected. What is the priority?

A. Restore partition availability and identify leader failure  
B. Tune producer batching  
C. Change schema compatibility  
D. Increase consumer count

### 5.
A broker's disk reaches 95%. What is a sensible response?

A. Increase capacity/rebalance/review retention before exhaustion  
B. Ignore it  
C. Delete all topics  
D. Disable monitoring

### 6.
A consumer repeatedly leaves and rejoins the group. What should you investigate?

A. Processing time, poll interval, network, GC, and consumer stability  
B. Topic compaction only  
C. Broker certificate only  
D. Schema subject only

### 7.
A producer experiences frequent timeouts. What should you inspect?

A. Network, broker health, request latency, load, and configuration  
B. Only schema compatibility  
C. Only consumer offsets  
D. Only tombstones

### 8.
A producer receives errors after broker failure. What should you inspect?

A. Leader availability, ISR, retries, and client configuration  
B. Only consumer groups  
C. Only schemas  
D. Only Connect workers

### 9.
A consumer sees duplicate processing after restart. What is a likely explanation?

A. Processing occurred before the offset commit completed  
B. Kafka always duplicates records  
C. Replication creates duplicate offsets  
D. Compaction creates duplicates

### 10.
What delivery model does this commonly represent?

A. At-least-once  
B. At-most-once  
C. Exactly-once by definition  
D. Zero-times

### 11.
What is a safe response to poison-pill records?

A. Isolate/retry according to policy and preserve observability  
B. Infinite retries  
C. Delete the topic  
D. Stop the entire cluster

### 12.
A Connect sink task fails repeatedly on one record. What is a likely operational solution?

A. Error handling/DLQ policy where appropriate  
B. Increase broker count  
C. Reset all Kafka offsets  
D. Disable schemas globally

### 13.
A Kafka Streams application repeatedly crashes during state restoration. What should you inspect?

A. Changelog/state-store health, disk, Kafka connectivity, and state compatibility  
B. Only producer batching  
C. Only ACLs  
D. Only topic names

### 14.
A Kafka Streams join produces unexpected results. What should you inspect?

A. Keys, partitioning, repartitioning, and join semantics  
B. Disk retention only  
C. TLS only  
D. Consumer count only

### 15.
A producer's throughput is low but CPU is low. What might help?

A. Batching/compression/network tuning after measurement  
B. Randomly increase every timeout  
C. Delete partitions  
D. Disable acknowledgments immediately

### 16.
Why can increasing `linger.ms` improve throughput?

A. It can allow larger batches  
B. It increases replication  
C. It increases partition count  
D. It removes network traffic entirely

### 17.
Why can increasing `linger.ms` hurt latency?

A. Producer may wait longer before sending a batch  
B. Consumer commits stop  
C. Broker storage disappears  
D. Schemas are rejected

### 18.
A topic has a hot key causing one partition to receive most traffic. What is the likely issue?

A. Key distribution is skewed  
B. Replication is disabled  
C. TLS is broken  
D. Consumer offset reset

### 19.
What is a hot partition?

A. Partition receiving disproportionate traffic/load  
B. Partition with a schema  
C. Partition with no leader  
D. Partition with no records

### 20.
What can mitigate key-based hot partitioning?

A. Better key distribution/domain design where business ordering permits  
B. Always increase replication only  
C. Disable keys  
D. Delete the topic

### 21.
A new broker is added but remains mostly idle. Why?

A. Existing partition assignments were not necessarily moved  
B. Kafka does not support new brokers  
C. Consumers reject new brokers  
D. Schemas block it

### 22.
What operational action may be needed?

A. Partition reassignment  
B. Schema reset  
C. Consumer offset reset  
D. TLS rotation

### 23.
Reassignment causes application latency to increase. Why?

A. Replica movement consumes network/disk resources  
B. Kafka stops serving all traffic  
C. Schemas are deleted  
D. Consumer groups disappear

### 24.
What can help?

A. Throttle reassignment and monitor resource usage  
B. Increase all client timeouts indefinitely  
C. Delete the moved topic  
D. Disable replication

### 25.
A client can bootstrap but cannot connect to the broker returned in metadata. What should you check?

A. advertised.listeners and network reachability  
B. Consumer lag only  
C. Retention only  
D. Schema compatibility only

### 26.
A TLS client fails during handshake. What should you check?

A. Certificate chain, truststore, hostname, protocol, and key material  
B. Consumer partition count  
C. Topic retention  
D. Consumer offset

### 27.
A client authenticates successfully but receives an authorization error. What should you check?

A. ACLs and principal/resource/operation permissions  
B. DNS only  
C. Producer batching  
D. Replication factor only

### 28.
A client cannot authenticate. What should you check?

A. Credentials, SASL mechanism, security protocol, and broker configuration  
B. Topic compaction  
C. Consumer lag  
D. Partition count

### 29.
Broker CPU is high and request latency is high. What should you inspect?

A. Traffic, request rates, partition leadership, compression, GC, and resource bottlenecks  
B. Only schemas  
C. Only consumer offsets  
D. Only ACLs

### 30.
A broker has unusually high network output. What might explain it?

A. Consumer traffic or replication/recovery activity  
B. Schema compatibility  
C. Consumer group deletion  
D. Topic name length

### 31.
A consumer group has many rebalances. What should you investigate?

A. Membership stability, processing time, polling, deployment behavior, and networking  
B. Disk retention only  
C. Schema version only  
D. Topic name only

### 32.
A consumer is processing slowly because a database is overloaded. Should you immediately add consumers?

A. Not necessarily; more consumers may increase database load  
B. Yes, always  
C. Only if topic is compacted  
D. Only if TLS is enabled

### 33.
What is a bottleneck?

A. Resource or component limiting overall system throughput  
B. A schema subject  
C. A Kafka offset  
D. A topic partition only

### 34.
What is the best performance-tuning process?

A. Measure -> identify bottleneck -> change one relevant variable -> benchmark -> verify  
B. Change all settings  
C. Copy another cluster's configuration  
D. Disable safety features

### 35.
What should you preserve during an incident?

A. Evidence such as metrics, logs, timestamps, and configuration state  
B. Only screenshots  
C. Nothing  
D. Only consumer offsets

### 36.
Why avoid restarting everything during an incident?

A. It can destroy evidence and introduce additional failure modes  
B. Restarting is never useful  
C. Kafka cannot restart  
D. It changes schemas

### 37.
What is a runbook?

A. Documented operational procedure for a known scenario  
B. A Kafka topic  
C. A schema format  
D. A broker process

### 38.
What should a good runbook include?

A. Symptoms, checks, decisions, actions, verification, and rollback/escalation  
B. Only a title  
C. Only commands  
D. Only logs

### 39.
What does MTTR measure?

A. Mean time to recovery/restore service  
B. Maximum topic retention  
C. Message transaction rate  
D. Metadata transfer rate

### 40.
What does RPO describe?

A. Maximum acceptable data loss measured in time/objective terms  
B. Consumer partition count  
C. Broker CPU  
D. Schema version

### 41.
What does RTO describe?

A. Target time to restore service  
B. Record offset  
C. Replication factor  
D. Topic retention

### 42.
Why is replication not the same as backup?

A. Replicas can share the same operational failure domain and are part of the live cluster  
B. Replication is always a backup  
C. Backups cannot exist in Kafka  
D. Replication stores schemas only

### 43.
Why monitor disk recovery time?

A. Replica recovery can consume substantial resources and affect availability/performance  
B. Disk has no relation to recovery  
C. Schemas determine recovery  
D. Consumers determine disk speed

### 44.
What is graceful degradation?

A. Maintaining partial service under failure rather than failing completely  
B. Deleting data  
C. Disabling replication  
D. Resetting every offset

### 45.
What is blast radius?

A. Scope of systems/resources affected by a failure or change  
B. Disk capacity  
C. Consumer lag  
D. Partition count

### 46.
Why use incremental operational changes?

A. Reduce blast radius and improve diagnosis  
B. Increase uncertainty  
C. Hide failures  
D. Prevent monitoring

### 47.
What should you do after resolving an incident?

A. Verify service, document root cause, and capture preventive actions  
B. Immediately delete logs  
C. Disable alerts  
D. Ignore the incident

### 48.
What is a useful post-incident question?

A. Why did existing controls fail to prevent or detect the issue earlier?  
B. Who should be blamed?  
C. How can monitoring be removed?  
D. Which data can be deleted?

### 49.
What is the strongest troubleshooting principle?

A. Correlate symptoms across layers before changing configuration  
B. Always restart brokers  
C. Always increase timeouts  
D. Always increase consumers

### 50.
What is the operational goal?

A. Reliable service with controlled failure, measurable recovery, and predictable behavior  
B. Maximum configuration complexity  
C. Maximum partition count  
D. Minimum observability

# Answer Keys

## Mock Exam F — Operations / Troubleshooting

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