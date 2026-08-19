# Mock Exam C — Schema / Connect / Streams

**Questions: 50**

### 1.
What problem does Schema Registry primarily solve?

A. Broker leader election  
B. Central schema storage and compatibility management  
C. Consumer assignment  
D. Disk management

### 2.
Why use schemas with Kafka records?

A. To make data contracts explicit and support controlled evolution  
B. To increase broker count  
C. To eliminate partitions  
D. To replace consumer groups

### 3.
Which is a common schema format?

A. Avro  
B. JPEG  
C. CSS  
D. Bash

### 4.
What is schema evolution?

A. Changing a data schema over time while managing compatibility  
B. Changing broker IPs  
C. Moving partitions  
D. Resetting offsets

### 5.
What does backward compatibility generally mean?

A. New schema can read data written with the previous schema  
B. Old consumers can always read future data regardless of changes  
C. Brokers can read any format  
D. Producers can ignore schemas

### 6.
What does forward compatibility generally mean?

A. Older consumers can read data produced using newer compatible schema versions  
B. Brokers automatically upgrade clients  
C. New producers ignore schemas  
D. Consumers read only latest offsets

### 7.
What does full compatibility aim to provide?

A. Both backward and forward compatibility  
B. No schema validation  
C. Broker encryption  
D. Consumer load balancing

### 8.
Why can removing a required field break consumers?

A. Existing consumers may depend on that field  
B. It changes broker IDs  
C. It increases replication  
D. It disables compression

### 9.
What is a schema subject?

A. A logical namespace under which schema versions are managed  
B. A Kafka broker  
C. A partition  
D. A consumer group

### 10.
Why is schema compatibility important in event-driven systems?

A. Producers and consumers evolve independently  
B. Brokers cannot store bytes  
C. Kafka requires SQL  
D. Consumers never change

### 11.
What is Kafka Connect?

A. A framework for integrating Kafka with external systems  
B. A replacement for Kafka brokers  
C. A schema format  
D. A consumer protocol only

### 12.
What is a source connector?

A. Moves data from an external system into Kafka  
B. Moves Kafka data into an external system  
C. Stores schemas  
D. Manages brokers

### 13.
What is a sink connector?

A. Moves Kafka data to an external system  
B. Moves database data into Kafka  
C. Manages KRaft  
D. Creates partitions

### 14.
What is a Connect worker?

A. Runtime process that executes connectors/tasks  
B. Kafka controller  
C. Schema Registry database  
D. Consumer group coordinator

### 15.
What is a connector?

A. Logical integration configuration defining how data should be moved  
B. Kafka partition  
C. Consumer group  
D. Broker

### 16.
What is a task?

A. Execution unit that performs connector work  
B. Kafka topic  
C. Schema subject  
D. Broker replica

### 17.
Why scale connector tasks?

A. Increase parallel work where the connector supports it  
B. Increase topic retention  
C. Change schema compatibility  
D. Elect leaders

### 18.
Can setting more connector tasks always increase throughput?

A. No; the connector, source/sink system, partitioning, and bottlenecks determine scalability  
B. Yes, always  
C. Only for compacted topics  
D. Only with TLS

### 19.
What is a Single Message Transform?

A. A lightweight record transformation applied within Connect  
B. A Kafka Streams topology  
C. A broker replication mechanism  
D. A schema registry operation

### 20.
What is a dead-letter/error topic in Connect commonly used for?

A. Records that fail processing according to connector error-handling configuration  
B. Broker metadata  
C. Partition leaders  
D. TLS keys

### 21.
What is Kafka Streams?

A. A Java library for building stream-processing applications on Kafka  
B. A Kafka storage format  
C. A broker protocol  
D. A schema registry

### 22.
What is a KStream?

A. An abstraction representing an unbounded stream of records  
B. A broker  
C. A compacted topic  
D. A schema subject

### 23.
What is a KTable conceptually?

A. A table-like view of the latest value per key  
B. A physical Kafka broker  
C. A consumer group  
D. A Connect worker

### 24.
What is a state store?

A. Local persistent state used by a Kafka Streams application  
B. Kafka controller metadata  
C. Schema Registry storage  
D. Consumer group metadata

### 25.
What is a changelog topic?

A. Kafka topic used to back up state-store changes  
B. Dead-letter topic only  
C. Broker log  
D. Schema topic

### 26.
Why are changelog topics important?

A. They allow state restoration after application failure  
B. They replace brokers  
C. They eliminate partitions  
D. They provide TLS

### 27.
What is repartitioning?

A. Redistributing records according to a new key so downstream operations can be correctly partitioned  
B. Replicating all topics  
C. Resetting offsets  
D. Moving brokers

### 28.
When might Kafka Streams require repartitioning?

A. When a key-changing operation is followed by a key-dependent operation  
B. Whenever a record is compressed  
C. Whenever a broker restarts  
D. Whenever a schema changes

### 29.
Why is repartitioning potentially expensive?

A. It introduces additional Kafka traffic and processing  
B. It deletes data  
C. It disables state stores  
D. It disables caching

### 30.
What is a windowed aggregation?

A. Aggregation over records associated with time windows  
B. Broker reassignment  
C. Schema versioning  
D. Consumer offset reset

### 31.
What is a tumbling window?

A. Non-overlapping fixed-duration windows  
B. Infinite window  
C. Random window  
D. Broker window

### 32.
What is a hopping window?

A. Fixed-size windows that advance by a smaller hop interval and may overlap  
B. A Kafka broker  
C. A schema version  
D. A consumer timeout

### 33.
What is a session window?

A. Window based on periods of activity separated by inactivity gaps  
B. A security session  
C. A consumer group  
D. A partition leader

### 34.
What is stream-time in Kafka Streams?

A. Time progression derived from record timestamps as observed by the stream task  
B. Broker wall-clock time only  
C. Schema creation time  
D. Consumer commit time

### 35.
What is a grace period used for?

A. Allowing late records to be considered for a windowed result  
B. Increasing replication  
C. Increasing partitions  
D. Extending broker session timeout

### 36.
What is exactly-once processing in Kafka Streams intended to provide?

A. Atomicity of relevant Kafka Streams processing, including state/output/offset coordination under supported semantics  
B. No state stores  
C. No retries  
D. No partitions

### 37.
What is a topology?

A. Graph of stream-processing operations  
B. Broker configuration file  
C. Schema subject  
D. Consumer group

### 38.
What is a source node?

A. A topology node that consumes records from Kafka  
B. A broker  
C. A schema registry  
D. A sink connector

### 39.
What is a sink node?

A. A topology node that writes records to Kafka  
B. A broker controller  
C. A consumer group coordinator  
D. A schema subject

### 40.
What is a processor node?

A. A topology operation that processes records  
B. A broker  
C. A connector worker  
D. A partition replica

### 41.
What is a materialized view?

A. Queryable state derived from stream processing and stored locally/statefully  
B. A broker replica  
C. A schema file  
D. A TLS certificate

### 42.
What is Interactive Queries used for?

A. Querying local/stateful Kafka Streams application state where supported  
B. Managing brokers  
C. Registering schemas  
D. Creating topics

### 43.
What is a foreign-key join?

A. Join based on a key from one table matching a different key field in another table  
B. Broker replication  
C. Consumer assignment  
D. Schema evolution

### 44.
Why can joins require repartitioning?

A. The records may need to be colocated by the join key  
B. Kafka always repartitions every join  
C. Brokers cannot perform joins  
D. Schemas require it

### 45.
What is a GlobalKTable intended for?

A. A replicated table-like view available to all stream tasks  
B. A broker cluster  
C. A Connect worker  
D. A schema registry

### 46.
What is the trade-off of a GlobalKTable?

A. It can avoid repartitioning for some joins but replicates the table data to each application instance  
B. It always reduces memory  
C. It eliminates state  
D. It eliminates Kafka traffic

### 47.
What is a connector configuration?

A. Set of properties controlling connector behavior  
B. Kafka topic data  
C. Schema version  
D. Consumer offset

### 48.
What is distributed mode in Kafka Connect?

A. Multiple workers coordinate to execute connectors/tasks as a distributed service  
B. One worker only  
C. Kafka broker mode  
D. Schema Registry mode

### 49.
What does Connect's internal storage support?

A. Configuration, offsets, and status information for the Connect cluster  
B. Kafka broker log segments  
C. TLS certificates only  
D. Consumer application state

### 50.
What is the most important design principle when combining schemas, Connect, and Streams?

A. Treat data contracts, partitioning, serialization, state, and evolution as one end-to-end design  
B. Ignore schemas  
C. Avoid keys  
D. Maximize repartitioning

# Answer Keys

## Mock Exam C — Schema / Connect / Streams

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