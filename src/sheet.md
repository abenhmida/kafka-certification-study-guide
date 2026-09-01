# High-Value Concepts to Memorize

## Developer

* Partition = ordering + parallelism
* Offset = position
* Key = partitioning identity
* Consumer group = parallel consumption
* Lag = producer/consumer position difference
* At-least-once = duplicates possible
* Idempotence = safe producer retries
* Transactions = atomic Kafka processing
* Schema = data contract
* Connect = integration
* Streams = processing/state
* Repartition = colocate by required key
* Changelog = state recovery

## Administrator

* RF = replica count
* ISR = replicas currently in sync
* Offline partition = no leader
* Under-replicated = ISR < assigned replicas
* KRaft = metadata quorum
* Reassignment = move replicas
* Preferred leader = preferred replica leadership
* Retention = data lifecycle
* Compaction = latest value per key
* Advertised listener = client-facing broker address
* Quota = resource protection

## Security

* Authentication = identity
* Authorization = permission
* TLS = secure transport
* SASL = authentication mechanisms
* ACL = authorization rule
* Truststore = trusted certificates
* Keystore = identity/private-key material
* Least privilege = minimum required access


## Mental Model

```text
                    KAFKA
                      |
       +--------------+--------------+
       |              |              |
    PRODUCER       BROKER          CONSUMER
       |              |              |
       v              v              v
    batching       partition       group
    retries        leader          assignment
    idempotence    replicas        offsets
    transactions   ISR             lag
       |              |              |
       +--------------+--------------+
                      |
                      v
                  DATA MODEL
                      |
               +------+------+
               |             |
            SCHEMA        KEY
               |             |
               v             v
          compatibility   partitioning
                      |
                      v
                 PROCESSING
                      |
              +-------+-------+
              |               |
            CONNECT         STREAMS
              |               |
          integrations    state/repartition
              |               |
              +-------+-------+
                      |
                      v
                 OPERATIONS
                      |
       +--------------+--------------+
       |              |              |
    SECURITY       MONITORING     RECOVERY
       |              |              |
      TLS            lag           ISR
      SASL           disk          reassignment
      ACLs           latency       failover
      auth           traffic       capacity
```