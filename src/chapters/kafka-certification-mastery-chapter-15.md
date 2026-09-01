# Chapter 15 — Kafka Schema Management & Data Contracts Deep Dive

### Kafka Developer & Administrator Certification Preparation

> Certification track: Kafka Developer + Kafka Administrator  
> Reference foundation: *Kafka: The Definitive Guide*, 2nd Edition

---

## 1. Learning Objectives

By the end of this chapter, you should be able to explain and troubleshoot:

1. [x] Kafka serialization and deserialization
2. [x] Schemas and data contracts
3. [x] Avro
4. [x] JSON Schema
5. [x] Protocol Buffers
6. [x] Schema Registry
7. [x] subjects
8. [x] schema IDs
9. [x] schema versions
10. [x] compatibility modes
11. [x] backward compatibility
12. [x] forward compatibility
13. [x] full compatibility
14. [x] schema evolution
15. [x] breaking changes
16. [x] optional vs required fields
17. [x] defaults
18. [x] producer/consumer compatibility
19. [x] serialization failures
20. [x] deserialization failures
21. [x] Schema Registry operational concerns
22. [x] schema governance
23. [x] production schema-management patterns
24. [x] certification-style scenarios

## 2. Why Schema Management Matters

Kafka transports bytes. At the protocol level, Kafka does not inherently understand:

```text
Customer
Order
Payment
Temperature
```

A producer ultimately sends serialized bytes:

```text
Application Object
        |
        v
    Serializer
        |
        v
      bytes
        |
        v
      Kafka
```

The consumer reverses the process:

```text
      Kafka
        |
        v
      bytes
        |
        v
   Deserializer
        |
        v
Application Object
```

The schema defines how those bytes should be interpreted.

## 3. The Core Problem

Imagine Producer V1 sends:

```json
{
  "id": 123,
  "name": "Alice"
}
```

Later Producer V2 sends:

```json
{
  "id": 123,
  "name": "Alice",
  "country": "FR"
}
```

Can old consumers still understand the message? That is the fundamental schema-evolution question.

## 4. Schema Evolution

Schema evolution means changing the structure of an event while maintaining appropriate compatibility between producers
and consumers.

Example:

```text
    V1
    id
    name

       |
       | evolution
       v

    V2
    id
    name
    country
```

A good schema strategy allows systems to evolve independently.

## 5. Why Kafka Makes This Important

Kafka messages can live for a long time because of `retention + replay + consumer lag + backfills`. A consumer may read
an event produced **hours ago** or **days ago** or **months ago**.

Therefore, schema compatibility is not merely about today's producer and today's consumer. It is also about **old data
being read by newer applications.**

## 6. Serialization Formats

Common schema-based formats include:

* Avro
* JSON Schema
* Protocol Buffers

Kafka itself does not require one particular serialization format. You can technically use:

* JSON
* String
* byte[]
* Avro
* Protobuf
* JSON Schema
* custom binary format

The important question is how producers and consumers agree on the format.

### 6.1. Avro

Avro is a compact serialization system commonly used with Kafka.

Example conceptual schema:

```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    {
      "name": "id",
      "type": "string"
    },
    {
      "name": "amount",
      "type": "double"
    }
  ]
}
```

The schema defines:

```text
Order
├── id
└── amount
```

#### 6.1.1. Why Avro Is Popular with Kafka

Avro provides:

1. [x] compact binary encoding
2. [x] explicit schemas
3. [x] schema evolution support
4. [x] language interoperability
5. [x] integration with Schema Registry

### 6.2. JSON Schema

JSON Schema describes the structure and constraints of JSON documents.

Example:

```json
{
  "type": "object",
  "properties": {
    "id": {
      "type": "string"
    },
    "amount": {
      "type": "number"
    }
  },
  "required": [
    "id",
    "amount"
  ]
}

```

It is particularly useful when systems already work heavily with JSON.

### 6.3. Protocol Buffers

Protocol Buffers, commonly called Protobuf, is another schema-based serialization format.

Example:

```Protobuf 
message Order {
  required string id = 1;
  required double amount = 2;
}
```

The numbered fields are important for Protobuf compatibility and evolution.

### 6.4. Comparing Formats

| Feature                 | Avro      | JSON Schema                   | Protobuf  |
|-------------------------|-----------|-------------------------------|-----------|
| Schema-based            | Yes       | Yes                           | Yes       |
| Binary encoding         | Yes       | Typically JSON representation | Yes       |
| Human-readable payload  | No        | Yes                           | No        |
| Compact                 | Yes       | Usually less compact          | Yes       |
| Schema evolution        | Strong    | Strong                        | Strong    |
| Kafka ecosystem support | Excellent | Excellent                     | Excellent |

The certification question is rarely **Which format is universally best?**. Instead, understand:

**Why would an organization choose one format and how does schema evolution work with it?**

## 7. Schema Registry

A Schema Registry provides centralized schema management.

Conceptually:

```text

              Schema Registry
                    |
       +------------+------------+
       |            |            |
    Schema V1    Schema V2    Schema V3
```

Producers and consumers can interact with the registry to obtain schemas.

## 8. Kafka + Schema Registry

Typical flow:

```text
   Producer
      |
      | serialize
      v
Schema Registry
      |
      | schema ID
      v
    Kafka
      |
      v
   Consumer
      |
      | deserialize
      v
Schema Registry
```

The actual implementation details depend on the serializer/deserializer and registry technology.

### 8.1. Important Distinction

Schema Registry is not Kafka. Kafka stores **events** schema Registry stores **schemas**

Conceptually:

```text
    Kafka
      |
      +--> Event data
    
Schema Registry
      |
      +--> Schema definitions
```

This distinction is fundamental.

## 9. Schema IDs

With common Confluent serialization formats, serialized messages can contain a schema identifier.

```text
Serialized record

+----------------+----------------+
| Schema ID      | Encoded data   |
+----------------+----------------+
```

The consumer uses the schema ID to retrieve the corresponding schema, this avoids embedding the complete schema in every
message.

### 9.1. Why Schema IDs Are Useful

Without a registry, you could potentially have `Message + complete schema` for every message.

With a registry `Message + schema ID`. The schema is centrally managed.

Benefits include:

* reduced payload overhead
* centralized governance
* schema version management
* compatibility validation

## 10. Subjects

A Schema Registry organizes schemas using subjects. A subject is a logical name under which schema versions are managed.

For example:

```text
orders-value
```

might contain:

```text
V1
V2
V3
```

Another subject ```orders-key``` might manage key schemas.

### 10.1. Key vs Value Subjects

Kafka records contain **key** and **value**, schema Registry can manage schemas for both.

```text
orders-key
orders-value
```

This is important because key and value schemas may evolve independently.

### 10.2. Topic-Name Strategy

One common strategy derives subjects from topic names. For example a topic `orders` can result in subjects such as
`orders-key` or `orders-value`

The exact subject naming behavior depends on the configured **subject-name strategy**.

### 10.3. Record-Name Strategy

Another strategy can organize schemas around the record name. For example `com.example.Order` instead of
`orders-value`. This can be useful when the same logical event type is shared across multiple topics.

### 10.4. Topic-Record Strategy

A further strategy combines topic and record identity `topic + record type`. This allows more granular schema
organization. Certification questions may test that **Subject naming strategy affects how schemas are grouped and
therefore how compatibility is evaluated.**

## 11. Schema Version

Suppose `orders-value` contains:

```text
Version 1
Version 2
Version 3
```

These are schema versions under the subject. The schema version is not the same thing as **Kafka message offset** or
**Kafka topic partition**. Do not confuse them.

### 11.1. Schema Version vs Schema ID

Important distinction:

1. **Schema ID** = global registry identifier for a schema
2. **Schema version** = version of the schema under a subject

A schema ID and a subject-specific version are different concepts.

## 12. Compatibility

Compatibility determines whether a new schema can safely interact with existing data/applications.

Typical modes include:

1. [x] BACKWARD
2. [x] FORWARD
3. [x] FULL
4. [x] NONE

and their transitive variants.

### 12.1. Backward Compatibility

Backward compatibility asks **Can the new schema read data written using the old schema?**

```text
 Old data
    |
    v
New schema
    |
    v
  works
```

Example:

```text
V1:
id
name

V2:
id
name
country = "unknown"
```

A V2 reader can interpret V1 data because country has an appropriate default.

#### 12.1.1. Backward Compatibility Mental Model

Memorize:

````text
BACKWARD

New reader
    |
    v
 Old data
````

This is one of the most important certification concepts.

### 12.2. Forward Compatibility

Forward compatibility asks **Can the old schema/application read data written using the new schema?**

```text
 New data
    |
    v
Old schema
    |
    v
  works
```

For example, a new producer adds a field that an old consumer can safely ignore.

#### 12.2.1. Forward Compatibility Mental Model

Memorize:

```text
FORWARD

Old reader
    |
    v
 New data
```

### 12.3. Full Compatibility

Full compatibility combines both directions.

```text
Old reader <----> New data
New reader <----> Old data
```

This is useful when producers and consumers evolve independently.

### 12.4. Compatibility Summary

| Mode     | Main Question                    |
|----------|----------------------------------|
| BACKWARD | Can new consumers read old data? |
| FORWARD  | Can old consumers read new data? |
| FULL     | Can both directions work?        |
| NONE     | No compatibility validation      |

### 12.5. Transitive Compatibility

Suppose you have ``V1 -> V2 -> V3``. Non-transitive compatibility may compare the new schema against only the
immediately previous version. Transitive compatibility evaluates against older versions as required by the configured
transitive mode.

Examples:

1. [x] BACKWARD
2. [x] BACKWARD_TRANSITIVE
3. [x] FORWARD
4. [x] FORWARD_TRANSITIVE
5. [x] FULL
6. [x] FULL_TRANSITIVE

### 12.6. Why Transitive Compatibility Matters

Suppose:

V1 V2 V3 V4

A change in V4 might be compatible with V3 but incompatible with V1.

With ordinary backward compatibility ``V4 <-> V3`` may be sufficient for validation.

With backward-transitive compatibility ``V4 <-> V3`` and ``V4 <-> V2`` and ``V4 <-> V1``
are considered. This gives stronger long-term guarantees.

### 12.7. The Default Compatibility Trap

Do not assume the compatibility mode from memory alone. For certification **Know the semantics of each mode and know
that compatibility is configurable.**
In real systems, always verify the registry configuration.

### 12.8. Adding a Field

Suppose V1 is:

```json
{
  "id": "....",
  "name": "............"
}

```

V2 adds `country`. Question: **Is this backward compatible?**

It depends on the schema format and whether the new field can be resolved when reading old records.

For Avro, _a newly added field generally needs an appropriate default for backward compatibility_.

### 12.9. Why Defaults Matter

Suppose old data contains:

```json
{
  "id": 123,
  "name": "Alice"
}
```

New schema expects:

```text
id
name
country
```

When reading old data ``country`` doesn't exist. A default, tells the reader what value to use. Example
``country = "UNKNOWN"``. Therefore ``Old data + New schema + default = successful resolution``

### 12.10. Removing a Field

Suppose:

```text
V1:
id
name
country

V2:
id
name
```

Removing fields can be compatible in one direction because readers can ignore data that they no longer need. But
compatibility depends on:

* direction
* schema format
* reader/writer resolution

Do not memorize simplistic rules without understanding the direction.

### 12.11. Adding a Required Field

This is a classic breaking-change trap.

```text
V1:
id
name

V2:
id
name
country   REQUIRED
```

If old data doesn't contain country, a V2 reader may not know what value to use. Therefore, the change can break
backward compatibility.

### 12.12. Safe Evolution Pattern

Instead of:

```text
V1
id
name

V2
id
name
country REQUIRED
```

prefer:

```text
V2
id
name
country OPTIONAL/defaulted
```

Then later, once the ecosystem has migrated, stronger constraints can potentially be introduced through a controlled
evolution strategy.

### 12.13. Renaming a Field

Suppose:

```text
V1:
customerId
```

becomes:

```text
V2:
clientId
```

This is dangerous because to a schema system, these can be different fields. You need an explicit migration/evolution
strategy.

### 12.14. Changing Field Types

Suppose ``amount: int`` becomes ``amount: string``. This can be a breaking change. Type changes should always be
evaluated against the specific serialization format's compatibility rules.

### 12.15. Enum Evolution

Enums deserve special attention.

Suppose:

```text
V1:
PENDING
COMPLETED

V2 adds:

CANCELLED
```

A newer producer can emit ``CANCELLED`` but can an older consumer understand it? Possibly not. Therefore, enum evolution
must be analyzed according to:

* producer version
* consumer version
* compatibility direction
* serialization format

## 13. Schema Evolution Is a Distributed-Systems Problem

Imagine:

```text
Producer V1 -----> Kafka -----> Consumer V1
|
| upgrade
v
Producer V2 -----> Kafka -----> Consumer V1
```

Then ``Consumer V2`` may be deployed later. During deployment, you can have:

```text
P1 + C1
P2 + C1
P2 + C2
```

Your schema strategy must support these intermediate states.

## 14. Rolling Deployment

A safe deployment might be:

```text
Step 1

P1 ---> Kafka ---> C1


Step 2

P2 ---> Kafka ---> C1


Step 3

P2 ---> Kafka ---> C2
```

Therefore, schema evolution must support ``P2 + C1`` not merely ``P2 + C2``
This is why compatibility matters operationally.

## 15. Consumer-First vs Producer-First

The correct deployment order depends on compatibility strategy.

For a **backward-compatible** change:

```text
  Deploy new consumer
          |
          v
Then deploy new producer
```

is often a useful pattern. Why? **Because the new consumer can read old data before the new producer starts producing
the new schema.**

## 16. Forward-Compatible Deployment

For forward compatibility, the deployment strategy can favor having old consumers tolerate new producer data.

Again ``compatibility mode + deployment sequence`` must be designed together.

## 17. Schema Registry Validation

When a producer registers a new schema, the registry can validate compatibility.

```text
     Producer
        |
        | Register V3
        v
  Schema Registry
        |
        +--> compatible
        |       |
        |       v
        |     accept
        |
        +--> incompatible
                |
                v
              reject
```

This is a powerful governance mechanism.

## 18. Producer-Side Schema Registration

Typical flow:

```text
       Producer
          |
          v
      Serializer
          |
          v
    Schema Registry
          |
          | register/find schema
          v
       schema ID
          |
          v
serialized Kafka record
```

The serializer integrates the application with the registry.

## 19. Consumer-Side Schema Lookup

Typical flow:

```text
   Kafka record
        |
        v
   Deserializer
        |
        | schema ID
        v
  Schema Registry
        |
        v
      Schema
        |
        v
Deserialized object
```

The consumer doesn't necessarily need to receive the entire schema with every message.

## 20. Schema Caching

Schema-aware serializers/deserializers commonly cache schemas.Why? **Because constantly retrieving the same schema from
the registry would introduce unnecessary network calls.**

```text
Deserializer
     |
     +--> local schema cache
     |
     +--> hit -> deserialize
     |
     +--> miss -> Registry
```

Caching improves performance and reduces registry load.

## 21. Schema Registry Is a Dependency

If the producer cannot access the registry when a new schema must be registered, producing new records may fail.

Likewise, a consumer may have trouble deserializing records if it cannot obtain a required schema and doesn't already
have it cached.

Therefore, ``Kafka + Schema Registry`` should be treated as part of the operational dependency chain.

## 22. What If Schema Registry Is Down?

This is an important production question. Possible behavior depends on:

* serializer
* schema cache
* whether schema is already registered
* consumer/producer configuration

For a producer sending a schema already known locally and registered, the serializer may be able to operate from cache.

For a new schema requiring registration:

````text
Registry unavailable
        |
        v
 registration fails
        |
        v
 producer may fail
````

The exact behavior depends on the client implementation/configuration.

## 23. Schema Registry High Availability

A production registry should itself be highly available.

```text
 Producer
    |
    +----> Registry Node 1
    |
    +----> Registry Node 2
```

The registry's deployment model depends on the chosen Schema Registry implementation. The key architectural principle is
**Do not make schema management a single point of failure.**

## 24. Schema Governance

A mature organization defines:

1. [ ] who can create schemas
2. [ ] who can modify schemas
3. [ ] compatibility policies
4. [ ] naming conventions
5. [ ] ownership
6. [ ] versioning
7. [ ] deprecation
8. [ ] retention
9. [ ] documentation

This prevents schema chaos.

## 25. Data Contracts

A schema is more than a serialization structure.

A data contract defines expectations between producers and consumers.

It can include:

1. [x] field names
2. [x] types
3. [x] required/optional semantics
4. [x] meaning
5. [x] units
6. [x] valid values
7. [x] ownership
8. [x] compatibility guarantees
9. [x] evolution policy

Example:

```text
temperature
type: double
unit: Celsius
range: -100..200
producer: sensor-service
```

That is closer to a real data contract.

## 26. Schema vs Data Contract

Think ``Schema = technical structure`` while
``Data contract = technical structure + semantic expectations + ownership + compatibility rules``

This distinction becomes important in enterprise event-driven architecture.

## 27. Event Naming

Good event names communicate business meaning.

Prefer:

* OrderCreated
* PaymentCaptured
* CustomerRegistered

over vague:

* OrderEvent
* DataChanged
* Message

A clear event name is part of a good contract.

## 28. Event vs State

Consider `OrderCreated`, this represents something that happened, whereas:

```text
Order
{
    status: CREATED
}
```

can represent current state. This distinction affects how events are designed and evolved.

## 29. Schema Evolution Strategy

A practical strategy:

1. [x] Prefer additive changes
2. [x] Add optional/defaulted fields
3. [x] Avoid changing field meaning
4. [x] Avoid destructive renames
5. [x] Avoid incompatible type changes
6. [x] Test compatibility automatically
7. [x] Deploy producers/consumers deliberately

## 30. Additive Evolution

Best-case evolution:

```text
V1:
id
name

V2:
id
name
country

V3:
id
name
country
language
```

The contract grows without changing the meaning of existing fields. This is usually easier to manage.

## 31. Semantic Compatibility

A schema can be technically compatible while being semantically incompatible.

Example:

```text
V1:
amount = USD

V2:

amount = EUR
```

Same field, same type.

Schema Registry may see no structural problem, but the business meaning changed completely.

Schema compatibility does not guarantee semantic compatibility.

## 32. Units Are Part of the Contract

Bad, `temperature: 20.0`, what is it? **20 °C?**, **20 °F?** or **20 K?**.

Better, `temperatureCelsius: 20.0` or an explicit documented contract.

## 33. Nullable vs Optional

Do not automatically assume ``nullable = optional``

They can have different meanings depending on the schema format.

For example **field exists with null** is different from **field does not exist**.

This distinction matters during evolution.

## 34. Schema Compatibility Testing

Compatibility should be tested in CI.

```text
    Pull Request
        |
        v
  Schema validation
        |
        +--> compatible
        |       |
        |       v
        |     merge
        |
        +--> incompatible
                |
                v
              reject
```

This prevents breaking contracts from reaching production.

## 35. Schema Registry as a CI Gate

A mature pipeline:

```text
     Developer
        |
        v
     Git commit
        |
        v
        CI
        |
        +--> Schema compatibility check
        |
        +--> Unit tests
        |
        +--> Integration tests
        |
        v
     Deploy
```

This is much safer than discovering compatibility problems after deployment.

## 36. Contract Testing

Contract tests verify ``producer expectations + consumer expectations``

```text
Producer:
OrderCreated V3

Consumer:
Can deserialize V3?
Can interpret fields?
```

Contract testing complements Schema Registry compatibility checks.

## 37. Schema Registry vs Contract Testing

Schema Registry checks structural compatibility.

Contract testing can verify broader behavior.

```text
  Schema Registry
        |
        +--> structure

  Contract tests
        |
        +--> structure
        +--> expected semantics
        +--> consumer behavior
```

Use both where appropriate.

## 38. Serialization Failure

A producer can fail before Kafka receives the record. Potential causes:

1. [x] invalid field
2. [x] wrong type
3. [x] schema mismatch
4. [x] invalid enum
5. [x] registry failure

The record may never reach Kafka.

## 39. Deserialization Failure

A consumer can fail while converting Kafka bytes into an application object. Potential causes:

1. [x] unknown schema
2. [x] incompatible schema
3. [x] corrupt data
4. [x] unexpected type
5. [x] invalid payload

This is different from a Kafka broker failure.

## 40. Poison Pill

A poison pill is a record that repeatedly causes consumer processing/deserialization failure.

Example:

```text
offset 100 -> OK
offset 101 -> BAD
offset 102 -> OK
```

If the consumer cannot handle offset 101:

```text
   101
    |
    X
    |
  retry
    |
    X
    |
  retry

```

The consumer can become stuck.

## 41. Handling Poison Pills

A production architecture may use:

```text
deserialization error
        |
        v
   error handling
        |
        +--> DLQ / quarantine
        |
        +--> alert
        |
        +--> continue
```

The exact mechanism depends on the Kafka client/framework.

## 42. Schema Evolution and DLQ

Suppose producer deploys an incompatible schema accidentally, the consumers begin receiving
``deserialization failures``.

A **DLQ** can prevent one malformed/incompatible event from blocking an entire processing pipeline.

But a **DLQ** is not a substitute for **schema governance**.

## 43. Schema Compatibility Failure

Suppose:

```text
Current schema = V2
New schema = V3
```

Registry compatibility check returns ``INCOMPATIBLE``. A correctly governed producer should not simply deploy V3 anyway,
instead ``developer -> understand incompatibility -> redesign schema``

## 44. Schema Evolution Anti-Pattern

Bad:

```text
   Producer team
        |
        +--> changes schema
        |
        +--> deploys
        |
        X
  Consumer breaks
```

Better:

```text
    Producer team
          |
          v
    Schema change
          |
          v
   Compatibility test
          |
          v
 Consumer compatibility
          |
          v
 Controlled deployment
```

## 45. Topic Ownership

A good contract should have a clear owner.

Example:

```text
Topic:
orders.events

Owner:
Order Platform Team
```

Consumers know whom to contact when the contract changes.

## 46. Schema Deprecation

Schemas should not remain indefinitely without governance.

A lifecycle might be:

```text
 Proposed
    |
    v
  Active
    |
    v
Deprecated
    |
    v
 Retired
```

Consumers should migrate before a schema is retired where required.

## 47. Versioning

There are multiple forms of versioning:

1. [ ] schema version
2. [ ] event version
3. [ ] application version
4. [ ] API version

Do not assume they are identical. For example ``OrderCreated schema V3``
doesn't necessarily mean **Order service V3**

## 48. Version in Event Payload?

Should you include:

```json
{
  "eventType": "OrderCreated",
  "version": 3
}

```

Sometimes this can be useful but don't blindly duplicate information already managed by your serialization/schema
infrastructure. The design should have a clear reason.

## 49. Schema Registry Does Not Version Business Semantics

Suppose ``status = "PAID"`` initially means **Payment successfully captured.**

Later someone changes it to mean **Payment authorization completed.**

The schema may remain ``status: string``

Schema Registry cannot detect that semantic change. Therefore, data contracts need human/business governance too.

## 50. Schema Compatibility Matrix

For certification preparation:

| Change                             | Backward            | Forward                 | Full                |
|------------------------------------|---------------------|-------------------------|---------------------|
| Add field with appropriate default | Often ✓            | Depends on format/rules | Depends             |
| Remove field                       | Direction-dependent | Direction-dependent     | Direction-dependent |
| Rename field                       | Usually problematic | Usually problematic     | Usually problematic |
| Arbitrary type change              | Often ✗            | Often ✗                | Often ✗            |
| Change semantic meaning            | May appear ✓       | May appear ✓           | May appear ✓       |

Important: exact compatibility results are format-specific. Do not use this table as a substitute for the schema
format's resolution rules.

## 51. Certification traps

### 51.1. Certification Trap — "Schema Registry Prevents All Breaking Changes"

**False**, schema Registry can enforce configured compatibility rules.

It cannot understand every business semantic.

For example ``amount: 100`` changing from ``USD`` to `EUR` may remain structurally valid.

### 51.2. Certification Trap — "Kafka Stores Schemas"

**Not necessarily**. Kafka stores records and the schema Registry is a separate system for centralized schema
management.

A Kafka message can technically contain any bytes.

### 51.3. Certification Trap — "Every Message Contains the Full Schema"

**Not necessarily.** With common Schema Registry serializers, the message can contain a schema identifier and encoded
payload while the schema itself is managed by the registry.

### 51.4. Certification Trap — "Schema Version Equals Schema ID"

**False**. A schema ID is an identifier for a schema.

A version is its position/version under a subject, these are different concepts.

### 51.5. Certification Trap — Backward

Question: **What does backward compatibility mean?**

<details>
<summary>Correct mental model</summary>

```text
NEW schema/reader
       |
       v
   OLD data
```

Not:

``old schema -> new data`` => That is forward compatibility.
</details>

### 51.6. Certification Trap — Forward

Question: **What does forward compatibility mean?**

Remember:

```text
OLD reader
    |
    v
 NEW data
```

### 51.7. Certification Trap — Full

Full compatibility means supporting both:

```text
new reader -> old data
old reader -> new data
```

## 52. Certification Scenarios

### 52.1. Certification Scenario — New Optional Field

```text
V1:

Order
id
amount

V2:

Order
id
amount
currency = "EUR"
```

Question: **Why can a default make this safer?**

Because old records do not contain the new field.

The reader can resolve the missing field using the default.

### 52.2. Certification Scenario — Required Field

```text
V1:
id

V2:
id
customerId REQUIRED
```

Question: **What happens when V2 reads old V1 records?** There is no customerId. Without an appropriate
resolution/default mechanism deserialization/schema resolution goes on error. This can violate backward compatibility.

### 52.3. Certification Scenario — Old Consumer

```text
V1:
id
name
```

V2 producer adds ``country``

Question: **What compatibility direction is important if V1 consumers must read V2 records?**

Think:

old reader | v new data

Therefore:

Forward compatibility.

### 52.4. Certification Scenario — New Consumer

V2 consumer must read records produced by V1.

Think:

```text
new reader
    |
    v
 old data
```

Therefore, Backward compatibility.

### 52.5. Certification Scenario — Full Compatibility

A company allows producers and consumers to upgrade independently.

During deployment:

```text
P1 + C1
P2 + C1
P1 + C2
P2 + C2
```

They want both old/new readers and old/new data to coexist safely. A strong compatibility requirement may be `FULL`
depending on the actual schema evolution.

### 52.6. Certification Scenario — Registry Outage

Producer wants to introduce a new schema. Schema Registry is unavailable.

**What is likely to happen?**, if the schema must be registered and cannot be found in the local cache:

```text
     register schema
            |
            X
    registry unavailable
            |
            v
serialization/production failure
```

The exact behavior depends on the serializer implementation and configuration.

### 52.7. Certification Scenario — Semantic Breaking Change

V1 `amount = USD`, V2 `amount = EUR`. The schema remains `amount: double`

Question: **Will Schema Registry necessarily detect the problem?**

**No.** The structure is unchanged on the other hand the semantic contract has changed.

### 52.8. Certification Scenario — Subject Strategy

Two topics:

```text
orders-eu
orders-us
```

Both use `OrderCreated`

You want schemas to be managed by record identity rather than separate topic-value subjects.

**Which concept should you investigate?**. **Record-name subject** strategy.

The key is understanding how subject **naming** controls **schema grouping**.

## 53. Administrator Perspective

Administrators need to understand:

1. [x] Schema Registry availability
2. [x] Schema compatibility configuration
3. [x] schema storage
4. [x] authentication
5. [x] authorization
6. [x] TLS
7. [x] monitoring
8. [x] capacity
9. [x] backup/recovery

Schema management is an operational platform.

## 54. Security

Schema Registry can itself require:

1. [x] TLS
2. [x] authentication
3. [x] authorization

A production topology may therefore look like:

```text
     Producer
        |
     TLS/auth
        |
        v
  Schema Registry
        |
      Kafka
```

The security model must protect schema modification as well as schema retrieval.

## 55. Why Schema Modification Is Sensitive

Imagine an unauthorized user changes a schema contract. Potential consequences:

1. [x] producer failures
2. [x] consumer failures
3. [x] data corruption
4. [x] semantic inconsistency
5. [x] production outage

Therefore, **Schema registration and modification should be governed and access-controlled.**

## 56. Monitoring Schema Registry

Useful operational signals include:

1. [x] request rate
2. [x] request latency
3. [x] error rate
4. [x] availability
5. [x] storage/database health
6. [x] authentication failures
7. [x] schema registration failures
8. [x] compatibility failures

Also monitor Kafka clients for:

1. [x] serialization errors
2. [x] deserialization errors

## 57. Schema Registry Capacity

Schema Registry traffic is generally much smaller than Kafka event traffic, but it still needs appropriate capacity.

Potential bottlenecks include:

1. [x] CPU
2. [x] memory
3. [x] network
4. [x] backing store
5. [x] request latency

Caching reduces repeated schema lookups.

## 58. Disaster Recovery for Schemas

A DR design should consider both **Kafka data** and **schema metadata**

If Kafka data is recovered but the required schemas cannot be recovered:

```text
   Kafka records
        |
        v
     consumer
        |
        X
 schema unavailable
```

Therefore, schema infrastructure belongs in the DR plan.

## 59. Schema Backup

A mature platform should have a strategy to recover:

1. [x] subjects
2. [x] schema versions
3. [x] compatibility configuration
4. [x] access control

Do not assume that recovering Kafka topics automatically recovers Schema Registry state.

## 60. Schema Governance Workflow

A production workflow:

```text
     Developer
        |
        v
Define event contract
        |
        v
      Review
        |
        v
 Compatibility test
        |
        v
   CI validation
        |
        v
 Register schema
        |
        v
  Deploy producer
        |
        v
 Deploy consumers
        |
        v
     Monitor
```

## 61. Recommended Production Rules

1. **Rule 1**: Prefer additive evolution.
2. **Rule 2**: Give new fields appropriate defaults when backward compatibility requires them.
3. **Rule 3**: Avoid changing the meaning of existing fields.
4. **Rule 4**: Avoid arbitrary type changes.
5. **Rule 5**: Treat schema changes like API changes.
6. **Rule 6**: Validate compatibility in CI.
7. **Rule 7**: Document semantic meaning.
8. **Rule 8**: Assign ownership.
9. **Rule 9**: Test rolling deployments.
10. **Rule 10**: Include Schema Registry in DR planning.

## 62. Advanced Mental Model

Think about schema evolution as a matrix:

```text
                    DATA
             OLD            NEW
              |               |
              v               v
        +-------------+-------------+
        |             |             |
READER  |             |             |
        |             |             |
OLD     |     ?       |     ?       |
        |             |             |
NEW     |     ?       |     ?       |
        +-------------+-------------+
```

Compatibility determines which combinations are safe. The important combinations are:

1. [x] `NEW reader + OLD data`
2. [x] `OLD reader + NEW data`
3. [x] Full compatibility wants both.

## 63. Architecture: Schema Registry in the Event Platform

A mature architecture:

```text
                    +------------------+
                    | Schema Registry  |
                    +--------+---------+
                             ^
                             |
                    schema lookup/register
                             |
+----------+        +--------+---------+        +----------+
| Producer |------->|     Kafka        |------->| Consumer |
+----------+        +------------------+        +----------+
     |                                                 |
     +----------------- Schema-aware ------------------+
```

1. [x] Kafka remains the event transport.
2. [x] Schema Registry manages the contracts.
3. [x] Applications implement the business semantics.

## 64. Three Layers of Data Contracts

Think of contracts as three layers:

```text
Layer 1
Serialization
----------------
Avro / JSON Schema / Protobuf


Layer 2
Schema compatibility
----------------
Backward / Forward / Full


Layer 3
Business semantics
----------------
- meaning
- units
- invariants
- ownership
```

A production-grade event platform needs all three.

## 65. Common Mistakes

1. [x] **Mistake 1**: Using JSON everywhere because it is human-readable without defining a contract.
2. [x] **Mistake 2**: Allowing producers to make arbitrary breaking changes.
3. [x] **Mistake 3**: Ignoring old retained messages.
4. [x] **Mistake 4**: Treating Schema Registry as part of Kafka itself.
5. [x] **Mistake 5**: Confusing schema ID and schema version.
6. [x] **Mistake 6**: Ignoring semantic compatibility.
7. [x] **Mistake 7**: Not testing rolling deployments.
8. [x] **Mistake 8**: No ownership for schemas.
9. [x] **Mistake 9**: No recovery strategy for Schema Registry.
10. [x] **Mistake 10**: Assuming compatibility rules are identical across Avro, JSON Schema and Protobuf.

## 66. Developer Exam Cheat Sheet

1. Schema = structure of data
2. Serialization = `object -> bytes`
3. Deserialization = `bytes -> object`
4. Schema Registry = central schema management
5. Subject = logical schema namespace/group
6. Schema ID = registry identifier
7. Schema version = version under a subject
8. BACKWARD = `new reader -> old data`
9. FORWARD = `old reader -> new data`
10. FULL = both directions
11. TRANSITIVE = check compatibility across historical versions
12. Default = can enable safe resolution of missing fields
13. Schema compatibility != semantic compatibility
14. Replication != schema management

## 67. Administrator Cheat Sheet

### 67.1. Monitor:

1. [x] Registry availability
2. [x] request latency
3. [x] errors
4. [x] schema registration
5. [x] compatibility failures

### 67.2. Protect:

1. [x] TLS
2. [x] authentication
3. [x] authorization

### 67.3. Operate:

1. [x] compatibility policy
2. [x] subject naming
3. [x] schema lifecycle
4. [x] ownership

### 67.4. Recover:

1. [x] schemas
2. [x] subjects
3. [x] compatibility configuration
4. [x] registry infrastructure

### 67.5. Test:

1. [x] schema evolution
2. [x] rolling deployment
3. [x] old data replay
4. [x] consumer compatibility

## 68. Certification Questions

1. [x] Question: **What does backward compatibility mean?**

<details>
<summary>Answer</summary>
A new reader/schema can read old data.
</details>

2. [x] Question: **What does forward compatibility mean?**

<details>
<summary>Answer</summary>
An old reader/schema can read new data.
</details>

3. [x] Question: **What does full compatibility provide?**

<details>
<summary>Answer</summary>
Compatibility in both directions.
</details>

4. [x] Question: **What is a Schema Registry?**

<details>
<summary>Answer</summary>
A centralized system for managing schemas, versions, compatibility and related metadata.
</details>

5. [x] Question: **Does Kafka require Schema Registry?**

<details>
<summary>Answer</summary>
No. Kafka can transport arbitrary bytes. Schema Registry is an external schema-management component.
</details>

6. [x] Question: **What is a subject?**

<details>
<summary>Answer</summary>
A logical namespace/group under which schema versions are managed.
</details>

7. [x] Question: **Is schema ID the same as schema version?**

<details>
<summary>Answer</summary>
No.
</details>

8. [x] Question: **Why are defaults important?**

<details>
<summary>Answer</summary>
They can allow a new reader to resolve fields absent from older records.
</details>

9. [x] Question: **Why is adding a required field potentially dangerous?**

<details>
<summary>Answer</summary>
Older records don't contain it, so the new reader may be unable to resolve the field.
</details>

10. [x] Question: **Can Schema Registry detect a change in business meaning?**

<details>
<summary>Answer</summary>
Not necessarily. Structural compatibility does not guarantee semantic compatibility.
</details>

11. [x] Question: **What is a poison pill?**

<details>
<summary>Answer</summary>
A record that repeatedly causes processing or deserialization failure.
</details>

12. [x] Question: **Why use compatibility checks in CI?**

<details>
<summary>Answer</summary>
To detect breaking schema changes before deployment.
</details>

13. [x] Question: **Why is Schema Registry part of DR planning?**

<details>
<summary>Answer</summary>
Recovered Kafka data may be unusable if required schemas and registry metadata are unavailable.
</details>

14. [x] Question: **Why are subject naming strategies important?**

<details>
<summary>Answer</summary>
They determine how schemas are grouped and therefore how compatibility relationships are managed.
</details>

15. [x] Question: **What is the safest general schema evolution strategy?**

<details>
<summary>Answer</summary>
1. Prefer additive 
2. backward-compatible changes
3. appropriate defaults 
4. semantic stability
5. automated compatibility testing
6. controlled deployment.
</details>

## 69. Final Exam Mental Model

Memorize this:

```text
                 KAFKA DATA CONTRACTS
                         |
          +--------------+--------------+
          |              |              |
     Serialization    Registry       Governance
          |              |              |
      Avro/JSON/      Subjects        Ownership
      Protobuf        Versions        Lifecycle
          |              |              |
          +--------------+--------------+
                         |
                    Compatibility
                         |
             +-----------+-----------+
             |           |           |
         BACKWARD     FORWARD      FULL
             |           |           |
         New reader   Old reader   Both
         Old data     New data     directions
                         |
                         v
                  Schema Evolution
                         |
             +-----------+-----------+
             |           |           |
          Additive     Defaults    Semantic
           changes                  contract
```

The most important principle is:

> A Kafka schema is not just a serialization detail. 
> It is a contract between independently evolving producers and consumers.

And at certification level, always connect:

````text
      Schema
         +
   Serialization
         +
   Schema Registry
         +
    Compatibility
         +
   Deployment order
         +
   Consumer behavior
         +
    Data semantics
````

That is the complete mental model for Kafka schema management.