# Mock Exam E — Security / Networking

**Questions: 50**

### 1.
Authentication answers which question?

A. What can you do?  
B. Who are you?  
C. Which partition is leader?  
D. How long is retention?

### 2.
Authorization answers which question?

A. Who are you?  
B. What are you allowed to do?  
C. Where is the broker?  
D. What schema version exists?

### 3.
What is TLS primarily used for?

A. Encryption and endpoint authentication  
B. Partition assignment  
C. Consumer offsets  
D. Topic compaction

### 4.
What is SASL primarily associated with?

A. Authentication  
B. Replication  
C. Retention  
D. Partition count

### 5.
Which is a SASL mechanism?

A. SCRAM  
B. JSON  
C. Avro  
D. gzip

### 6.
What does an SSL/TLS truststore generally contain?

A. Trusted certificates/certificate authorities  
B. Kafka records  
C. Consumer offsets  
D. Topic partitions

### 7.
What does a keystore commonly contain?

A. Private key and certificate material for an identity  
B. Consumer offsets  
C. Topic data  
D. Schemas only

### 8.
Why is hostname verification important?

A. It helps ensure the server certificate identity matches the intended endpoint  
B. It increases partitions  
C. It changes retention  
D. It resets offsets

### 9.
What is an ACL?

A. Authorization rule controlling access to Kafka resources  
B. A serialization format  
C. A broker log  
D. A consumer offset

### 10.
What can an ACL identify?

A. Principal, resource, operation, permission, and potentially host  
B. Only partition count  
C. Only schema version  
D. Only broker disk

### 11.
What does least privilege mean?

A. Grant only the permissions required  
B. Give every service cluster-admin access  
C. Disable authentication  
D. Disable ACLs

### 12.
Why should application and admin credentials be separated?

A. Reduce blast radius and privilege  
B. Increase throughput  
C. Increase retention  
D. Enable compaction

### 13.
What is `security.protocol` used for?

A. Select the client security/transport protocol combination  
B. Select partition count  
C. Select retention  
D. Select schema compatibility

### 14.
Why must client and broker security settings match?

A. They need compatible protocol/authentication configuration to communicate  
B. Kafka otherwise creates a new topic  
C. Consumer offsets disappear  
D. Replication stops permanently

### 15.
What is a common cause of TLS handshake failures?

A. Certificate trust, hostname, protocol, or credential mismatch  
B. Consumer lag only  
C. Partition count  
D. Topic compaction

### 16.
What can cause `SSLHandshakeException`?

A. TLS certificate/configuration incompatibility  
B. Consumer offset reset  
C. Topic retention  
D. Schema evolution

### 17.
What is SASL/SCRAM?

A. Username/password-style SASL authentication mechanism using salted challenge-response credentials  
B. Compression  
C. Partition assignment  
D. Schema format

### 18.
What is Kerberos commonly associated with in Kafka?

A. GSSAPI authentication  
B. Record compression  
C. Topic compaction  
D. Consumer lag

### 19.
What is OAuth/OAUTHBEARER associated with?

A. Token-based authentication  
B. Partition reassignment  
C. Retention  
D. Replication

### 20.
What is mTLS?

A. Mutual TLS where both sides authenticate with certificates  
B. Multi-topic log storage  
C. Manual topic leadership  
D. Message timestamp logging

### 21.
What is the difference between encryption in transit and at rest?

A. In transit protects network communication; at rest protects stored data  
B. They are identical  
C. At rest means TLS  
D. In transit means disk encryption

### 22.
What does an advertised listener provide?

A. Broker address information clients should use  
B. ACL rules  
C. Schema versions  
D. Consumer offsets

### 23.
Why can NAT complicate Kafka networking?

A. Clients may receive broker addresses that are not reachable through the translated network  
B. It changes schemas  
C. It changes partitions  
D. It disables compaction

### 24.
What is DNS important for in Kafka deployments?

A. Resolving broker/controller/service hostnames  
B. Consumer offsets  
C. Schema compatibility  
D. Retention

### 25.
A client can resolve the bootstrap host but cannot connect to a broker returned in metadata. What should you inspect?

A. advertised.listeners and routing/firewall/security  
B. Schema Registry only  
C. Topic retention only  
D. Consumer offset only

### 26.
What is a security principal?

A. Identity used by authorization decisions  
B. Kafka partition  
C. Broker replica  
D. Consumer offset

### 27.
What is resource authorization applied to?

A. Resources such as topics, groups, cluster operations, and other supported Kafka resources  
B. Only brokers  
C. Only schemas  
D. Only disks

### 28.
Why should ACLs be tested after deployment?

A. Incorrect ACLs can prevent required operations or grant excessive access  
B. ACLs always work automatically  
C. Testing changes partition count  
D. ACLs control compression

### 29.
What should be done with private keys?

A. Protect them as secrets with restricted access  
B. Put them in public repositories  
C. Print them in logs  
D. Embed them in topic records

### 30.
Why rotate certificates?

A. Reduce exposure from expired/compromised credentials and meet operational security requirements  
B. Increase partitions  
C. Increase consumer lag  
D. Enable compaction

### 31.
What happens when a certificate expires?

A. TLS-authenticated connections can fail  
B. Kafka automatically increases retention  
C. Consumer groups are deleted  
D. Partitions are compacted

### 32.
What is a trust anchor?

A. Certificate authority/root certificate trusted to validate certificate chains  
B. Kafka topic  
C. Consumer offset  
D. Broker replica

### 33.
What is certificate chain validation?

A. Verifying that a certificate chains to a trusted authority and meets validation rules  
B. Checking consumer lag  
C. Checking partition count  
D. Checking retention

### 34.
What is SASL authentication different from TLS encryption?

A. SASL primarily authenticates; TLS provides secure transport and can authenticate endpoints  
B. They are exactly the same  
C. SASL stores records  
D. TLS controls consumer groups

### 35.
Can Kafka use TLS and SASL together?

A. Yes  
B. No  
C. Only for compacted topics  
D. Only in development

### 36.
Why should security configuration be externalized from application code?

A. Easier secret management, rotation, and environment-specific deployment  
B. It increases partitions  
C. It disables authentication  
D. It removes schemas

### 37.
What is a common mistake with ACLs?

A. Granting wildcard permissions broader than necessary  
B. Using least privilege  
C. Testing access  
D. Separating principals

### 38.
What is a common network mistake?

A. Advertising container-only hostnames to external clients  
B. Monitoring DNS  
C. Testing connectivity  
D. Using TLS

### 39.
What is a common Docker/Kafka networking problem?

A. Internal broker hostname is advertised to a client outside the Docker network  
B. Consumer lag is too low  
C. Schema is backward compatible  
D. Disk has too much capacity

### 40.
What should a Kafka security troubleshooting process begin with?

A. Identify protocol, endpoint, identity, and exact failure stage  
B. Disable all security  
C. Delete ACLs  
D. Restart every broker

### 41.
What does a connection timeout generally suggest?

A. Network reachability/routing/firewall/listener issues are possibilities  
B. Schema compatibility only  
C. Consumer offset only  
D. Compaction only

### 42.
What does an authentication failure generally suggest?

A. Credentials/mechanism/identity configuration problem  
B. Partition reassignment  
C. Retention policy  
D. Consumer lag

### 43.
What does an authorization failure generally suggest?

A. Principal authenticated but lacks required permission  
B. Broker is necessarily down  
C. Topic is necessarily deleted  
D. Schema is necessarily invalid

### 44.
Why distinguish authentication and authorization during troubleshooting?

A. They have different root causes and remedies  
B. They are the same operation  
C. They both control retention  
D. They both control partition count

### 45.
What should production security monitoring include?

A. Authentication failures, authorization failures, certificate expiry, unusual access, and network anomalies  
B. Only topic names  
C. Only consumer offsets  
D. Only compression

### 46.
What is defense in depth?

A. Multiple independent security controls  
B. One password for all systems  
C. No encryption  
D. No ACLs

### 47.
Why use separate principals for services?

A. Better auditability and least privilege  
B. More partitions  
C. Faster compression  
D. Larger messages

### 48.
What is an audit trail useful for?

A. Understanding who accessed or changed protected resources  
B. Increasing retention  
C. Creating partitions  
D. Resetting offsets

### 49.
What is the safest approach to a security configuration change?

A. Test, roll out incrementally, monitor, and maintain rollback capability  
B. Disable security first  
C. Change every listener simultaneously without testing  
D. Delete all ACLs

### 50.
What is the best networking mental model?

A. Bootstrap address -> metadata -> advertised broker addresses -> actual broker connectivity  
B. Client connects only once to bootstrap forever  
C. DNS is irrelevant  
D. Kafka never returns broker addresses

# Answer Keys

## Mock Exam E — Security / Networking

```text
1 B
2 B
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