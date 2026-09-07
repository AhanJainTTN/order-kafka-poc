# Apache Kafka: A Practical Learning Guide

This guide builds a complete mental model of Apache Kafka: what it is, when to use it, how records move through it, how it scales and fails over, and how to design topics, partitions, producers, and consumers safely.

## 1. What Kafka Is

Apache Kafka is a distributed event-streaming platform. Applications publish records describing things that happened, Kafka stores those records in durable ordered logs, and one or more applications read and process them independently.

Kafka combines three useful capabilities:

- **Messaging:** producers send records and consumers receive them asynchronously.
- **Durable log storage:** records remain available according to retention policy instead of disappearing as soon as one consumer reads them.
- **Stream processing foundation:** applications can transform, aggregate, join, and react to continuously arriving records.

A useful mental model is:

> Kafka is a replicated, partitioned commit log that many independent applications can read at their own pace.

Kafka is especially useful when a system needs several of the following:

- high or growing event throughput;
- several independent consumers of the same data;
- decoupled services;
- retention, replay, or audit history;
- real-time pipelines or stream processing;
- buffering when producers and consumers run at different speeds.

Typical use cases include:

- order, payment, inventory, and shipment events;
- clickstream and product analytics;
- application logs and operational metrics;
- change-data capture from databases;
- fraud detection and other real-time decisions;
- IoT and location telemetry;
- data-lake and warehouse ingestion;
- event-sourced systems.

Kafka can be unnecessary for a small, low-volume workflow with one producer, one consumer, no replay requirement, and simple request-response behavior. A database, direct API, managed queue, or traditional broker may be easier to operate.

## 2. Kafka, APIs, RabbitMQ, and WebSockets

These technologies overlap at the edges but solve different problems.

### Kafka vs synchronous and asynchronous APIs

An API expresses a directed interaction with a known service.

- A synchronous HTTP or gRPC call asks for a result now: `GET /balance`, `validateOtp()`, or `reserveInventory()`.
- An asynchronous API accepts work now and supplies the result later through polling, a callback, or a webhook.
- A Kafka producer usually announces a fact without knowing which consumers exist: `OrderCreated`, `PaymentCompleted`, or `DriverLocationUpdated`.

Use an API when the caller needs a response, current state, validation, or a controlled command. Use Kafka when consumers should react independently, the event should be retained, or the data must feed multiple pipelines.

Most systems use both:

```text
client -> Order API -> database/outbox -> Kafka
                                      |-> payment consumer
                                      |-> inventory consumer
                                      |-> analytics consumer
```

Kafka does not eliminate APIs, databases, or workflow coordination. It supplies an asynchronous event backbone.

### Kafka vs RabbitMQ


| Concern          | Kafka                                                    | RabbitMQ                                                           |
| ---------------- | -------------------------------------------------------- | ------------------------------------------------------------------ |
| Primary model    | Replicated event log                                     | Message broker and queues                                          |
| Main abstraction | Topic partition                                          | Exchange, queue, binding                                           |
| Consumption      | Consumers fetch records and track offsets                | Broker delivers messages; consumers acknowledge them               |
| Record lifecycle | Retained by time/size/compaction policy                  | Commonly removed after acknowledgement                             |
| Replay           | Native through offsets while data is retained            | Not the default queue model                                        |
| Fan-out          | Add independent consumer groups                          | Bind separate queues to an exchange                                |
| Parallelism      | Partition ownership within a group                       | Competing consumers on a queue                                     |
| Routing          | Mainly topic, key, and partition                         | Rich exchange and binding rules                                    |
| Strong fit       | Event history, pipelines, analytics, high-volume streams | Work queues, commands, per-message routing, conventional messaging |


Useful heuristics:

- Choose RabbitMQ when the central idea is **“perform this task”**, especially when flexible routing, per-message acknowledgement, priority, or dead-letter queue patterns matter.
- Choose Kafka when the central idea is **“this event happened”**, especially when several systems need the event or it may be replayed.

Avoid absolute performance claims. Either system can achieve low latency, and both can be configured for durability. Actual latency and throughput depend on batching, persistence, acknowledgements, replication, payloads, hardware, topology, and client settings. Benchmark the intended workload.

### Kafka vs WebSockets

WebSockets maintain bidirectional, long-lived connections to clients. They are a delivery channel, not a durable backend event log.

In a location system, the technologies complement each other:

```text
driver app -> ingestion API -> Kafka -> tracking consumer -> cache/database
                                      -> gateway consumer  -> WebSocket -> rider app
```

- Kafka buffers, retains, fans out, and supports backend processing of location events.
- WebSockets push selected live updates to connected users.
- Stream-processing code, not Kafka itself, performs geofencing, aggregation, anomaly detection, or nearest-driver calculations.



## 3. Core Terminology



### Record, event, or message

The unit written to Kafka. A record normally has:

- topic;
- optional key;
- value/payload;
- timestamp;
- headers;
- a partition and offset assigned as it is written.

Kafka treats keys and values as bytes. JSON, Avro, and Protobuf are application-level serialization choices. A schema registry can provide schema discovery, compatibility checks, and governance.

### Topic

A named logical stream, such as `orders`, `payments`, or `location-updates`. A topic usually represents a cohesive category of records, but topic boundaries are design choices rather than a requirement that every event type have its own topic.

### Partition

A numbered, append-only log belonging to exactly one topic. A topic has one or more partitions. Partitions provide:

- parallel writes and reads;
- distribution across brokers;
- the boundary of Kafka's ordering guarantee.

Partitions are not created per user or key. Millions of keys can map into a fixed set of partitions.

### Key

An optional field commonly used by the producer's partitioner. With a stable partition count and partitioning algorithm, equal serialized keys map to the same partition.

The key therefore helps define:

- which related records share an ordered log;
- how load is distributed;
- which records are grouped for keyed processing or compaction.



### Offset

A monotonically increasing position assigned to a record within one partition. Offsets are not globally unique and are not business event IDs.

For example:

```text
orders-0: offset 40, offset 41, offset 42
orders-1: offset 40, offset 41, offset 42
```

The identity of a physical log position is the tuple `(topic, partition, offset)`.

### Producer

An application or library instance that publishes records. Producers are independent; Kafka has no producer-group equivalent. Application replicas may each own a long-lived producer client and write concurrently.

### Consumer

An application or library instance that fetches records and runs application logic. Kafka supplies records; the consumer is responsible for interpreting them and performing side effects.

### Consumer group

Consumers sharing a `group.id` cooperate to divide the subscribed topic-partitions. In a traditional Kafka consumer group, each topic-partition is assigned to exactly one group member at a time.

Different groups can read the same topic independently. For example:

```text
location-updates
  -> live-tracking group
  -> history-storage group
  -> traffic-analytics group
```

Each group has its own progress and gets its own logical view of the retained records.

Current Kafka also has **share groups**, a separate queue-style group type in which consumers can share partitions and acknowledge individual records. This guide uses “consumer group” for the traditional ordered-stream model unless it explicitly says otherwise; the one-consumer-per-partition rule does not apply to share groups.

### Broker and cluster

A broker is a Kafka server that stores partition replicas and serves client requests. A cluster is a coordinated set of brokers plus its metadata/control plane. Modern Kafka uses KRaft controllers; ZooKeeper belongs to older deployments.

A broker normally holds replicas from many topics. A topic is not necessarily stored in full on any one broker.

### Replication factor

The replication factor, or RF, is the number of replicas kept for each partition. With RF 3, a partition has one leader and two followers placed on three distinct brokers.

### Leader, follower, and ISR

- **Leader:** the authoritative replica that orders writes for a partition and normally serves client traffic.
- **Follower:** a replica that fetches and copies the leader's log.
- **ISR (in-sync replicas):** the leader and follower replicas that satisfy Kafka's criteria for being sufficiently caught up. An ISR follower may lag momentarily; “in sync” does not mean every byte is present at every instant.

Traditionally, safe leader election chooses a replica from the ISR. Current Kafka can also track **Eligible Leader Replicas (ELR)**: with ELR enabled and strict minimum-ISR safety, certain replicas outside the current ISR can still be known-safe candidates. An unclean election is different—it may choose an out-of-sync replica when no safe candidate is available, trading possible data loss for availability.

### Segment, retention, and compaction

A partition log is split into segment files and indexes. Kafka removes old eligible segments according to time or size retention. With log compaction, Kafka eventually retains at least the latest record for each key within the compacted log, subject to compaction behavior and tombstone rules.

### Consumer lag

Lag measures how far a consumer group's progress is behind the partition's end. It is useful but not a complete health metric: also monitor processing time, error rate, rebalance frequency, and the age of the oldest unprocessed event.

## 4. End-to-End Record Flow

Assume topic `location-updates` has three partitions, RF 2, and three brokers:


| Partition            | Leader | Follower |
| -------------------- | ------ | -------- |
| `location-updates-0` | B1     | B2       |
| `location-updates-1` | B2     | B3       |
| `location-updates-2` | B3     | B1       |


For a record keyed by `driver-42`, the flow is:

1. The producer connects to one of the configured bootstrap addresses.
2. It retrieves cluster metadata, including the topic's partitions and current leaders.
3. Its partitioner maps `driver-42` to a partition, for example partition 1.
4. It sends the record directly to B2, the leader of partition 1.
5. B2 appends the record and assigns its next offset, for example 150.
6. B3 fetches and appends the record to its follower replica.
7. B2 responds according to the producer's `acks` policy.
8. The group coordinator assigns partition 1 to one consumer in the `live-tracking` group.
9. That consumer polls, receives offset 150, updates its application state, and advances its in-memory position.
10. After successful processing, it commits offset 151, meaning “resume at 151 next time.”

If the consumer crashes after the side effect but before the commit, offset 150 can be processed again. If B2 fails while B3 remains an eligible replica, B3 can become leader and clients refresh metadata before continuing.

## 5. Partitioning and Ordering



### What Kafka actually orders

Kafka orders records by offset within a partition. It does not sort by timestamp, key, or business sequence number.

```text
partition 0
offset 8  -> event timestamp 10:05
offset 9  -> event timestamp 10:01
```

Kafka reads offset 8 before offset 9. Timestamps remain useful for retention and event-time processing, but they do not determine log order.

There is no total order across partitions:

```text
partition 0: A -> B -> C
partition 1: X -> Y -> Z
combined observation: no Kafka-defined order
```

There is also no Kafka-defined order across topics, even when both topics use the same key:

```text
orders:   OrderCreated -> OrderCancelled
payments: PaymentCompleted
```

A consumer subscribed to both topics may observe `OrderCreated`, `OrderCancelled`, and then `PaymentCompleted`, or another interleaving. Each topic-partition has its own offset sequence.

If one strict order is essential, the relevant events must usually share one partition in one topic. Alternatives include business sequence numbers, buffering, a state machine, an orchestrator, or a stream processor—but these add application-level coordination and do not create a native cross-topic Kafka order.

### Key-based partitioning

A simplified model is:

```text
partition = hash(serialized_key) mod partition_count
```

The exact algorithm is client-specific. Do not assume different client libraries serialize or hash a key identically unless verified.

With `driver_id` as the key:

```text
driver-42 update A -> partition 1, offset 10
driver-99 update X -> partition 2, offset 30
driver-42 update B -> partition 1, offset 11
```

The relative log order A before B is preserved for `driver-42`. Many other drivers can share partition 1 without breaking that relative order.

Two cautions matter:

1. Several producers writing the same key create one broker append order, but that order may not match an intended business or wall-clock order unless the producers coordinate or attach a business sequence.
2. Increasing a topic's partition count can remap a key. Old records remain in the old partition while new records may go to another, so longitudinal per-key ordering can be disrupted during the change.



### Records without keys

With no key, modern producers commonly use a sticky strategy: they send a batch to one partition, then choose another, improving batching and load distribution. “Round robin” is a useful approximation but not universally exact.

No-key records have no stable entity-to-partition affinity, so do not rely on their relative order across sends.

### Custom or explicit partitioning

A producer can choose an explicit partition or supply a custom partitioner. This can implement a mapping such as:

```text
India -> partition 0
US    -> partition 1
UK    -> partition 2
```

This is valid, but it couples business logic to partition numbers. It can create hotspots and makes partition-count changes, failover assumptions, and operational evolution harder.

The earlier blanket statement “partitions cannot be logic-specific” is too strong. They **can** carry intentional semantics because the producer controls routing; Kafka simply does not name or enforce those semantics. Prefer a topic when the category needs different retention, security, schema, ownership, or independent scaling. Prefer a key when the goal is affinity and ordering. Use semantic partition numbers only for a well-justified, carefully controlled design.

### Choosing a key

A good key is:

- stable for the required ordering lifetime;
- the entity whose changes must remain ordered;
- sufficiently well distributed for the workload;
- serialized consistently across producers.

Common choices:


| Stream            | Possible key               | Ordering obtained |
| ----------------- | -------------------------- | ----------------- |
| Orders            | `order_id`                 | Per order         |
| User activity     | `user_id`                  | Per user          |
| Bank transactions | `account_id`               | Per account       |
| Location updates  | `driver_id` or `device_id` | Per driver/device |
| IoT readings      | `device_id`                | Per device        |


Low-cardinality keys such as `country`, `status`, or `priority` still preserve partition order for records mapping to each key, but often concentrate traffic in a few partitions. A single unusually busy key can create a hot partition even when the key has high cardinality overall.

Changing keys, random UUID keys, timestamp-suffixed keys, or sometimes omitting a key can split one entity's records across partitions and lose its ordering boundary.

### Choosing the partition count

Partition count controls several things at once:

- maximum partition-level parallelism within each consumer group;
- aggregate write/read throughput potential;
- metadata, file, memory, replication, and operational overhead;
- the granularity of load distribution;
- recovery and reassignment cost.

Do not use only “number of expected consumers” as a formula. Estimate both producer and consumer throughput:

```text
partitions >= max(
  target_total_write_rate / tested_write_rate_per_partition,
  target_total_consume_rate / tested_consume_rate_per_consumer_partition,
  desired_consumer_parallelism
)
```

Then add measured growth headroom and validate on representative hardware. Partitions can be added but not normally reduced in place, and adding them can change keyed routing. Avoid universal claims such as “2,000–4,000 partitions per broker”; supported density changes with Kafka version, workload, hardware, leadership distribution, segment count, and operational objectives.

## 6. Producers

Producers are not permanently linked to one partition or broker. They select a partition per record, batch records by destination, and route requests to current leaders using cached cluster metadata.

Applications configure several bootstrap broker addresses for discovery and resilience:

```python
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "broker-1:9092,broker-2:9092,broker-3:9092",
    "acks": "all",
    "enable.idempotence": True,
})

producer.produce(
    "location-updates",
    key="driver-42",
    value='{"lat": 12.9, "lng": 77.6, "sequence": 184}',
)
producer.flush()
```

The addresses are entry points, not fixed data destinations. The producer discovers the cluster and sends each request to the appropriate leader. Applications should not target a broker for business routing because leaders can move. Broker-specific connections are mainly an administrative or diagnostic concern.

### Producer acknowledgements

The `acks` setting controls when a write is considered successful:

- `acks=0`: do not wait for a broker response; fastest but the producer cannot know whether the broker accepted the record.
- `acks=1`: the leader acknowledges after appending locally; a leader failure before replication can lose an acknowledged record.
- `acks=all` (or `-1`): the leader waits for the required in-sync replicas, providing the strongest Kafka producer durability when combined with an appropriate `min.insync.replicas` value.

`acks=all` is not a magic guarantee against every failure. Durability also depends on replication health, `min.insync.replicas`, unclean leader election, retries, timeouts, and whether the application treats a timeout as an unknown outcome rather than proof of failure.

### Producer idempotence

An idempotent producer uses a producer identity and per-partition sequence numbers so broker-visible retries from that producer session do not append duplicate records and ordering is preserved under supported retry settings.

It does **not** make arbitrary business events or downstream side effects idempotent. Re-sending an event from a new producer session, replaying it, or executing a database update twice can still duplicate business effects.

Transactions extend this mechanism so a producer can atomically write to multiple Kafka partitions and, in consume-transform-produce pipelines, commit consumed offsets with produced output. Kafka's exactly-once semantics require compatible producer, broker, and consumer settings and do not automatically make external database or HTTP effects exactly once.

### How many producers?

There is no cluster-wide producer count formula. Usually each application instance owns a small number of thread-safe, long-lived producer clients and relies on their batching. Scale application instances based on throughput and service needs; avoid creating a producer per record.

## 7. Consumers, Polling, Groups, and Offsets



### Polling

Kafka consumers fetch rather than receive server-pushed callbacks. A typical loop is:

```python
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "broker-1:9092,broker-2:9092,broker-3:9092",
    "group.id": "live-tracking",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

consumer.subscribe(["location-updates"])

try:
    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            raise RuntimeError(message.error())

        process_idempotently(message)
        consumer.commit(message=message, asynchronous=False)
finally:
    consumer.close()
```

The broker can hold a fetch request briefly until data or size thresholds are available, so polling need not mean busy-waiting. The consumer must keep polling often enough to remain a healthy group member; long processing may require tuned limits or handing work to a carefully controlled worker model.

### Assignment and parallelism

For a traditional coordinated consumer group:

- one topic-partition is assigned to at most one consumer in that group at a time;
- one consumer may own many topic-partitions;
- if consumers exceed assigned partitions, some consumers are idle;
- different groups may consume the same partition independently.

Share groups are intentionally different: multiple members may share records from one partition, which is useful for queue-like work but does not provide the traditional consumer group's partition-ordered processing model.

With six partitions:

```text
2 consumers -> about 3 partitions each
6 consumers -> up to 1 partition each
8 consumers -> at least 2 consumers idle
```

This assignment prevents concurrent group members from independently advancing the same partition's group offset. It does not by itself guarantee that application code completes side effects in order—for example, handing records from one partition to an unordered worker pool can reorder completion.

### Current position vs committed offset

Keep three positions separate:

1. **Log end offset:** where the partition currently ends.
2. **Consumer position:** the next record the running consumer will fetch; normally advances as records are returned.
3. **Committed offset:** the group's durable recovery checkpoint stored by Kafka.

If a consumer polls offsets 10–14, its in-memory position may advance to 15 before it commits. Its next poll in the same healthy assignment does not automatically reread 10–14. If it crashes or loses the partition before committing 15, the next owner resumes from the prior committed offset and may reread records.

Kafka commits the **next offset to read**. After successfully processing record 14, commit 15.

Committed offsets are stored per `(group, topic, partition)`, normally in Kafka's internal `__consumer_offsets` topic.

### Commit patterns and delivery semantics

- **Process, then commit:** at-least-once processing. A crash after the side effect but before the commit can duplicate the effect.
- **Commit, then process:** at-most-once processing. A crash after the commit but before the effect can lose the work.
- **Kafka transaction:** can provide exactly-once behavior for a properly configured Kafka-to-Kafka pipeline.

Auto-commit is easy to misuse because a returned record may be committed before application processing is actually durable. Manual commits after a successfully persisted batch are easier to reason about. Commit each partition only through its highest contiguously completed offset; committing past a failed or still-running record can skip it on recovery.

### Rebalancing

A **consumer-group rebalance** changes which group member owns which topic-partitions. It can occur when members join, leave, fail, change subscriptions, or when subscribed partition counts change. Depending on the assignment protocol, movement may be eager or incremental/cooperative.

During revocation, finish or stop work safely and commit only completed offsets. A rebalance can cause duplicates when another consumer resumes from an older committed checkpoint.

This is different from **partition replica reassignment**, which moves stored replicas and possibly leadership between brokers. The overloaded word “rebalance” often causes these two operations to be confused.

### Manual partition assignment

`subscribe()` participates in group-managed assignment and automatic rebalancing. `assign()` lets the application select exact topic-partitions and bypasses group assignment for that consumer:

```python
from confluent_kafka import TopicPartition

consumer.assign([
    TopicPartition("orders", 0),
    TopicPartition("orders", 1),
])
```

Manual assignment is useful for debugging, controlled affinity, or specialized jobs. The application must prevent overlapping readers, handle failover, and manage offsets deliberately. A `group.id` may still be used for offset commits, but it does not restore automatic assignment coordination.

### Multi-topic consumers

A group can subscribe to one or many topics. Assignments and committed offsets are still per topic-partition.

A consumer subscribed to `orders` and `payments` must dispatch each record to the correct handler and deserialize it using the correct schema:

```python
if message.topic() == "orders":
    handle_order(message)
elif message.topic() == "payments":
    handle_payment(message)
```

Use one multi-topic service when the streams belong to one cohesive responsibility and share operational needs. Separate services/groups when handlers have unrelated ownership, failure modes, scaling, latency, or deployment requirements. Remember that one poll can contain an interleaving from several partitions, not a meaningful global order.

## 8. Idempotency and Failure-Safe Processing

Design consumers to tolerate duplicate delivery whenever they produce side effects. Common patterns include:

- a globally unique `event_id` with a database uniqueness constraint;
- an inbox table recording processed event IDs in the same database transaction as the business change;
- an upsert or compare-and-set rather than a blind insert/increment;
- an entity sequence number that rejects stale or repeated updates;
- an outbox table to atomically persist a business change and an event for later publication;
- an idempotency key supported by a downstream API.

Example for a payment event:

```text
transaction begins
  insert event_id into processed_events  -- unique constraint
  apply payment state change
transaction commits
commit Kafka offset
```

If the event is delivered again, the unique constraint identifies it and the handler can safely treat it as already applied.

Avoid assuming that “exactly once” means every external effect happens exactly once. There is no atomic transaction spanning Kafka, an arbitrary database, an email provider, and every HTTP service unless the system adds a specific coordination design. Idempotency remains the practical default.

## 9. Brokers, Replication, and Durability



### Replica placement

For each partition, Kafka places its RF replicas on distinct brokers. Therefore:

```text
replication_factor <= broker_count
```

With rack awareness configured, assignments try to spread replicas across failure domains such as racks or availability zones. The desired state also balances replica counts, leader counts, disk use, and throughput, but perfect balance is an operational goal rather than a simple hard constraint.

Example with `orders` having four partitions and RF 2:


| Partition | Leader | Follower |
| --------- | ------ | -------- |
| P0        | B1     | B2       |
| P1        | B2     | B3       |
| P2        | B3     | B1       |
| P3        | B1     | B3       |


The total number of replicas is:

```text
topic partition count x replication factor
```

This is not a required broker count. Each broker hosts many partition replicas. It can be entirely sensible to have more brokers than RF, or even more brokers than a small topic has replicas, because the cluster may host other topics and needs capacity, failure headroom, or workload isolation.

### RF, ISR, `min.insync.replicas`, and `acks`

These settings work together:

- **RF** decides how many copies should exist.
- **ISR** describes which replicas are currently eligible as synchronized replicas.
- `min.insync.replicas` is the minimum ISR size required for a producer using `acks=all` to accept a write.
- `acks` decides what replication condition the producer waits for.

A common production durability posture is RF 3, `min.insync.replicas=2`, `acks=all`, idempotence enabled, and unclean leader election disabled. In that setup, one broker can fail while writes continue if two in-sync replicas remain. More failures may leave an existing leader readable or electable in some circumstances, but writes stop once the minimum ISR condition cannot be met. Therefore “RF 3 tolerates two failures” is too vague; availability depends on which replicas fail, current ISR, the operation, and configuration.

Adding brokers does not increase RF. RF changes only through an explicit replica reassignment/configuration operation.

### Adding brokers and moving data

New brokers provide empty capacity. Existing replicas generally do not move to them merely because the brokers joined. New topics or partitions may use them; existing data needs a planned partition reassignment or a balancing system.

Before adding B4 and B5:


| Partition | Leader | Follower |
| --------- | ------ | -------- |
| P0        | B1     | B2       |
| P1        | B2     | B3       |
| P2        | B3     | B1       |
| P3        | B1     | B3       |


Possible assignment after controlled movement:


| Partition | Leader | Follower |
| --------- | ------ | -------- |
| P0        | B1     | B4       |
| P1        | B2     | B5       |
| P2        | B3     | B1       |
| P3        | B4     | B2       |


RF remains 2. Replica reassignment copies data over the network and consumes disk and broker capacity, so it should be throttled and observed. Moving replicas and balancing preferred leaders are related but distinct concerns.

### Sizing brokers

RF only imposes the minimum broker-count constraint. Size a cluster using measured requirements for:

- retained data, including replication and free-disk headroom;
- ingress and egress throughput;
- replication and recovery network traffic;
- disk throughput and latency;
- partition and leader density;
- connection and request counts;
- CPU for compression, encryption, and request handling;
- memory for the JVM and OS page cache;
- maintenance and broker-failure headroom;
- rack or availability-zone failure objectives.

A rough storage starting point is:

```text
required_cluster_disk ~= retained_primary_bytes x RF
                       + index/segment overhead
                       + compaction/reassignment headroom
```

Then test steady state and degraded state, especially leader failover and replica catch-up. Idle Kafka still consumes memory, metadata, file descriptors, and background network/disk activity; actual cost is far more deployment-specific than a fixed “megabytes per partition” estimate suggests.

## 10. Multiple Kafka Clusters

Kafka clusters are independent metadata and data boundaries. Organizations use multiple clusters for:

- development, test, and production isolation;
- regional latency and data residency;
- limiting blast radius;
- security or compliance separation;
- organizational ownership;
- workload isolation;
- scaling beyond a practical single-cluster boundary;
- disaster recovery.

A normal producer or consumer client instance is configured for one cluster through its bootstrap addresses. An application can talk to multiple clusters by creating and operating separate clients for each one.

Topics, consumer groups, and offsets are cluster-local. Two clusters can both have an `orders` topic and a `fraud-service` group, but those are separate objects with separate offsets.

Clusters do not coordinate automatically. Data can be copied using cluster-linking or replication tooling, Kafka Connect-based pipelines, or an application that consumes from one cluster and produces to another. Cross-cluster replication is usually asynchronous, so plan for lag, duplicates, failover policy, offset translation, conflict ownership, and loop prevention. Do not assume it creates one globally ordered log.

## 11. Topic and Partition Examples


| Domain        | Topic                                       | Typical key                      | Why                                               |
| ------------- | ------------------------------------------- | -------------------------------- | ------------------------------------------------- |
| E-commerce    | `orders`                                    | `order_id` or `customer_id`      | Order per entity and scale writes                 |
| Payments      | `transactions`                              | `account_id`                     | Preserve account-relative append order            |
| Ride hailing  | `location-updates`                          | `driver_id`                      | Preserve each driver's update order               |
| IoT           | `sensor-readings`                           | `device_id`                      | Spread devices while preserving device order      |
| Notifications | `notifications`                             | `user_id`                        | Keep a user's notification stream together        |
| Logs          | `application-logs`                          | `service_instance_id`, or no key | Affinity when needed; otherwise balanced batching |
| Fraud         | input `transactions`, output `fraud-alerts` | entity/event IDs                 | Separate source and derived-event streams         |


Topic boundaries should reflect more than labels. Separate topics often make sense when streams need different:

- schemas or compatibility rules;
- retention or compaction policies;
- access controls;
- owners and consumers;
- throughput or partition counts;
- availability requirements.



## 12. Final Mental Model

```text
topic      = named logical event stream
partition  = ordered append-only log and unit of parallelism
key        = producer input for partition affinity
offset     = a record's position inside one partition
producer   = selects a partition and writes to its leader
broker     = stores replicas and serves Kafka requests
cluster    = brokers plus their shared control plane
RF         = desired copies per partition
ISR        = replicas currently eligible under in-sync rules
consumer   = polls records and runs application logic
group      = consumers cooperating on partition ownership
commit     = durable group recovery checkpoint
retention  = how long Kafka keeps records independently of consumption
```

The most important sentence to retain is:

> Kafka stores each topic as partitioned, replicated logs; producers append to partition leaders, while consumer groups independently poll those logs and checkpoint their progress with offsets.



## 13. Further Reading

The most authoritative place to confirm version-specific behavior and defaults is the Apache Kafka documentation:

- [Kafka introduction and key concepts](https://kafka.apache.org/documentation/)
- [Kafka design: persistence, consumers, delivery semantics, replication, and compaction](https://kafka.apache.org/43/design/design/)
- [Producer configuration](https://kafka.apache.org/43/configuration/producer-configs/)
- [Consumer and share-consumer configuration](https://kafka.apache.org/43/configuration/consumer-configs/)
- [Topic configuration, including](https://kafka.apache.org/43/configuration/topic-configs/) `min.insync.replicas`
- [Traditional](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html) `KafkaConsumer` [groups, offsets, assignment, and rebalancing](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
- [Eligible Leader Replicas](https://kafka.apache.org/43/operations/eligible-leader-replicas/)
- [Basic operations for topics, consumer groups, share groups, and assignments](https://kafka.apache.org/43/operations/basic-kafka-operations/)

Kafka defaults and features evolve. Check the documentation matching the broker and client versions actually deployed before choosing production settings.