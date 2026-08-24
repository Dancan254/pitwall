# Pitwall

### A high-throughput F1 telemetry ingestion platform

*Learn Today, Teach Tomorrow*

---

## 1. What Pitwall Is

Pitwall is a reference implementation of a real-time, high-throughput telemetry ingestion platform, modelled on how a Formula 1 team streams data off the car and onto the pit wall.

A modern F1 car carries around 300 sensors, which the ECU turns into 1,000 to 2,000 channels sampled anywhere from 1 Hz to 1 kHz: roughly 150,000 data points per second measured onboard, and over 1.5 TB accumulated across a race weekend. Only a prioritised subset leaves the car live: about 30 MB per lap, which is a mere 2.7 Mbit/s. Everything else is logged onboard and offloaded over a wired umbilical when the car stops in the garage. Across the whole grid, that live stream runs on the order of 1.1 million data points per second.

That live data has to arrive fast enough to act on within a lap, survive radio dropouts through tunnels and between cells, and feed two completely different audiences at once: engineers watching live traces on the pit wall, and a factory analytics team running historical comparisons lap after lap.

Sensor counts, sample rates, transmission volumes, and the radio link itself are documented with sources in [`docs/how-f1-transmits-telemetry.md`](docs/how-f1-transmits-telemetry.md). Every number in this section comes from there.

Strip away the racing and what is left is one of the most common hard problems in backend engineering: **high-throughput time-series ingestion**. Pitwall exists to model that problem end to end and to demonstrate, concretely, the design decisions that separate a system that merely works from one that holds up under load.

The name comes from the F1 term itself. The pit wall is where a team's engineers sit trackside to receive the stream and make decisions in real time. That is exactly what the serving layer of this platform does.

---

## 2. The Problem It Models

The reason this project is worth building is that the F1 telemetry shape is not exotic. It is the same shape as:

- **Vehicle and trip telemetry** at a company like Uber (millions of GPS and sensor events per second, per-vehicle ordering, live dispatch plus historical analytics).
- **Playback quality monitoring** at Netflix (per-session QoE events streamed in, aggregated into real-time dashboards, retained for trend analysis).
- **GPU-fleet and datacenter telemetry** at Nvidia (dense hardware metrics, high cardinality, threshold alerting before failure).
- **The entire observability industry** (Datadog, Grafana, Prometheus): metrics, logs, and traces are all high-cardinality time series ingested at volume.
- **Industrial IoT and financial tick data**, which share the same append-heavy, bursty, out-of-order characteristics.

Every one of these systems has the same skeleton. If you can reason clearly about the F1 version, you can reason about all of them. That is the point of the project: not to build a toy, but to demonstrate transferable understanding of a pattern that shows up everywhere.

### The defining characteristics

What makes this class of problem hard, and what Pitwall deliberately exercises:

| Characteristic | Why it matters |
|---|---|
| High write volume | Writes dominate reads by orders of magnitude. Naive row-by-row inserts collapse. |
| High cardinality | Many cars times many sensors means a very large key space. Hot partitions become a real risk. |
| Bursty, not steady | Traffic spikes. The ingestion path must absorb spikes without dropping data or falling over. |
| Out-of-order and duplicate arrival | Dropout-then-replay means events arrive late and more than once. The system must be correct anyway. |
| Two read patterns | Live monitoring wants the freshest data now. Analytics wants downsampled history fast. One store cannot optimise for both. |

---

## 3. Design Goals and Non-Goals

### Goals

1. Ingest a realistic, tunable telemetry firehose without data loss under load.
2. Keep the producer decoupled from consumer capacity, so a spike upstream never takes down the write path.
3. Guarantee correctness in the face of duplicates and late data (effectively-once semantics).
4. Serve both a live real-time view and efficient historical queries from appropriate stores.
5. Instrument the pipeline itself, so its health is observable and its behaviour under load is provable.

### Non-Goals

To keep the project focused on the ingestion problem, the following are explicitly out of scope:

- A production-grade UI. The serving layer proves the data is reachable and live; it is not a polished dashboard product.
- Multi-region replication and disaster recovery. Named as an extension, not built.
- Auth, tenancy, and billing. This is an engineering demonstrator, not a SaaS.
- Exactly-once semantics via Kafka transactions. We deliberately choose the simpler at-least-once plus idempotency approach and explain the tradeoff, rather than paying the full cost of transactional exactly-once.

Stating non-goals is itself part of the demonstration. Knowing what not to build is a senior signal.

---

## 4. Architecture

The platform is five stages plus a band of cross-cutting concerns. Data flows left to right, mirroring the physical journey of telemetry off an F1 car.

```
 Source  ->  Kafka Log  ->  Stream Processor  ->  Time-Series Store  ->  Serving / Pit Wall
                                    |                                            ^
                                    +-------------- live push (alerts) ----------+
                                                                 |
                                                          Parquet cold archive
```

### 4.1 Telemetry Source Service (the producer)

A generator that simulates a fleet of cars, each with a configurable sensor set emitting at a configurable rate. The entire purpose of this service is that the load is a dial you can turn, which is what lets you later prove the system holds.

To force good design downstream, the simulation is deliberately realistic:

- **Bursts, not steady state**, so the buffer downstream has to earn its place.
- **Dropout-then-replay**, reproducing the onboard-buffer behaviour of a real car going through a tunnel. This is the most important detail: replay produces duplicates and out-of-order arrival, which forces the consumer to handle idempotency and late data properly instead of pretending the stream is clean.
- **Injected anomalies**, such as a brake temperature spiking, so the alerting path has something real to catch.

Serialization is Protobuf (or Avro) over JSON, backed by a Schema Registry. Real F1 compresses before it transmits; here, the compactness comes from choosing a binary wire format, and the registry gives schema evolution without breaking existing consumers.

### 4.2 Kafka (the ingestion log and shock absorber)

Kafka sits between producer and consumer as the durable, replayable buffer that decouples producer spikes from consumer capacity. This is the single most important architectural choice in the platform.

Design points:

- **Partition by car_id** (or car_id plus sensor group). This gives per-car ordering and horizontal parallelism at once. It also opens the honest discussion of cardinality and hot partitions: a partitioning key that is too coarse creates skew, one that is too fine wastes coordination.
- **Retention is the replay window.** Because Kafka is a log and not a queue that deletes on read, the retention period defines how far back a consumer can replay after a failure or a bug fix.

### 4.3 Stream Processor (the consumer)

A consumer group that scales with the partition count. This is where most of the senior-level detail lives:

- **Micro-batching** writes to storage to cut write amplification. Committing a batch of events per transaction rather than one row per insert is the difference between a store that keeps up and one that melts.
- **Idempotent writes** keyed on (car_id, sensor_id, timestamp) or a monotonic sequence number. At-least-once delivery plus idempotent writes yields effectively-once results without the cost of full transactional exactly-once.
- **Backpressure**: bounded in-flight work, pause and resume on the consumer, and consumer lag as the primary health signal.
- **Windowed rollups**: tumbling one-second and one-minute aggregates, with **watermarks** so that late and replayed data is folded in gracefully instead of being silently dropped or corrupting a closed window.
- **Threshold alerting**: the real-time equivalent of catching an oil-pump issue before it becomes engine damage.

### 4.4 Time-Series Store (the meaty layer)

The storage layer is where the write-heavy, read-diverse nature of the problem gets solved:

- **Hypertables** for automatic time-based partitioning, so inserts always hit a small, recent chunk.
- **Continuous aggregates** to downsample raw data into 1-second, 1-minute, and 1-hour rollups. This is the classic time-series trade: write more on ingest so that reads are cheap.
- **Native compression** using columnar layout plus delta-of-delta timestamp encoding, which is how time-series stores reach ten-times-plus compression ratios.
- **Retention and tiering**: raw high-resolution data expires quickly, aggregates live long, and cold data spills to Parquet on object storage. That cold archive is the equivalent of the multi-terabyte-per-car weekend record an F1 team keeps for the season.

### 4.5 Serving Layer (the pit wall analog)

Two read paths, because the problem has two audiences:

- **Live monitoring** over WebSocket or SSE, pushing fresh events and alerts to a client. This is fed directly by the stream processor, bypassing the store, so latency stays low.
- **A query API** that reads recent high-resolution data from the hot store and history from the continuous aggregates.

This layer is also the "check the data" surface: a way to watch the pipeline live and validate what is actually flowing through it.

---

## 5. Data Model and Flow

A single telemetry event is intentionally small and flat:

```
TelemetryEvent {
  car_id        // partition key, provides per-car ordering
  sensor_id     // high-cardinality dimension
  timestamp     // event time, used for windowing and idempotency
  value         // the reading
  sequence_no   // monotonic per car, used for dedup and gap detection
}
```

The flow, end to end:

1. Source generates events, batches and serializes them, and produces to a car-partitioned Kafka topic.
2. Kafka persists them to the log. Producer spikes are absorbed here, not passed downstream.
3. The consumer group reads in parallel, one or more partitions per instance, deduplicates on (car_id, sensor_id, timestamp), and micro-batches idempotent writes into the store.
4. In the same pass, windowed rollups and threshold checks run. Alerts are pushed live to serving.
5. The store compresses and ages data, tiering cold chunks to Parquet.
6. Serving reads live from the processor and historical from the store's aggregates.

---

## 6. Key Design Decisions

The value of this project is in the decisions, not the boxes. Each of these is a defensible, explainable tradeoff.

### 6.1 Kafka over RabbitMQ

RabbitMQ is a smart broker built around routing and per-message acknowledgement, which is an excellent fit for task and work-queue semantics. It is the wrong tool for a telemetry firehose. Pitwall wants a **replayable log** with consumer-group parallelism and very high sequential-write throughput, and it may want to replay history after a bug fix. That is Kafka's model exactly: a dumb-broker, smart-consumer design where the log is the source of truth. Same project, and it makes the Kafka-versus-RabbitMQ distinction concrete rather than theoretical.

### 6.2 At-least-once plus idempotency, not exactly-once

Kafka can provide transactional exactly-once, but it adds coordination cost and complexity. Because every write is keyed on a natural identity (car_id, sensor_id, timestamp), a duplicate write is simply an overwrite of the same value. At-least-once delivery combined with idempotent writes therefore gives the same end result as exactly-once, at a fraction of the cost. Choosing the cheaper mechanism and being able to explain why it is sufficient is the senior move.

### 6.3 TimescaleDB over ClickHouse

The storage fork is the one real decision that shapes everything else.

- **TimescaleDB** keeps the platform on Postgres, which means hypertables, continuous aggregates, and delta-of-delta compression can all be discussed credibly and built on familiar ground. It is the natural choice when the depth on show is relational and time-series craft.
- **ClickHouse** is the columnar-analytics-at-scale answer, stronger for very large aggregate scans, but it pulls the project off the Postgres and JVM story.

Pitwall picks TimescaleDB because it plays to genuine depth rather than chasing a benchmark. ClickHouse is named as the alternative and the direction you would go if analytics volume became the dominant concern.

### 6.4 Kafka Streams over Flink

For the stateful windowing and rollups, Kafka Streams keeps everything on the JVM and inside the same deployment, with no separate cluster to operate. Flink is the more powerful engine for very large stateful workloads and complex event-time processing, and it is named as the scale-up option. For this workload, Streams is the right amount of machinery.

---

## 7. Cross-Cutting Concerns

These are the difference between "I used Kafka" and "I understand streaming." They are not features bolted on; they are properties the whole pipeline has to hold.

**Backpressure.** The system must slow gracefully under load, never drop silently. Bounded queues, consumer pause and resume, and lag-based flow control all serve this.

**Idempotency.** Every write must be safe to repeat, because at-least-once delivery guarantees repeats will happen. Natural-key dedup makes this true by construction.

**Watermarks and late data.** Event time is not arrival time. Watermarks let windowed aggregates wait a bounded amount for stragglers, then close, so replayed bursts are counted correctly rather than lost.

**Schema evolution.** The wire format will change. A schema registry with backward and forward compatibility rules lets producers and consumers deploy independently without a coordinated big-bang release.

**Observability of the pipeline itself.** Consumer lag, throughput, and p99 write latency are first-class metrics. Instrumenting your own pipeline is both the most meta and the most convincing part of the whole build, because it proves the claims instead of asserting them.

---

## 8. Proving It: The Load Story

The entire thesis of the project is "we can handle this workload," so the demonstration has to show that, not state it.

The demo is a single, tight sequence:

1. Start the generator at a baseline rate and let the pipeline settle.
2. **Crank the load.** Throughput climbs. Watch **consumer lag stay flat**, which proves the consumer group is keeping pace and the buffer is doing its job.
3. **Trigger a dropout.** The source stops, buffers, then replays a duplicate-heavy, out-of-order burst.
4. **Show recovery.** The pipeline absorbs the replay, deduplicates it, and the stored counts and aggregates remain correct with no double-counting.

That one demo proves buffering, backpressure, and idempotency at the same time. If consumer lag stays flat through the spike and the numbers stay correct through the replay, the system has earned its claims.

---

## 9. Tech Stack

| Layer | Choice | Rationale |
|---|---|---|
| Source | Java / Spring Boot | Tunable load generator, native Kafka producer, Protobuf serialization |
| Wire format | Protobuf + Schema Registry | Compact, schema-evolvable, strongly typed |
| Ingestion log | Apache Kafka | Replayable log, partitioned parallelism, high write throughput |
| Stream processing | Kafka Streams | Stateful windowing on the JVM, no separate cluster |
| Storage | TimescaleDB | Hypertables, continuous aggregates, native compression |
| Cold archive | Parquet on object storage | Cheap, columnar, long-term retention |
| Serving | Spring Boot + WebSocket/SSE | Live push plus historical query API |
| Observability | Micrometer + Prometheus + Grafana | Consumer lag, throughput, latency percentiles |

---

## 10. What This Demonstrates (The Learnings)

This is the section to read if you only read one. Pitwall is built to make the following understanding legible, both to an interviewer and to a student.

**1. Decoupling producers from consumers is the whole game.** The log in the middle is not plumbing; it is the design. It converts an unpredictable producer into a workload the consumer can drain at its own pace. Almost every scalable ingestion system is some version of this idea.

**2. Correctness under retry is a design property, not an afterthought.** Once you accept at-least-once delivery, idempotency stops being optional. Keying writes on natural identity makes duplicates harmless by construction, which is far more robust than trying to prevent duplicates from ever occurring.

**3. Event time is not arrival time.** The moment data can be late or replayed, any aggregation has to reason about event time with watermarks. This single insight separates people who have run a real stream from people who have only read about one.

**4. You cannot optimise one store for both write volume and read variety.** The answer is not a better single database; it is the right shape of data in the right place: raw data partitioned by time for fast writes, pre-aggregated rollups for fast reads, and cold storage for cheap history.

**5. A pipeline you cannot observe is a pipeline you cannot trust.** Consumer lag is the heartbeat. Instrumenting the system to prove its own behaviour under load is what turns a claim into evidence.

**6. Choosing the cheaper correct mechanism is seniority.** Exactly-once, Flink, ClickHouse: each is a legitimate tool, and each is overkill here. Being able to name the heavier option and articulate why the lighter one is sufficient is worth more than reaching for the biggest hammer.

The through-line, and the reason this doubles as teaching material: these are patterns, not trivia. Recognise the shape once, in F1, and you recognise it everywhere it recurs.

---

## 11. Roadmap and Extensions

Named deliberately as future work, to show the boundaries are chosen, not accidental:

- **Exactly-once via Kafka transactions**, as a comparison branch to quantify the cost against the idempotency approach.
- **Flink migration** for the stream layer, to handle far larger stateful workloads.
- **ClickHouse analytics store** alongside Timescale, to compare columnar scan performance on the historical path.
- **Multi-region ingest** with replication and failover.
- **Tiered storage automation**, moving chunks between hot, warm, and cold based on age and access.
- **A schema-evolution demo**, deploying a v2 event format with zero downtime.

---

## 12. Where This Pattern Lives in Industry

Pitwall is a racing story on the surface and a general ingestion story underneath. The same architecture, with different nouns, runs vehicle telemetry at Uber, playback quality monitoring at Netflix, GPU-fleet telemetry at Nvidia, the observability stacks at Datadog and Grafana, industrial IoT platforms, and financial market data feeds.

Learn the shape once. Teach it forward.
