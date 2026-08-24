# Pitwall - Build and Learn Roadmap

### A phased path from what you know to what you're claiming

*Learn Today, Teach Tomorrow*

---

## How to Use This Roadmap

This is a build-driven learning plan, not a tutorial list. You already have the foundation: Java, Spring Boot, microservices, Postgres. The new surface is Kafka, stream processing, and time-series storage. This roadmap bolts those onto what you have, one concept at a time, by making you feel each problem before you fix it.

Four rules govern everything:

1. **Gate on understanding, not time.** The day estimates are guides. Move to the next phase when you can explain the current one from memory, not when a clock says so.
2. **Never commit code you can't explain line by line.** If AI writes it, make it explain it, then retype it yourself. Retyping engages your brain in a way pasting never does.
3. **Break it before you fix it.** Every phase has a deliberate failure. The pain is the lesson. Do not skip it.
4. **Teach each slice as you finish it.** A post, a thread, or a video. Teaching is the hardest test of whether you actually learned. Every gap it exposes is one you go back and fill.

Each phase below gives you: what you build, the failure that forces the lesson, the concepts, the durable learnings, how to drive the AI, a definition of done, and the content piece it produces.

---

## Phase 0: Foundation and the Source

**Estimated: 2 to 3 days**

### Build

The telemetry source service and the project scaffold. A Spring Boot generator that simulates a fleet of cars, each emitting a configurable set of sensor readings at a configurable rate. Define the `TelemetryEvent` schema in Protobuf. Get the load dial working: cars, sensors per car, events per second, all tunable from config.

No Kafka yet. No fancy sink yet. Just a generator you can point somewhere and turn up.

### The forcing function

None yet. This is setup. But make the generator realistic now, because every later phase depends on it: build in the ability to emit bursts and, crucially, a dropout-then-replay mode you can toggle. You won't use replay until Phase 3, but wiring it in now saves you rework.

### Concepts you'll meet

Protobuf and why a binary wire format beats JSON for volume, schema definition, and the shape of a good time-series event (small, flat, keyed).

### Learnings

The event model is a design decision, not a formality. Choosing `car_id`, `sensor_id`, `timestamp`, `value`, and a `sequence_no` up front is what makes partitioning, ordering, and deduplication possible later. A good schema pays you back three phases down the line.

### How to drive the AI

Ask it to explain Protobuf versus JSON for high-volume telemetry and the tradeoffs, before you generate any schema. Then write the `.proto` yourself and have it review. Ask what fields a time-series event needs and why, and push back until the answer justifies each field.

### Done when

- You can start the generator, set it to 10 cars at 100 events per second, and see events being produced to stdout or a stub sink.
- You can explain every field in your event schema and why it's there.

### Teach-forward

Short post: "Why F1 telemetry (and your IoT app) should not use JSON on the wire." Protobuf, compactness, schema evolution.

---

## Phase 1: Break It

**Estimated: 1 to 2 days**

### Build

The naive version. Point the generator straight at Postgres. One insert per event, synchronous, no buffer in between. Get it working at a gentle rate first so you see green.

### The forcing function

Now crank the dial. Push the rate up and watch it fall over: insert latency climbs, the producer blocks, connections exhaust, throughput collapses. Do not fix it. Sit with it. Measure where it breaks.

### Concepts you'll meet

Write amplification, connection pool exhaustion, synchronous coupling, and the basic truth that a fast producer tied directly to a slower consumer is a system with no shock absorber.

### Learnings

This is the most important phase in the roadmap despite being the shortest. You will never forget why a buffer exists once you have watched a direct producer-to-database path die under load. Every later architectural choice traces back to this pain. When an interviewer asks "why Kafka?", your answer will not be a definition. It will be a story about the night you broke this on purpose.

### How to drive the AI

After it breaks, describe the symptoms to the AI and ask it to diagnose what's happening and why. Ask it to name the failure modes precisely. Do not ask it to fix the architecture yet. You want the diagnosis to land before the solution arrives.

### Done when

- You have a load number at which the naive system fails, and you can explain the failure mechanism.
- You can articulate, in your own words, what a buffer between producer and consumer would change.

### Teach-forward

Your strongest early piece: "I connected a producer straight to Postgres and cranked the load until it died. Here's exactly what broke and why." Real numbers, real failure. This is the post that proves you build, not just read.

---

## Phase 2: Kafka, the Shock Absorber

**Estimated: 4 to 6 days**

### Build

Introduce Kafka between the generator and the database. Producer writes to a Kafka topic, a consumer reads from it and writes to Postgres. Partition the topic by `car_id`. Run the same load that killed the naive version and watch it hold.

### The forcing function

Once it works, break it a different way. Kill a consumer mid-stream and restart it. Watch offsets and see what happens to in-flight data. Add a second consumer to the group and watch partitions rebalance. Make a partition hot by sending most traffic to one car and observe the skew.

### Concepts you'll meet

Topics, partitions, offsets, consumer groups, rebalancing, partition keys, ordering guarantees (per partition, not global), and hot partitions. This is the densest phase for new vocabulary, and it's the heart of your Kafka versus RabbitMQ content.

### Learnings

The log in the middle is not plumbing, it is the whole design. Kafka decouples an unpredictable producer from a consumer that drains at its own pace. Partitioning by `car_id` buys you per-car ordering and parallelism at the same time, and the cost of that choice is skew when one key dominates. You now understand, concretely, why Kafka is a log and not a queue: the data stays after it's read, which is what makes replay possible later.

### How to drive the AI

Ask for the mental model first: what a partition actually is, why ordering is per-partition, what a consumer group guarantees. Then have it walk you through your partitioning choice and its failure mode (hot partitions) before you write consumer code. End the phase by having it interview you: "why did you partition by car_id and not sensor_id?" Defend it out loud.

### Done when

- Same load that killed Phase 1 now runs steadily.
- You can explain what happens to offsets when a consumer dies and restarts.
- You can explain, with a diagram, the difference between Kafka and RabbitMQ for this workload.

### Teach-forward

The big one you already have queued: Kafka versus RabbitMQ, now grounded in a system you actually built. Partitions, consumer groups, log versus queue.

---

## Phase 3: Idempotency

**Estimated: 3 to 5 days**

### Build

Turn on the dropout-then-replay mode from Phase 0. The source stops, buffers, then replays a burst of events, some of which the consumer already processed.

### The forcing function

Check your stored data. The counts are now wrong. Duplicates have landed, and some events arrived out of order. This is not a bug in your code, it is the nature of at-least-once delivery meeting replay. Feel it before you fix it.

### Concepts you'll meet

At-least-once versus exactly-once delivery, idempotency, natural keys, upserts, deduplication, and why exactly-once is expensive.

### Learnings

Correctness under retry is a design property, not an afterthought. Once you accept at-least-once delivery (and you should, because it's cheap and robust), idempotency stops being optional. Keying every write on natural identity, `(car_id, sensor_id, timestamp)`, makes a duplicate write a harmless overwrite instead of a double-count. This is the phase where you earn the right to say "at-least-once plus idempotency gives me effectively-once without paying for Kafka transactions," which is a genuine senior-level sentence.

### How to drive the AI

Ask it to explain the delivery-guarantee spectrum and where the cost lives, before touching code. When you see the wrong counts, ask it to explain why at-least-once caused them. Then design the idempotent write together and have it challenge your key choice. Ask it explicitly: "when would exactly-once be worth the cost, and why isn't it here?"

### Done when

- You replay a duplicate-heavy burst and your stored counts stay correct.
- You can explain the difference between at-least-once, exactly-once, and effectively-once, and defend your choice.

### Teach-forward

"Your stream will deliver the same message twice. Here's why that's fine." Idempotency through natural keys, with the F1 dropout as the concrete example.

---

## Phase 4: Windowing and Watermarks

**Estimated: 5 to 7 days**

### Build

Add stream processing with Kafka Streams. Compute tumbling one-second and one-minute rollups per car and per sensor: averages, maxima, counts. Add threshold alerting so a spiking value fires an alert.

### The forcing function

Replay a late burst into a window you thought had closed. Watch your aggregates either drop the late data or corrupt a finished result. This is the hardest conceptual phase, and this failure is the reason watermarks exist.

### Concepts you'll meet

Event time versus processing time, tumbling and sliding windows, watermarks, allowed lateness, stateful stream processing, and Kafka Streams state stores.

### Learnings

Event time is not arrival time. The moment data can be late or replayed, every aggregation has to reason about when an event happened, not when it arrived. Watermarks let a window wait a bounded amount for stragglers, then close. This single insight is what separates people who have run a real stream from people who have only read about one. If you internalize nothing else from the whole project, internalize this.

### How to drive the AI

This phase needs the most patient tutoring. Ask for the event-time versus processing-time distinction with a concrete timeline example before any code. Have it explain what a watermark actually is in plain language, then in your code. When late data breaks your window, ask it to trace exactly what happened tick by tick. Rebuild the explanation from memory afterward; if you can't, you haven't got it yet.

### Done when

- Rollups compute correctly under normal load.
- A replayed late burst is folded into the right window instead of being lost or corrupting a closed one.
- You can explain watermarks to a beginner without notes.

### Teach-forward

"Event time vs processing time: the idea that separates real stream engineers from tutorial watchers." This is a flagship explainer, and watermarks are a topic most content gets wrong or skips.

---

## Phase 5: Time-Series Storage Done Right

**Estimated: 4 to 6 days**

### Build

Replace the naive Postgres table with TimescaleDB. Convert to a hypertable. Build continuous aggregates for your 1-second, 1-minute, and 1-hour rollups. Turn on native compression. Set a retention policy so raw data expires and aggregates live long.

### The forcing function

Before Timescale, run a historical range query against the raw high-resolution table at volume and time it. It will be slow. Then run the same query against a continuous aggregate. The gap is the lesson.

### Concepts you'll meet

Hypertables and time-based partitioning, continuous aggregates, columnar compression, delta-of-delta timestamp encoding, retention and data tiering, and the write-more-to-read-fast tradeoff.

### Learnings

You cannot optimize one store for both write volume and read variety. The answer is not a better single database, it is the right shape of data in the right place: raw data partitioned by time for fast writes, pre-aggregated rollups for fast reads, cold storage for cheap history. This plays directly to your Postgres depth, and understanding delta-of-delta compression alone reads as genuinely deep in an interview.

### How to drive the AI

Ask it to explain why time-series workloads defeat a normal Postgres table and what hypertables change. Have it explain continuous aggregates as a materialized-view-that-maintains-itself, and delta-of-delta encoding from first principles. Since this is your home turf, push harder here: ask it to quiz you on when you'd reach for ClickHouse instead.

### Done when

- Historical queries hit continuous aggregates and return fast.
- Compression is on and you can state your ratio.
- You can explain hypertables, continuous aggregates, and delta-of-delta to a student.

### Teach-forward

"How time-series databases pull off 10x compression." Delta-of-delta and columnar layout, a great deep-dive for your Postgres-adjacent audience.

---

## Phase 6: Observability, the Load Story, and Serving

**Estimated: 4 to 6 days**

### Build

Two things. First, instrument the whole pipeline: consumer lag, throughput, and p99 write latency into Micrometer, Prometheus, and Grafana. Second, build the serving layer: a WebSocket or SSE live feed fed by the stream processor, plus a query API over the store. This is your pit wall.

### The forcing function

Run the full load story end to end and record it. Crank the load and watch consumer lag on the Grafana dashboard. If lag stays flat while throughput climbs, you've proven it. Then trigger a dropout and replay, and watch the counts stay correct through the burst.

### Concepts you'll meet

Consumer lag as a health signal, the four golden signals, real-time push over WebSocket and SSE, and the difference between serving live data and serving historical data.

### Learnings

A pipeline you cannot observe is a pipeline you cannot trust. Consumer lag is the heartbeat of the whole system, and instrumenting it is what turns your claims into evidence. This phase is where the project stops being "I built a thing" and becomes "here is the graph that proves it holds under load." That graph is the single most convincing artifact you will produce.

### How to drive the AI

Ask what metrics actually matter for a streaming pipeline and why lag is the primary one. Have it help you design the dashboard, then interpret the graphs with you under load. For serving, ask it to explain WebSocket versus SSE for a live feed and which fits here.

### Done when

- A Grafana dashboard shows lag staying flat through a load spike.
- The replay demo keeps counts correct, on camera.
- A live view shows telemetry streaming in real time.

### Teach-forward

The capstone: a full walkthrough video of Pitwall handling the load, with the lag graph as the hero shot. This is the piece that anchors the whole project and the one you send with applications.

---

## The Flywheel

Notice what this roadmap quietly does. Every phase produces a piece of content, so none of your learning time is spent only on learning. Phase 1 alone gives you a post that proves you build. Phase 2 is the Kafka versus RabbitMQ video you already had queued. Phase 4 is a flagship explainer on a topic most people get wrong. Phase 6 is the capstone walkthrough.

By the time Pitwall is done, you will have shipped six or seven pieces of content, learned five hard concepts by feeling them rather than memorizing them, and built one repo you can defend line by line in any interview. That is the "Learn Today, Teach Tomorrow" loop running at full speed, and it fits comfortably inside your window.

## Sequencing Note

If you need an interview-ready artifact fast, phases 1 and 2 alone are worth presenting. "I broke the naive version, then fixed it with a partitioned Kafka log, and here's why" is already a strong story. You do not need the whole platform before it earns its keep. Build in slices, ship in slices, teach in slices.
