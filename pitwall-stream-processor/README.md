# pitwall-stream-processor

The consumer. Reads `telemetry.events` from Kafka in batches and writes them into Postgres with
batched JDBC inserts. Owns the schema — Flyway migrations live here, and no other service creates
tables.

## Run

```bash
docker compose up -d
./mvnw -pl pitwall-stream-processor -am spring-boot:run
```

Listens on `8082`. Flyway applies `db/migration` at startup. Postgres is published on host port
**5434**, not 5432, to stay out of the way of other local Postgres containers.

## How it keeps up

Three settings do most of the work, and each one is a decision worth being able to defend:

| Setting | Value | Why |
|---|---|---|
| `spring.kafka.listener.type` | `batch` | The listener receives a whole poll, not one record at a time |
| `max.poll.records` | 2000 | Enough work per poll to make batching worthwhile |
| `spring.kafka.listener.concurrency` | 6 | Six consumers in the group, each owning a slice of the 12 partitions |
| `pitwall.processor.write-batch-size` | 500 | One JDBC round trip per 500 rows instead of per row |

`ack-mode: batch` commits offsets after the listener returns, which is **at-least-once**: a crash
between the write and the commit replays the batch. That is the intended semantic — slice 3 makes the
writes idempotent so a replay stops mattering.

## Idempotency: why replay is harmless

`ack-mode: batch` gives at-least-once delivery, and the source can replay a dropout burst on demand.
Both mean the same event will arrive more than once. The fix is not to prevent that — it is to make
a second arrival a no-op.

Every write is keyed on the reading's **natural identity**, `(car_id, sensor_id, event_time)`, which
is the primary key of the table. The insert is:

```sql
insert into telemetry_event (car_id, sensor_id, event_time, value, sequence_no)
values (?, ?, ?, ?, ?)
on conflict (car_id, sensor_id, event_time) do nothing
```

A duplicate is not an error and not a double-count; it is a row Postgres declines to write, and the
per-row count coming back from the JDBC batch is what feeds
`pitwall.processor.events.duplicates`. **At-least-once delivery plus idempotent writes gives the same
stored result as exactly-once, without paying for Kafka transactions.**

### Measured, before and after

Same generator, same dropout, same 2,000-event replay overlap:

| | Rows stored | Distinct readings | Duplicates |
|---|---|---|---|
| Before (`V1`, no unique key) | 162,487 | 160,487 | **2,000** |
| After (`V2`, natural key) | 293,418 | 293,418 | **0** |

In the second run the source published 134,931 events, the consumer read all 134,931, wrote 132,931,
and reported exactly 2,000 rejected by the key. The duplicates did not disappear — they became
*visible in a metric* instead of *invisible in the data*. That is the difference between a store you
can trust and one you cannot.

### Why the surrogate key went away

`V1` had a `bigserial id`. `V2` drops it. It identified a *row*, not a *reading*, so it made two
copies of the same measurement look like two different facts — which is precisely how the duplicates
got in. Once the natural key is the primary key, the surrogate has no job left, and the sequence it
needed is one less point of contention on the write path.

### Why `do nothing` rather than `do update`

The same natural key means the same reading, so overwriting `value` writes back what is already
there. `do nothing` is cheaper and its row count tells you how many duplicates arrived. An upsert
would be the right choice only if a later arrival could legitimately *correct* an earlier value —
which is a different problem from replay.

## Measured end to end

Source on the `race` profile (232,600 events/sec target) feeding Kafka, this consumer draining into
Postgres, on one development machine:

| | Value |
|---|---|
| Produced to Kafka | ~211,000 events/sec, 0 failures |
| Consumed and written | ~50,000 events/sec |
| Write batch time | ~55 ms per 500 rows |
| Consumer lag | climbs steadily, reached 20M records |
| Rows landed | 13.5M in a few minutes |

Read that table carefully, because it says two different things at once. **Kafka absorbed the full
firehose without dropping anything** — that is the shock absorber doing its job. **Postgres drains at
a quarter of the produce rate** — so lag grows, and the pipeline is only sustainable at ~50k
events/sec today. Nothing is lost, but the backlog is real, and consumer lag is the metric that says
so honestly. Fixing the write ceiling is what TimescaleDB, hypertables, and compression are for a few
slices from now.

## Windowed rollups and watermarks

Alongside the raw-write consumer, this module runs a **Kafka Streams** topology that turns the event
stream into tumbling-window aggregates and threshold alerts.

```
telemetry.events ──rekey by (carId|sensorId)──► tumbling windows (1s, 1m)
                                                        │
                                          suppress until window closes
                                                        │
                                    ┌───────────────────┴───────────────────┐
                                    ▼                                       ▼
                            telemetry.rollups                       telemetry.alerts
                     count / average / min / max            windowed max over a threshold
```

Each rollup carries `carId`, `sensorId`, `windowLength`, `windowStart`, `windowEnd`, `count`,
`average`, `minimum`, `maximum`. Alert rules are configured per sensor:

```yaml
pitwall:
  processor:
    rollup:
      windows: [1s, 1m]
      grace: 10s
    alerts:
      - sensor-id: brake-temperature-front-left
        maximum: 1050
```

Sample output from a live run, which also shows the source's per-channel sample rates surviving all
the way through:

```
CAR-01  brake-temperature-front-left  window=PT1S  n=20   avg=947.93  min=939.45  max=958.39
CAR-01  speed                         window=PT1S  n=98   avg=336.65  min=331.75  max=341.04
CAR-01  damper-travel-front-right     window=PT1S  n=460  avg=-23.50  min=-26.99  max=29.77
```

20 samples/second for brake temperature, 98 for speed, 460 for damper travel — the catalogue rates,
measured at the far end of the pipeline.

### Event time, not arrival time

`TelemetryEventTimestampExtractor` makes the topology window on `TelemetryEvent.timestamp()` — when
the sensor took the reading — rather than on when Kafka received the record. Without it, a replayed
burst would be bucketed into *the window it arrived in*, and every aggregate would be wrong in a way
no test on the raw table would catch.

### The failure that makes watermarks necessary

Idempotency (the previous slice) made the raw store immune to replay. It does nothing for aggregates.
Same generator, same 20-second dropout, same replay, measured twice:

| Grace period | Raw store after replay | Streams `dropped-records` |
|---|---|---|
| `10s` | 5,193,852 rows / 5,193,852 distinct — correct | **+295,410** |
| `60s` | correct | **0** |

With a 10-second grace, ~295,000 replayed readings arrived after their windows had closed and were
**silently discarded from the aggregates** — while the raw table stayed perfect. Two different
correctness problems, two different fixes: a natural key protects the store, a grace period protects
the windows.

`TimeWindows.ofSizeAndGrace(windowLength, grace)` is the watermark: a window waits `grace` past its
end for stragglers, then closes for good. `Suppressed.untilWindowCloses` means one final record per
window instead of a running update on every event. Sizing `grace` is the real decision — too short
and you lose late data, too long and every result is delayed by the grace period. The dropout
duration your source can produce is the floor.

`RollupTopologyTest` pins this down deterministically with `TopologyTestDriver`: the same late event
is folded in within grace, dropped beyond it, and kept again when grace is widened.

### Why the streams app starts at `latest`

`spring.kafka.streams.properties.auto.offset.reset: latest`. Kafka Streams defaults to `earliest`, so
a restart re-reads the whole retained topic and re-aggregates hours of history whose timestamps are
long past — which showed up as 235,305 dropped records on the first run here, before a single live
event arrived. Live rollups want live data; historical aggregates are the time-series store's job.

## The time-series store

The raw table is a **TimescaleDB hypertable** partitioned on `event_time` in 5-minute chunks, with
columnstore compression and two continuous aggregates. Migrations `V3` and `V4` do the conversion.

```
telemetry_event            hypertable, 5-minute chunks, columnstore after 1 hour, dropped after 7 days
  └─ telemetry_rollup_1m   continuous aggregate, 1-minute buckets, refreshed every minute
       └─ telemetry_rollup_1h   hierarchical continuous aggregate, built from the 1-minute one
```

`telemetry_rollup_1h` is built **from** `telemetry_rollup_1m` rather than from raw data — a
hierarchical continuous aggregate. The hourly refresh reads 60 pre-computed rows per series instead of
tens of thousands of raw ones. Note the weighted average: `sum(average * samples) / sum(samples)`, not
`avg(average)`, because averaging averages over unequal buckets is wrong.

### Measured

12,000,000 rows, 5 cars x 25 sensors over 48 minutes, seeded identically on both engines by
[`benchmark/seed-telemetry.sql`](../benchmark/seed-telemetry.sql). Query is a full per-minute rollup
(`avg`, `min`, `max`, `count`) grouped by car and sensor.

| Store | Query time (3 runs) |
|---|---|
| Plain PostgreSQL 18, raw table | 2,544 / 2,579 / 2,981 ms |
| TimescaleDB hypertable, raw | 4,634 / 4,671 / 5,051 ms |
| TimescaleDB continuous aggregate | **21 / 24 / 35 ms** |

**~100x faster than plain Postgres, ~200x faster than the hypertable it is built from.**

Storage, same data:

| | Size |
|---|---|
| Plain PostgreSQL 18 table | 2,367 MB |
| Hypertable, uncompressed | 2,510 MB |
| Hypertable, columnstore | **96 MB** (26.2x) |
| `telemetry_rollup_1m` | 1,040 kB |
| `telemetry_rollup_1h` | 128 kB |

A time-bounded query — one car, one sensor, a 10-minute slice — runs in **26-39 ms** and touches 3 of
the 10 chunks. That is chunk exclusion: the planner discards whole chunks by their time range before
reading anything.

### The hypertable alone made this query slower

Read the first table again. Converting to a hypertable and changing nothing made the analytics query
**about 1.7x slower**, not faster. There is no time predicate to exclude chunks on, so the planner
scans all ten and merges the results, paying per-chunk overhead for nothing.

That is the honest lesson, and it is the opposite of how time-series databases are usually pitched. A
hypertable is a *partitioning strategy*, not a speed-up. It pays off in three specific places:

1. **Time-bounded queries**, where chunk exclusion skips most of the data.
2. **Compression**, which is per-chunk and gave 26x here.
3. **Retention**, where dropping old data is a chunk drop rather than a mass `DELETE`.

The query speed-up came from the **continuous aggregate**, which is a different mechanism entirely:
compute the rollup once on write, read it many times. That is the real trade — write more on ingest so
reads are cheap — and it is why the answer to "make the dashboard fast" is not "a better database" but
"the right shape of data in the right place".

### Operational notes found by running it

- `add_columnstore_policy` is a **procedure** in TimescaleDB 2.29, so it needs `call`, not `select`.
  `add_retention_policy` and `add_continuous_aggregate_policy` are still functions.
- Creating a continuous aggregate cannot run inside a transaction, so `V4` ships with a
  `V4__....sql.conf` containing `executeInTransaction=false`. Flyway applies it per-migration, which
  keeps the other migrations transactional.
- Continuous aggregates are created `with no data`; the refresh policy fills them going forward. To
  backfill an existing table:
  `call refresh_continuous_aggregate('telemetry_rollup_1m', null, null);`
- Job `policy_telemetry` shows `last_run_status = Failed` on a machine with no internet. That is
  Timescale's usage reporter phoning home, not a pipeline problem.
- Benchmark numbers move a lot while the compression policy is still running in the background. The
  first hypertable run measured 13-25 seconds mid-compression and settled at ~4.7 s once 8 of 10
  chunks had been converted. Wait for `timescaledb_information.job_stats` to go quiet before timing
  anything.

### Reproducing the benchmark

```bash
docker exec -i pitwall-postgres psql -U pitwall -d pitwall -f - < benchmark/seed-telemetry.sql
docker exec -it pitwall-postgres psql -U pitwall -d pitwall -c \
  "call refresh_continuous_aggregate('telemetry_rollup_1m', null, null);"
```

The seed is deterministic in shape and anchored to the previous whole hour, so it always lands inside
the retention window and mostly outside the compression delay.

## Metrics

Exported over OTLP to `grafana/otel-lgtm` every 5s, and readable locally on
`/actuator/metrics`. There is no `/actuator/prometheus` — this platform does not carry a
Prometheus registry, and nothing scrapes it. Names below are the Micrometer names; see
[`observability/README.md`](../observability/README.md) for how OTLP renames them in Grafana.

| Metric | Meaning |
|---|---|
| `pitwall.processor.events.consumed` | Events polled from Kafka |
| `pitwall.processor.events.written` | Events inserted into the store |
| `pitwall.processor.events.duplicates` | Events the natural key rejected as already stored |
| `pitwall.processor.poll.batch.size` | Distribution of poll sizes — shows whether batching is working |
| `pitwall.processor.write.duration` | Per-batch write time, with p50/p95/p99 |
| `pitwall.processor.rate.consumed` | Events consumed per second, sampled every second |
| `pitwall.processor.rollups.emitted` | Windowed rollups emitted, tagged by window length |
| `pitwall.processor.alerts.fired` | Threshold alerts, tagged by sensor |
| `kafka.stream.task.dropped.records.total` | **Events that arrived too late for their window** |
| `kafka.stream.task.record.lateness.max` | Worst observed lateness |

Consumer lag comes from the Kafka client's own metrics, exposed through Micrometer as
`kafka.consumer.fetch.manager.records.lag`. That is the number that matters: if lag stays flat while
the source rate climbs, the pipeline is keeping pace.

## Checking the data

```bash
docker exec -it pitwall-postgres psql -U pitwall -d pitwall -c \
  "select car_id, count(*), max(event_time) from telemetry_event group by car_id order by car_id;"
```

The check that matters after a replay — these two numbers must be equal:

```bash
docker exec -it pitwall-postgres psql -U pitwall -d pitwall -c \
  "select count(*), count(distinct (car_id, sensor_id, event_time)) from telemetry_event;"
```
