# Pitwall

**A high-throughput F1 telemetry ingestion platform.** A Formula 1 car measures far more than its
radio link can carry. This models the whole path — off the car, through a log, into a time-series
store, and onto the engineers' screens — and measures every claim it makes.

Strip away the racing and it is one of the most common hard problems in backend engineering:
**high-throughput, high-cardinality time-series ingestion.** The same shape as vehicle telemetry,
playback-quality monitoring, GPU-fleet metrics, and every observability product you have used.

```
 pitwall-source ──► Kafka ──► pitwall-stream-processor ──► TimescaleDB ──► pitwall-serving
  telemetry           the         batch writes +              hypertable +      SSE live push +
  generator +      shock          Kafka Streams              continuous         query API +
  fault dial      absorber        rollups + alerts           aggregates         dashboard
                                          │                                          ▲
                                          └──────── alerts, pushed live ─────────────┘
```

---

## What it actually does, with numbers

Every figure below was measured on one development machine, not estimated. Each has a section in a
module README explaining how.

| | Measured |
|---|---|
| Generator throughput | **211,000 events/sec** to Kafka, zero failures |
| Naive path, same load | **0–1,441 events/sec** — a 221x collapse |
| Wire format | **57 vs 136 bytes/event** serialized (2.38x), **36 vs 54** on the broker after lz4 (1.50x) |
| Duplicate replay | 2,000 duplicates → **0 stored**, counted instead |
| Late data past the grace period | **295,410 records** silently dropped from aggregates |
| Historical query, 12M rows | **2,544 ms** raw Postgres → **21 ms** continuous aggregate |
| Compression | **2,510 MB → 96 MB** (26.2x) |
| Sustained pipeline | 3,892 produced/s, 3,880 written/s, **lag 0** |

---

## The services

Six Maven modules. Three are bootable Spring Boot 4 applications; three are libraries and config.

| Module | Kind | Port | One line |
|---|---|---|---|
| [`pitwall-commons`](pitwall-commons) | library | — | The shared contract every service agrees on |
| [`pitwall-schema`](pitwall-schema/README.md) | library | — | The Protobuf wire format and its Kafka serdes |
| [`pitwall-source`](pitwall-source/README.md) | **app** | 8081 | The car: generates telemetry and breaks on demand |
| [`pitwall-stream-processor`](pitwall-stream-processor/README.md) | **app** | 8082 | The garage: writes it down and rolls it up |
| [`pitwall-serving`](pitwall-serving/README.md) | **app** | 8083 | The pit wall: live feed, query API, dashboard |
| [`observability/`](observability/README.md) | config | 3000 | OTLP collection and the Grafana dashboard |

### `pitwall-commons` — the contract

A plain library with **no infrastructure dependencies at all**: no Kafka, no JDBC, no Protobuf. That
constraint is the point — it is what lets the non-web generator and the non-web processor depend on
it without inheriting a web server.

- **`TelemetryEvent`** — `(carId, sensorId, timestamp, value, sequenceNo)`. A Java record. Every
  service speaks this; only `pitwall-schema` knows how it is encoded on the wire.
- **`TelemetryRollup` / `TelemetryAlert`** — the windowed and threshold contracts. They live here
  because both the processor (producer) and serving (consumer) need them.
- **`GlobalExceptionHandler`** — one `@RestControllerAdvice` returning `ProblemDetail` (RFC 9457), so
  all three services fail in the same shape.
- Auto-configuration is **conditional** (`@ConditionalOnWebApplication`, `@ConditionalOnClass`), so a
  service only gets the web pieces if it is actually a web service.

### `pitwall-schema` — the wire format

`telemetry.proto` plus the Kafka serializer, deserializer, and Streams serde. `protoc` is downloaded
by the Maven plugin, so there is nothing to install.

Encoding is deliberately separate from the contract: `TelemetryEvent` stays a plain record in
commons, and this module is the only place that knows about bytes. Flip both services to
`wire-format: json` and the platform still runs — that is how the 2.38x comparison was measured.

Rollups and alerts stay JSON on purpose: they are low-volume and human-readable matters more there.

### `pitwall-source` — the car :8081

The telemetry generator, and the only service with a control plane. `SensorCatalog` carries **40 real
named channels** — speed, engine rpm, brake temperatures, damper travel, MGU-K power — each with its
own unit, range, and sample rate from **1 Hz to 1 kHz**, padded with synthetic channels to whatever
the load dial asks for. Values follow a lap-time simulation rather than random noise, so a brake
temperature actually rises into a corner.

| Piece | What it does |
|---|---|
| `SensorCatalog` / `SignalGenerator` | Per-channel definitions and realistic signal shapes |
| `CarEmitter` | One virtual-thread emitter per car, on that car's own schedule |
| `LoadDial` | Cars × channels × rate scale, changeable at runtime |
| `FaultInjection` | Dropout buffer, replay-with-overlap, burst, anomaly |
| `TelemetryDispatcher` | Routes to one of three sinks |

**Three sinks, and the second one is the whole argument:**

- `kafka` — the real path. 211,000 events/sec, zero failures.
- `database` — the **control group**. One row per event, straight to Postgres. It is not dead code:
  it collapses to 0–1,441 events/sec under the same load, and *nothing errors* — the connection pool
  just makes the producer queue. That is the load story in one flag.
- `logging` — no infrastructure needed; how you run the source with nothing else up.

**Fault injection is a feature, not an error path**, because on a real circuit it is normal: tunnels
and cell handovers drop the link, and the car replays its backlog when it reconnects. Replay
deliberately re-sends the last `overlap-size` events that already arrived, so the downstream consumer
receives genuine duplicates and out-of-order data to cope with.

### `pitwall-stream-processor` — the garage :8082

Two independent pipelines off the same topic.

**1. Ingest → TimescaleDB.** A batch `@KafkaListener` (2,000 records per poll, concurrency 6) hands
each poll to `TelemetryIngestService`, which splits it into 500-row JDBC batches.

- `telemetry_event` is keyed on **natural identity** `(car_id, sensor_id, event_time)` — that is the
  primary key. Writes are `on conflict do nothing`, so an at-least-once redelivery is a no-op instead
  of a double-count. That is exactly-once *storage* without paying for Kafka transactions.
- **Flyway owns the schema**, Hibernate never does. `V1` table → `V2` natural key → `V3` hypertable →
  `V4` continuous aggregates. `V4` cannot run in a transaction, hence its companion `.sql.conf`.
- Sustainable ceiling on a development machine: **~5,000 writes/second** into the compressed
  hypertable. Above that, lag grows — Kafka absorbs it without loss, but the backlog is real, and the
  Grafana dashboard is where you watch it happen.

**2. Kafka Streams → rollups and alerts.** `RollupTopology` computes tumbling 1s and 1m windows per
`(car, sensor)` and publishes to `telemetry.rollups`, plus threshold breaches to `telemetry.alerts`.

- Windows are on **event time, not arrival time**, via a custom `TimestampExtractor`. This is the
  part that idempotency does *not* fix: the same duplicate replay that stored cleanly dropped 295,410
  readings out of already-closed windows.
- The grace period (`pitwall.processor.rollup.grace`) must exceed the longest dropout the source can
  produce, or late data vanishes from the aggregates while the raw table stays perfectly correct.
- Rollups and alerts are **Kafka topics only, never tables** — TimescaleDB's continuous aggregates
  own historical rollups, so a table here would be duplicated work.

### `pitwall-serving` — the pit wall :8083

What the engineers actually look at. Two Kafka consumer groups (rollups and alerts, each with its own
typed deserializer), a query API over the continuous aggregates, and a dashboard with no build step —
one `index.html`, served static.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/live/stream` | SSE feed; events named `rollup` and `alert` |
| `GET` | `/api/v1/live/alerts` | Recent alerts, newest first |
| `GET` | `/api/v1/telemetry/cars` | Car ids present in the aggregates |
| `GET` | `/api/v1/telemetry/sensors` | Sensor ids present in the aggregates |
| `GET` | `/api/v1/telemetry/rollups` | `carId`, `sensorId`, `resolution=ONE_MINUTE\|ONE_HOUR`, `from`, `to` |

**Live push is SSE, not WebSocket.** The feed is one-way, so a return channel would sit unused — and
it mirrors the real thing, since FIA rules have made F1 telemetry one-way since 2003. A browser
closing a stream is normal, so `AsyncRequestNotUsableException` gets its own no-body handler rather
than becoming a 500.

Queries read the **aggregates, not the raw table** — `telemetry_rollup_1m` and the hierarchical
`telemetry_rollup_1h` on top of it. That is the 2,544 ms → 21 ms.

### `observability/` — the instruments :3000

**OTLP only.** Every bootable service carries `spring-boot-starter-opentelemetry` and exports metrics,
traces, and logs to one `grafana/otel-lgtm` container. No Prometheus registry, no scrape config, no
custom observability beans — OTel already carries `service_name` from `spring.application.name`.

The dashboard's headline panel is **consumer lag**, because that is the pipeline's heartbeat: it falls
to zero and stays flat while throughput holds, then climbs without bound the moment the source
outruns the consumer's write ceiling.

Two things that will bite you and are documented in [`observability/README.md`](observability/README.md):
OTLP renames things (timers in milliseconds, client-side percentiles as a `quantile` label, so
`histogram_quantile()` does not apply), and Grafana panels silently render **completely empty** unless
they carry `fieldConfig.defaults.color`, `mappings`, and `thresholds`.

---

## The five things it demonstrates

**1. A log in the middle is the whole design.** The `database` sink is still in the repo as a control
group: point the generator straight at Postgres, crank the dial, and throughput collapses 221x. The
interesting part is that *nothing errors* — the connection pool makes the producer queue, so a fast
producer wired to a slow consumer silently becomes as slow as the consumer.

**2. Correctness under retry is a design property.** Replay a dropout and 2,000 duplicate readings
land in the store. Keying every write on natural identity `(car_id, sensor_id, event_time)` with
`on conflict do nothing` turns a redelivery into a no-op — at-least-once plus idempotency gives the
same stored result as exactly-once, without paying for Kafka transactions.

**3. Event time is not arrival time.** Idempotency fixes the raw store and does **nothing** for
aggregates. The same replay dropped 295,410 readings out of closed windows while every integrity
check on the table came back clean. A natural key protects the store; a grace period protects the
windows. Two problems, two fixes.

**4. A hypertable is not a speed-up.** Converting to TimescaleDB and changing nothing made the
analytics query *1.7x slower* — no time predicate means no chunk exclusion. The 100x came from
continuous aggregates: compute once on write, read many times. Partitioning buys compression and
retention; pre-aggregation buys speed.

**5. A pipeline you cannot observe is one you cannot trust.** Consumer lag is the heartbeat. The
Grafana dashboard shows it fall off a cliff to zero and stay flat while throughput holds — and shows
it climb without bound the moment the source outruns the consumer's write ceiling. Metrics and traces
leave every service over OTLP into a single `grafana/otel-lgtm` container; there is no scrape config
and no custom observability code.

---

## Run it

### Prerequisites

| | |
|---|---|
| **JDK 25** | `java -version` should say 25 |
| **Docker** | daemon running — `docker ps` must work |
| **Disk** | ~5 GB for images on first run |
| **Free host ports** | `9092` Kafka · `5434` Postgres · `3000` Grafana · `4327`/`4328` OTLP · `8081`/`8082`/`8083` services |

Nothing else to install. Maven comes from the wrapper, and `protoc` is downloaded by the build.

### One command

```bash
./scripts/pitwall.sh start
```

`scripts/pitwall.sh` is the supported way to run the platform. It is not a convenience wrapper around
`docker compose up` — it enforces the startup order the pipeline actually needs.

**What `start` does, in order:**

1. **Infrastructure.** `docker compose up -d`, then blocks until Kafka, TimescaleDB, and otel-lgtm
   each report `healthy` — not merely *started*. A service that connects to a Kafka which is still
   electing a controller fails in confusing ways, so it waits.
2. **Build if needed.** If any service jar is missing, it runs `./mvnw clean install -DskipTests`.
   Present jars are reused; `PITWALL_BUILD=1` forces a rebuild.
3. **Services, in dependency order** — each one waited on before the next starts:
   - `pitwall-stream-processor` (8082) **first**, because it runs Flyway. It owns the schema, so it
     must have created the tables before anything queries them.
   - `pitwall-serving` (8083) next, so the dashboard is already listening when data starts flowing.
   - `pitwall-source` (8081) **last**, so nothing is generated into a pipeline that cannot receive it.

   Each runs detached via `setsid nohup`, so they survive closing the terminal. Pids go to
   `.run/<service>.pid`, logs to `.run/logs/<service>.log`.

**First run takes a while:** ~5 GB of images to pull and a full Maven build. Later runs start in
under a minute.

You should end on:

```
Pitwall is up

  Dashboard    http://localhost:8083
  Grafana      http://localhost:3000
  Source API   http://localhost:8081/api/v1/source/status
```

### Check it is actually ingesting

Open **http://localhost:8083** — the status pill should read `live` and the numbers should be moving.
Or from a terminal:

```bash
curl -s localhost:8081/api/v1/source/status   # actualEventsPerSecond should be non-zero
./scripts/pitwall.sh status                   # every service ✓, every container healthy
```

### Every command

| Command | What it does |
|---|---|
| `start [profile]` | Containers → build if needed → all three services, in order |
| `stop` | Stops the three services. **Leaves the containers up.** |
| `stop --all` | Stops the services *and* the containers |
| `restart [profile]` | `stop` then `start` — how you switch load profile |
| `status` | Each service: running, port, pid, and whether it answers health. Then container status. |
| `logs` | Tails all three logs at once |
| `logs <service>` | Tails one — `source`, `processor`, `serving` |
| `reset` | Stops everything, deletes the three Kafka topics, resets Kafka Streams state |
| `help` | The same list, from the script |

```bash
./scripts/pitwall.sh start race        # 20 cars x 100 channels
./scripts/pitwall.sh logs processor    # watch the consumer keep up, or not
./scripts/pitwall.sh restart breakit   # switch profile
./scripts/pitwall.sh stop              # done for now, containers stay warm
PITWALL_BUILD=1 ./scripts/pitwall.sh start   # after changing code
```

### Load profiles

The profile applies to **the source only** — it is the size of the firehose. Omit it for the default.

| Profile | Cars | Channels each | Rate scale | For |
|---|---|---|---|---|
| *(none)* / `dev` | 5 | 20 | 0.1 | Poking at it. Comfortable on a laptop. |
| `race` | 20 | 100 | 1.0 | A full grid at realistic volume — about 232,000 events/sec |
| `breakit` | 20 | 300 | 4.0 | Past the real grid's wire volume, to find the ceiling |

`breakit` is meant to fail. The consumer's sustainable ceiling is ~5,000 writes/second into the
compressed hypertable, so lag will climb — that is the demonstration, not a bug. Kafka absorbs it
without loss.

### Two commands that are deliberately non-obvious

**`stop` leaves the containers running.** Kafka and TimescaleDB are slow to come back and you almost
always want them for the next run. Use `stop --all` when you actually want the ports back.

**`reset` does not touch your data.** It wipes the three Kafka topics and the Kafka Streams state
directory, then tells you so. `telemetry_event` rows are kept — truncate manually if you want a clean
store.

You need `reset` after **changing the wire format**. Kafka Streams keeps internal repartition topics,
and if they still hold JSON when the app restarts expecting Protobuf, the deserializer fails hard and
kills the Streams client with `Not a valid TelemetryEventMessage`. Deleting the topics is not enough
on its own — the state directory has to go too, which is what `reset` handles.

### Where things live

| | |
|---|---|
| **Dashboard** | http://localhost:8083 |
| **Grafana** | http://localhost:3000 (anonymous, or `admin` / `pitwall`) |
| Source control API | http://localhost:8081/api/v1/source/status |
| Health endpoints | `localhost:8082/actuator/health`, `localhost:8083/actuator/health` |
| OTLP endpoint | `localhost:4328` (HTTP), `localhost:4327` (gRPC) |
| TimescaleDB | `localhost:5434`, database `pitwall`, `pitwall` / `pitwall` |
| Logs and pids | `.run/logs/*.log`, `.run/*.pid` |

### If something will not start

| Symptom | Cause and fix |
|---|---|
| `Bind for 0.0.0.0:XXXX failed: port is already allocated` | Another stack owns that port. Change the host side of the mapping in `docker-compose.yml` — this is why Postgres is on **5434** and OTLP on **4328**, not their defaults. |
| A service never goes green | `./scripts/pitwall.sh logs <service>` — it is almost always Kafka or Postgres not reachable |
| `Not a valid TelemetryEventMessage` in the processor log | Kafka still holds records in the other wire format. `./scripts/pitwall.sh reset` |
| `missing ...jar — run with PITWALL_BUILD=1` | The jar was cleaned away. Do exactly that. |
| Service reports "already running" but nothing answers | A stale pid in `.run/`. `./scripts/pitwall.sh stop`, then start again. |
| Grafana dashboard is empty | Give it 30s — metrics arrive over OTLP every 5s and panels need a couple of points |
| IntelliJ cannot resolve `TelemetryEvent` | The IDE has a stale module list. Maven tool window → **Reload All Maven Projects** |

### Driving it by hand

Useful when you want one service in a debugger. Same order the script uses, and the processor still
has to be first because it runs the migrations:

```bash
docker compose up -d
./mvnw clean install                                     # integration tests need Docker
./mvnw -pl pitwall-stream-processor -am spring-boot:run  # consumer + rollups   :8082
./mvnw -pl pitwall-serving -am spring-boot:run           # the pit wall         :8083
./mvnw -pl pitwall-source -am spring-boot:run            # generator            :8081
```

Everything connects to `localhost`, so the services run on the host and only the infrastructure is
containerised. With no compose running at all, the source still works on its own:

```bash
./mvnw -pl pitwall-source -am spring-boot:run -Dspring-boot.run.arguments=--pitwall.source.sink=logging
```

### Turn the dial

Watch the effect on the dashboard (http://localhost:8083) and in Grafana (http://localhost:3000).

```bash
# 20 cars, 100 channels each — about 232,000 events/second
curl -X PUT localhost:8081/api/v1/source/load -H 'Content-Type: application/json' \
     -d '{"cars":20,"sensorsPerCar":100,"rateScale":1.0}'

# break it on purpose: send the firehose straight at Postgres
curl -X PUT localhost:8081/api/v1/source/sink -H 'Content-Type: application/json' \
     -d '{"name":"database"}'

# lose the radio link, then replay the backlog with duplicates
curl -X PUT localhost:8081/api/v1/source/faults/dropout -H 'Content-Type: application/json' \
     -d '{"active":true}'
curl -X POST localhost:8081/api/v1/source/faults/replay

# push one sensor past its threshold and watch alerts appear on the dashboard
curl -X PUT localhost:8081/api/v1/source/faults/anomaly -H 'Content-Type: application/json' \
     -d '{"carId":"CAR-04","sensorId":"brake-temperature-front-left"}'
```

The full control plane is in [`pitwall-source/README.md`](pitwall-source/README.md).

---

## Documents

| | |
|---|---|
| [How F1 actually transmits telemetry](docs/how-f1-transmits-telemetry.md) | Sourced research — sensor counts, sample rates, the WiMAX link, volumes. Every number the simulator is calibrated against. |
| [Architecture](pitwall-architecture.md) | The design, the decisions, and the non-goals |
| [Roadmap](pitwall-roadmap.md) | The phased build, each phase with the failure that teaches it |

---

## The F1 part

The simulator is not hand-waved. From the research doc:

- A car carries **~300 sensors** feeding **1,000–2,000 channels** sampled from **1 Hz to 1 kHz** —
  roughly **150,000 data points/second** measured onboard, **1.5 TB per car per race weekend**.
- Only a prioritised subset leaves the car live: about **30 MB per lap**, which is **2.7 Mbit/s**.
  Everything else is logged onboard and offloaded over a wired umbilical in the garage.
- Across the grid, the live stream runs at roughly **1.1 million data points/second**.
- Transport is a **WiMAX 802.16 mesh at ~3.5 GHz** around the circuit, encrypted, and **one-way**
  since the 2003 ban on two-way telemetry. Tunnels and cell handovers make dropout normal.

Those facts drive real decisions here: channels carry individual sample rates rather than one global
one, dropout-then-replay is a first-class feature rather than an error case, the live feed is SSE
because the real link is one-way, and the 2.7 Mbit/s budget is what justifies a binary wire format.

## What this is not

- Not a production UI. The serving layer proves the data is live; it is not a product.
- No multi-region replication or DR, no auth, tenancy, or billing.
- No exactly-once via Kafka transactions — deliberately at-least-once plus idempotency, and the
  README of the processor explains why that is sufficient.
- No Schema Registry yet. Protobuf is on the wire; enforced schema evolution is the next step.
