# Pitwall

## What this is
A high-throughput F1 telemetry ingestion platform: a reference implementation of real-time,
high-cardinality time-series ingestion (the same shape as vehicle telemetry, QoE monitoring, and
observability pipelines). Full architecture: `pitwall-architecture.md`. How real F1 telemetry is transmitted, with sources and
the numbers the simulator is calibrated against: `docs/how-f1-transmits-telemetry.md`.

All four modules are built: `pitwall-commons`, `pitwall-source`, `pitwall-stream-processor`, and
`pitwall-serving`, plus Prometheus and Grafana under `observability/`.

## What it is NOT
- Not a production UI. The serving layer only proves the data is live.
- No multi-region replication / DR, no auth / tenancy / billing.
- No exactly-once via Kafka transactions. Deliberately at-least-once + idempotency.

## Module layout
```
pitwall/                        parent pom (packaging=pom)
├── pitwall-commons             shared library (jar): cross-cutting contract and conventions
│   └── com.yourjavaguy.pitwall.commons
│       ├── event/              TelemetryEvent (shared contract, Java record)
│       ├── exception/          BaseException, ResourceNotFoundException, GlobalExceptionHandler
│       ├── observability/      ObservabilityAutoConfiguration (common metric tags)
│       └── autoconfigure/      WebExceptionAutoConfiguration (wires the handler in web services)
├── pitwall-schema              Protobuf wire format + Kafka serdes (jar)
│   └── src/main/proto/telemetry.proto, TelemetryEventWireFormat, protobuf serializer/deserializer/serde
├── pitwall-source              telemetry generator (jar, bootable, port 8081)
│   └── com.yourjavaguy.pitwall.source
│       ├── config/             SourceProperties, KafkaTopicConfiguration
│       ├── sensor/             SensorCatalog, SensorDefinition, SensorState, SignalGenerator
│       ├── generator/          TelemetryGenerator, CarEmitter, LoadDial
│       ├── fault/              FaultInjection (dropout buffer, replay overlap, burst, anomaly)
│       ├── sink/               TelemetrySink + kafka / database / logging, TelemetryDispatcher
│       ├── metrics/            SourceMetrics
│       ├── control/            SourceControlService, SourceStatus
│       └── web/                SourceController + request DTOs
├── pitwall-stream-processor    Kafka consumer + Streams rollups (jar, bootable, port 8082)
│   └── com.yourjavaguy.pitwall.processor
│       ├── config/             ProcessorProperties, KafkaTopicConfiguration
│       ├── consumer/           TelemetryConsumer (batch @KafkaListener)
│       ├── service/            TelemetryIngestService (splits polls into write batches)
│       ├── persistence/        TelemetryEventRepository (batched JDBC), Flyway owns the schema
│       │                       V1 table, V2 natural key, V3 hypertable, V4 continuous aggregates
│       ├── rollup/             RollupTopology (Kafka Streams), TelemetryEventTimestampExtractor,
│       │                       RollupAccumulator
│       └── metrics/            IngestMetrics, RollupMetrics
└── pitwall-serving             the pit wall: SSE + query API + dashboard (jar, bootable, port 8083)
    └── com.yourjavaguy.pitwall.serving
        ├── config/             ServingProperties, KafkaConsumerConfiguration (one factory per type)
        ├── live/               LiveFeed (SSE fan-out), AlertHistory, TelemetryStreamConsumer
        ├── query/              TelemetryRollupRepository, TelemetryQueryService, Resolution
        └── web/                LiveFeedController, TelemetryQueryController
            resources/static/   index.html: the dashboard, no build step
```

Module details: `pitwall-source/README.md`, `pitwall-stream-processor/README.md`,
`pitwall-serving/README.md`, `observability/README.md`.

## What belongs where
- **commons** = cross-cutting only: the shared event contract, the exception convention, the
  observability baseline. Auto-config beans are gated (`@ConditionalOnWebApplication`,
  `@ConditionalOnClass`) so the non-web producer/processor are not forced onto web/actuator.
- **Infrastructure stays out of commons.** Kafka, TimescaleDB/JPA, Protobuf, and WebSocket/SSE deps
  live in the service that needs them, never here.

## Follow-ups
- **Protobuf + Schema Registry** is the platform's wire format (`TelemetryEvent` is a plain record for
  now). When building `pitwall-source`/`pitwall-stream-processor`, add the `.proto` schema and
  codegen, then decide whether the schema lives in commons or a dedicated `pitwall-schema` module.

## Per-service config baseline
Each bootable service (source/processor/serving) should set, in its own `application.yml`:
```yaml
spring:
  application:
    name: <service-name>        # OTel exports this as the service_name resource attribute
  threads:
    virtual:
      enabled: true

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics      # no prometheus; export is OTLP, nothing scrapes
  endpoint:
    health:
      show-details: when-authorized
  otlp:
    metrics:
      export:
        url: http://localhost:4328/v1/metrics
        step: 5s
  opentelemetry:
    tracing:
      export:
        otlp:
          endpoint: http://localhost:4328/v1/traces
  observations:
    enable:
      "[tasks.scheduled.execution]": false
  tracing:
    sampling:
      probability: 1.0         # 0.05 on the processor; never leave it at the 0.1 default
```

## How to run
```bash
./scripts/pitwall.sh start [dev|race|breakit]   # containers + all three services, in order
./scripts/pitwall.sh status | logs | reset | stop [--all]
```

`scripts/pitwall.sh` is the supported way to run the platform: it waits on container health, builds
when a jar is missing (`PITWALL_BUILD=1` forces), writes pids and logs under `.run/`, and starts
processor → serving → source. `reset` clears the Kafka topics and Streams state, which is required
after changing the wire format.

By hand:
```bash
docker compose up -d                                        # Kafka 9092, Timescale 5434, otel-lgtm 3000/4328
./mvnw -pl pitwall-stream-processor -am spring-boot:run     # consumer + rollups, 8082
./mvnw -pl pitwall-serving -am spring-boot:run              # pit wall, 8083
./mvnw -pl pitwall-source -am spring-boot:run               # generator, 8081
```

Dashboard at http://localhost:8083, Grafana at http://localhost:3000.

Both services connect to `localhost`, so they run on the host and only the infrastructure is
containerised. Without compose running, start the source on the logging sink:
`--pitwall.source.sink=logging`.

## How to build
```bash
./mvnw clean package            # full reactor; integration tests need a running Docker daemon
```

## Conventions specific to this repo
- **Wire format is Protobuf**, defined in `pitwall-schema` with the Kafka serdes beside it. Commons
  keeps the plain `TelemetryEvent` record as the contract; the encoding lives in the schema module so
  commons stays infrastructure-free. `pitwall.source.wire-format` / `pitwall.processor.wire-format`
  switch to `json` for the comparison and must match. Rollups and alerts stay JSON on purpose.
- **Custom Kafka factories must honour `KafkaConnectionDetails`.** Building a `ProducerFactory` or
  `ConsumerFactory` from `KafkaProperties` alone silently ignores `@ServiceConnection`, so
  Testcontainers tests talk to `localhost:9092` instead of the container.
- **The naive path stays in the repo.** `pitwall-source`'s `database` sink writes one row per event
  straight to Postgres. It is the control group for the load story, not dead code.
- **`telemetry_event` is keyed on natural identity** `(car_id, sensor_id, event_time)`, which is the
  primary key. Writes are `on conflict do nothing`, so an at-least-once replay is a no-op rather than
  a double-count. The `V1` surrogate `bigserial id` was dropped in `V2`; it identified a row, not a
  reading.
- **Kafka serializers are the Jackson 3 variants** (`JacksonJsonSerializer` / `JacksonJsonDeserializer`).
  The Jackson 2 `JsonSerializer` classes are still on the classpath and will fail on `Instant`.

- **Aggregates window on event time, not arrival time**, via `TelemetryEventTimestampExtractor`. The
  grace period (`pitwall.processor.rollup.grace`) must exceed the longest dropout the source can
  produce, or replayed readings are silently dropped from the rollups while the raw table stays
  correct.
- **Rollups and alerts live on Kafka topics only** (`telemetry.rollups`, `telemetry.alerts`). They are
  deliberately not persisted, because TimescaleDB continuous aggregates own historical rollups in the next
  slice, and a table here would be thrown away.
- **`TelemetryRollup` / `TelemetryAlert` live in the processor**, not commons. Move them to commons
  when `pitwall-serving` needs to consume them.

- **The store is TimescaleDB, not plain Postgres.** `telemetry_event` is a hypertable (5-minute
  chunks, columnstore after 1 hour, dropped after 7 days) with `telemetry_rollup_1m` and a
  hierarchical `telemetry_rollup_1h` continuous aggregate on top. Dashboards read the aggregates, not
  the raw table.
- **A hypertable on its own is not a speed-up.** It made the unbounded analytics query ~1.7x slower
  than plain Postgres here. The wins are chunk exclusion on time-bounded queries, per-chunk
  compression (26x measured), and retention as a chunk drop. Query speed comes from the continuous
  aggregates (~100x measured).
- **Continuous aggregate migrations cannot run in a transaction.** `V4` has a companion
  `V4__....sql.conf` with `executeInTransaction=false`. `add_columnstore_policy` is a procedure in
  TimescaleDB 2.29 and needs `call`, not `select`.
- **Integration tests run against the TimescaleDB image**, not `postgres:18-alpine`, because the
  migrations need the extension.
- **`TelemetryRollup` and `TelemetryAlert` live in commons** now that serving consumes them.
  `RollupAccumulator` stays in the processor; it is topology internals, not a contract.
- **Live push is SSE, not WebSocket.** The feed is one-way, so a return channel would be unused.
  `GlobalExceptionHandler` has a dedicated no-body handler for `AsyncRequestNotUsableException`,
  because a browser closing a stream is normal and must not be turned into a 500.
- **Observability is OTLP only.** `spring-boot-starter-opentelemetry` in every bootable service
  exporting to one `grafana/otel-lgtm` container. No Prometheus registry, no scrape config, and no
  custom observability beans. The old commons `ObservabilityAutoConfiguration` was deleted because
  OTel carries `service_name` from `spring.application.name` already.
- **Set `management.tracing.sampling.probability` explicitly** in every service; it defaults to 0.1.
  Scheduled tasks are excluded from tracing via
  `management.observations.enable."[tasks.scheduled.execution]": false`. The bracket quoting is
  required or Spring mangles the dotted map key, and without it the metric samplers produce most of
  the spans.
- **OTLP renames things.** Timers export in milliseconds and client-side percentiles arrive as a
  `quantile` label, not histogram buckets, so `histogram_quantile()` does not apply. See
  `observability/README.md`.
- **Grafana panels need `fieldConfig.defaults.color`, `mappings`, and `thresholds`** or the dashboard
  renders completely empty with no error.
- **The consumer's sustainable ceiling is ~5,000 writes/second** into the compressed hypertable on a
  development machine. Above that, lag grows. Kafka absorbs it without loss, but the backlog is real.

- **`pitwall-schema` owns codegen.** `protobuf-maven-plugin` downloads `protoc`; no system install.

## External dependencies
Kafka (`apache/kafka:4.1.0`), TimescaleDB (`timescale/timescaledb:2.29.2-pg17`, PostgreSQL 17.11), and
`grafana/otel-lgtm:0.30.2` via `docker-compose.yml`. Postgres is published on host port **5434** and
OTLP on **4328/4327**, to avoid colliding with other local projects' containers.
`benchmark/seed-telemetry.sql` generates a reproducible 12M-row dataset for the query benchmark.
