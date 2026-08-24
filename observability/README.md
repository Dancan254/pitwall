# Observability

All three services export **OTLP** to a single `grafana/otel-lgtm` container: Grafana, Prometheus,
Tempo and Loki in one image. There is no scrape config, no Prometheus registry, and no custom
observability code in the project.

| | |
|---|---|
| Grafana | http://localhost:3000 (anonymous viewer, or `admin` / `pitwall`) |
| OTLP HTTP | `localhost:4328` |
| OTLP gRPC | `localhost:4327` |
| Dashboard | **Pitwall: telemetry pipeline** |

OTLP is published on **4328/4327**, not the usual 4318/4317, to stay out of the way of other local
LGTM containers, the same reason Postgres sits on 5434.

## How it is wired

`spring-boot-starter-opentelemetry` in each bootable service. The starter **exports**; it does not
instrument. Micrometer does the instrumenting and was already there, so the starter's job is to carry
those signals out over OTLP. It brings `micrometer-registry-otlp` for metrics and
`micrometer-tracing-bridge-otel` for traces.

Two prefixes, because there are two export paths:

```yaml
management:
  otlp:
    metrics:
      export:
        url: http://localhost:4328/v1/metrics   # metrics, via Micrometer's registry
        step: 5s
  opentelemetry:
    tracing:
      export:
        otlp:
          endpoint: http://localhost:4328/v1/traces   # traces, via the OTel SDK
  observations:
    enable:
      "[tasks.scheduled.execution]": false
  tracing:
    sampling:
      probability: 1.0
```

**Sampling is set explicitly in every service**, because it defaults to `0.1` and silently discards
90% of traces. The processor runs at `0.05`, because it has Kafka listener observation on, so a span per poll
at full sampling would be a lot of noise for very little signal. Source and serving run at `1.0`;
their spans are HTTP control-plane calls and there are few of them.

**Scheduled tasks are excluded from tracing.** The metric samplers run once a second in every service,
and before this was set they produced ~90% of all spans, burying the real HTTP work. The map key needs
bracket quoting in YAML or Spring's relaxed binding mangles the dots.

## Signals

Metrics, traces, and logs, all three over OTLP.

Logs need more wiring than the other two, because Spring Boot 4 configures the export side but not
the input side. `OpenTelemetryLoggingAutoConfiguration` and `OtlpLoggingAutoConfiguration` build the
`SdkLoggerProvider` and the OTLP exporter for you, but nothing feeds them until you add the Logback
appender yourself:

- `io.opentelemetry.instrumentation:opentelemetry-logback-appender-1.0` on the classpath. It has no
  stable release; it ships only from the instrumentation `-alpha` line. `2.21.0-alpha` is the
  release that pairs with the OpenTelemetry 1.55.0 that Boot 4.0.0 manages, and the version is
  pinned in the parent pom rather than by importing the alpha BOM, which would drag the whole OTel
  API forward with it.
- `logback-spring.xml` in each service, attaching the appender to the root logger.
- `OpenTelemetryAppender.install(openTelemetry)` at startup. Boot does not do this. Commons carries
  it as a gated auto-configuration, ordered **after**
  `OpenTelemetrySdkAutoConfiguration`; without that ordering `@ConditionalOnBean(OpenTelemetry.class)`
  is evaluated before the SDK bean exists, the installer never runs, and the appender drops every
  record without a word. That failure looks exactly like a broken endpoint.

### The export threshold

The console and the export are deliberately on different levels. The console is for one developer
watching one service; Loki is the whole platform at telemetry volume.

```yaml
pitwall:
  observability:
    log-export-level: WARN     # default INFO
```

Measured: at `INFO` a service exports around 60 records over a startup and a minute of running. At
`WARN` the same run exports zero while the console still shows all 60.

`LoggingTelemetrySink` is excluded from export entirely, with `additivity="false"` and a console-only
appender. It writes one sampled line per N events and exists precisely so the source can run with no
infrastructure at all, so shipping it over the wire is both pointless and the fastest way to drown
Loki on the `race` profile.

## The panels

| Panel | What it proves |
|---|---|
| **Consumer lag: the heartbeat** | Whether the consumer group is keeping pace. The single most important number. |
| **Throughput: produced vs consumed vs written** | Where the pipeline narrows |
| **Source load dial: target vs actual** | Whether the generator is hitting the rate you asked for |
| **Write latency per batch** | p50/p95/p99 into the store |
| **Correctness: duplicates rejected and late data dropped** | Idempotency and watermarks, as live counters |
| **Dropout buffer and sink failures** | What the fault injection is doing |
| **Rollups, alerts, and dashboard subscribers** | The serving side |

## Watching it in Grafana

Grafana is at **http://localhost:3000** with anonymous viewer access, so there is nothing to log into.
The dashboard is provisioned at a fixed URL, which beats hunting through the dashboard list:

```
http://localhost:3000/d/pitwall-pipeline
```

Set the time picker to **Last 15 minutes** and refresh to **5s**. Metrics arrive over OTLP every 5
seconds, so a panel needs two or three points before it draws a line. A dashboard opened in the first
ten seconds of a run looks broken and is not.

### What Grafana can and cannot show you

Worth getting straight before you go looking for something that is not there. Grafana holds **metrics
about the pipeline**, not the telemetry flowing through it:

| Question | Where the answer is |
|---|---|
| Is the consumer keeping up? | Grafana, **consumer lag** |
| How many events per second are produced, consumed, written? | Grafana, **throughput** |
| How many duplicate readings did the natural key reject? | Grafana, **correctness** |
| How late was the latest reading to arrive? | Grafana, `kafka_stream_task_record_lateness_max` |
| What did a service log when it failed? | Grafana, **Loki**, `{service_name="pitwall-processor"}` |
| What is CAR-01 doing right now? | The pit wall, http://localhost:8083 |
| What did CAR-01's brake temperature read at 14:32? | The query API, or `psql` |

There is no panel that shows an individual reading, and no query you can write to get one. The logs
that do reach Loki are application logs, startup and lifecycle and warnings, not telemetry; the
traces are HTTP control-plane spans, not data. Individual readings live in `telemetry_event` and
reach the browser over SSE; they never reach Grafana.

### A run worth watching

Each of these changes something you can see on the dashboard within about ten seconds.

**1. Steady state.** Start on the default profile and let it settle:

```bash
./scripts/pitwall.sh start
```

Consumer lag falls to zero and stays flat. Throughput shows produced, consumed, and written sitting
on top of each other. That flat zero is the shape everything else is measured against.

**2. Outrun the writer.** Push the source past the consumer's write ceiling:

```bash
curl -X PUT localhost:8081/api/v1/source/load -H 'Content-Type: application/json' \
     -d '{"cars":20,"sensorsPerCar":100,"rateScale":1.0}'
```

Produced climbs immediately; written does not follow. **Consumer lag leaves zero and keeps climbing.**
Nothing errors and nothing is lost, which is the point: Kafka absorbs the difference and the backlog
is visible instead of silent.

**3. Break the naive path.** Point the generator straight at Postgres:

```bash
curl -X PUT localhost:8081/api/v1/source/sink -H 'Content-Type: application/json' \
     -d '{"name":"database"}'
```

Watch **source load dial: target vs actual** separate. The target stays where you set it and the
actual collapses, because the connection pool makes the emitter threads queue. Put it back with
`{"name":"kafka"}` and the two lines rejoin.

**4. Lose the radio link, then replay it.**

```bash
curl -X PUT localhost:8081/api/v1/source/faults/dropout -H 'Content-Type: application/json' \
     -d '{"active":true}'
curl -X POST localhost:8081/api/v1/source/faults/replay
```

**Correctness: duplicates rejected and late data dropped** is the panel to have open. The duplicate
counter climbs, because the natural key rejected replayed rows the store already had. If the replayed
readings landed outside their window's grace period, `dropped-records` climbs too, and those readings
are gone from the aggregates while the raw table stays perfectly correct. Two counters, two different
correctness problems.

### When a panel is empty

- **Every panel empty, no error.** Almost always a provisioning problem rather than a data problem.
  Check the panel carries `fieldConfig.defaults.color`, `mappings`, and `thresholds`; without them
  Grafana renders an empty canvas silently.
- **One panel empty.** Check the metric name in Prometheus directly, remembering that OTLP renames
  things (see below). The datasource proxy answers without leaving the terminal:
  ```bash
  curl -s 'http://localhost:3000/api/datasources/proxy/uid/prometheus/api/v1/query?query=pitwall_processor_events_consumed_total' | jq '.data.result | length'
  ```
- **Everything empty and the services are running.** Confirm the service is exporting at all:
  `curl -s localhost:8082/actuator/metrics | jq '.names | map(select(startswith("pitwall")))'`.

## The load story

Measured on the development machine, 6 cars x 25 channels:

| | produced/s | written/s | lag |
|---|---|---|---|
| Backlog draining | 3,662 | 603 | 75,914 |
| Catching up | 3,844 | 2,848 | 75,914 |
| Caught up | 3,888 | 3,882 | 38 |
| Steady | 3,892 | 3,880 | **0** |

Lag falls off a cliff and then sits flat on zero while throughput holds.

The honest counterpart: the consumer's ceiling into the compressed hypertable is around **5,000
writes/second** on this machine. Push the source above that and lag grows without bound. Kafka absorbs
the difference without losing anything, but a backlog is still a backlog.

## OTLP changes some metric names

Worth knowing before you write a query, because the OTLP registry does not name things the way the
Prometheus registry did:

- **Timers export in milliseconds**, not seconds. `pitwall.processor.write.duration` arrives as
  `pitwall_processor_write_duration_milliseconds`.
- **Client-side percentiles arrive as a `quantile` label**, not histogram buckets. The write-latency
  panel queries `pitwall_processor_write_duration_milliseconds{quantile="0.95"}` directly;
  `histogram_quantile(...)` has nothing to work with.
- `DistributionSummary` still produces `_bucket` / `_count` / `_sum`, so
  `pitwall_processor_poll_batch_size_bucket` behaves as expected.
- Every series carries a `service_name` resource attribute from `spring.application.name`, which is
  why the old custom `MeterRegistryCustomizer` that added a `service` tag was deleted rather than
  ported.

## Gotchas found while wiring this up

- The `grafana/otel-lgtm` image has `curl` but **no `wget`**, so a `wget`-based healthcheck reports
  unhealthy forever while the container is perfectly fine.
- Grafana renders **nothing** if a provisioned dashboard's panels omit `fieldConfig.defaults.color`,
  `mappings`, and `thresholds`. No error, no console warning, just an empty canvas.
- otel-lgtm's bundled Prometheus datasource has uid `prometheus`; reference it explicitly from every
  panel and target.
