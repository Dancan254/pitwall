# Observability

All three services export **OTLP** to a single `grafana/otel-lgtm` container — Grafana, Prometheus,
Tempo and Loki in one image. There is no scrape config, no Prometheus registry, and no custom
observability code in the project.

| | |
|---|---|
| Grafana | http://localhost:3000 (anonymous viewer, or `admin` / `pitwall`) |
| OTLP HTTP | `localhost:4328` |
| OTLP gRPC | `localhost:4327` |
| Dashboard | **Pitwall — telemetry pipeline** |

OTLP is published on **4328/4327**, not the usual 4318/4317, to stay out of the way of other local
LGTM containers — the same reason Postgres sits on 5434.

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
90% of traces. The processor runs at `0.05` — it has Kafka listener observation on, so a span per poll
at full sampling would be a lot of noise for very little signal. Source and serving run at `1.0`;
their spans are HTTP control-plane calls and there are few of them.

**Scheduled tasks are excluded from tracing.** The metric samplers run once a second in every service,
and before this was set they produced ~90% of all spans — real HTTP work was buried. The map key needs
bracket quoting in YAML or Spring's relaxed binding mangles the dots.

## Signals

Metrics and traces. Logs are not exported: the OTLP logging appender has to be wired into Logback by
hand, and at telemetry volumes the log stream needs a plan of its own.

## The panels

| Panel | What it proves |
|---|---|
| **Consumer lag — the heartbeat** | Whether the consumer group is keeping pace. The single most important number. |
| **Throughput — produced vs consumed vs written** | Where the pipeline narrows |
| **Source load dial — target vs actual** | Whether the generator is hitting the rate you asked for |
| **Write latency per batch** | p50/p95/p99 into the store |
| **Correctness — duplicates rejected and late data dropped** | Idempotency and watermarks, as live counters |
| **Dropout buffer and sink failures** | What the fault injection is doing |
| **Rollups, alerts, and dashboard subscribers** | The serving side |

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

- The `grafana/otel-lgtm` image has `curl` but **no `wget`** — a `wget`-based healthcheck reports
  unhealthy forever while the container is perfectly fine.
- Grafana renders **nothing** if a provisioned dashboard's panels omit `fieldConfig.defaults.color`,
  `mappings`, and `thresholds`. No error, no console warning, just an empty canvas.
- otel-lgtm's bundled Prometheus datasource has uid `prometheus`; reference it explicitly from every
  panel and target.
