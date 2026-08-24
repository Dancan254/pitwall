# pitwall-source

The telemetry generator. Simulates a grid of F1 cars, each emitting a catalogue of sensor channels at
their own individual sample rates, and exposes a runtime control plane for load, bursts, dropout,
replay, and anomalies.

Sensor names, ranges, and sample rates are derived from
[`docs/how-f1-transmits-telemetry.md`](../docs/how-f1-transmits-telemetry.md).

## Run

Kafka and Postgres first:

```bash
docker compose up -d
```

Then:

```bash
./mvnw -pl pitwall-source -am spring-boot:run                                  # dev, 870 events/sec
./mvnw -pl pitwall-source -am spring-boot:run -Dspring-boot.run.profiles=race  # 232,600 events/sec
```

Listens on `8081`. With no infrastructure running, start it on the logging sink instead:
`-Dspring-boot.run.arguments=--pitwall.source.sink=logging`.

## Sinks

Three implementations of `TelemetrySink`, chosen by `pitwall.source.sink` and switchable at runtime:

| Sink | What it does | Why it exists |
|---|---|---|
| `kafka` | Produces to `telemetry.events`, keyed by `carId` | The real path |
| `database` | One synchronous `INSERT` per event, straight to Postgres | The naive path, kept so it can be broken on purpose |
| `logging` | Samples one event in `log-sample-interval` to the log | Runs with no infrastructure at all |

`kafka` is the default. The `database` sink is not a mistake left in the repo; it is the control
group. Point the generator at it, turn the dial up, and watch the connection pool exhaust and
throughput collapse. Every architectural decision downstream traces back to that failure.

```bash
curl -X PUT localhost:8081/api/v1/source/sink -H 'Content-Type: application/json' -d '{"name":"database"}'
curl -X PUT localhost:8081/api/v1/source/load -H 'Content-Type: application/json' \
     -d '{"cars":20,"sensorsPerCar":100,"rateScale":1.0}'
watch -n1 'curl -s localhost:8081/api/v1/source/status'
```

### Measured, same load, same machine

| Sink | Target events/sec | Sustained events/sec | Failed |
|---|---|---|---|
| `kafka` | 232,600 | ~211,000 | 0 |
| `database` | 232,600 | 0 – 1,441, erratic | 0 |

A **221× collapse**, and the most interesting part is that nothing errors. Hikari does not reject the
work, it makes the emitter threads queue for a connection, so the generator is dragged down to the
speed of the database instead of failing loudly. A fast producer wired straight to a slower consumer
does not break; it silently becomes as slow as the slowest part. That is what a log in the middle
buys you, and switching the sink back to `kafka` returns the generator to ~213,000 events/sec
immediately.

## Kafka

Producer is tuned for throughput, not for the smallest possible latency: `acks=all`, lz4 compression,
64 KB batches, and `linger.ms=20` so records accumulate before a send. Records are keyed by `carId`,
which is what buys per-car ordering and parallelism at once, and what makes hot partitions possible
if one car dominates.

The wire format is JSON for now. That is deliberate: Protobuf lands next, and switching with the
byte-size and throughput numbers already measured makes the case far better than asserting it.

## Profiles

| Profile | Cars | Channels/car | Rate scale | Target events/sec |
|---|---|---|---|---|
| default / `dev` | 5 | 20 | 0.1 | 870 |
| `race` | 20 | 100 | 1.0 | 232,600 |
| `breakit` | 20 | 300 | 4.0 | 2,702,400 |

`rate-scale` multiplies every channel's own sample rate, so the stream keeps its non-uniform shape as
the dial turns up.

## Control plane

| Method | Path | Body |
|---|---|---|
| `GET` | `/api/v1/source/status` | |
| `POST` | `/api/v1/source/start` | |
| `POST` | `/api/v1/source/stop` | |
| `PUT` | `/api/v1/source/load` | `{"cars":20,"sensorsPerCar":100,"rateScale":1.0}` |
| `PUT` | `/api/v1/source/sink` | `{"name":"kafka"}` |
| `PUT` | `/api/v1/source/faults/dropout` | `{"active":true}` |
| `POST` | `/api/v1/source/faults/replay` | |
| `POST` | `/api/v1/source/faults/burst` | `{"multiplier":3.0,"durationSeconds":30}` |
| `PUT` | `/api/v1/source/faults/anomaly` | `{"carId":"CAR-01","sensorId":"brake-temperature-front-left"}` |
| `DELETE` | `/api/v1/source/faults/anomaly` | |

Applying a load change restarts the emitters. Triggering a replay ends the dropout and drains the
buffer on a background thread.

## The demo sequence

```bash
curl -X PUT localhost:8081/api/v1/source/faults/dropout -H 'Content-Type: application/json' -d '{"active":true}'
curl localhost:8081/api/v1/source/status
curl -X POST localhost:8081/api/v1/source/faults/replay
```

The dropout buffers events instead of publishing them, and drops them once the buffer is full. The
replay drains the buffer **and re-sends the last `overlap-size` events that were already delivered**,
so the downstream consumer receives genuine duplicates and out-of-order data. The consumer keys every
write on `(car_id, sensor_id, event_time)`, so those duplicates land as no-ops rather than
double-counts; see `pitwall-stream-processor/README.md`.

## Metrics

Exported over OTLP to `grafana/otel-lgtm` every 5s, and readable locally on
`/actuator/metrics`. There is no `/actuator/prometheus`, because this platform does not carry a
Prometheus registry, and nothing scrapes it. Names below are the Micrometer names; see
[`observability/README.md`](../observability/README.md) for how OTLP renames them in Grafana.

| Metric | Meaning |
|---|---|
| `pitwall.source.events.published` | Events handed to the sink |
| `pitwall.source.events.buffered` | Events held back during a dropout |
| `pitwall.source.events.dropped` | Events lost to a full dropout buffer |
| `pitwall.source.events.replayed` | Events re-sent after a dropout, duplicates included |
| `pitwall.source.events.failed` | Events the active sink rejected |
| `pitwall.source.rate.target` | What the load dial asks for |
| `pitwall.source.rate.actual` | What is actually being published, sampled every second |
| `pitwall.source.buffer.depth` | Events currently buffered |

Target and actual are separate on purpose. The generator never catches up on missed ticks: under
pressure it just runs slower, and the gap between the two is the honest signal.
