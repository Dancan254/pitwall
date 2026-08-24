# pitwall-serving

The pit wall itself. Pushes live rollups and alerts to the browser over SSE, serves historical queries
from the TimescaleDB continuous aggregates, and hosts the dashboard.

## Run

Everything else first (`docker compose up -d`, then the processor), because this service consumes the
topics the processor produces.

```bash
./mvnw -pl pitwall-serving -am spring-boot:run
```

Then open **http://localhost:8083**.

## The dashboard

One static page, no build step, no npm in a Java repo. Four things on it:

- **The garage**: the two Mercedes cars in full, driven by the latest 1-second rollup per channel.
  Radial dials for `speed` and `engine-rpm` printed with their scale, a gear readout, throttle and
  brake bars, and running average and top speed accumulated from the rollup stream. A car the source
  has gone quiet on says `No signal` rather than showing a stale reading.
- **Rest of the grid**: every other car as one line, current speed only. The demo is about the
  Mercedes garage; the other cars are there to show the pipeline is carrying all of them.
- **Alerts**: threshold breaches as they fire, newest first, backfilled on load from
  `/api/v1/live/alerts` so a browser opening late still sees recent history.
- **Telemetry analysis**: pick a car, a resolution, and up to six channels, then query the continuous
  aggregates. The page reports how long the query took, which is the point: it is reading
  pre-computed buckets, not raw rows.

### The page is sized to the screen, not to its content

The dashboard is a wall display: `body` is pinned to `100dvh` with `overflow: hidden`, and the
analysis panel takes whatever height the garage and the controls leave behind. The chart reads its
lane height from that leftover space rather than reserving a fixed strip, so there is no dead
rectangle waiting for someone to press a button, and the page never trails off into empty
background. Below 1040px the document gets its normal scrolling back, because a single column cannot
fit a phone screen and pinning it there would clip content instead of fitting it.

The chart's share of the shell keeps changing after first paint, as the garage cards fill in and the
channel picker collapses. That resize is picked up from the existing 400ms render tick rather than a
`ResizeObserver`: observer callbacks and `requestAnimationFrame` are both throttled in a hidden tab,
which left the canvas stuck at whatever height it happened to get first.

The breach band keeps its row even when there is nothing to report. It is the one piece of reserved
empty space on the page, and it buys the thing a two-car comparison needs most: both drivers' dials
and readouts stay on the same baseline, so the eye can scan straight across.

### A breach names the channel, it does not colour the frame

Every border on the page is the same neutral hairline, in every state. Turning a card's frame red
says only that something on that car is wrong; the engineer still has to hunt for what. So the breach
signal sits on the data instead: a band across the driver's nameplate carrying the channel id and
`observed / threshold`, and the affected readout turns to the alert colour. The dial arc for a
breached channel turns with it.

Breaches expire 30 seconds after the last alert for that car. A breach is an event, not a permanent
property, and a card that stays marked forever after one transient spike stops meaning anything.

### Driver names are a display mapping, nothing more

The pipeline has no concept of a driver, a team, a lap, or a circuit. `TelemetryEvent` carries
`(carId, sensorId, timestamp, value, sequenceNo)` and that is all. The page maps `CAR-01` and
`CAR-02` to the two Mercedes drivers in a lookup table at the top of the script so the demo reads
like a garage instead of a spreadsheet. Nothing downstream knows or cares. Every number on the page
came out of the pipeline; only the names on the cards did not.

### Why the analysis chart uses lanes instead of an overlay

Most of the interesting channels (`speed`, `engine-rpm`, `throttle-position`, `brake-pressure`) are
`LAP_CORRELATED` in the generator, so they all follow the same lap simulation. Scale each to its own
range and overlay them and they land on top of one another exactly: you see one line and assume the
chart is broken. Each channel gets its own lane, which is also how real telemetry analysis clients
show stacked channels.

## API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/live/stream` | SSE feed, events named `rollup` and `alert` |
| `GET` | `/api/v1/live/alerts` | Recent alerts, newest first |
| `GET` | `/api/v1/telemetry/cars` | Car ids present in the aggregates |
| `GET` | `/api/v1/telemetry/sensors` | Sensor ids present in the aggregates |
| `GET` | `/api/v1/telemetry/rollups` | `carId`, `sensorId`, `resolution=ONE_MINUTE\|ONE_HOUR`, `from`, `to` |

```bash
curl -N localhost:8083/api/v1/live/stream
curl "localhost:8083/api/v1/telemetry/rollups?carId=CAR-01&sensorId=speed&resolution=ONE_MINUTE"
```

### The read path never touches the raw table

`resolution` selects which continuous aggregate answers the query. The raw hypertable feeds the
aggregates on write and is never read by this service.

```mermaid
flowchart LR
    C["GET /api/v1/telemetry/rollups<br/>carId, sensorId, from, to"]
    R{resolution}
    M[("<b>telemetry_rollup_1m</b><br/>1-minute buckets")]
    H[("<b>telemetry_rollup_1h</b><br/>built from the 1-minute one")]
    RAW[("telemetry_event<br/>raw hypertable")]

    C --> R
    R -->|ONE_MINUTE| M
    R -->|ONE_HOUR| H
    RAW -.->|"refreshed every minute<br/>by the processor's policy"| M
    M -.->|hierarchical refresh| H
```

## Why SSE and not WebSocket

The feed is one-way: server to browser, no client-to-server messages. SSE is plain HTTP, reconnects
on its own, and needs no protocol upgrade or extra infrastructure. A WebSocket would buy a return
channel this feed has no use for.

It also happens to mirror the real thing: FIA rules have made F1 telemetry one-way since 2003.

## Two consumer groups, two typed deserializers

`telemetry.rollups` and `telemetry.alerts` carry different record types, so a single global
`spring.json.value.default.type` cannot serve both. `KafkaConsumerConfiguration` builds one listener
container factory per type, each with its own `JacksonJsonDeserializer` and type headers off, and the
listeners select between them with `containerFactory`.

The rollup listener returns early when nobody is subscribed; with no dashboard open there is no
reason to fan anything out.

```mermaid
flowchart LR
    RT(["<b>telemetry.rollups</b>"]) --> CR["rollup listener<br/>typed deserializer"]
    AT(["<b>telemetry.alerts</b>"]) --> CA["alert listener<br/>typed deserializer"]

    CR -->|"skipped when<br/>no subscribers"| LF["<b>LiveFeed</b><br/>SSE fan-out"]
    CA --> LF
    CA --> AH["<b>AlertHistory</b><br/>bounded deque,<br/>newest first"]

    LF ==>|"event: rollup<br/>event: alert"| B["browsers on<br/>/api/v1/live/stream"]
    AH -.->|"GET /api/v1/live/alerts<br/>backfill on page load"| B
```

## Metrics

Exported over OTLP to `grafana/otel-lgtm` every 5s, and readable locally on
`/actuator/metrics`. There is no `/actuator/prometheus`, because this platform does not carry a
Prometheus registry, and nothing scrapes it. Names below are the Micrometer names; see
[`observability/README.md`](../observability/README.md) for how OTLP renames them in Grafana.

| Metric | Meaning |
|---|---|
| `pitwall.serving.subscribers` | Browsers attached to the live feed |
| `pitwall.serving.events.delivered` | Server-sent events pushed |
| `pitwall.serving.subscribers.dropped` | Subscribers removed after a failed push |
