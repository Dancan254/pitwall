# pitwall-schema

The Protobuf wire format for the telemetry firehose, and the Kafka serdes that use it.

`TelemetryEvent` stays a plain Java record in `pitwall-commons`, which is the shared *contract*, and
commons stays free of infrastructure. This module owns the *encoding*: a `.proto` schema, the
generated message class, and the mapping between the two. Services depend on this module only because
they touch Kafka.

## The schema

```proto
message TelemetryEventMessage {
  string car_id       = 1;
  string sensor_id    = 2;
  int64  timestamp_ms = 3;
  int32  timestamp_ns = 4;
  double value        = 5;
  int64  sequence_no  = 6;
}
```

The timestamp is split across two fields on purpose. Protobuf has no instant type, and a single
`int64` of nanoseconds would overflow readability while a millisecond field alone would silently
truncate. `timestamp_ms` plus a sub-millisecond `timestamp_ns` remainder round-trips
`java.time.Instant` at full nanosecond precision, which `TelemetryEventWireFormatTest` pins down.

Codegen runs through `protobuf-maven-plugin`, which downloads `protoc` from Maven Central, so no system
`protoc` needed.

## Why binary, measured

Same 10,000 events, serialized both ways:

| Format | Total | Per event |
|---|---|---|
| JSON | 1,358,810 bytes | **135.9** |
| Protobuf | 569,870 bytes | **57.0** |

**2.38x smaller before compression.** Now the same comparison after ~200,000 events actually reached
the broker, where the producer applies lz4:

| Format | On the broker | Per event |
|---|---|---|
| JSON | 11,104,251 bytes | **54.0** |
| Protobuf | 7,288,848 bytes | **36.1** |

**1.50x smaller on disk.** That gap is narrower than 2.38x, and the reason is worth knowing: JSON
repeats its field names on every record, which is exactly the redundancy lz4 is good at: JSON
compresses 2.5x here, Protobuf only 1.6x, because Protobuf had already removed the redundancy.

So the honest claim is not "Protobuf is 2.4x smaller on the wire". It is:

- **2.38x fewer bytes produced** by the serializer, so less allocation and less to compress.
- **1.50x fewer bytes stored and transferred** after compression.
- The CPU spent compressing 2.4x more input is real cost that the table does not show.

Against the real constraint from [the research](../docs/how-f1-transmits-telemetry.md), about
**2.7 Mbit/s per car** on the live radio link, that is the difference between fitting a channel set
on the wire and not.

## Switching formats

Both services read a `wire-format` property, so the comparison is reproducible:

```bash
./mvnw -pl pitwall-source -am spring-boot:run \
  -Dspring-boot.run.arguments=--pitwall.source.wire-format=json
```

`pitwall.source.wire-format` and `pitwall.processor.wire-format` must match, and both default to
`protobuf`. `RollupTopologyTest` runs the whole rollup path against both formats to prove the
topology cannot tell the difference.

## Changing format requires a Streams reset

Kafka Streams keeps internal repartition and changelog topics, and those hold records in whatever
format was in use when they were written. Flipping `wire-format` on a running system makes the new
deserializer fail on old internal records, and `LogAndFailExceptionHandler` shuts the whole Streams
client down, which is exactly what happened the first time here.

```bash
docker exec pitwall-kafka /opt/kafka/bin/kafka-streams-application-reset.sh \
  --bootstrap-server localhost:9092 --application-id pitwall-rollups
rm -rf ${TMPDIR:-/tmp}/pitwall-kafka-streams
```

This is a small preview of the problem a Schema Registry exists to solve.

## What is deliberately still JSON

`telemetry.rollups` and `telemetry.alerts`. They carry roughly one record per window per channel
against 500 raw samples per second, so they are a rounding error in volume, and they are consumed by
the browser-facing service. Binary encoding buys nothing there and costs clarity.

## Not done yet: Schema Registry

Protobuf gives compact bytes and a schema you can read. It does **not** by itself stop someone
deploying a producer whose field 5 changed meaning. A Schema Registry adds enforced compatibility
checks and puts a schema id on the wire instead of relying on both sides sharing this jar. That is
the remaining piece of the platform's stated wire format.
