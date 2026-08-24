package com.yourjavaguy.pitwall.schema;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class WirePayloadSizeTest {

    private static final int SAMPLE_SIZE = 10_000;
    private static final String TOPIC = "telemetry.events";

    private static List<TelemetryEvent> sampleEvents() {
        var start = Instant.parse("2026-08-23T12:00:00Z");
        return IntStream.range(0, SAMPLE_SIZE)
                .mapToObj(index -> new TelemetryEvent(
                        "CAR-%02d".formatted(index % 20 + 1),
                        "brake-temperature-front-left",
                        start.plusMillis(index * 2L),
                        200 + (index % 800) + 0.123456,
                        index))
                .toList();
    }

    private static long totalBytes(List<TelemetryEvent> events, java.util.function.Function<TelemetryEvent, byte[]> encode) {
        return events.stream().mapToLong(event -> encode.apply(event).length).sum();
    }

    @Test
    void should_produce_a_smaller_payload_than_json_for_the_same_events() {
        var events = sampleEvents();
        var protobuf = new TelemetryEventProtobufSerializer();
        var json = new JacksonJsonSerializer<TelemetryEvent>().noTypeInfo();

        var protobufBytes = totalBytes(events, event -> protobuf.serialize(TOPIC, event));
        var jsonBytes = totalBytes(events, event -> json.serialize(TOPIC, event));

        System.out.printf("json=%d bytes (%.1f/event)  protobuf=%d bytes (%.1f/event)  ratio=%.2fx%n",
                jsonBytes, (double) jsonBytes / SAMPLE_SIZE,
                protobufBytes, (double) protobufBytes / SAMPLE_SIZE,
                (double) jsonBytes / protobufBytes);

        assertThat(protobufBytes).isLessThan(jsonBytes);
    }

    @Test
    void should_keep_a_single_event_under_sixty_bytes_on_the_wire() {
        var protobuf = new TelemetryEventProtobufSerializer();

        var encoded = protobuf.serialize(TOPIC, sampleEvents().getFirst());

        assertThat(encoded.length).isLessThan(60);
    }
}
