package com.yourjavaguy.pitwall.schema;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryEventWireFormatTest {

    private static final TelemetryEvent EVENT = new TelemetryEvent(
            "CAR-07", "brake-temperature-front-left",
            Instant.parse("2026-08-23T12:34:56.123456789Z"), 947.9312345, 4_294_967_296L);

    @Test
    void should_preserve_every_field_when_an_event_makes_a_round_trip() {
        var restored = TelemetryEventWireFormat.toEvent(TelemetryEventWireFormat.toMessage(EVENT));

        assertThat(restored).isEqualTo(EVENT);
    }

    @Test
    void should_preserve_nanosecond_precision_when_an_event_makes_a_round_trip() {
        var restored = TelemetryEventWireFormat.toEvent(TelemetryEventWireFormat.toMessage(EVENT));

        assertThat(restored.timestamp().getNano()).isEqualTo(123_456_789);
    }

    @Test
    void should_survive_the_kafka_serializer_and_deserializer_pair() {
        var serializer = new TelemetryEventProtobufSerializer();
        var deserializer = new TelemetryEventProtobufDeserializer();

        var restored = deserializer.deserialize("telemetry.events",
                serializer.serialize("telemetry.events", EVENT));

        assertThat(restored).isEqualTo(EVENT);
    }

    @Test
    void should_return_null_when_the_serializer_is_given_no_event() {
        assertThat(new TelemetryEventProtobufSerializer().serialize("telemetry.events", null)).isNull();
        assertThat(new TelemetryEventProtobufDeserializer().deserialize("telemetry.events", null)).isNull();
    }
}
