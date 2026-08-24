package com.yourjavaguy.pitwall.schema;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import com.yourjavaguy.pitwall.schema.proto.TelemetryEventMessage;

import java.time.Instant;

public final class TelemetryEventWireFormat {

    private TelemetryEventWireFormat() {
    }

    public static TelemetryEventMessage toMessage(TelemetryEvent event) {
        return TelemetryEventMessage.newBuilder()
                .setCarId(event.carId())
                .setSensorId(event.sensorId())
                .setTimestampMs(event.timestamp().getEpochSecond() * 1_000
                        + event.timestamp().getNano() / 1_000_000)
                .setTimestampNs(event.timestamp().getNano() % 1_000_000)
                .setValue(event.value())
                .setSequenceNo(event.sequenceNo())
                .build();
    }

    public static TelemetryEvent toEvent(TelemetryEventMessage message) {
        var timestamp = Instant.ofEpochMilli(message.getTimestampMs())
                .plusNanos(message.getTimestampNs());
        return new TelemetryEvent(
                message.getCarId(),
                message.getSensorId(),
                timestamp,
                message.getValue(),
                message.getSequenceNo());
    }
}
