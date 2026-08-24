package com.yourjavaguy.pitwall.commons.event;

import java.time.Instant;

public record TelemetryEvent(
        String carId,
        String sensorId,
        Instant timestamp,
        double value,
        long sequenceNo
) {
}
