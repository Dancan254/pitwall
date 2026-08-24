package com.yourjavaguy.pitwall.commons.event;

import java.time.Instant;

public record TelemetryAlert(
        String carId,
        String sensorId,
        Instant windowStart,
        Instant windowEnd,
        double observedMaximum,
        double threshold
) {
}
