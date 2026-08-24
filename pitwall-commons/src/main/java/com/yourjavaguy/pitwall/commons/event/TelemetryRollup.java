package com.yourjavaguy.pitwall.commons.event;

import java.time.Duration;
import java.time.Instant;

public record TelemetryRollup(
        String carId,
        String sensorId,
        Duration windowLength,
        Instant windowStart,
        Instant windowEnd,
        long count,
        double average,
        double minimum,
        double maximum
) {
}
