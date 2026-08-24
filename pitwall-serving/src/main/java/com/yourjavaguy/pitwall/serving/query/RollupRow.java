package com.yourjavaguy.pitwall.serving.query;

import java.time.Instant;

public record RollupRow(
        String carId,
        String sensorId,
        Instant bucket,
        long samples,
        double average,
        double minimum,
        double maximum
) {
}
