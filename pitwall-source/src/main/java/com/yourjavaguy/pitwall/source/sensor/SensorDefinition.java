package com.yourjavaguy.pitwall.source.sensor;

public record SensorDefinition(
        String sensorId,
        String unit,
        double minimumValue,
        double maximumValue,
        int rateHz,
        SignalShape shape
) {
}
