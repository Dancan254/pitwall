package com.yourjavaguy.pitwall.source.sensor;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;

import java.time.Instant;

public final class SensorState {

    private final String carId;
    private final SensorDefinition definition;
    private final SignalGenerator signalGenerator;
    private final long basePeriodNanos;

    private long nextDueNanos;
    private long sequenceNumber;

    public SensorState(String carId, SensorDefinition definition, SignalGenerator signalGenerator) {
        this.carId = carId;
        this.definition = definition;
        this.signalGenerator = signalGenerator;
        this.basePeriodNanos = 1_000_000_000L / definition.rateHz();
        this.nextDueNanos = System.nanoTime();
    }

    public SensorDefinition definition() {
        return definition;
    }

    public long nextDueNanos() {
        return nextDueNanos;
    }

    public boolean isDue(long nowNanos) {
        return nowNanos >= nextDueNanos;
    }

    public void scheduleNext(long nowNanos, double rateMultiplier) {
        var period = (long) (basePeriodNanos / rateMultiplier);
        nextDueNanos = nowNanos + Math.max(period, 1_000L);
    }

    public TelemetryEvent nextEvent(double lapPhase) {
        return new TelemetryEvent(
                carId,
                definition.sensorId(),
                Instant.now(),
                signalGenerator.valueFor(carId, definition, lapPhase),
                sequenceNumber++);
    }
}
