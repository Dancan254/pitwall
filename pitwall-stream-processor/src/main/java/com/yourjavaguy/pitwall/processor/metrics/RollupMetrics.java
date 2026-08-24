package com.yourjavaguy.pitwall.processor.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class RollupMetrics {

    private final MeterRegistry meterRegistry;

    public RollupMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void recordRollup(Duration windowLength) {
        meterRegistry.counter("pitwall.processor.rollups.emitted", "window", windowLength.toString())
                .increment();
    }

    public void recordAlert(String sensorId) {
        meterRegistry.counter("pitwall.processor.alerts.fired", "sensor", sensorId).increment();
    }
}
