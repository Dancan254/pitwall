package com.yourjavaguy.pitwall.processor.rollup;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;

public record RollupAccumulator(long count, double sum, double minimum, double maximum) {

    public static RollupAccumulator empty() {
        return new RollupAccumulator(0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    }

    public RollupAccumulator add(TelemetryEvent event) {
        return new RollupAccumulator(
                count + 1,
                sum + event.value(),
                Math.min(minimum, event.value()),
                Math.max(maximum, event.value()));
    }

    public double average() {
        return count == 0 ? 0 : sum / count;
    }
}
