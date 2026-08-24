package com.yourjavaguy.pitwall.source.sensor;

import com.yourjavaguy.pitwall.source.fault.FaultInjection;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Component
public class SignalGenerator {

    private static final double ANOMALY_OVERSHOOT = 0.25;
    private static final double SPIKE_PROBABILITY = 0.02;

    private final FaultInjection faults;

    public SignalGenerator(FaultInjection faults) {
        this.faults = faults;
    }

    public double valueFor(String carId, SensorDefinition definition, double lapPhase) {
        var span = definition.maximumValue() - definition.minimumValue();
        if (faults.isAnomalous(carId, definition.sensorId())) {
            return definition.maximumValue() + span * ANOMALY_OVERSHOOT;
        }
        var fraction = switch (definition.shape()) {
            case STEADY -> 0.5 + jitter(0.02);
            case LAP_CORRELATED -> 0.5 + 0.45 * Math.sin(lapPhase * 2 * Math.PI) + jitter(0.01);
            case SPIKY -> spikeFraction();
        };
        return definition.minimumValue() + span * clampToUnit(fraction);
    }

    private double spikeFraction() {
        var random = ThreadLocalRandom.current();
        if (random.nextDouble() < SPIKE_PROBABILITY) {
            return 0.7 + random.nextDouble() * 0.3;
        }
        return 0.1 + jitter(0.05);
    }

    private double jitter(double amplitude) {
        return ThreadLocalRandom.current().nextDouble(-amplitude, amplitude);
    }

    private double clampToUnit(double fraction) {
        return Math.min(1.0, Math.max(0.0, fraction));
    }
}
