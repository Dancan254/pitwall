package com.yourjavaguy.pitwall.source.sensor;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class SensorCatalog {

    private static final List<SensorDefinition> BASE_CHANNELS = List.of(
            new SensorDefinition("speed", "km/h", 0, 360, 100, SignalShape.LAP_CORRELATED),
            new SensorDefinition("engine-rpm", "rpm", 4000, 15000, 200, SignalShape.LAP_CORRELATED),
            new SensorDefinition("throttle-position", "%", 0, 100, 100, SignalShape.LAP_CORRELATED),
            new SensorDefinition("brake-pressure", "bar", 0, 140, 200, SignalShape.SPIKY),
            new SensorDefinition("steering-angle", "deg", -360, 360, 100, SignalShape.LAP_CORRELATED),
            new SensorDefinition("gear", "ratio", 1, 8, 50, SignalShape.LAP_CORRELATED),
            new SensorDefinition("drs-state", "state", 0, 1, 10, SignalShape.SPIKY),
            new SensorDefinition("lateral-acceleration", "g", -6, 6, 200, SignalShape.LAP_CORRELATED),
            new SensorDefinition("longitudinal-acceleration", "g", -6, 6, 200, SignalShape.LAP_CORRELATED),
            new SensorDefinition("vertical-acceleration", "g", -8, 8, 500, SignalShape.SPIKY),
            new SensorDefinition("tyre-temperature-front-left", "C", 60, 130, 5, SignalShape.STEADY),
            new SensorDefinition("tyre-temperature-front-right", "C", 60, 130, 5, SignalShape.STEADY),
            new SensorDefinition("tyre-temperature-rear-left", "C", 60, 130, 5, SignalShape.STEADY),
            new SensorDefinition("tyre-temperature-rear-right", "C", 60, 130, 5, SignalShape.STEADY),
            new SensorDefinition("tyre-pressure-front-left", "psi", 18, 26, 5, SignalShape.STEADY),
            new SensorDefinition("tyre-pressure-front-right", "psi", 18, 26, 5, SignalShape.STEADY),
            new SensorDefinition("tyre-pressure-rear-left", "psi", 18, 26, 5, SignalShape.STEADY),
            new SensorDefinition("tyre-pressure-rear-right", "psi", 18, 26, 5, SignalShape.STEADY),
            new SensorDefinition("brake-temperature-front-left", "C", 200, 1000, 20, SignalShape.LAP_CORRELATED),
            new SensorDefinition("brake-temperature-front-right", "C", 200, 1000, 20, SignalShape.LAP_CORRELATED),
            new SensorDefinition("brake-temperature-rear-left", "C", 200, 1000, 20, SignalShape.LAP_CORRELATED),
            new SensorDefinition("brake-temperature-rear-right", "C", 200, 1000, 20, SignalShape.LAP_CORRELATED),
            new SensorDefinition("damper-travel-front-left", "mm", -30, 30, 500, SignalShape.SPIKY),
            new SensorDefinition("damper-travel-front-right", "mm", -30, 30, 500, SignalShape.SPIKY),
            new SensorDefinition("damper-travel-rear-left", "mm", -30, 30, 500, SignalShape.SPIKY),
            new SensorDefinition("damper-travel-rear-right", "mm", -30, 30, 500, SignalShape.SPIKY),
            new SensorDefinition("oil-temperature", "C", 80, 140, 2, SignalShape.STEADY),
            new SensorDefinition("oil-pressure", "bar", 2, 8, 10, SignalShape.STEADY),
            new SensorDefinition("water-temperature", "C", 70, 120, 2, SignalShape.STEADY),
            new SensorDefinition("fuel-flow", "kg/h", 0, 100, 50, SignalShape.LAP_CORRELATED),
            new SensorDefinition("fuel-remaining", "kg", 0, 110, 1, SignalShape.STEADY),
            new SensorDefinition("energy-store-charge", "%", 0, 100, 20, SignalShape.LAP_CORRELATED),
            new SensorDefinition("energy-store-temperature", "C", 30, 70, 5, SignalShape.STEADY),
            new SensorDefinition("mgu-k-power", "kW", -120, 120, 100, SignalShape.LAP_CORRELATED),
            new SensorDefinition("mgu-h-rpm", "rpm", 0, 125000, 100, SignalShape.LAP_CORRELATED),
            new SensorDefinition("turbo-pressure", "bar", 1, 5, 100, SignalShape.LAP_CORRELATED),
            new SensorDefinition("exhaust-temperature", "C", 400, 1000, 20, SignalShape.LAP_CORRELATED),
            new SensorDefinition("gearbox-oil-temperature", "C", 80, 150, 2, SignalShape.STEADY),
            new SensorDefinition("chassis-vibration", "g", 0, 20, 1000, SignalShape.SPIKY),
            new SensorDefinition("ride-height-front", "mm", 10, 60, 200, SignalShape.SPIKY)
    );

    private static final SignalShape[] SYNTHETIC_SHAPES = SignalShape.values();
    private static final int[] SYNTHETIC_RATES = {1, 5, 10, 20, 50, 100, 200, 500};

    private final SignalGenerator signalGenerator;

    public SensorCatalog(SignalGenerator signalGenerator) {
        this.signalGenerator = signalGenerator;
    }

    public List<SensorDefinition> definitions(int sensorCount) {
        var definitions = new ArrayList<SensorDefinition>(sensorCount);
        for (var index = 0; index < sensorCount; index++) {
            definitions.add(index < BASE_CHANNELS.size()
                    ? BASE_CHANNELS.get(index)
                    : syntheticChannel(index));
        }
        return List.copyOf(definitions);
    }

    public List<SensorState> statesFor(String carId, int sensorCount) {
        return definitions(sensorCount).stream()
                .map(definition -> new SensorState(carId, definition, signalGenerator))
                .toList();
    }

    public int baseChannelCount() {
        return BASE_CHANNELS.size();
    }

    private SensorDefinition syntheticChannel(int index) {
        var offset = index - BASE_CHANNELS.size();
        return new SensorDefinition(
                "aux-channel-%03d".formatted(offset),
                "unit",
                0,
                100,
                SYNTHETIC_RATES[offset % SYNTHETIC_RATES.length],
                SYNTHETIC_SHAPES[offset % SYNTHETIC_SHAPES.length]);
    }
}
