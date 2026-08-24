package com.yourjavaguy.pitwall.source.sensor;

import com.yourjavaguy.pitwall.schema.WireFormat;
import com.yourjavaguy.pitwall.source.config.SourceProperties;
import com.yourjavaguy.pitwall.source.fault.FaultInjection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

class SignalGeneratorTest {

    private static final SourceProperties PROPERTIES = new SourceProperties(1, 1, 1.0, 90, false, 1_000_000, "recording", WireFormat.PROTOBUF,
            new SourceProperties.Dropout(1000, 100, 100), new SourceProperties.Kafka("telemetry.events", 1, (short) 1));

    private final FaultInjection faults = new FaultInjection(PROPERTIES);
    private final SignalGenerator signalGenerator = new SignalGenerator(faults);

    @ParameterizedTest
    @EnumSource(SignalShape.class)
    void should_stay_within_the_declared_range_when_no_anomaly_is_active(SignalShape shape) {
        var definition = new SensorDefinition("brake-temperature", "C", 200, 1000, 20, shape);

        for (var sample = 0; sample < 500; sample++) {
            var value = signalGenerator.valueFor("CAR-01", definition, sample / 500.0);
            assertThat(value).isBetween(definition.minimumValue(), definition.maximumValue());
        }
    }

    @Test
    void should_overshoot_the_maximum_when_the_sensor_is_flagged_anomalous() {
        var definition = new SensorDefinition("brake-temperature", "C", 200, 1000, 20, SignalShape.STEADY);
        faults.startAnomaly("CAR-01", "brake-temperature");

        var value = signalGenerator.valueFor("CAR-01", definition, 0.5);

        assertThat(value).isGreaterThan(definition.maximumValue());
    }

    @Test
    void should_leave_other_cars_untouched_when_one_car_is_flagged_anomalous() {
        var definition = new SensorDefinition("brake-temperature", "C", 200, 1000, 20, SignalShape.STEADY);
        faults.startAnomaly("CAR-01", "brake-temperature");

        var value = signalGenerator.valueFor("CAR-02", definition, 0.5);

        assertThat(value).isLessThanOrEqualTo(definition.maximumValue());
    }
}
