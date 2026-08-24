package com.yourjavaguy.pitwall.source.sensor;

import com.yourjavaguy.pitwall.schema.WireFormat;
import com.yourjavaguy.pitwall.source.config.SourceProperties;
import com.yourjavaguy.pitwall.source.fault.FaultInjection;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SensorCatalogTest {

    private static final SourceProperties PROPERTIES = new SourceProperties(1, 1, 1.0, 90, false, 1_000_000, "recording", WireFormat.PROTOBUF,
            new SourceProperties.Dropout(1000, 100, 100), new SourceProperties.Kafka("telemetry.events", 1, (short) 1));

    private final SensorCatalog sensorCatalog =
            new SensorCatalog(new SignalGenerator(new FaultInjection(PROPERTIES)));

    @Test
    void should_return_named_channels_when_the_count_fits_the_base_catalogue() {
        var definitions = sensorCatalog.definitions(5);

        assertThat(definitions).extracting(SensorDefinition::sensorId)
                .containsExactly("speed", "engine-rpm", "throttle-position", "brake-pressure", "steering-angle");
    }

    @Test
    void should_synthesize_extra_channels_when_the_count_exceeds_the_base_catalogue() {
        var count = sensorCatalog.baseChannelCount() + 3;

        var definitions = sensorCatalog.definitions(count);

        assertThat(definitions).hasSize(count);
        assertThat(definitions.subList(sensorCatalog.baseChannelCount(), count))
                .extracting(SensorDefinition::sensorId)
                .containsExactly("aux-channel-000", "aux-channel-001", "aux-channel-002");
    }

    @Test
    void should_vary_the_sample_rate_across_channels_when_building_a_full_car() {
        var rates = sensorCatalog.definitions(40).stream().map(SensorDefinition::rateHz).distinct().toList();

        assertThat(rates).hasSizeGreaterThan(1);
    }
}
