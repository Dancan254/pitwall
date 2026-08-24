package com.yourjavaguy.pitwall.processor;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import com.yourjavaguy.pitwall.processor.config.ProcessorProperties;
import com.yourjavaguy.pitwall.processor.metrics.RollupMetrics;
import com.yourjavaguy.pitwall.processor.rollup.RollupTopology;
import com.yourjavaguy.pitwall.schema.TelemetryEventProtobufSerde;
import com.yourjavaguy.pitwall.schema.WireFormat;
import com.yourjavaguy.pitwall.commons.event.TelemetryAlert;
import com.yourjavaguy.pitwall.commons.event.TelemetryRollup;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class RollupTopologyTest {

    private static final String EVENTS_TOPIC = "telemetry.events";
    private static final String ROLLUPS_TOPIC = "telemetry.rollups";
    private static final String ALERTS_TOPIC = "telemetry.alerts";
    private static final Instant WINDOW_START = Instant.parse("2026-08-23T12:00:00Z");
    private static final Duration GRACE = Duration.ofSeconds(10);

    private TopologyTestDriver driver;
    private TestInputTopic<String, TelemetryEvent> events;
    private TestOutputTopic<String, TelemetryRollup> rollups;
    private TestOutputTopic<String, TelemetryAlert> alerts;

    @AfterEach
    void closeDriver() {
        if (driver != null) {
            driver.close();
        }
    }

    private void startTopology(Duration grace, List<ProcessorProperties.AlertRule> alertRules) {
        startTopology(grace, alertRules, WireFormat.PROTOBUF);
    }

    private static Serde<TelemetryEvent> serdeFor(WireFormat wireFormat) {
        return wireFormat == WireFormat.PROTOBUF
                ? new TelemetryEventProtobufSerde()
                : new JacksonJsonSerde<>(TelemetryEvent.class).noTypeInfo();
    }

    private void startTopology(Duration grace,
                               List<ProcessorProperties.AlertRule> alertRules,
                               WireFormat wireFormat) {
        var properties = new ProcessorProperties(EVENTS_TOPIC, 500, wireFormat,
                new ProcessorProperties.Rollup(ROLLUPS_TOPIC, ALERTS_TOPIC,
                        List.of(Duration.ofSeconds(1)), grace),
                alertRules);

        var eventSerde = serdeFor(wireFormat);
        var builder = new StreamsBuilder();
        new RollupTopology(properties, new RollupMetrics(new SimpleMeterRegistry()), eventSerde)
                .telemetryStream(builder);

        var config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "rollup-topology-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        driver = new TopologyTestDriver(builder.build(), config);
        events = driver.createInputTopic(EVENTS_TOPIC,
                Serdes.String().serializer(), eventSerde.serializer());
        rollups = driver.createOutputTopic(ROLLUPS_TOPIC,
                Serdes.String().deserializer(),
                new JacksonJsonSerde<>(TelemetryRollup.class).noTypeInfo().deserializer());
        alerts = driver.createOutputTopic(ALERTS_TOPIC,
                Serdes.String().deserializer(),
                new JacksonJsonSerde<>(TelemetryAlert.class).noTypeInfo().deserializer());
    }

    private void send(Instant eventTime, double value) {
        events.pipeInput("CAR-01", new TelemetryEvent("CAR-01", "speed", eventTime, value, 0), eventTime);
    }

    private void advanceStreamTimePast(Instant point) {
        events.pipeInput("CAR-99", new TelemetryEvent("CAR-99", "speed", point, 0, 0), point);
    }

    @Test
    void should_emit_one_rollup_per_window_when_the_window_closes() {
        startTopology(GRACE, List.of());

        send(WINDOW_START.plusMillis(100), 200);
        send(WINDOW_START.plusMillis(500), 220);
        advanceStreamTimePast(WINDOW_START.plusSeconds(1).plus(GRACE).plusSeconds(1));

        var emitted = rollups.readValuesToList().stream()
                .filter(rollup -> rollup.carId().equals("CAR-01"))
                .toList();

        assertThat(emitted).hasSize(1);
        assertThat(emitted.getFirst().windowStart()).isEqualTo(WINDOW_START);
        assertThat(emitted.getFirst().windowEnd()).isEqualTo(WINDOW_START.plusSeconds(1));
    }

    @Test
    void should_compute_count_average_minimum_and_maximum_over_the_window() {
        startTopology(GRACE, List.of());

        send(WINDOW_START.plusMillis(100), 200);
        send(WINDOW_START.plusMillis(300), 300);
        send(WINDOW_START.plusMillis(700), 100);
        advanceStreamTimePast(WINDOW_START.plusSeconds(1).plus(GRACE).plusSeconds(1));

        var rollup = rollups.readValuesToList().stream()
                .filter(value -> value.carId().equals("CAR-01"))
                .findFirst()
                .orElseThrow();

        assertThat(rollup.count()).isEqualTo(3);
        assertThat(rollup.average()).isEqualTo(200.0);
        assertThat(rollup.minimum()).isEqualTo(100.0);
        assertThat(rollup.maximum()).isEqualTo(300.0);
    }

    @Test
    void should_fold_a_late_event_into_its_window_when_it_arrives_within_the_grace_period() {
        startTopology(GRACE, List.of());

        send(WINDOW_START.plusMillis(100), 200);
        advanceStreamTimePast(WINDOW_START.plusSeconds(5));
        send(WINDOW_START.plusMillis(900), 400);
        advanceStreamTimePast(WINDOW_START.plusSeconds(1).plus(GRACE).plusSeconds(1));

        var rollup = rollups.readValuesToList().stream()
                .filter(value -> value.carId().equals("CAR-01"))
                .findFirst()
                .orElseThrow();

        assertThat(rollup.count()).isEqualTo(2);
        assertThat(rollup.maximum()).isEqualTo(400.0);
    }

    @Test
    void should_drop_a_late_event_when_it_arrives_after_the_grace_period() {
        startTopology(GRACE, List.of());

        send(WINDOW_START.plusMillis(100), 200);
        advanceStreamTimePast(WINDOW_START.plusSeconds(1).plus(GRACE).plusSeconds(1));
        send(WINDOW_START.plusMillis(900), 400);
        advanceStreamTimePast(WINDOW_START.plusSeconds(60));

        var forCarOne = rollups.readValuesToList().stream()
                .filter(value -> value.carId().equals("CAR-01"))
                .toList();

        assertThat(forCarOne).hasSize(1);
        assertThat(forCarOne.getFirst().count()).isEqualTo(1);
        assertThat(forCarOne.getFirst().maximum()).isEqualTo(200.0);
    }

    @Test
    void should_keep_the_same_late_event_when_the_grace_period_is_long_enough() {
        startTopology(Duration.ofSeconds(60), List.of());

        send(WINDOW_START.plusMillis(100), 200);
        advanceStreamTimePast(WINDOW_START.plusSeconds(11));
        send(WINDOW_START.plusMillis(900), 400);
        advanceStreamTimePast(WINDOW_START.plusSeconds(1).plusSeconds(61));

        var rollup = rollups.readValuesToList().stream()
                .filter(value -> value.carId().equals("CAR-01"))
                .findFirst()
                .orElseThrow();

        assertThat(rollup.count()).isEqualTo(2);
        assertThat(rollup.maximum()).isEqualTo(400.0);
    }

    @ParameterizedTest
    @EnumSource(WireFormat.class)
    void should_compute_the_same_rollup_whichever_wire_format_carries_the_events(WireFormat wireFormat) {
        startTopology(GRACE, List.of(), wireFormat);

        send(WINDOW_START.plusMillis(100), 200);
        send(WINDOW_START.plusMillis(300), 300);
        send(WINDOW_START.plusMillis(700), 100);
        advanceStreamTimePast(WINDOW_START.plusSeconds(1).plus(GRACE).plusSeconds(1));

        var rollup = rollups.readValuesToList().stream()
                .filter(value -> value.carId().equals("CAR-01"))
                .findFirst()
                .orElseThrow();

        assertThat(rollup.count()).isEqualTo(3);
        assertThat(rollup.average()).isEqualTo(200.0);
        assertThat(rollup.minimum()).isEqualTo(100.0);
        assertThat(rollup.maximum()).isEqualTo(300.0);
    }

    @Test
    void should_fire_an_alert_when_the_windowed_maximum_exceeds_the_threshold() {
        startTopology(GRACE, List.of(new ProcessorProperties.AlertRule("speed", 350)));

        send(WINDOW_START.plusMillis(100), 200);
        send(WINDOW_START.plusMillis(400), 380);
        advanceStreamTimePast(WINDOW_START.plusSeconds(1).plus(GRACE).plusSeconds(1));

        var fired = alerts.readValuesToList();

        assertThat(fired).hasSize(1);
        assertThat(fired.getFirst().carId()).isEqualTo("CAR-01");
        assertThat(fired.getFirst().observedMaximum()).isEqualTo(380.0);
        assertThat(fired.getFirst().threshold()).isEqualTo(350.0);
    }

    @Test
    void should_not_fire_an_alert_when_the_windowed_maximum_stays_below_the_threshold() {
        startTopology(GRACE, List.of(new ProcessorProperties.AlertRule("speed", 350)));

        send(WINDOW_START.plusMillis(100), 200);
        send(WINDOW_START.plusMillis(400), 340);
        advanceStreamTimePast(WINDOW_START.plusSeconds(1).plus(GRACE).plusSeconds(1));

        assertThat(alerts.readValuesToList()).isEmpty();
    }

    @Test
    void should_not_fire_an_alert_when_the_rule_names_a_different_sensor() {
        startTopology(GRACE, List.of(new ProcessorProperties.AlertRule("brake-temperature-front-left", 100)));

        send(WINDOW_START.plusMillis(100), 900);
        advanceStreamTimePast(WINDOW_START.plusSeconds(1).plus(GRACE).plusSeconds(1));

        assertThat(alerts.readValuesToList()).isEmpty();
    }
}
