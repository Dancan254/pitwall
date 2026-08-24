package com.yourjavaguy.pitwall.processor.rollup;

import com.yourjavaguy.pitwall.commons.event.TelemetryAlert;
import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import com.yourjavaguy.pitwall.commons.event.TelemetryRollup;
import com.yourjavaguy.pitwall.processor.config.ProcessorProperties;
import com.yourjavaguy.pitwall.processor.metrics.RollupMetrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.common.utils.Bytes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.time.Duration;
import java.util.List;

@Configuration
@EnableKafkaStreams
public class RollupTopology {

    private static final String KEY_SEPARATOR = "|";
    private static final String KEY_SEPARATOR_PATTERN = "\\|";

    private final ProcessorProperties properties;
    private final RollupMetrics metrics;

    private final Serde<TelemetryEvent> eventSerde;
    private final Serde<RollupAccumulator> accumulatorSerde =
            new JacksonJsonSerde<>(RollupAccumulator.class).noTypeInfo();
    private final Serde<TelemetryRollup> rollupSerde = new JacksonJsonSerde<>(TelemetryRollup.class).noTypeInfo();
    private final Serde<TelemetryAlert> alertSerde = new JacksonJsonSerde<>(TelemetryAlert.class).noTypeInfo();

    public RollupTopology(ProcessorProperties properties,
                          RollupMetrics metrics,
                          Serde<TelemetryEvent> eventSerde) {
        this.properties = properties;
        this.metrics = metrics;
        this.eventSerde = eventSerde;
    }

    @Bean
    public KStream<String, TelemetryEvent> telemetryStream(StreamsBuilder streamsBuilder) {
        var events = streamsBuilder
                .stream(properties.topic(), Consumed.with(Serdes.String(), eventSerde)
                        .withTimestampExtractor(new TelemetryEventTimestampExtractor()))
                .selectKey((carId, event) -> event.carId() + KEY_SEPARATOR + event.sensorId());

        properties.rollup().windows().forEach(windowLength -> addWindow(events, windowLength));
        return events;
    }

    private void addWindow(KStream<String, TelemetryEvent> events, Duration windowLength) {
        var rollups = events
                .groupByKey(Grouped.with(Serdes.String(), eventSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(windowLength, properties.rollup().grace()))
                .aggregate(RollupAccumulator::empty,
                        (key, event, accumulator) -> accumulator.add(event),
                        Materialized.<String, RollupAccumulator, WindowStore<Bytes, byte[]>>as(storeName(windowLength))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(accumulatorSerde))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((windowedKey, accumulator) ->
                        KeyValue.pair(windowedKey.key(), toRollup(windowedKey, accumulator, windowLength)));

        rollups.peek((key, rollup) -> metrics.recordRollup(windowLength))
                .to(properties.rollup().topic(), Produced.with(Serdes.String(), rollupSerde));

        rollups.flatMap(this::alertsFor)
                .peek((key, alert) -> metrics.recordAlert(alert.sensorId()))
                .to(properties.rollup().alertTopic(), Produced.with(Serdes.String(), alertSerde));
    }

    private List<KeyValue<String, TelemetryAlert>> alertsFor(String key, TelemetryRollup rollup) {
        return properties.alerts().stream()
                .filter(rule -> rule.sensorId().equals(rollup.sensorId()))
                .filter(rule -> rollup.maximum() > rule.maximum())
                .map(rule -> KeyValue.pair(key, new TelemetryAlert(
                        rollup.carId(),
                        rollup.sensorId(),
                        rollup.windowStart(),
                        rollup.windowEnd(),
                        rollup.maximum(),
                        rule.maximum())))
                .toList();
    }

    private static TelemetryRollup toRollup(Windowed<String> windowedKey,
                                            RollupAccumulator accumulator,
                                            Duration windowLength) {
        var identity = windowedKey.key().split(KEY_SEPARATOR_PATTERN, 2);
        return new TelemetryRollup(
                identity[0],
                identity[1],
                windowLength,
                windowedKey.window().startTime(),
                windowedKey.window().endTime(),
                accumulator.count(),
                accumulator.average(),
                accumulator.minimum(),
                accumulator.maximum());
    }

    private static String storeName(Duration windowLength) {
        return "telemetry-rollup-" + windowLength.toString().toLowerCase();
    }
}
