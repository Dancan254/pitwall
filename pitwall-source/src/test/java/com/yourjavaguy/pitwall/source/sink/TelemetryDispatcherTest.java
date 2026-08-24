package com.yourjavaguy.pitwall.source.sink;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import com.yourjavaguy.pitwall.schema.WireFormat;
import com.yourjavaguy.pitwall.source.config.SourceProperties;
import com.yourjavaguy.pitwall.source.fault.FaultInjection;
import com.yourjavaguy.pitwall.source.metrics.SourceMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryDispatcherTest {

    private static final int OVERLAP_SIZE = 10;

    private final List<TelemetryEvent> delivered = new ArrayList<>();
    private final TelemetrySink recordingSink = new TelemetrySink() {

        @Override
        public void publish(TelemetryEvent event) {
            delivered.add(event);
        }

        @Override
        public String name() {
            return "recording";
        }
    };

    private final SourceProperties properties = new SourceProperties(1, 1, 1.0, 90, false, 1_000_000, "recording", WireFormat.PROTOBUF,
            new SourceProperties.Dropout(1000, OVERLAP_SIZE, 5),
            new SourceProperties.Kafka("telemetry.events", 1, (short) 1));
    private final FaultInjection faults = new FaultInjection(properties);
    private final SourceMetrics metrics = new SourceMetrics(new SimpleMeterRegistry());
    private final TelemetryDispatcher dispatcher =
            new TelemetryDispatcher(List.of(recordingSink), properties, faults, metrics);

    private static TelemetryEvent event(long sequenceNumber) {
        return new TelemetryEvent("CAR-01", "speed", Instant.now(), 200.0, sequenceNumber);
    }

    @Test
    void should_hold_events_back_from_the_sink_when_a_dropout_is_active() {
        faults.startDropout();

        dispatcher.dispatch(event(1));

        assertThat(delivered).isEmpty();
        assertThat(faults.bufferDepth()).isEqualTo(1);
    }

    @Test
    void should_replay_each_buffered_event_exactly_once_when_the_batch_size_is_smaller_than_the_buffer() {
        faults.startDropout();
        for (var sequenceNumber = 1; sequenceNumber <= 40; sequenceNumber++) {
            dispatcher.dispatch(event(sequenceNumber));
        }
        faults.stopDropout();

        var replayed = dispatcher.replayBuffered();

        assertThat(replayed).isEqualTo(40);
        assertThat(delivered).extracting(TelemetryEvent::sequenceNo).doesNotHaveDuplicates();
    }

    @Test
    void should_count_the_event_as_failed_when_the_active_sink_throws() {
        var failingSink = new TelemetrySink() {

            @Override
            public void publish(TelemetryEvent event) {
                throw new IllegalStateException("connection pool exhausted");
            }

            @Override
            public String name() {
                return "failing";
            }
        };
        var dispatcherWithFailingSink = new TelemetryDispatcher(
                List.of(recordingSink, failingSink), properties, faults, metrics);
        dispatcherWithFailingSink.useSink("failing");

        dispatcherWithFailingSink.dispatch(event(1));

        assertThat(metrics.failedCount()).isEqualTo(1);
        assertThat(metrics.publishedCount()).isZero();
    }

    @Test
    void should_re_send_the_overlap_window_as_duplicates_when_a_replay_follows_a_dropout() {
        dispatcher.dispatch(event(1));
        dispatcher.dispatch(event(2));
        faults.startDropout();
        dispatcher.dispatch(event(3));
        faults.stopDropout();

        var replayed = dispatcher.replayBuffered();

        assertThat(replayed).isEqualTo(3);
        assertThat(delivered).extracting(TelemetryEvent::sequenceNo).containsExactly(1L, 2L, 1L, 2L, 3L);
    }
}
