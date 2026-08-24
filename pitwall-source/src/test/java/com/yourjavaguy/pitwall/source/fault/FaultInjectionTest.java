package com.yourjavaguy.pitwall.source.fault;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import com.yourjavaguy.pitwall.schema.WireFormat;
import com.yourjavaguy.pitwall.source.config.SourceProperties;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class FaultInjectionTest {

    private static SourceProperties propertiesWith(int bufferCapacity, int overlapSize, int replayBatchSize) {
        return new SourceProperties(1, 1, 1.0, 90, false, 1_000_000, "recording", WireFormat.PROTOBUF,
                new SourceProperties.Dropout(bufferCapacity, overlapSize, replayBatchSize), new SourceProperties.Kafka("telemetry.events", 1, (short) 1));
    }

    private static TelemetryEvent event(long sequenceNumber) {
        return new TelemetryEvent("CAR-01", "speed", Instant.now(), 200.0, sequenceNumber);
    }

    @Test
    void should_reject_the_event_when_the_dropout_buffer_is_full() {
        var faults = new FaultInjection(propertiesWith(2, 10, 10));

        assertThat(faults.buffer(event(1))).isTrue();
        assertThat(faults.buffer(event(2))).isTrue();
        assertThat(faults.buffer(event(3))).isFalse();
    }

    @Test
    void should_keep_only_the_most_recent_deliveries_when_the_overlap_window_is_full() {
        var faults = new FaultInjection(propertiesWith(10, 2, 10));

        faults.recordDelivered(event(1));
        faults.recordDelivered(event(2));
        faults.recordDelivered(event(3));

        assertThat(faults.takeOverlap()).extracting(TelemetryEvent::sequenceNo).containsExactly(2L, 3L);
    }

    @Test
    void should_separate_the_duplicate_overlap_from_the_buffered_events_when_a_dropout_ends() {
        var faults = new FaultInjection(propertiesWith(10, 10, 10));
        faults.recordDelivered(event(1));
        faults.startDropout();
        faults.buffer(event(2));
        faults.buffer(event(3));

        assertThat(faults.takeOverlap()).extracting(TelemetryEvent::sequenceNo).containsExactly(1L);
        assertThat(faults.nextBufferedBatch()).extracting(TelemetryEvent::sequenceNo).containsExactly(2L, 3L);
    }

    @Test
    void should_return_an_empty_overlap_when_it_has_already_been_taken() {
        var faults = new FaultInjection(propertiesWith(10, 10, 10));
        faults.recordDelivered(event(1));

        assertThat(faults.takeOverlap()).hasSize(1);
        assertThat(faults.takeOverlap()).isEmpty();
    }

    @Test
    void should_split_the_replay_across_batches_when_more_events_are_buffered_than_the_batch_size() {
        var faults = new FaultInjection(propertiesWith(10, 0, 2));
        faults.buffer(event(1));
        faults.buffer(event(2));
        faults.buffer(event(3));

        assertThat(faults.nextBufferedBatch()).hasSize(2);
        assertThat(faults.nextBufferedBatch()).hasSize(1);
        assertThat(faults.hasBufferedEvents()).isFalse();
    }

    @Test
    void should_report_no_anomaly_when_a_different_sensor_is_flagged() {
        var faults = new FaultInjection(propertiesWith(10, 10, 10));
        faults.startAnomaly("CAR-01", "speed");

        assertThat(faults.isAnomalous("CAR-01", "speed")).isTrue();
        assertThat(faults.isAnomalous("CAR-01", "engine-rpm")).isFalse();
        assertThat(faults.isAnomalous("CAR-02", "speed")).isFalse();
    }
}
