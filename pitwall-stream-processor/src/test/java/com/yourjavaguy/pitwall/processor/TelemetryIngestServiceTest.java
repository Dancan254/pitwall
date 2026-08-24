package com.yourjavaguy.pitwall.processor;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import com.yourjavaguy.pitwall.processor.config.ProcessorProperties;
import com.yourjavaguy.pitwall.schema.WireFormat;
import com.yourjavaguy.pitwall.processor.metrics.IngestMetrics;
import com.yourjavaguy.pitwall.processor.persistence.TelemetryEventRepository;
import com.yourjavaguy.pitwall.processor.service.TelemetryIngestService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryIngestServiceTest {

    private final List<Integer> writtenBatchSizes = new ArrayList<>();
    private final IngestMetrics metrics = new IngestMetrics(new SimpleMeterRegistry());

    private int duplicatesPerBatch;

    private final TelemetryEventRepository recordingRepository = new TelemetryEventRepository(null) {

        @Override
        public int insertBatch(List<TelemetryEvent> events) {
            writtenBatchSizes.add(events.size());
            return events.size() - duplicatesPerBatch;
        }
    };

    private final TelemetryIngestService ingestService = new TelemetryIngestService(
            recordingRepository, metrics, new ProcessorProperties("telemetry.events", 100, WireFormat.PROTOBUF,
                    new ProcessorProperties.Rollup("telemetry.rollups", "telemetry.alerts",
                            List.of(Duration.ofSeconds(1)), Duration.ofSeconds(10)),
                    List.of()));

    private static List<TelemetryEvent> events(int count) {
        return IntStream.range(0, count)
                .mapToObj(index -> new TelemetryEvent("CAR-01", "speed", Instant.now(), 200.0, index))
                .map(TelemetryEvent.class::cast)
                .toList();
    }

    @Test
    void should_split_the_poll_into_write_batches_when_the_poll_exceeds_the_batch_size() {
        ingestService.ingest(events(250));

        assertThat(writtenBatchSizes).containsExactly(100, 100, 50);
    }

    @Test
    void should_write_nothing_when_the_poll_is_empty() {
        ingestService.ingest(List.of());

        assertThat(writtenBatchSizes).isEmpty();
        assertThat(metrics.consumedCount()).isZero();
    }

    @Test
    void should_count_every_event_in_the_poll_when_the_batch_is_written() {
        ingestService.ingest(events(250));

        assertThat(metrics.consumedCount()).isEqualTo(250);
        assertThat(metrics.writtenCount()).isEqualTo(250);
        assertThat(metrics.duplicateCount()).isZero();
    }

    @Test
    void should_count_the_rows_the_natural_key_rejected_as_duplicates() {
        duplicatesPerBatch = 10;

        ingestService.ingest(events(250));

        assertThat(metrics.consumedCount()).isEqualTo(250);
        assertThat(metrics.writtenCount()).isEqualTo(220);
        assertThat(metrics.duplicateCount()).isEqualTo(30);
    }
}
