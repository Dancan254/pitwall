package com.yourjavaguy.pitwall.processor.service;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import com.yourjavaguy.pitwall.processor.config.ProcessorProperties;
import com.yourjavaguy.pitwall.processor.metrics.IngestMetrics;
import com.yourjavaguy.pitwall.processor.persistence.TelemetryEventRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TelemetryIngestService {

    private final TelemetryEventRepository repository;
    private final IngestMetrics metrics;
    private final int writeBatchSize;

    public TelemetryIngestService(TelemetryEventRepository repository,
                                  IngestMetrics metrics,
                                  ProcessorProperties properties) {
        this.repository = repository;
        this.metrics = metrics;
        this.writeBatchSize = properties.writeBatchSize();
    }

    public void ingest(List<TelemetryEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        metrics.recordPoll(events.size());
        for (var start = 0; start < events.size(); start += writeBatchSize) {
            var chunk = events.subList(start, Math.min(start + writeBatchSize, events.size()));
            metrics.recordWrite(chunk.size(), () -> repository.insertBatch(chunk));
        }
    }
}
