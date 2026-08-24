package com.yourjavaguy.pitwall.processor.consumer;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import com.yourjavaguy.pitwall.processor.service.TelemetryIngestService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class TelemetryConsumer {

    private final TelemetryIngestService ingestService;

    public TelemetryConsumer(TelemetryIngestService ingestService) {
        this.ingestService = ingestService;
    }

    @KafkaListener(topics = "${pitwall.processor.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(List<TelemetryEvent> events) {
        ingestService.ingest(events);
    }
}
