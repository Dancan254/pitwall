package com.yourjavaguy.pitwall.source.sink;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import com.yourjavaguy.pitwall.source.config.SourceProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaTelemetrySink implements TelemetrySink {

    private final KafkaTemplate<String, TelemetryEvent> kafkaTemplate;
    private final String topic;

    public KafkaTelemetrySink(KafkaTemplate<String, TelemetryEvent> kafkaTemplate, SourceProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = properties.kafka().topic();
    }

    @Override
    public void publish(TelemetryEvent event) {
        kafkaTemplate.send(topic, event.carId(), event);
    }

    @Override
    public String name() {
        return "kafka";
    }
}
