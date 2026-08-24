package com.yourjavaguy.pitwall.source.config;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import com.yourjavaguy.pitwall.schema.TelemetryEventProtobufSerializer;
import com.yourjavaguy.pitwall.schema.WireFormat;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

@Configuration
public class KafkaWireFormatConfiguration {

    private static final Logger log = LoggerFactory.getLogger(KafkaWireFormatConfiguration.class);

    @Bean
    ProducerFactory<String, TelemetryEvent> telemetryProducerFactory(KafkaProperties kafkaProperties,
                                                                     KafkaConnectionDetails connectionDetails,
                                                                     SourceProperties sourceProperties) {
        log.info("producing telemetry with the {} wire format", sourceProperties.wireFormat());
        var config = kafkaProperties.buildProducerProperties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionDetails.getBootstrapServers());
        return new DefaultKafkaProducerFactory<>(
                config,
                new StringSerializer(),
                valueSerializer(sourceProperties.wireFormat()));
    }

    @Bean
    KafkaTemplate<String, TelemetryEvent> telemetryKafkaTemplate(
            ProducerFactory<String, TelemetryEvent> telemetryProducerFactory) {
        return new KafkaTemplate<>(telemetryProducerFactory);
    }

    private static Serializer<TelemetryEvent> valueSerializer(WireFormat wireFormat) {
        return switch (wireFormat) {
            case PROTOBUF -> new TelemetryEventProtobufSerializer();
            case JSON -> new JacksonJsonSerializer<TelemetryEvent>().noTypeInfo();
        };
    }
}
