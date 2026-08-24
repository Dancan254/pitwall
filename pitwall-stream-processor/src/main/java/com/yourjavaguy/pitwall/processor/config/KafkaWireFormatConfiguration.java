package com.yourjavaguy.pitwall.processor.config;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import com.yourjavaguy.pitwall.schema.TelemetryEventProtobufDeserializer;
import com.yourjavaguy.pitwall.schema.TelemetryEventProtobufSerde;
import com.yourjavaguy.pitwall.schema.WireFormat;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

@Configuration
public class KafkaWireFormatConfiguration {

    private static final Logger log = LoggerFactory.getLogger(KafkaWireFormatConfiguration.class);

    @Bean
    ConsumerFactory<String, TelemetryEvent> telemetryConsumerFactory(KafkaProperties kafkaProperties,
                                                                     KafkaConnectionDetails connectionDetails,
                                                                     ProcessorProperties processorProperties) {
        log.info("consuming telemetry with the {} wire format", processorProperties.wireFormat());
        var config = kafkaProperties.buildConsumerProperties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionDetails.getBootstrapServers());
        return new DefaultKafkaConsumerFactory<>(
                config,
                new StringDeserializer(),
                valueDeserializer(processorProperties.wireFormat()));
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, TelemetryEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, TelemetryEvent> telemetryConsumerFactory,
            KafkaProperties kafkaProperties) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, TelemetryEvent>();
        factory.setConsumerFactory(telemetryConsumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(kafkaProperties.getListener().getConcurrency().intValue());
        return factory;
    }

    @Bean
    Serde<TelemetryEvent> telemetryEventSerde(ProcessorProperties processorProperties) {
        return switch (processorProperties.wireFormat()) {
            case PROTOBUF -> new TelemetryEventProtobufSerde();
            case JSON -> new JacksonJsonSerde<>(TelemetryEvent.class).noTypeInfo();
        };
    }

    private static Deserializer<TelemetryEvent> valueDeserializer(WireFormat wireFormat) {
        return switch (wireFormat) {
            case PROTOBUF -> new TelemetryEventProtobufDeserializer();
            case JSON -> {
                var deserializer = new JacksonJsonDeserializer<>(TelemetryEvent.class);
                deserializer.setUseTypeHeaders(false);
                yield deserializer;
            }
        };
    }
}
