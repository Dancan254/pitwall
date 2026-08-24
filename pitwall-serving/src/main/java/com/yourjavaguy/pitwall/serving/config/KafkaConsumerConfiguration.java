package com.yourjavaguy.pitwall.serving.config;

import com.yourjavaguy.pitwall.commons.event.TelemetryAlert;
import com.yourjavaguy.pitwall.commons.event.TelemetryRollup;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfiguration {

    private final KafkaProperties kafkaProperties;
    private final KafkaConnectionDetails connectionDetails;

    public KafkaConsumerConfiguration(KafkaProperties kafkaProperties,
                                      KafkaConnectionDetails connectionDetails) {
        this.kafkaProperties = kafkaProperties;
        this.connectionDetails = connectionDetails;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, TelemetryRollup> rollupListenerFactory() {
        return listenerFactory(TelemetryRollup.class);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, TelemetryAlert> alertListenerFactory() {
        return listenerFactory(TelemetryAlert.class);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> listenerFactory(Class<T> payloadType) {
        Map<String, Object> config = new HashMap<>(kafkaProperties.buildConsumerProperties());
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionDetails.getBootstrapServers());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        var deserializer = new JacksonJsonDeserializer<>(payloadType);
        deserializer.setUseTypeHeaders(false);

        var factory = new ConcurrentKafkaListenerContainerFactory<String, T>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(
                config, new StringDeserializer(), deserializer));
        factory.setBatchListener(true);
        return factory;
    }
}
