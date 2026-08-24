package com.yourjavaguy.pitwall.processor.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfiguration {

    @Bean
    NewTopic telemetryEventsTopic(ProcessorProperties properties) {
        return TopicBuilder.name(properties.topic()).partitions(12).replicas(1).build();
    }

    @Bean
    NewTopic telemetryRollupsTopic(ProcessorProperties properties) {
        return TopicBuilder.name(properties.rollup().topic()).partitions(12).replicas(1).build();
    }

    @Bean
    NewTopic telemetryAlertsTopic(ProcessorProperties properties) {
        return TopicBuilder.name(properties.rollup().alertTopic()).partitions(3).replicas(1).build();
    }
}
