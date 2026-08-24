package com.yourjavaguy.pitwall.source;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.yourjavaguy.pitwall.schema.TelemetryEventProtobufDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
@TestPropertySource(properties = {
        "pitwall.source.auto-start=true",
        "pitwall.source.sink=kafka",
        "pitwall.source.cars=6",
        "pitwall.source.sensors-per-car=4",
        "pitwall.source.rate-scale=1.0",
        "pitwall.source.log-sample-interval=1000000",
        "pitwall.source.kafka.partitions=6",
        "pitwall.source.wire-format=protobuf"
})
class KafkaTelemetrySinkIntegrationTest {

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.0"));

    private KafkaConsumer<String, TelemetryEvent> consumer;

    @AfterEach
    void closeConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void should_key_every_record_by_car_id_and_spread_them_across_partitions() {
        consumer = createConsumer();
        consumer.subscribe(List.of("telemetry.events"));

        var keys = new HashSet<String>();
        var partitions = new HashSet<Integer>();
        var deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();

        while (System.currentTimeMillis() < deadline && (keys.size() < 6 || partitions.size() < 2)) {
            for (var record : consumer.poll(Duration.ofMillis(500))) {
                keys.add(record.key());
                partitions.add(record.partition());
                assertThat(record.value().carId()).isEqualTo(record.key());
            }
        }

        assertThat(keys).hasSize(6).allSatisfy(key -> assertThat(key).startsWith("CAR-"));
        assertThat(partitions).hasSizeGreaterThan(1);
    }

    private KafkaConsumer<String, TelemetryEvent> createConsumer() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "sink-assertion");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TelemetryEventProtobufDeserializer.class);
        return new KafkaConsumer<>(config);
    }
}
