package com.yourjavaguy.pitwall.processor;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import com.yourjavaguy.pitwall.processor.persistence.TelemetryEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
@TestPropertySource(properties = {
        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.kafka.producer.value-serializer=com.yourjavaguy.pitwall.schema.TelemetryEventProtobufSerializer",
        "pitwall.processor.write-batch-size=100",
        "spring.kafka.streams.application-id=pitwall-rollups-test-${random.uuid}",
        "spring.kafka.streams.state-dir=${java.io.tmpdir}/pitwall-streams-test-${random.uuid}"
})
class TelemetryIngestIntegrationTest {

    private static final int EVENT_COUNT = 1000;

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.0"));

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer(
            DockerImageName.parse("timescale/timescaledb:2.29.2-pg17").asCompatibleSubstituteFor("postgres"));

    @Autowired
    private KafkaTemplate<String, TelemetryEvent> kafkaTemplate;

    @Autowired
    private TelemetryEventRepository repository;

    @Test
    void should_write_every_produced_event_to_the_store_when_the_consumer_drains_the_topic() {
        var baseline = repository.count();

        produce(distinctEvents("speed"));

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertThat(repository.count()).isEqualTo(baseline + EVENT_COUNT));
    }

    @Test
    void should_leave_the_stored_count_unchanged_when_the_same_events_are_replayed() {
        var events = distinctEvents("engine-rpm");
        produce(events);
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertThat(readingsFor("engine-rpm")).isEqualTo(EVENT_COUNT));

        produce(events);
        produce(events);

        await().during(Duration.ofSeconds(5)).atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertThat(readingsFor("engine-rpm")).isEqualTo(EVENT_COUNT));
    }

    @Test
    void should_store_one_row_per_natural_key_when_a_replay_carries_duplicates() {
        var events = distinctEvents("brake-pressure");
        produce(events);
        produce(events.subList(0, 200));

        await().during(Duration.ofSeconds(5)).atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertThat(readingsFor("brake-pressure")).isEqualTo(EVENT_COUNT));
        assertThat(repository.count()).isEqualTo(repository.countDistinctReadings());
    }

    private List<TelemetryEvent> distinctEvents(String sensorId) {
        return IntStream.range(0, EVENT_COUNT)
                .mapToObj(sequenceNumber -> new TelemetryEvent(
                        "CAR-%02d".formatted(sequenceNumber % 20 + 1),
                        sensorId,
                        Instant.now().plusMillis(sequenceNumber),
                        210.5,
                        sequenceNumber))
                .toList();
    }

    private void produce(List<TelemetryEvent> events) {
        events.forEach(event -> kafkaTemplate.send("telemetry.events", event.carId(), event));
        kafkaTemplate.flush();
    }

    private long readingsFor(String sensorId) {
        return repository.countBySensor(sensorId);
    }
}
