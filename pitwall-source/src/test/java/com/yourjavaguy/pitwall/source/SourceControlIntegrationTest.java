package com.yourjavaguy.pitwall.source;

import com.yourjavaguy.pitwall.source.control.SourceStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.client.RestTestClient;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(webEnvironment = RANDOM_PORT)
@TestPropertySource(properties = {
        "pitwall.source.auto-start=false",
        "pitwall.source.sink=logging",
        "pitwall.source.cars=2",
        "pitwall.source.sensors-per-car=4",
        "pitwall.source.rate-scale=1.0",
        "pitwall.source.log-sample-interval=1000000",
        "pitwall.source.dropout.buffer-capacity=100000"
})
class SourceControlIntegrationTest {

    @LocalServerPort
    private int port;

    private RestTestClient client;

    @BeforeEach
    void createClient() {
        client = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    }

    @AfterEach
    void stopGenerator() {
        client.post().uri("/api/v1/source/stop").exchange().expectStatus().isOk();
    }

    @Test
    void should_report_a_stopped_generator_when_auto_start_is_disabled() {
        var status = getStatus();

        assertThat(status.running()).isFalse();
        assertThat(status.cars()).isEqualTo(2);
        assertThat(status.sensorsPerCar()).isEqualTo(4);
        assertThat(status.sink()).isEqualTo("logging");
        assertThat(status.availableSinks()).contains("logging", "kafka", "database");
    }

    @Test
    void should_derive_the_target_rate_from_the_catalogue_when_the_load_is_applied() {
        var status = client.put().uri("/api/v1/source/load")
                .contentType(MediaType.APPLICATION_JSON)
                .body("""
                        {"cars": 3, "sensorsPerCar": 2, "rateScale": 2.0}""")
                .exchange()
                .expectStatus().isOk()
                .expectBody(SourceStatus.class)
                .returnResult()
                .getResponseBody();

        assertThat(status.targetEventsPerSecond()).isEqualTo(3 * (100 + 200) * 2);
    }

    @Test
    void should_publish_events_when_the_generator_is_started() {
        client.post().uri("/api/v1/source/start").exchange().expectStatus().isOk();

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(getStatus().publishedEvents()).isGreaterThan(0));
    }

    @Test
    void should_buffer_events_instead_of_publishing_them_when_a_dropout_is_active() {
        client.post().uri("/api/v1/source/start").exchange().expectStatus().isOk();
        setDropout(true);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(getStatus().bufferedEvents()).isGreaterThan(0));
    }

    @Test
    void should_drain_the_buffer_when_a_replay_is_triggered() {
        client.post().uri("/api/v1/source/start").exchange().expectStatus().isOk();
        setDropout(true);
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(getStatus().bufferedEvents()).isGreaterThan(0));

        client.post().uri("/api/v1/source/faults/replay").exchange().expectStatus().isAccepted();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(getStatus().dropoutActive()).isFalse();
            assertThat(getStatus().bufferedEvents()).isZero();
        });
    }

    @Test
    void should_reject_the_load_change_when_the_car_count_is_below_one() {
        client.put().uri("/api/v1/source/load")
                .contentType(MediaType.APPLICATION_JSON)
                .body("""
                        {"cars": 0, "sensorsPerCar": 4, "rateScale": 1.0}""")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.title").isEqualTo("Invalid Request");
    }

    @Test
    void should_reject_the_sink_change_when_no_sink_has_that_name() {
        client.put().uri("/api/v1/source/sink")
                .contentType(MediaType.APPLICATION_JSON)
                .body("""
                        {"name": "nowhere"}""")
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.title").isEqualTo("Resource Not Found");
    }

    @Test
    void should_expose_the_anomaly_target_when_an_anomaly_is_set() {
        client.put().uri("/api/v1/source/faults/anomaly")
                .contentType(MediaType.APPLICATION_JSON)
                .body("""
                        {"carId": "CAR-01", "sensorId": "brake-temperature-front-left"}""")
                .exchange()
                .expectStatus().isOk();

        assertThat(getStatus().anomalyCarId()).isEqualTo("CAR-01");

        client.delete().uri("/api/v1/source/faults/anomaly").exchange().expectStatus().isOk();

        assertThat(getStatus().anomalyCarId()).isNull();
    }

    private void setDropout(boolean active) {
        client.put().uri("/api/v1/source/faults/dropout")
                .contentType(MediaType.APPLICATION_JSON)
                .body("{\"active\": %s}".formatted(active))
                .exchange()
                .expectStatus().isOk();
    }

    private SourceStatus getStatus() {
        return client.get().uri("/api/v1/source/status")
                .exchange()
                .expectStatus().isOk()
                .expectBody(SourceStatus.class)
                .returnResult()
                .getResponseBody();
    }
}
