package com.yourjavaguy.pitwall.serving;

import com.yourjavaguy.pitwall.commons.event.TelemetryAlert;
import com.yourjavaguy.pitwall.serving.config.ServingProperties;
import com.yourjavaguy.pitwall.serving.live.AlertHistory;
import com.yourjavaguy.pitwall.serving.live.LiveFeed;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class LiveFeedTest {

    private static ServingProperties propertiesWith(int alertHistorySize) {
        return new ServingProperties("telemetry.rollups", "telemetry.alerts",
                Duration.ofMinutes(30), alertHistorySize, 5000);
    }

    private static TelemetryAlert alert(int index) {
        return new TelemetryAlert("CAR-01", "sensor-" + index,
                Instant.parse("2026-08-23T12:00:00Z"), Instant.parse("2026-08-23T12:00:01Z"), 1200, 1050);
    }

    @Test
    void should_have_no_subscribers_when_nothing_has_connected() {
        var feed = new LiveFeed(propertiesWith(10), new SimpleMeterRegistry());

        assertThat(feed.subscriberCount()).isZero();
    }

    @Test
    void should_track_the_subscriber_when_a_client_connects() {
        var feed = new LiveFeed(propertiesWith(10), new SimpleMeterRegistry());

        feed.subscribe();

        assertThat(feed.subscriberCount()).isEqualTo(1);
    }

    @Test
    void should_not_fail_when_broadcasting_with_no_subscribers() {
        var feed = new LiveFeed(propertiesWith(10), new SimpleMeterRegistry());

        feed.broadcast("alert", alert(1));

        assertThat(feed.subscriberCount()).isZero();
    }

    @Test
    void should_return_the_newest_alert_first_when_history_is_read() {
        var history = new AlertHistory(propertiesWith(10));

        history.record(List.of(alert(1), alert(2), alert(3)));

        assertThat(history.mostRecent()).extracting(TelemetryAlert::sensorId)
                .containsExactly("sensor-3", "sensor-2", "sensor-1");
    }

    @Test
    void should_discard_the_oldest_alert_when_history_is_full() {
        var history = new AlertHistory(propertiesWith(3));

        history.record(IntStream.rangeClosed(1, 5).mapToObj(LiveFeedTest::alert).toList());

        assertThat(history.mostRecent()).extracting(TelemetryAlert::sensorId)
                .containsExactly("sensor-5", "sensor-4", "sensor-3");
    }
}
