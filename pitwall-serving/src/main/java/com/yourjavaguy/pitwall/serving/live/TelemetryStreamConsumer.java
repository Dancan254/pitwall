package com.yourjavaguy.pitwall.serving.live;

import com.yourjavaguy.pitwall.commons.event.TelemetryAlert;
import com.yourjavaguy.pitwall.commons.event.TelemetryRollup;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class TelemetryStreamConsumer {

    private final LiveFeed liveFeed;
    private final AlertHistory alertHistory;

    public TelemetryStreamConsumer(LiveFeed liveFeed, AlertHistory alertHistory) {
        this.liveFeed = liveFeed;
        this.alertHistory = alertHistory;
    }

    @KafkaListener(topics = "${pitwall.serving.rollup-topic}",
            groupId = "pitwall-serving-rollups",
            containerFactory = "rollupListenerFactory")
    public void consumeRollups(List<TelemetryRollup> rollups) {
        if (liveFeed.subscriberCount() == 0) {
            return;
        }
        liveFeed.broadcastAll("rollup", rollups);
    }

    @KafkaListener(topics = "${pitwall.serving.alert-topic}",
            groupId = "pitwall-serving-alerts",
            containerFactory = "alertListenerFactory")
    public void consumeAlerts(List<TelemetryAlert> alerts) {
        alertHistory.record(alerts);
        liveFeed.broadcastAll("alert", alerts);
    }
}
