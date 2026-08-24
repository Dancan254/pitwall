package com.yourjavaguy.pitwall.serving.live;

import com.yourjavaguy.pitwall.commons.event.TelemetryAlert;
import com.yourjavaguy.pitwall.serving.config.ServingProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

@Component
public class AlertHistory {

    private final Deque<TelemetryAlert> recent = new ArrayDeque<>();
    private final int capacity;

    public AlertHistory(ServingProperties properties) {
        this.capacity = properties.alertHistorySize();
    }

    public synchronized void record(List<TelemetryAlert> alerts) {
        for (var alert : alerts) {
            if (recent.size() >= capacity) {
                recent.removeLast();
            }
            recent.addFirst(alert);
        }
    }

    public synchronized List<TelemetryAlert> mostRecent() {
        return List.copyOf(recent);
    }
}
